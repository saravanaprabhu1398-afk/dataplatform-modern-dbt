"""Canonical event model for the streaming correctness harness.

Every synthetic event carries enough structure for the verifier to prove four
things about a sink without trusting the pipeline that filled it:

* ``key`` + ``seq``   -- a gapless, monotonic sequence per key, so missing and
  duplicated records are detectable by identity alone.
* ``event_time``      -- deliberately out of order relative to arrival, so
  event-time windowing can be distinguished from processing-time windowing.
* ``amount_cents``    -- a numeric payload that makes window aggregates
  comparable against a batch recompute.
* ``checksum``        -- a digest over the immutable fields, so a mutated or
  truncated record is caught even when its identity survives.

The model is deliberately dependency-free: the harness must keep working when
Kafka, Spark, or a warehouse driver is unavailable.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Tuple

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

#: Fields covered by the checksum. Order matters -- it is part of the digest.
CHECKSUM_FIELDS = ("key", "seq", "event_time", "amount_cents", "payload")


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def to_iso(value: datetime) -> str:
    """Render a datetime as a UTC ISO-8601 string with microseconds."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime(ISO_FORMAT)


def from_iso(value: str) -> datetime:
    """Parse a UTC ISO-8601 string produced by :func:`to_iso`."""
    return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=timezone.utc)


def window_start(event_time: str, window_seconds: int) -> str:
    """Return the start of the tumbling window *event_time* falls into."""
    if window_seconds <= 0:
        raise ValueError("window_seconds must be positive")
    epoch = int(from_iso(event_time).timestamp())
    floored = epoch - (epoch % window_seconds)
    return to_iso(datetime.fromtimestamp(floored, tz=timezone.utc))


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------

def compute_checksum(
    key: str, seq: int, event_time: str, amount_cents: int, payload: str
) -> str:
    """Return a short digest over the immutable fields of an event."""
    canonical = "|".join(
        [key, str(seq), event_time, str(amount_cents), payload]
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:16]


@dataclass(frozen=True)
class StreamEvent:
    """One synthetic event, as produced by the generator."""

    key: str
    seq: int
    event_time: str
    ingest_time: str
    amount_cents: int
    payload: str
    checksum: str

    @classmethod
    def create(
        cls,
        key: str,
        seq: int,
        event_time: str,
        ingest_time: str,
        amount_cents: int,
        payload: str,
    ) -> "StreamEvent":
        """Build an event, computing its checksum."""
        return cls(
            key=key,
            seq=seq,
            event_time=event_time,
            ingest_time=ingest_time,
            amount_cents=amount_cents,
            payload=payload,
            checksum=compute_checksum(key, seq, event_time, amount_cents, payload),
        )

    @property
    def identity(self) -> Tuple[str, int]:
        """The (key, seq) pair that uniquely identifies this event."""
        return (self.key, self.seq)

    @property
    def identity_str(self) -> str:
        """JSON-safe rendering of :attr:`identity`."""
        return identity_str(self.key, self.seq)

    def is_intact(self) -> bool:
        """True when the checksum still matches the event's own fields."""
        return self.checksum == compute_checksum(
            self.key, self.seq, self.event_time, self.amount_cents, self.payload
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "seq": self.seq,
            "event_time": self.event_time,
            "ingest_time": self.ingest_time,
            "amount_cents": self.amount_cents,
            "payload": self.payload,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "StreamEvent":
        """Rebuild an event from a sink row.

        Tolerates rows that carry extra columns and rows whose numeric fields
        arrived back as strings, which is normal for CSV and some drivers.
        """
        return cls(
            key=str(row["key"]),
            seq=int(row["seq"]),
            event_time=str(row["event_time"]),
            ingest_time=str(row.get("ingest_time", "")),
            amount_cents=int(row["amount_cents"]),
            payload=str(row["payload"]),
            checksum=str(row.get("checksum", "")),
        )


def identity_str(key: str, seq: int) -> str:
    """Stable string form of an identity, usable as a JSON object key."""
    return "{0}#{1}".format(key, seq)


def parse_identity(value: str) -> Tuple[str, int]:
    """Inverse of :func:`identity_str`."""
    key, _, seq = value.rpartition("#")
    return key, int(seq)


# ---------------------------------------------------------------------------
# JSONL io
# ---------------------------------------------------------------------------

def write_jsonl(path: str, events: Iterable[StreamEvent]) -> int:
    """Write events as JSON lines. Returns the number written."""
    count = 0
    with open(path, "w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event.as_dict(), sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    """Read JSON lines back into plain dicts."""
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows
