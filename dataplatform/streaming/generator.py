"""Deterministic synthetic event generator for the streaming harness.

The generator produces a stream in *arrival* order whose event times are
deliberately out of order, plus a manifest describing what a correct sink must
contain once the whole stream has been consumed.  The manifest is the oracle:
the verifier compares a sink against it rather than against the pipeline's own
opinion of what it did.

Everything is seeded, so two runs with the same config produce byte-identical
streams -- a benchmark you cannot reproduce is an anecdote.

    from dataplatform.streaming.generator import GeneratorConfig, generate

    stream = generate(GeneratorConfig(keys=50, events_per_key=200, seed=7))
    stream.write("data/stream/events.jsonl", "data/stream/manifest.json")

No production data is involved, and none should ever be: the point of the
harness is that the expected result is known by construction.
"""
from __future__ import annotations

import json
import logging
import random
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dataplatform.streaming.model import (
    StreamEvent,
    identity_str,
    to_iso,
    window_start,
    write_jsonl,
)

logger = logging.getLogger(__name__)

#: Fixed default epoch so a run without an explicit start time is still
#: reproducible across machines and days.
DEFAULT_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

_WORDS = (
    "booking", "amend", "cancel", "reissue", "refund",
    "seat", "bag", "upgrade", "voucher", "credit",
)


@dataclass
class GeneratorConfig:
    """Shape of the synthetic stream.

    The lateness knobs are the interesting ones: ``late_fraction`` produces
    records that arrive after their window would close under a naive
    processing-time pipeline, and ``very_late_fraction`` produces records that
    fall outside any sane allowed-lateness bound so the side-output path can be
    exercised.
    """

    keys: int = 25
    events_per_key: int = 100
    seed: int = 1337
    start_time: Optional[str] = None
    event_interval_seconds: float = 30.0
    base_delay_seconds: float = 0.5
    late_fraction: float = 0.08
    late_delay_seconds: float = 5400.0        # 1.5h -- inside a 6h lateness bound
    very_late_fraction: float = 0.01
    very_late_delay_seconds: float = 172800.0  # 48h -- outside it
    duplicate_fraction: float = 0.0            # simulated broker redelivery
    window_seconds: int = 3600
    amount_min_cents: int = 100
    amount_max_cents: int = 250000

    def resolved_start(self) -> datetime:
        if self.start_time:
            from dataplatform.streaming.model import from_iso

            return from_iso(self.start_time)
        return DEFAULT_START


@dataclass
class Manifest:
    """What a correct sink must contain after consuming the whole stream."""

    total_unique: int
    keys: Dict[str, int]                      # key -> highest seq (0-based count-1)
    checksums: Dict[str, str]                 # identity -> checksum
    windows: Dict[str, Dict[str, int]]        # window_start -> {count, sum_amount_cents}
    window_seconds: int
    duplicates_injected: int
    config: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Manifest":
        return cls(
            total_unique=int(data["total_unique"]),
            keys={str(k): int(v) for k, v in data["keys"].items()},
            checksums={str(k): str(v) for k, v in data["checksums"].items()},
            windows={
                str(w): {
                    "count": int(agg["count"]),
                    "sum_amount_cents": int(agg["sum_amount_cents"]),
                }
                for w, agg in data["windows"].items()
            },
            window_seconds=int(data["window_seconds"]),
            duplicates_injected=int(data.get("duplicates_injected", 0)),
            config=dict(data.get("config", {})),
        )

    def write(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.as_dict(), handle, indent=2, sort_keys=True)

    @classmethod
    def read(cls, path: str) -> "Manifest":
        with open(path, "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


@dataclass
class GeneratedStream:
    """The stream in arrival order, plus the manifest that describes it."""

    events: List[StreamEvent]
    manifest: Manifest

    @property
    def out_of_order_count(self) -> int:
        """How many events arrive with an event time older than a predecessor.

        A stream where this is zero cannot distinguish an event-time pipeline
        from a processing-time one, so the harness asserts on it.
        """
        inversions = 0
        highest = None
        for event in self.events:
            if highest is not None and event.event_time < highest:
                inversions += 1
            if highest is None or event.event_time > highest:
                highest = event.event_time
        return inversions

    def write(self, events_path: str, manifest_path: str) -> int:
        written = write_jsonl(events_path, self.events)
        self.manifest.write(manifest_path)
        logger.info(
            "Generated %d stream records (%d unique) -> %s",
            written,
            self.manifest.total_unique,
            events_path,
        )
        return written


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def generate(config: Optional[GeneratorConfig] = None) -> GeneratedStream:
    """Produce a deterministic out-of-order stream and its manifest."""
    config = config or GeneratorConfig()
    if config.keys <= 0 or config.events_per_key <= 0:
        raise ValueError("keys and events_per_key must be positive")

    rng = random.Random(config.seed)
    start = config.resolved_start()

    events: List[StreamEvent] = []
    checksums: Dict[str, str] = {}
    keys: Dict[str, int] = {}
    windows: Dict[str, Dict[str, int]] = {}

    for key_index in range(config.keys):
        key = "k{0:04d}".format(key_index)
        keys[key] = config.events_per_key - 1

        for seq in range(config.events_per_key):
            occurred = start + timedelta(
                seconds=config.event_interval_seconds * seq
                + rng.uniform(0, config.event_interval_seconds)
            )
            event_time = to_iso(occurred)

            roll = rng.random()
            if roll < config.very_late_fraction:
                delay = config.very_late_delay_seconds * rng.uniform(0.8, 1.2)
            elif roll < config.very_late_fraction + config.late_fraction:
                delay = config.late_delay_seconds * rng.uniform(0.5, 1.0)
            else:
                delay = config.base_delay_seconds * rng.uniform(0.5, 2.0)

            amount = rng.randint(config.amount_min_cents, config.amount_max_cents)
            payload = "{0}-{1}".format(rng.choice(_WORDS), rng.randint(1000, 9999))

            event = StreamEvent.create(
                key=key,
                seq=seq,
                event_time=event_time,
                ingest_time=to_iso(occurred + timedelta(seconds=delay)),
                amount_cents=amount,
                payload=payload,
            )
            events.append(event)

            checksums[event.identity_str] = event.checksum
            bucket = windows.setdefault(
                window_start(event_time, config.window_seconds),
                {"count": 0, "sum_amount_cents": 0},
            )
            bucket["count"] += 1
            bucket["sum_amount_cents"] += amount

    total_unique = len(events)

    # Arrival order, not event order. This is what makes the stream honest.
    events.sort(key=lambda e: (e.ingest_time, e.key, e.seq))

    duplicates_injected = 0
    if config.duplicate_fraction > 0:
        redelivered: List[StreamEvent] = []
        for event in events:
            redelivered.append(event)
            if rng.random() < config.duplicate_fraction:
                redelivered.append(event)
                duplicates_injected += 1
        events = redelivered

    manifest = Manifest(
        total_unique=total_unique,
        keys=keys,
        checksums=checksums,
        windows=windows,
        window_seconds=config.window_seconds,
        duplicates_injected=duplicates_injected,
        config=asdict(config),
    )
    return GeneratedStream(events=events, manifest=manifest)
