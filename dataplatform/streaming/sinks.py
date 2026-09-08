"""Transactional sinks: where exactly-once actually happens.

The guarantee this module provides is narrow and worth stating precisely:

    Records and the offsets that produced them are written in ONE database
    transaction, under a fencing token.  A crash anywhere before that
    transaction commits leaves nothing behind, so the next poll replays the
    same records.  A crash after it commits leaves the offsets advanced, so the
    next poll moves on.  There is no window in which one landed without the
    other.

That is why the sink also owns the offset store.  A sink that cannot share a
transaction with the offset store cannot offer this; it can only offer
*effectively*-once through idempotent writes keyed on ``(key, seq)`` -- which
this sink also does, so a replay is harmless either way.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from dataplatform.core.chaos import CrashPoint, maybe_crash
from dataplatform.core.database import _get_engine, _now_iso, _is_postgres
from dataplatform.plugins.base import Fence, Offsets, StaleFence, TransactionalSink

logger = logging.getLogger(__name__)

#: Sink tables are named by config, so the name is validated rather than
#: interpolated blindly -- it reaches SQL as an identifier, not a parameter.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

RECORD_COLUMNS = (
    "key",
    "seq",
    "event_time",
    "ingest_time",
    "amount_cents",
    "payload",
    "checksum",
)


def _validate_table(name: str) -> str:
    if not _SAFE_IDENTIFIER.match(name or ""):
        raise ValueError("invalid sink table name: {0!r}".format(name))
    return name


class SqlTransactionalSink(TransactionalSink):
    """Upsert records and offsets into the metadata store, atomically."""

    def __init__(self, table: str, stream: str) -> None:
        self.table = _validate_table(table)
        self.stream = stream
        self._ensure_table()

    # -- schema -----------------------------------------------------------

    def _ensure_table(self) -> None:
        with _get_engine().begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS {table} (
                        key           VARCHAR NOT NULL,
                        seq           INTEGER NOT NULL,
                        event_time    VARCHAR NOT NULL,
                        ingest_time   VARCHAR,
                        amount_cents  INTEGER NOT NULL,
                        payload       VARCHAR,
                        checksum      VARCHAR,
                        written_at    VARCHAR NOT NULL,
                        PRIMARY KEY (key, seq)
                    )
                    """.format(table=self.table)
                )
            )

    # -- offsets ----------------------------------------------------------

    def read_offsets(self, stream: Optional[str] = None) -> Offsets:
        """Return the offsets last committed for the stream."""
        target = stream or self.stream
        with _get_engine().connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT partition, next_offset FROM stream_state WHERE stream = :s"
                ),
                {"s": target},
            ).fetchall()
        offsets = Offsets()
        for row in rows:
            offsets.advance(str(row._mapping["partition"]), int(row._mapping["next_offset"]))
        return offsets

    def committed_attempt(self, stream: Optional[str] = None) -> int:
        """Highest fencing token recorded for the stream (0 when unwritten)."""
        target = stream or self.stream
        with _get_engine().connect() as conn:
            value = conn.execute(
                text(
                    "SELECT MAX(attempt) FROM stream_state WHERE stream = :s"
                ),
                {"s": target},
            ).scalar()
        return int(value or 0)

    # -- the commit boundary ----------------------------------------------

    def commit(
        self, records: List[Dict[str, Any]], offsets: Offsets, fence: Fence
    ) -> int:
        """Write records and offsets in a single transaction, under *fence*.

        Raises :class:`StaleFence` if a newer attempt has already written to
        this stream: that means we were reaped while we were away, and the run
        that replaced us owns the offsets now.
        """
        now = _now_iso()
        written = 0

        with _get_engine().begin() as conn:
            self._assert_fence(conn, fence)

            for record in records:
                conn.execute(text(self._upsert_sql()), self._record_params(record, now))
                written += 1

            # Killed here, the records above are already written but the
            # offsets are not -- and the whole transaction rolls back, which is
            # the entire point of this being one transaction.
            maybe_crash(CrashPoint.AFTER_WRITE)

            for partition, next_offset in sorted(offsets.positions.items()):
                conn.execute(
                    text(self._offset_upsert_sql()),
                    {
                        "stream": fence.stream,
                        "partition": partition,
                        "next_offset": next_offset,
                        "attempt": fence.attempt,
                        "now": now,
                    },
                )

            maybe_crash(CrashPoint.BEFORE_COMMIT)

        maybe_crash(CrashPoint.AFTER_COMMIT)
        return written

    def _assert_fence(self, conn: Any, fence: Fence) -> None:
        """Reject a writer whose attempt has been superseded."""
        lock = " FOR UPDATE" if _is_postgres() else ""
        row = conn.execute(
            text(
                "SELECT attempt FROM stream_state WHERE stream = :s "
                "ORDER BY attempt DESC LIMIT 1" + lock
            ),
            {"s": fence.stream},
        ).fetchone()
        if row is None:
            return

        committed = int(row._mapping["attempt"])
        if committed > fence.attempt:
            raise StaleFence(
                "fenced out of stream {0}: committed attempt {1}, ours {2}".format(
                    fence.stream, committed, fence.attempt
                )
            )

    # -- SQL --------------------------------------------------------------

    def _upsert_sql(self) -> str:
        return """
            INSERT INTO {table}
                (key, seq, event_time, ingest_time, amount_cents, payload, checksum, written_at)
            VALUES
                (:key, :seq, :event_time, :ingest_time, :amount_cents, :payload, :checksum, :now)
            ON CONFLICT (key, seq) DO UPDATE SET
                event_time   = EXCLUDED.event_time,
                ingest_time  = EXCLUDED.ingest_time,
                amount_cents = EXCLUDED.amount_cents,
                payload      = EXCLUDED.payload,
                checksum     = EXCLUDED.checksum,
                written_at   = EXCLUDED.written_at
        """.format(table=self.table)

    @staticmethod
    def _offset_upsert_sql() -> str:
        return """
            INSERT INTO stream_state (stream, partition, next_offset, attempt, updated_at)
            VALUES (:stream, :partition, :next_offset, :attempt, :now)
            ON CONFLICT (stream, partition) DO UPDATE SET
                next_offset = EXCLUDED.next_offset,
                attempt     = EXCLUDED.attempt,
                updated_at  = EXCLUDED.updated_at
        """

    @staticmethod
    def _record_params(record: Dict[str, Any], now: str) -> Dict[str, Any]:
        params = {column: record.get(column) for column in RECORD_COLUMNS}
        params["seq"] = int(params["seq"])
        params["amount_cents"] = int(params["amount_cents"])
        params["now"] = now
        return params

    # -- reading back ------------------------------------------------------

    def rows(self) -> List[Dict[str, Any]]:
        """Everything in the sink, for the verifier."""
        with _get_engine().connect() as conn:
            result = conn.execute(
                text(
                    "SELECT key, seq, event_time, ingest_time, amount_cents, payload, "
                    "checksum FROM {table}".format(table=self.table)
                )
            ).fetchall()
        return [dict(row._mapping) for row in result]
