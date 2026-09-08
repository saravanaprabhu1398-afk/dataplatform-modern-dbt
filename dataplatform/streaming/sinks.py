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
from dataplatform.streaming.windows import (
    BatchPlan,
    WatermarkTracker,
    WindowPlanner,
    WindowPolicy,
)

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

    def __init__(
        self,
        table: str,
        stream: str,
        windowing: Optional[WindowPolicy] = None,
        partition_of: Optional[Any] = None,
    ) -> None:
        self.table = _validate_table(table)
        self.stream = stream
        self.windowing = windowing
        self._partition_of = partition_of or (lambda record: "0")
        self._ensure_table()

        self._planner: Optional[WindowPlanner] = None
        if windowing is not None:
            tracker = WatermarkTracker(windowing)
            tracker.restore(self.read_watermarks())
            self._planner = WindowPlanner(windowing, tracker)

    @property
    def watermark(self) -> Optional[str]:
        """Current watermark, or None before any record has been seen."""
        return self._planner.tracker.watermark() if self._planner else None

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

    def read_watermarks(self, stream: Optional[str] = None) -> Dict[str, str]:
        """Per-partition high-water event times, for restoring after a restart."""
        target = stream or self.stream
        with _get_engine().connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT partition, watermark FROM stream_state "
                    "WHERE stream = :s AND watermark IS NOT NULL"
                ),
                {"s": target},
            ).fetchall()
        return {str(r._mapping["partition"]): str(r._mapping["watermark"]) for r in rows}

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

        try:
            with _get_engine().begin() as conn:
                self._assert_fence(conn, fence)
                advanced = self._offsets_advance(conn, fence.stream, offsets)

                for record in records:
                    conn.execute(text(self._upsert_sql()), self._record_params(record, now))
                    written += 1

                # Killed here, the records above are already written but the
                # offsets are not -- and the whole transaction rolls back, which
                # is the entire point of this being one transaction.
                maybe_crash(CrashPoint.AFTER_WRITE)

                plan = None
                if self._planner is not None and advanced:
                    plan = self._planner.plan(
                        records,
                        self._partition_of,
                        self._open_windows(conn, fence.stream),
                    )
                    self._apply_plan(conn, fence.stream, plan, now)

                watermarks = plan.partition_max_event_time if plan else {}
                for partition, next_offset in sorted(offsets.positions.items()):
                    conn.execute(
                        text(self._offset_upsert_sql()),
                        {
                            "stream": fence.stream,
                            "partition": partition,
                            "next_offset": next_offset,
                            "attempt": fence.attempt,
                            "watermark": watermarks.get(partition),
                            "now": now,
                        },
                    )

                maybe_crash(CrashPoint.BEFORE_COMMIT)
        except Exception:
            # The transaction rolled back, but the in-memory watermark did not.
            # Put it back where the database says it is.
            self._resync_watermarks()
            raise

        maybe_crash(CrashPoint.AFTER_COMMIT)
        return written

    # -- window state ------------------------------------------------------

    def _offsets_advance(self, conn: Any, stream: str, offsets: Offsets) -> bool:
        """True when this commit moves at least one partition forward.

        Record upserts are idempotent, but window aggregates are additive, so a
        replayed batch would double-count them.  A commit that advances nothing
        is exactly that replay: the records are rewritten harmlessly and the
        aggregates are left alone.

        This assumes a replay resends a whole batch from the committed offsets,
        which is what the runner does.  A batch that partially overlaps
        committed offsets is not supported by the aggregate path.
        """
        rows = conn.execute(
            text("SELECT partition, next_offset FROM stream_state WHERE stream = :s"),
            {"s": stream},
        ).fetchall()
        committed = {
            str(r._mapping["partition"]): int(r._mapping["next_offset"]) for r in rows
        }
        if not committed:
            return True
        return any(
            position > committed.get(partition, 0)
            for partition, position in offsets.positions.items()
        )

    def _open_windows(self, conn: Any, stream: str) -> set:
        """Windows that have not fired yet -- candidates for closing."""
        rows = conn.execute(
            text(
                "SELECT window_start FROM stream_windows "
                "WHERE stream = :s AND closed_at IS NULL"
            ),
            {"s": stream},
        ).fetchall()
        return {str(row._mapping["window_start"]) for row in rows}

    def _apply_plan(self, conn: Any, stream: str, plan: BatchPlan, now: str) -> None:
        """Write window deltas, corrections and side output -- same transaction."""
        revision_bumps: Dict[str, int] = {}
        for correction in plan.corrections:
            revision_bumps[correction.window] = revision_bumps.get(correction.window, 0) + 1

        for window, (count, amount) in sorted(plan.deltas.items()):
            conn.execute(
                text(self._window_upsert_sql()),
                {
                    "stream": stream,
                    "window_start": window,
                    "count": count,
                    "sum": amount,
                    "revision": revision_bumps.get(window, 0),
                    "now": now,
                },
            )

        for window in sorted(revision_bumps):
            revision = conn.execute(
                text(
                    "SELECT revision FROM stream_windows "
                    "WHERE stream = :s AND window_start = :w"
                ),
                {"s": stream, "w": window},
            ).scalar()
            for correction in [c for c in plan.corrections if c.window == window]:
                conn.execute(
                    text(
                        """
                        INSERT INTO window_corrections
                            (stream, window_start, revision, delta_count,
                             delta_amount_cents, reason, emitted_at)
                        VALUES (:stream, :w, :revision, :count, :amount, :reason, :now)
                        """
                    ),
                    {
                        "stream": stream,
                        "w": window,
                        "revision": int(revision or 0),
                        "count": correction.delta_count,
                        "amount": correction.delta_amount_cents,
                        "reason": correction.reason,
                        "now": now,
                    },
                )

        for late in plan.late_records:
            conn.execute(
                text(
                    """
                    INSERT INTO late_events
                        (stream, key, seq, event_time, window_start, watermark,
                         lateness_seconds, reason, counted, recorded_at)
                    VALUES (:stream, :key, :seq, :event_time, :window_start, :watermark,
                            :lateness, :reason, :counted, :now)
                    ON CONFLICT (stream, key, seq) DO NOTHING
                    """
                ),
                {
                    "stream": stream,
                    "key": late.key,
                    "seq": late.seq,
                    "event_time": late.event_time,
                    "window_start": late.window,
                    "watermark": late.watermark or None,
                    "lateness": late.lateness_seconds,
                    "reason": late.reason,
                    "counted": 1 if late.counted else 0,
                    "now": now,
                },
            )

        for window in plan.closed_windows:
            conn.execute(
                text(
                    "UPDATE stream_windows SET closed_at = :now "
                    "WHERE stream = :s AND window_start = :w AND closed_at IS NULL"
                ),
                {"now": now, "s": stream, "w": window},
            )

    def _resync_watermarks(self) -> None:
        if self._planner is None:
            return
        tracker = WatermarkTracker(self.windowing)
        tracker.restore(self.read_watermarks())
        self._planner = WindowPlanner(self.windowing, tracker)

    @staticmethod
    def _window_upsert_sql() -> str:
        return """
            INSERT INTO stream_windows
                (stream, window_start, event_count, sum_amount_cents, revision, updated_at)
            VALUES (:stream, :window_start, :count, :sum, :revision, :now)
            ON CONFLICT (stream, window_start) DO UPDATE SET
                event_count      = stream_windows.event_count + EXCLUDED.event_count,
                sum_amount_cents = stream_windows.sum_amount_cents + EXCLUDED.sum_amount_cents,
                revision         = stream_windows.revision + EXCLUDED.revision,
                updated_at       = EXCLUDED.updated_at
        """

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
            INSERT INTO stream_state
                (stream, partition, next_offset, attempt, watermark, updated_at)
            VALUES (:stream, :partition, :next_offset, :attempt, :watermark, :now)
            ON CONFLICT (stream, partition) DO UPDATE SET
                next_offset = EXCLUDED.next_offset,
                attempt     = EXCLUDED.attempt,
                -- Never null out a known watermark: a non-windowed writer
                -- advancing offsets must not erase event-time progress.
                watermark   = COALESCE(EXCLUDED.watermark, stream_state.watermark),
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


    def window_rows(self) -> List[Dict[str, Any]]:
        """Window aggregates as the sink currently holds them."""
        with _get_engine().connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT window_start, event_count, sum_amount_cents, revision, "
                    "closed_at FROM stream_windows WHERE stream = :s ORDER BY window_start"
                ),
                {"s": self.stream},
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def late_rows(self) -> List[Dict[str, Any]]:
        """Records that arrived after their window had fired."""
        with _get_engine().connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT key, seq, event_time, window_start, watermark, "
                    "lateness_seconds, reason, counted FROM late_events "
                    "WHERE stream = :s ORDER BY key, seq"
                ),
                {"s": self.stream},
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def correction_rows(self) -> List[Dict[str, Any]]:
        """Restatements emitted for windows that had already been reported."""
        with _get_engine().connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT window_start, revision, delta_count, delta_amount_cents, "
                    "reason FROM window_corrections WHERE stream = :s "
                    "ORDER BY window_start, revision"
                ),
                {"s": self.stream},
            ).fetchall()
        return [dict(row._mapping) for row in rows]
