"""Event time, watermarks, and what to do about stragglers.

Processing time is easy and wrong: it windows records by when they happened to
arrive, so a delayed batch silently moves revenue from one hour to another.
Event time is right, and costs you a decision -- how long to wait for records
that haven't arrived yet.  That decision is a watermark.

Three ideas, kept separate on purpose:

``out_of_orderness``
    How far the watermark trails the highest event time seen.  Routine
    disorder: the stream is always a little shuffled.

``allowed_lateness``
    How long a window keeps accepting updates after it has fired.  An arrival
    in this period produces a *correction*, not a wrong number.

idle partitions
    The watermark is the minimum across partitions, so one silent partition
    would freeze every window in the stream.  A partition that has said
    nothing for ``idle_partition_timeout`` stops holding the watermark back.

Lateness is always judged against the watermark *at the start of the batch*.
A record cannot be late relative to a watermark that it itself advanced.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from dataplatform.streaming.model import from_iso, to_iso, window_start

logger = logging.getLogger(__name__)

ON_LATE_SIDE_OUTPUT = "side_output"
ON_LATE_UPDATE = "update"

#: Why a record was not counted in its window the normal way.
REASON_CORRECTION = "late_within_allowed_lateness"
REASON_BEYOND_LATENESS = "beyond_allowed_lateness"


@dataclass(frozen=True)
class WindowPolicy:
    """Resolved windowing configuration."""

    window_seconds: int = 3600
    out_of_orderness_seconds: int = 300
    allowed_lateness_seconds: int = 21600
    idle_partition_timeout_seconds: int = 60
    event_time_field: str = "event_time"
    amount_field: str = "amount_cents"
    on_late: str = ON_LATE_SIDE_OUTPUT

    @classmethod
    def from_config(cls, config: Any) -> "WindowPolicy":
        """Build from a :class:`~dataplatform.core.config.StreamingWindows`."""
        return cls(
            window_seconds=config.window,
            out_of_orderness_seconds=config.out_of_orderness,
            allowed_lateness_seconds=config.allowed_lateness,
            idle_partition_timeout_seconds=config.idle_partition_timeout,
            event_time_field=config.event_time_field,
            on_late=config.on_late,
        )


@dataclass
class LateRecord:
    """A record that arrived after its window had already fired."""

    key: str
    seq: int
    event_time: str
    window: str
    watermark: str
    lateness_seconds: int
    reason: str
    counted: bool


@dataclass
class Correction:
    """A restatement of a window that had already been reported."""

    window: str
    delta_count: int
    delta_amount_cents: int
    reason: str = REASON_CORRECTION


@dataclass
class BatchPlan:
    """Everything a batch changes about window state, computed before writing."""

    deltas: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    corrections: List[Correction] = field(default_factory=list)
    late_records: List[LateRecord] = field(default_factory=list)
    partition_max_event_time: Dict[str, str] = field(default_factory=dict)
    watermark_before: Optional[str] = None
    watermark_after: Optional[str] = None
    closed_windows: List[str] = field(default_factory=list)

    @property
    def on_time_count(self) -> int:
        return sum(count for count, _ in self.deltas.values()) - sum(
            correction.delta_count for correction in self.corrections
        )


class WatermarkTracker:
    """Tracks the highest event time per partition and derives the watermark.

    The watermark is the minimum across partitions that are still speaking,
    minus the configured out-of-orderness.  Partitions silent for longer than
    ``idle_partition_timeout`` are excluded rather than allowed to stall the
    stream -- and if *every* partition is idle, the last known minimum is used
    rather than jumping the watermark forward, because "nobody is talking" is
    not evidence that time has passed in the stream.
    """

    def __init__(
        self,
        policy: WindowPolicy,
        now_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.policy = policy
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._max_event_time: Dict[str, str] = {}
        self._last_seen: Dict[str, datetime] = {}

    # -- state ------------------------------------------------------------

    def restore(self, partition_max: Dict[str, str]) -> None:
        """Reload per-partition progress after a restart."""
        now = self._now_fn()
        for partition, event_time in partition_max.items():
            if event_time:
                self._max_event_time[partition] = event_time
                self._last_seen[partition] = now

    @property
    def partition_max_event_time(self) -> Dict[str, str]:
        return dict(self._max_event_time)

    def observe(self, partition: str, event_time: str) -> None:
        """Record that *partition* produced a record with this event time."""
        current = self._max_event_time.get(partition)
        if current is None or event_time > current:
            self._max_event_time[partition] = event_time
        self._last_seen[partition] = self._now_fn()

    def idle_partitions(self) -> Set[str]:
        """Partitions that have said nothing for longer than the timeout."""
        now = self._now_fn()
        cutoff = timedelta(seconds=self.policy.idle_partition_timeout_seconds)
        return {
            partition
            for partition, last_seen in self._last_seen.items()
            if now - last_seen > cutoff
        }

    def watermark(self) -> Optional[str]:
        """Current watermark, or None before any record has been seen."""
        if not self._max_event_time:
            return None

        idle = self.idle_partitions()
        active = {
            partition: event_time
            for partition, event_time in self._max_event_time.items()
            if partition not in idle
        }
        # Every partition idle: hold the line at the slowest one rather than
        # letting silence advance the watermark.
        source = active or self._max_event_time
        frontier = min(source.values())
        return to_iso(
            from_iso(frontier) - timedelta(seconds=self.policy.out_of_orderness_seconds)
        )


class WindowPlanner:
    """Turns a batch of records into window deltas, corrections and side output."""

    def __init__(self, policy: WindowPolicy, tracker: WatermarkTracker) -> None:
        self.policy = policy
        self.tracker = tracker

    def plan(
        self,
        records: Iterable[Dict[str, Any]],
        partition_of: Callable[[Dict[str, Any]], str],
        open_windows: Optional[Set[str]] = None,
    ) -> BatchPlan:
        """Decide what this batch does, without writing anything.

        ``open_windows`` is every window the sink still has open.  A window
        fires when the watermark passes its end, which has nothing to do with
        whether this particular batch carried a record for it -- a batch of
        traffic for hour 9 closes hour 3.

        Lateness itself is decided by the watermark alone, so restoring state
        after a restart cannot change how a record is classified.
        """
        open_windows = open_windows or set()
        plan = BatchPlan(watermark_before=self.tracker.watermark())

        for record in records:
            event_time = str(record[self.policy.event_time_field])
            window = window_start(event_time, self.policy.window_seconds)
            amount = int(record.get(self.policy.amount_field, 0) or 0)

            classification = self._classify(window, plan.watermark_before)
            if classification == "beyond" and self.policy.on_late == ON_LATE_SIDE_OUTPUT:
                plan.late_records.append(
                    self._late_record(record, event_time, window, plan.watermark_before,
                                      REASON_BEYOND_LATENESS, counted=False)
                )
            else:
                count, total = plan.deltas.get(window, (0, 0))
                plan.deltas[window] = (count + 1, total + amount)

                if classification in ("late", "beyond"):
                    reason = (
                        REASON_CORRECTION
                        if classification == "late"
                        else REASON_BEYOND_LATENESS
                    )
                    plan.corrections.append(
                        Correction(window=window, delta_count=1,
                                   delta_amount_cents=amount, reason=reason)
                    )
                    plan.late_records.append(
                        self._late_record(record, event_time, window,
                                          plan.watermark_before, reason, counted=True)
                    )

            self.tracker.observe(partition_of(record), event_time)

        plan.partition_max_event_time = self.tracker.partition_max_event_time
        plan.watermark_after = self.tracker.watermark()
        plan.closed_windows = self._newly_closed(plan, open_windows)
        return plan

    # -- internals ---------------------------------------------------------

    def _classify(self, window: str, watermark: Optional[str]) -> str:
        """``on_time`` / ``late`` (correctable) / ``beyond`` (past lateness)."""
        if watermark is None:
            return "on_time"

        window_end = self._window_end(window)
        if watermark < window_end:
            return "on_time"

        purge_at = to_iso(
            from_iso(window_end)
            + timedelta(seconds=self.policy.allowed_lateness_seconds)
        )
        return "late" if watermark < purge_at else "beyond"

    def _window_end(self, window: str) -> str:
        return to_iso(
            from_iso(window) + timedelta(seconds=self.policy.window_seconds)
        )

    def _late_record(
        self,
        record: Dict[str, Any],
        event_time: str,
        window: str,
        watermark: Optional[str],
        reason: str,
        counted: bool,
    ) -> LateRecord:
        lateness = 0
        if watermark:
            lateness = int(
                (from_iso(watermark) - from_iso(event_time)).total_seconds()
            )
        return LateRecord(
            key=str(record.get("key", "")),
            seq=int(record.get("seq", 0)),
            event_time=event_time,
            window=window,
            watermark=watermark or "",
            lateness_seconds=max(lateness, 0),
            reason=reason,
            counted=counted,
        )

    def _newly_closed(self, plan: BatchPlan, open_windows: Set[str]) -> List[str]:
        """Windows the watermark has now passed the end of."""
        if plan.watermark_after is None:
            return []
        candidates = set(plan.deltas) | open_windows
        return sorted(
            window
            for window in candidates
            if plan.watermark_after >= self._window_end(window)
        )
