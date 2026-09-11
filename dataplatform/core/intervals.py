"""The period of time a run is *for*, as distinct from when it ran.

A pipeline that loads "yesterday's orders" has two different clocks: the moment
the worker executed it, and the window of data it was responsible for. Until
those are separated a run cannot be repeated for an earlier window, which is
all a backfill is.

    >>> intervals_between({"hour": "6", "minute": "0"},
    ...                   "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    [Interval(2026-01-01T06:00 -> 2026-01-02T06:00), ...]

Intervals are half-open, ``[start, end)``, so consecutive runs neither overlap
nor leave a gap -- the same property the streaming watermarks rely on, for the
same reason.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

#: A backfill over a fine-grained schedule can enumerate an enormous number of
#: runs. Refusing beyond a bound is friendlier than queueing 500,000 of them.
MAX_INTERVALS = 10000


@dataclass(frozen=True)
class Interval:
    """The half-open window of data a run is responsible for."""

    start: str
    end: str

    @property
    def ds(self) -> str:
        """Start date as ``YYYY-MM-DD`` -- the value most pipelines actually use."""
        return self.start[:10]

    @property
    def ds_end(self) -> str:
        return self.end[:10]

    def as_parameters(self) -> Dict[str, str]:
        """The substitutable values this interval provides to a task."""
        return {
            "logical_start": self.start,
            "logical_end": self.end,
            "ds": self.ds,
            "ds_end": self.ds_end,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging affordance
        return "Interval({0} -> {1})".format(self.start, self.end)


def to_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime(ISO_FORMAT)


def from_iso(value: str) -> datetime:
    """Parse an ISO timestamp, tolerating the fractional seconds other tables use."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1]
    if "." in text:
        text = text.split(".", 1)[0]
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def _trigger(schedule: Dict[str, Any]) -> CronTrigger:
    """Build a cron trigger from the schedule block used in pipeline YAML."""
    fields = {
        key: str(value)
        for key, value in (schedule or {}).items()
        if key in {"year", "month", "day", "week", "day_of_week", "hour", "minute", "second"}
        and value is not None
    }
    if not fields:
        raise ValueError("schedule has no cron fields; cannot derive intervals")
    return CronTrigger(timezone=timezone.utc, **fields)


def fire_times(
    schedule: Dict[str, Any],
    start: str,
    end: str,
    limit: int = MAX_INTERVALS,
) -> List[datetime]:
    """Every scheduled fire time in ``[start, end]``, in order."""
    trigger = _trigger(schedule)
    begin, finish = from_iso(start), from_iso(end)
    if finish < begin:
        raise ValueError("end must not be before start")

    times: List[datetime] = []
    previous: Optional[datetime] = None
    cursor = begin
    while len(times) <= limit:
        nxt = trigger.get_next_fire_time(previous, cursor)
        if nxt is None or nxt > finish:
            break
        times.append(nxt)
        previous = nxt
        cursor = nxt
    return times


def intervals_between(
    schedule: Dict[str, Any],
    start: str,
    end: str,
    limit: int = MAX_INTERVALS,
) -> List[Interval]:
    """The data windows a schedule covers between two instants.

    Each interval runs from one fire time to the next, so a daily 06:00
    schedule backfilled across three days yields the windows a daily run would
    have owned -- not three copies of today.

    Raises when the range would produce more than *limit* intervals: a backfill
    that quietly queues half a million runs is an outage, not a feature.
    """
    times = fire_times(schedule, start, end, limit=limit)
    if len(times) > limit:
        raise ValueError(
            "range produces more than {0} intervals; narrow it or raise the limit".format(limit)
        )

    intervals = [
        Interval(start=to_iso(times[index]), end=to_iso(times[index + 1]))
        for index in range(len(times) - 1)
    ]
    if len(intervals) >= limit:
        raise ValueError(
            "range produces more than {0} intervals; narrow it or raise the limit".format(limit)
        )
    return intervals
