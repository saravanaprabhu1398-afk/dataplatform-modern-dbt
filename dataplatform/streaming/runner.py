"""The consume loop: poll, commit, repeat.

The loop is deliberately dull, because all of the correctness lives in the
sink's transaction.  What the runner contributes is the discipline around it:

* offsets come from the sink, never from the source's own bookkeeping;
* the source is only advanced locally *after* a commit succeeds;
* every step is a named crash point, so the loop can be killed anywhere and
  the result checked against the Week 0 oracle.

Run it directly for a chaos test::

    DATAPLATFORM_CHAOS="after_write:2" python -m dataplatform.streaming.runner \\
        --stream data/stream --table events_sink
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import DBAPIError, OperationalError

from dataplatform.core.chaos import CrashPoint, maybe_crash
from dataplatform.plugins.base import Fence, StaleFence, StreamingSource, TransactionalSink
from dataplatform.streaming.windows import WindowPolicy

logger = logging.getLogger(__name__)


#: Sink failures worth waiting out rather than dying on: the database went
#: away, not the data.
TRANSIENT_ERRORS = (OperationalError, DBAPIError)


def _percentile(values: List[float], pct: float) -> float:
    """Nearest-rank percentile; 0.0 for an empty sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, int(round(pct / 100.0 * len(ordered))))
    return ordered[min(rank, len(ordered)) - 1]


@dataclass
class RunStats:
    """What one runner invocation did."""

    stream: str
    attempt: int
    batches: int = 0
    records: int = 0
    resumed_from: int = 0
    fenced_out: bool = False
    retries: int = 0
    commit_ms: List[float] = field(default_factory=list)

    @property
    def p99_commit_ms(self) -> float:
        return _percentile(self.commit_ms, 99)

    @property
    def p50_commit_ms(self) -> float:
        return _percentile(self.commit_ms, 50)

    def summary(self) -> str:
        return (
            "stream={0} attempt={1} resumed_from={2} batches={3} records={4} "
            "retries={5}{6}".format(
                self.stream,
                self.attempt,
                self.resumed_from,
                self.batches,
                self.records,
                self.retries,
                " FENCED OUT" if self.fenced_out else "",
            )
        )


def run_stream(
    source: StreamingSource,
    sink: TransactionalSink,
    fence: Fence,
    batch_size: int = 100,
    max_batches: Optional[int] = None,
    retry_attempts: int = 5,
    retry_backoff_seconds: float = 0.2,
    sleep_fn: Optional[Any] = None,
) -> RunStats:
    """Drain *source* into *sink* under *fence*. Returns what happened.

    A sink that is temporarily unavailable is waited out with bounded
    exponential backoff rather than crashed on: the records are still in the
    source, and the offsets have not moved, so waiting costs nothing but
    latency.  Being fenced out is not transient and is never retried.
    """
    stats = RunStats(stream=fence.stream, attempt=fence.attempt)
    sleep = sleep_fn or time.sleep

    offsets = _with_retry(
        lambda: sink.read_offsets(fence.stream),
        "offset read", stats, retry_attempts, retry_backoff_seconds, sleep,
    )
    source.seek(offsets)
    stats.resumed_from = offsets.total()

    # Fail fast: the commit is fenced anyway, but a writer that has already
    # been replaced should not spend a poll finding that out -- and a stale
    # writer with an empty backlog would otherwise exit as though it had
    # succeeded.
    committed = _with_retry(
        lambda: sink.committed_attempt(fence.stream),
        "fence check", stats, retry_attempts, retry_backoff_seconds, sleep,
    )
    if committed > fence.attempt:
        logger.error(
            "refusing to run stream %s: committed attempt %s supersedes ours (%s)",
            fence.stream,
            committed,
            fence.attempt,
        )
        stats.fenced_out = True
        return stats
    logger.info(
        "runner starting: stream=%s attempt=%s resuming at %d record(s)",
        fence.stream,
        fence.attempt,
        stats.resumed_from,
    )

    while max_batches is None or stats.batches < max_batches:
        maybe_crash(CrashPoint.BEFORE_POLL)
        records, next_offsets = source.poll(batch_size)
        if not records:
            break
        maybe_crash(CrashPoint.AFTER_POLL)

        try:
            written = _commit_with_retry(
                sink, records, next_offsets, fence, stats,
                retry_attempts, retry_backoff_seconds, sleep,
            )
        except StaleFence as exc:
            logger.error("%s", exc)
            stats.fenced_out = True
            return stats

        # Only now is it safe to move the source's own cursor.
        source.commit_local(next_offsets)
        stats.batches += 1
        stats.records += written

    logger.info("runner finished: %s", stats.summary())
    return stats


def _with_retry(
    call: Any,
    what: str,
    stats: RunStats,
    attempts: int,
    backoff: float,
    sleep_fn: Any,
) -> Any:
    """Run *call*, waiting out transient sink failures.

    Every sink interaction goes through here, not just the commit: a runner
    that starts while the database is down should wait for it like any other
    outage, rather than dying on the first offset read.
    """
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            return call()
        except TRANSIENT_ERRORS as exc:
            if attempt >= attempts:
                logger.error("%s still failing after %d attempts: %s", what, attempt, exc)
                raise
            delay = backoff * (2 ** (attempt - 1))
            stats.retries += 1
            logger.warning(
                "%s unavailable (attempt %d/%d), retrying in %.2fs: %s",
                what, attempt, attempts, delay, exc,
            )
            sleep_fn(delay)

    raise RuntimeError("unreachable: retry loop exited without a result")


def _commit_with_retry(
    sink: TransactionalSink,
    records: List[Dict[str, Any]],
    offsets: Any,
    fence: Fence,
    stats: RunStats,
    attempts: int,
    backoff: float,
    sleep_fn: Any,
) -> int:
    """Commit, waiting out transient sink failures. Raises when they persist."""
    started = time.perf_counter()
    written = _with_retry(
        lambda: sink.commit(records, offsets, fence),
        "sink commit", stats, attempts, backoff, sleep_fn,
    )
    stats.commit_ms.append((time.perf_counter() - started) * 1000)
    return written


def build_and_run(
    stream_dir: str,
    table: str,
    stream_name: str = "events",
    partitions: int = 4,
    batch_size: int = 100,
    attempt: int = 1,
    max_batches: Optional[int] = None,
    windowing: Optional["WindowPolicy"] = None,
) -> RunStats:
    """Wire a JSONL source to a SQL sink and drain it."""
    from dataplatform.core.database import init_db
    from dataplatform.streaming.sinks import SqlTransactionalSink
    from dataplatform.streaming.sources import JsonlSource, partition_for

    init_db()
    source = JsonlSource(os.path.join(stream_dir, "events.jsonl"), partitions=partitions)
    sink = SqlTransactionalSink(
        table=table,
        stream=stream_name,
        windowing=windowing,
        # The sink must partition records exactly as the source did, or the
        # watermark would be computed over the wrong frontier.
        partition_of=lambda record: partition_for(str(record["key"]), partitions),
    )
    return run_stream(
        source,
        sink,
        Fence(stream=stream_name, attempt=attempt),
        batch_size=batch_size,
        max_batches=max_batches,
    )


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Drain a JSONL stream into a SQL sink.")
    parser.add_argument("--stream", default="data/stream", help="Directory with events.jsonl")
    parser.add_argument("--table", default="events_sink", help="Sink table name")
    parser.add_argument("--stream-name", default="events", help="Stream identity for offsets")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--attempt", type=int, default=1, help="Fencing token")
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--window", default="1h", help="Tumbling window size")
    parser.add_argument("--out-of-orderness", default="5m", help="Watermark lag")
    parser.add_argument("--allowed-lateness", default="6h", help="Window retention after firing")
    parser.add_argument("--idle-timeout", default="60s", help="Idle partition timeout")
    parser.add_argument("--on-late", default="side_output", choices=["side_output", "update"])
    parser.add_argument("--no-windowing", action="store_true", help="Skip event-time windowing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    from dataplatform.core.durations import parse_duration

    windowing = None if args.no_windowing else WindowPolicy(
        window_seconds=parse_duration(args.window),
        out_of_orderness_seconds=parse_duration(args.out_of_orderness),
        allowed_lateness_seconds=parse_duration(args.allowed_lateness),
        idle_partition_timeout_seconds=parse_duration(args.idle_timeout),
        on_late=args.on_late,
    )
    stats = build_and_run(
        stream_dir=args.stream,
        table=args.table,
        stream_name=args.stream_name,
        partitions=args.partitions,
        batch_size=args.batch_size,
        attempt=args.attempt,
        max_batches=args.max_batches,
        windowing=windowing,
    )
    print(stats.summary())
    return 2 if stats.fenced_out else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
