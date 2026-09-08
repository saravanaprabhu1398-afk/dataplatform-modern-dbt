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
from dataclasses import dataclass
from typing import Optional

from dataplatform.core.chaos import CrashPoint, maybe_crash
from dataplatform.plugins.base import Fence, StaleFence, StreamingSource, TransactionalSink
from dataplatform.streaming.windows import WindowPolicy

logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    """What one runner invocation did."""

    stream: str
    attempt: int
    batches: int = 0
    records: int = 0
    resumed_from: int = 0
    fenced_out: bool = False

    def summary(self) -> str:
        return (
            "stream={0} attempt={1} resumed_from={2} batches={3} records={4}{5}".format(
                self.stream,
                self.attempt,
                self.resumed_from,
                self.batches,
                self.records,
                " FENCED OUT" if self.fenced_out else "",
            )
        )


def run_stream(
    source: StreamingSource,
    sink: TransactionalSink,
    fence: Fence,
    batch_size: int = 100,
    max_batches: Optional[int] = None,
) -> RunStats:
    """Drain *source* into *sink* under *fence*. Returns what happened."""
    offsets = sink.read_offsets(fence.stream)
    source.seek(offsets)

    stats = RunStats(stream=fence.stream, attempt=fence.attempt, resumed_from=offsets.total())

    # Fail fast: the commit is fenced anyway, but a writer that has already
    # been replaced should not spend a poll finding that out -- and a stale
    # writer with an empty backlog would otherwise exit as though it had
    # succeeded.
    committed = sink.committed_attempt(fence.stream)
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
            written = sink.commit(records, next_offsets, fence)
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
