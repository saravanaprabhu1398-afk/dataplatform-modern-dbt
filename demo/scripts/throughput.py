"""Throughput and commit latency, measured rather than estimated.

What is measured: records committed per second, and the wall-clock cost of the
commit itself at the 50th and 99th percentile, across batch sizes and with the
event-time aggregate path both on and off.

What is deliberately *not* measured: end-to-end lag. The harness replays a file
whose ingest timestamps are stream time, not wall-clock arrival, so any
"end-to-end lag" computed here would be an artifact of the generator rather
than a property of the pipeline. Reporting it would be inventing a number.

    python demo/scripts/throughput.py
"""
import os
import sys
import tempfile
import time
from pathlib import Path

RECORDS_PER_KEY = 300
KEYS = 40
PARTITIONS = 4
BATCH_SIZES = (50, 250, 1000)


def main():
    workdir = Path(tempfile.mkdtemp(prefix="throughput-"))
    os.environ["DATABASE_PATH"] = str(workdir / "platform.db")

    import dataplatform.core.database as db
    from dataplatform.plugins.base import Fence
    from dataplatform.streaming.generator import GeneratorConfig, generate
    from dataplatform.streaming.runner import run_stream
    from dataplatform.streaming.sinks import SqlTransactionalSink
    from dataplatform.streaming.sources import JsonlSource, partition_for
    from dataplatform.streaming.windows import WindowPolicy

    db._DB_PATH = workdir / "platform.db"
    db._engine = None
    db.init_db()

    stream = generate(
        GeneratorConfig(keys=KEYS, events_per_key=RECORDS_PER_KEY, seed=5,
                        event_interval_seconds=144, late_fraction=0.08,
                        very_late_fraction=0.01)
    )
    stream.write(str(workdir / "events.jsonl"), str(workdir / "manifest.json"))
    total = stream.manifest.total_unique

    print("{0} records, {1} partitions, SQLite on local disk".format(total, PARTITIONS))
    print("{0} python {1}\n".format(sys.platform, sys.version.split()[0]))
    print("{0:<10} {1:<10} {2:>12} {3:>12} {4:>12} {5:>10}".format(
        "batch", "windowing", "records/s", "p50 commit", "p99 commit", "batches"))
    print("-" * 70)

    run_id = 0
    for windowing in (False, True):
        for batch_size in BATCH_SIZES:
            run_id += 1
            table = "bench_{0}".format(run_id)
            sink = SqlTransactionalSink(
                table=table,
                stream="bench-{0}".format(run_id),
                windowing=WindowPolicy() if windowing else None,
                partition_of=lambda record: partition_for(str(record["key"]), PARTITIONS),
            )
            source = JsonlSource(str(workdir / "events.jsonl"), partitions=PARTITIONS)

            started = time.perf_counter()
            stats = run_stream(source, sink, Fence("bench-{0}".format(run_id), 1),
                               batch_size=batch_size)
            elapsed = time.perf_counter() - started

            assert stats.records == total, "benchmark run dropped records"
            print("{0:<10} {1:<10} {2:>12,.0f} {3:>11.2f}ms {4:>11.2f}ms {5:>10}".format(
                batch_size, "on" if windowing else "off",
                stats.records / elapsed, stats.p50_commit_ms, stats.p99_commit_ms,
                stats.batches))

    print("\nEvery run was verified to have committed all {0} records.".format(total))


if __name__ == "__main__":
    main()
