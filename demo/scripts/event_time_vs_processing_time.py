"""Show what windowing on arrival time gets wrong.

Both pipelines see exactly the same records.  One buckets them by when they
happened (event time, with a watermark and a side output for stragglers); the
other buckets them by when they turned up (processing time), which is what you
get for free and what most first implementations do.

The difference is not noise.  Records that arrived late are counted in the
wrong hour by the processing-time pipeline -- revenue moves between periods,
and nothing in the pipeline reports that it happened.

    python demo/scripts/event_time_vs_processing_time.py
"""
import os
import tempfile
from pathlib import Path

WINDOW_SECONDS = 3600


def main():
    workdir = Path(tempfile.mkdtemp(prefix="event-time-demo-"))
    os.environ["DATABASE_PATH"] = str(workdir / "platform.db")

    import dataplatform.core.database as db
    from dataplatform.plugins.base import Fence
    from dataplatform.streaming.generator import GeneratorConfig, generate
    from dataplatform.streaming.model import window_start
    from dataplatform.streaming.runner import run_stream
    from dataplatform.streaming.sinks import SqlTransactionalSink
    from dataplatform.streaming.sources import JsonlSource, partition_for
    from dataplatform.streaming.verifier import verify_window_accounting
    from dataplatform.streaming.windows import WindowPolicy

    db._DB_PATH = workdir / "platform.db"
    db._engine = None
    db.init_db()

    stream = generate(
        GeneratorConfig(
            keys=40, events_per_key=300, seed=21, event_interval_seconds=144,
            late_fraction=0.08, late_delay_seconds=5400,
            very_late_fraction=0.01, very_late_delay_seconds=172800,
        )
    )
    stream.write(str(workdir / "events.jsonl"), str(workdir / "manifest.json"))

    policy = WindowPolicy(
        window_seconds=WINDOW_SECONDS,
        out_of_orderness_seconds=300,
        allowed_lateness_seconds=21600,
    )
    source = JsonlSource(str(workdir / "events.jsonl"), partitions=4)
    sink = SqlTransactionalSink(
        table="demo_sink", stream="demo", windowing=policy,
        partition_of=lambda record: partition_for(str(record["key"]), 4),
    )
    run_stream(source, sink, Fence("demo", 1), batch_size=500)

    # What a processing-time pipeline would have reported: bucket by arrival.
    processing = {}
    for event in stream.events:
        bucket = window_start(event.ingest_time, WINDOW_SECONDS)
        processing[bucket] = processing.get(bucket, 0) + 1

    event_time = {row["window_start"]: row["event_count"] for row in sink.window_rows()}
    truth = {window: agg["count"] for window, agg in stream.manifest.windows.items()}
    side_output = {}
    for row in sink.late_rows():
        if not row["counted"]:
            side_output[row["window_start"]] = side_output.get(row["window_start"], 0) + 1

    print("{0} records, {1} arriving out of order\n".format(
        len(stream.events), stream.out_of_order_count
    ))
    print("{0:<26} {1:>8} {2:>10} {3:>7} {4:>12} {5:>8}".format(
        "window (event time)", "truth", "event-time", "+side", "processing", "drift"
    ))
    for window in sorted(set(truth) | set(processing)):
        expected = truth.get(window, 0)
        measured = event_time.get(window, 0)
        excluded = side_output.get(window, 0)
        arrival = processing.get(window, 0)
        print("{0:<26} {1:>8} {2:>10} {3:>7} {4:>12} {5:>8}".format(
            window[:19].replace("T", " "),
            expected, measured, excluded, arrival, arrival - expected,
        ))

    accounting = verify_window_accounting(
        stream.manifest, sink.window_rows(), sink.late_rows(), sink_rows=sink.rows()
    )
    misplaced = sum(abs(processing.get(w, 0) - truth.get(w, 0)) for w in set(truth) | set(processing))

    print("")
    print(accounting.summary())
    print("")
    print("processing-time windowing misplaces {0} records across {1} buckets, "
          "and reports nothing".format(misplaced, len(set(processing) - set(truth)) + len(truth)))
    print("event-time windowing counts {0} of them as corrections and sets {1} aside, "
          "each one recorded".format(accounting.late_counted, accounting.late_side_output))


if __name__ == "__main__":
    main()
