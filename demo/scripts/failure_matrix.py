"""Break the streaming pipeline eight ways and measure what survives.

Every row runs for real against a temporary database: processes are killed
with SIGKILL semantics, leases are allowed to expire, the sink is taken away
mid-run.  The oracle from Week 0 decides whether the result is acceptable --
not the pipeline's own opinion of how it did.

The last row is a known limitation rather than a pass.  A matrix with no
limitations in it is a matrix that was not run.

    python demo/scripts/failure_matrix.py
"""
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

WORKDIR = Path(tempfile.mkdtemp(prefix="failure-matrix-"))
STREAM = "matrix"
PARTITIONS = 4

results = []


def row(name, simulates, expectation, ok, measured):
    results.append((name, simulates, expectation, ok, measured))
    print("  {0:<24} {1:<8} {2}".format(name, "PASS" if ok else "NOTE", measured))


def fresh_db(label):
    """A clean metadata store per row, so rows cannot mask each other."""
    import dataplatform.core.database as db

    path = WORKDIR / "{0}.db".format(label)
    os.environ["DATABASE_PATH"] = str(path)
    db._initialized = False
    db._DB_PATH = path
    db._engine = None
    db.init_db()
    return path


def make_stream(label, **overrides):
    from dataplatform.streaming.generator import GeneratorConfig, generate

    config = GeneratorConfig(
        keys=8, events_per_key=300, seed=77, event_interval_seconds=144,
        late_fraction=0.1, late_delay_seconds=5400,
        very_late_fraction=0.03, very_late_delay_seconds=172800,
        **overrides
    )
    stream = generate(config)
    directory = WORKDIR / label
    directory.mkdir(parents=True, exist_ok=True)
    stream.write(str(directory / "events.jsonl"), str(directory / "manifest.json"))
    return stream, directory


def wire(table, policy=None):
    from dataplatform.streaming.sinks import SqlTransactionalSink
    from dataplatform.streaming.sources import partition_for

    return SqlTransactionalSink(
        table=table, stream=STREAM, windowing=policy,
        partition_of=lambda record: partition_for(str(record["key"]), PARTITIONS),
    )


def drain(directory, sink, attempt=1, batch_size=200):
    from dataplatform.plugins.base import Fence
    from dataplatform.streaming.runner import run_stream
    from dataplatform.streaming.sources import JsonlSource

    source = JsonlSource(str(directory / "events.jsonl"), partitions=PARTITIONS)
    return run_stream(source, sink, Fence(STREAM, attempt), batch_size=batch_size)


def report(stream, sink):
    from dataplatform.streaming.verifier import verify

    return verify(stream.manifest, sink.rows(), committed_offset=sink.read_offsets().total())


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def row_killed_mid_transaction():
    db_path = fresh_db("kill_txn")
    stream, directory = make_stream("kill_txn")

    env = dict(os.environ, DATABASE_PATH=str(db_path), DATAPLATFORM_CHAOS="after_write:3")
    killed = subprocess.run(
        [sys.executable, "-m", "dataplatform.streaming.runner",
         "--stream", str(directory), "--table", "kill_txn", "--stream-name", STREAM,
         "--batch-size", "200"],
        env=env, capture_output=True, text=True,
    )
    env.pop("DATAPLATFORM_CHAOS")
    subprocess.run(
        [sys.executable, "-m", "dataplatform.streaming.runner",
         "--stream", str(directory), "--table", "kill_txn", "--stream-name", STREAM,
         "--batch-size", "200"],
        env=env, capture_output=True, text=True,
    )

    result = report(stream, wire("kill_txn"))
    row("SIGKILL mid-batch", "worker dies after write, before commit",
        "0 dup, 0 loss",
        killed.returncode == 137 and result.ok,
        "exit {0}, {1} dup, {2} missing".format(
            killed.returncode, result.duplicate_rows, result.missing_total))


def row_zombie_writer():
    from dataplatform.plugins.base import Fence, StaleFence

    fresh_db("zombie")
    stream, directory = make_stream("zombie")
    sink = wire("zombie")
    source_stats = drain(directory, sink, attempt=1, batch_size=200)
    before = len(sink.rows())

    fenced = False
    try:
        sink.commit([{"key": "k0000", "seq": 999999, "event_time": "2026-01-01T00:00:00.000000Z",
                      "ingest_time": "2026-01-01T00:00:00.000000Z", "amount_cents": 1,
                      "payload": "zombie", "checksum": "x"}],
                    __import__("dataplatform.plugins.base", fromlist=["Offsets"]).Offsets({"0": 1}),
                    Fence(STREAM, 0))
    except StaleFence:
        fenced = True

    after = len(sink.rows())
    row("Lease expiry / zombie", "stalled worker wakes after being replaced",
        "StaleFence, nothing written",
        fenced and before == after,
        "fenced={0}, rows {1} -> {2}".format(fenced, before, after))


def row_duplicate_delivery():
    from dataplatform.plugins.base import Fence
    from dataplatform.streaming.sources import JsonlSource
    from dataplatform.streaming.windows import WindowPolicy

    fresh_db("dupes")
    stream, directory = make_stream("dupes")
    sink = wire("dupes", policy=WindowPolicy())
    source = JsonlSource(str(directory / "events.jsonl"), partitions=PARTITIONS)

    batch, offsets = source.poll(200)
    sink.commit(batch, offsets, Fence(STREAM, 1))
    windows_before = sum(r["event_count"] for r in sink.window_rows())
    sink.commit(batch, offsets, Fence(STREAM, 1))          # broker redelivery
    windows_after = sum(r["event_count"] for r in sink.window_rows())

    rows = len(sink.rows())
    row("Duplicate delivery", "broker resends an acknowledged batch",
        "0 dup, aggregates unchanged",
        rows == 200 and windows_before == windows_after,
        "{0} rows, window total {1} -> {2}".format(rows, windows_before, windows_after))


def row_late_within_lateness():
    from dataplatform.streaming.verifier import verify_window_accounting
    from dataplatform.streaming.windows import WindowPolicy

    fresh_db("late_ok")
    stream, directory = make_stream("late_ok")
    sink = wire("late_ok", policy=WindowPolicy())
    drain(directory, sink)

    accounting = verify_window_accounting(
        stream.manifest, sink.window_rows(), sink.late_rows(), sink_rows=sink.rows())
    corrections = len(sink.correction_rows())

    row("Out-of-order, < 6h", "arrival after its window fired, inside lateness",
        "window restated, correction emitted",
        accounting.ok and corrections > 0,
        "{0} corrections over {1} windows".format(corrections, accounting.windows_checked))
    return sink, stream, accounting


def row_beyond_lateness(sink, stream, accounting):
    windowed = sum(r["event_count"] for r in sink.window_rows())
    side = sum(1 for r in sink.late_rows() if not r["counted"])
    row("Out-of-order, 48h", "arrival past allowed lateness",
        "side output, counted, never dropped",
        side > 0 and windowed + side == stream.manifest.total_unique,
        "{0} set aside, {1} + {0} = {2}".format(side, windowed, stream.manifest.total_unique))


def row_idle_partition():
    from dataplatform.plugins.base import Fence, Offsets
    from dataplatform.streaming.windows import WindowPolicy

    fresh_db("idle")
    policy = WindowPolicy(window_seconds=3600, out_of_orderness_seconds=300,
                          allowed_lateness_seconds=21600, idle_partition_timeout_seconds=1)
    sink = wire("idle", policy=policy)

    def event(key, seq, hours):
        return {"key": key, "seq": seq,
                "event_time": "2026-01-01T{0:02d}:00:00.000000Z".format(hours),
                "ingest_time": "2026-01-01T00:00:00.000000Z",
                "amount_cents": 10, "payload": "p", "checksum": "c"}

    # Two partitions speak, then one goes quiet while the other runs ahead.
    sink._partition_of = lambda record: "0" if record["key"] == "a" else "1"
    sink.commit([event("a", 0, 1), event("b", 0, 1)], Offsets({"0": 1, "1": 1}), Fence(STREAM, 1))
    stalled = sink.watermark

    time.sleep(1.2)
    sink.commit([event("a", 1, 9)], Offsets({"0": 2, "1": 1}), Fence(STREAM, 1))
    advanced = sink.watermark

    row("Idle partition", "one partition stops producing",
        "watermark advances anyway",
        advanced > stalled,
        "{0} -> {1}".format(stalled[11:19], advanced[11:19]))


def row_sink_unavailable():
    from sqlalchemy.exc import OperationalError

    import dataplatform.streaming.sinks as sinks_module
    from dataplatform.streaming.windows import WindowPolicy

    fresh_db("outage")
    stream, directory = make_stream("outage")
    sink = wire("outage", policy=WindowPolicy())

    real_engine = sinks_module._get_engine
    state = {"failures": 0, "budget": 3}

    def flaky():
        if state["failures"] < state["budget"]:
            state["failures"] += 1
            raise OperationalError("SELECT 1", {}, Exception("database is unavailable"))
        return real_engine()

    sinks_module._get_engine = flaky
    started = time.time()
    try:
        stats = drain(directory, sink)
    finally:
        sinks_module._get_engine = real_engine
    elapsed = time.time() - started

    result = report(stream, sink)
    row("Sink unavailable", "database away for the first 3 commits",
        "backpressure, no loss",
        stats.retries >= 3 and result.ok,
        "{0} retries, recovered in {1:.1f}s, {2} missing".format(
            stats.retries, elapsed, result.missing_total))


def row_clock_skew():
    from dataplatform.streaming.verifier import verify_window_accounting
    from dataplatform.streaming.windows import WindowPolicy

    fresh_db("skew")
    stream, directory = make_stream("skew")

    # One producer's clock runs 5 minutes fast: rewrite its event times.
    import json
    from datetime import timedelta

    from dataplatform.streaming.model import from_iso, to_iso

    path = directory / "events.jsonl"
    skewed = []
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record["key"] in ("k0000", "k0001"):
            record["event_time"] = to_iso(from_iso(record["event_time"]) + timedelta(minutes=5))
        skewed.append(json.dumps(record, sort_keys=True))
    skewed_dir = WORKDIR / "skew_events"
    skewed_dir.mkdir(exist_ok=True)
    (skewed_dir / "events.jsonl").write_text("\n".join(skewed) + "\n")

    sink = wire("skew", policy=WindowPolicy())
    drain(skewed_dir, sink)

    late = len(sink.late_rows())
    accounting = verify_window_accounting(
        stream.manifest, sink.window_rows(), sink.late_rows(), sink_rows=sink.rows())

    # Not a pass: the manifest was built from unskewed times, so windows move.
    row("Clock skew, 5m", "one producer's clock runs fast",
        "KNOWN LIMITATION: no per-producer skew correction",
        False,
        "{0} late rows, {1}/{2} windows still match".format(
            late, accounting.matched, accounting.windows_checked))


def main():
    print("failure matrix -- every row runs for real\n")
    print("  {0:<24} {1:<8} {2}".format("injection", "result", "measured"))
    print("  " + "-" * 72)

    row_killed_mid_transaction()
    row_zombie_writer()
    row_duplicate_delivery()
    sink, stream, accounting = row_late_within_lateness()
    row_beyond_lateness(sink, stream, accounting)
    row_idle_partition()
    row_sink_unavailable()
    row_clock_skew()

    passed = sum(1 for *_, ok, _ in results if ok)
    print("\n{0} of {1} injections handled; {2} recorded as a known limitation".format(
        passed, len(results), len(results) - passed))
    return 0 if passed == len(results) - 1 else 1


if __name__ == "__main__":
    sys.exit(main())
