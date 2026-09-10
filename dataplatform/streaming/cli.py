"""``dataplatform stream`` -- generate, verify, and measure a baseline.

    dataplatform stream generate --keys 50 --per-key 200 --out data/stream
    dataplatform stream baseline --stream data/stream
    dataplatform stream verify --stream data/stream --actual sink.jsonl

``baseline`` is the Week 0 deliverable: it runs the two naive consumer shapes
against a crash and prints the duplicate and loss rates they produce.  Those
numbers are the "before" column every later correctness claim is measured
against.
"""
from __future__ import annotations

import json
import os
from typing import Optional

import typer

from dataplatform.streaming.baseline import AT_LEAST_ONCE, AT_MOST_ONCE, run_baseline
from dataplatform.streaming.generator import (
    GeneratorConfig,
    Manifest,
    generate,
)
from dataplatform.streaming.model import StreamEvent, read_jsonl
from dataplatform.streaming.verifier import verify

stream_app = typer.Typer(help="Streaming correctness harness (generator + oracle).")

EVENTS_FILE = "events.jsonl"
MANIFEST_FILE = "manifest.json"


def _paths(directory: str):
    return os.path.join(directory, EVENTS_FILE), os.path.join(directory, MANIFEST_FILE)


@stream_app.command("generate")
def generate_stream(
    out: str = typer.Option("data/stream", help="Directory for events + manifest."),
    keys: int = typer.Option(25, help="Distinct entity keys."),
    per_key: int = typer.Option(100, help="Events per key."),
    seed: int = typer.Option(1337, help="Seed -- same seed, same stream."),
    window_seconds: int = typer.Option(3600, help="Tumbling window size."),
    late_fraction: float = typer.Option(0.08, help="Share arriving late."),
    duplicate_fraction: float = typer.Option(
        0.0, help="Share redelivered by the broker."
    ),
):
    """Generate a deterministic out-of-order stream and its manifest."""
    config = GeneratorConfig(
        keys=keys,
        events_per_key=per_key,
        seed=seed,
        window_seconds=window_seconds,
        late_fraction=late_fraction,
        duplicate_fraction=duplicate_fraction,
    )
    stream = generate(config)
    os.makedirs(out, exist_ok=True)
    events_path, manifest_path = _paths(out)
    written = stream.write(events_path, manifest_path)

    typer.echo("wrote {0} records to {1}".format(written, events_path))
    typer.echo("  unique events     {0}".format(stream.manifest.total_unique))
    typer.echo("  duplicates        {0}".format(stream.manifest.duplicates_injected))
    typer.echo("  out-of-order      {0}".format(stream.out_of_order_count))
    typer.echo("  event windows     {0}".format(len(stream.manifest.windows)))
    typer.echo("manifest -> {0}".format(manifest_path))


@stream_app.command("run")
def run_stream_cmd(
    stream: str = typer.Option("data/stream", help="Directory holding the stream."),
    table: str = typer.Option("events_sink", help="Sink table in the metadata store."),
    stream_name: str = typer.Option("events", help="Stream identity for offsets."),
    partitions: int = typer.Option(4, help="Source partitions."),
    batch_size: int = typer.Option(100, help="Records per commit."),
    attempt: int = typer.Option(1, help="Fencing token for this writer."),
    max_batches: Optional[int] = typer.Option(None, help="Stop after N commits."),
):
    """Drain the stream into a transactional sink, resuming from committed offsets."""
    from dataplatform.streaming.runner import build_and_run

    stats = build_and_run(
        stream_dir=stream,
        table=table,
        stream_name=stream_name,
        partitions=partitions,
        batch_size=batch_size,
        attempt=attempt,
        max_batches=max_batches,
    )
    typer.echo(stats.summary())
    if stats.fenced_out:
        raise typer.Exit(2)


@stream_app.command("verify")
def verify_sink(
    stream: str = typer.Option("data/stream", help="Directory holding the manifest."),
    actual: Optional[str] = typer.Option(None, help="JSONL file holding the sink contents."),
    table: Optional[str] = typer.Option(None, help="Sink table to read instead of a file."),
    committed_offset: Optional[int] = typer.Option(
        None, help="Offset the pipeline believes it committed."
    ),
    json_out: bool = typer.Option(False, "--json", help="Emit the report as JSON."),
):
    """Verify a sink against the manifest. Exits non-zero when it fails."""
    if not actual and not table:
        raise typer.BadParameter("pass --actual <file> or --table <sink table>")

    _, manifest_path = _paths(stream)
    manifest = Manifest.read(manifest_path)

    if table:
        from dataplatform.streaming.sinks import SqlTransactionalSink

        sink = SqlTransactionalSink(table=table, stream="events")
        rows = sink.rows()
        if committed_offset is None:
            committed_offset = sink.read_offsets().total()
    else:
        rows = read_jsonl(actual)

    report = verify(manifest, rows, committed_offset=committed_offset)

    typer.echo(json.dumps(report.as_dict(), indent=2) if json_out else report.summary())
    if not report.ok:
        raise typer.Exit(1)


@stream_app.command("baseline")
def baseline(
    stream: str = typer.Option("data/stream", help="Directory holding the stream."),
    batch_size: int = typer.Option(100, help="Records per consumer batch."),
    crash_after_batches: int = typer.Option(3, help="Batch to crash on."),
):
    """Measure the naive consumers: the numbers Week 1 has to beat."""
    events_path, manifest_path = _paths(stream)
    manifest = Manifest.read(manifest_path)
    events = [StreamEvent.from_dict(row) for row in read_jsonl(events_path)]

    for mode in (AT_LEAST_ONCE, AT_MOST_ONCE):
        run = run_baseline(
            events,
            mode=mode,
            batch_size=batch_size,
            crash_after_batches=crash_after_batches,
        )
        report = verify(manifest, run.rows, committed_offset=run.committed_offset)
        typer.echo("")
        typer.echo("=== {0} (crash at batch {1}) ===".format(mode, crash_after_batches))
        typer.echo(report.summary())
