"""Tests for the Week 2 commit boundary.

The claim under test: records and offsets land together or not at all, and a
writer whose fencing token has been superseded cannot land anything.

The end-to-end cases kill a real subprocess with ``SIGKILL`` semantics
(``os._exit``) partway through, restart it, and hand the result to the Week 0
verifier.  In-process tests can prove rollback; only a killed process proves
that nothing in the runtime was quietly holding the guarantee together.
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

import dataplatform.core.database as db
from dataplatform.core.chaos import CRASH_EXIT_CODE
from dataplatform.plugins.base import Fence, Offsets, StaleFence
from dataplatform.streaming.generator import GeneratorConfig, Manifest, generate
from dataplatform.streaming.runner import run_stream
from dataplatform.streaming.sinks import SqlTransactionalSink
from dataplatform.streaming.sources import JsonlSource, partition_for
from dataplatform.streaming.verifier import verify

STREAM = "events"
TABLE = "events_sink"


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    """A temp metadata store plus a small generated stream on disk."""
    db_file = tmp_path / "platform.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_file))
    db._initialized = False
    db._DB_PATH = Path(str(db_file))
    db._engine = None
    db.init_db()

    stream_dir = tmp_path / "stream"
    stream_dir.mkdir()
    stream = generate(GeneratorConfig(keys=8, events_per_key=25, seed=99))
    stream.write(str(stream_dir / "events.jsonl"), str(stream_dir / "manifest.json"))

    yield {
        "dir": str(stream_dir),
        "db": str(db_file),
        "manifest": stream.manifest,
        "total": stream.manifest.total_unique,
    }

    db._initialized = False
    db._engine = None


def _source(workspace, partitions=4):
    return JsonlSource(os.path.join(workspace["dir"], "events.jsonl"), partitions=partitions)


def _sink():
    return SqlTransactionalSink(table=TABLE, stream=STREAM)


class TestPartitioning:
    def test_assignment_is_stable_across_processes(self):
        # hash() is salted per process; crc32 is not. A partitioner that
        # disagrees between workers would reorder a key's events.
        assert partition_for("k0001", 4) == partition_for("k0001", 4)
        assert {partition_for("k{0:04d}".format(i), 4) for i in range(50)} == {"0", "1", "2", "3"}

    def test_every_record_is_placed_exactly_once(self, workspace):
        source = _source(workspace)
        assert source.total_records == workspace["total"]


class TestSourceResume:
    def test_poll_does_not_advance_on_its_own(self, workspace):
        source = _source(workspace)
        first, _ = source.poll(10)
        second, _ = source.poll(10)
        assert [r["checksum"] for r in first] == [r["checksum"] for r in second]

    def test_seek_resumes_after_committed_offsets(self, workspace):
        source = _source(workspace)
        first, offsets = source.poll(10)
        source.seek(offsets)
        second, _ = source.poll(10)

        assert len(second) == 10
        assert not ({r["checksum"] for r in first} & {r["checksum"] for r in second})

    def test_drains_exactly_the_whole_stream(self, workspace):
        source = _source(workspace)
        seen = []
        while True:
            batch, offsets = source.poll(37)
            if not batch:
                break
            seen.extend(batch)
            source.commit_local(offsets)

        assert len(seen) == workspace["total"]
        assert len({(r["key"], r["seq"]) for r in seen}) == workspace["total"]


class TestAtomicCommit:
    def test_records_and_offsets_land_together(self, workspace):
        source, sink = _source(workspace), _sink()
        batch, offsets = source.poll(20)

        written = sink.commit(batch, offsets, Fence(STREAM, 1))

        assert written == 20
        assert len(sink.rows()) == 20
        assert sink.read_offsets().positions == offsets.positions
        assert sink.committed_attempt() == 1

    def test_replayed_batch_does_not_duplicate(self, workspace):
        source, sink = _source(workspace), _sink()
        batch, offsets = source.poll(20)

        sink.commit(batch, offsets, Fence(STREAM, 1))
        sink.commit(batch, offsets, Fence(STREAM, 1))  # the replay after a crash

        assert len(sink.rows()) == 20

    def test_failure_inside_the_transaction_writes_nothing(self, workspace, monkeypatch):
        source, sink = _source(workspace), _sink()
        first, first_offsets = source.poll(10)
        sink.commit(first, first_offsets, Fence(STREAM, 1))
        source.commit_local(first_offsets)

        second, second_offsets = source.poll(10)

        def boom(point):
            if point == "after_write":
                raise RuntimeError("killed mid-transaction")

        monkeypatch.setattr("dataplatform.streaming.sinks.maybe_crash", boom)
        with pytest.raises(RuntimeError):
            sink.commit(second, second_offsets, Fence(STREAM, 1))

        # Neither the records nor the offsets moved.
        assert len(sink.rows()) == 10
        assert sink.read_offsets().positions == first_offsets.positions

    def test_full_drain_verifies_clean(self, workspace):
        source, sink = _source(workspace), _sink()
        stats = run_stream(source, sink, Fence(STREAM, 1), batch_size=33)

        report = verify(
            workspace["manifest"], sink.rows(), committed_offset=sink.read_offsets().total()
        )
        assert stats.records == workspace["total"]
        assert report.ok, report.summary()


class TestFencing:
    def test_newer_attempt_fences_out_the_old_writer(self, workspace):
        source, sink = _source(workspace), _sink()
        batch, offsets = source.poll(10)
        sink.commit(batch, offsets, Fence(STREAM, 1))

        source.commit_local(offsets)
        newer, newer_offsets = source.poll(10)
        sink.commit(newer, newer_offsets, Fence(STREAM, 2))

        stale, stale_offsets = source.poll(10)
        with pytest.raises(StaleFence):
            sink.commit(stale, stale_offsets, Fence(STREAM, 1))

        assert len(sink.rows()) == 20
        assert sink.committed_attempt() == 2

    def test_same_attempt_may_keep_writing(self, workspace):
        source, sink = _source(workspace), _sink()
        batch, offsets = source.poll(10)
        sink.commit(batch, offsets, Fence(STREAM, 3))
        source.commit_local(offsets)
        more, more_offsets = source.poll(10)

        assert sink.commit(more, more_offsets, Fence(STREAM, 3)) == 10

    def test_runner_stops_when_fenced(self, workspace):
        source, sink = _source(workspace), _sink()
        batch, offsets = source.poll(10)
        sink.commit(batch, offsets, Fence(STREAM, 5))

        stats = run_stream(_source(workspace), _sink(), Fence(STREAM, 4), batch_size=10)

        assert stats.fenced_out
        assert stats.records == 0


class TestKilledProcessRecovers:
    """Kill the runner for real, restart it, and check the sink."""

    def _run(self, workspace, chaos=None, attempt=1, batch_size=25, max_batches=None):
        env = dict(os.environ)
        env["DATABASE_PATH"] = workspace["db"]
        env.pop("DATAPLATFORM_CHAOS", None)
        if chaos:
            env["DATAPLATFORM_CHAOS"] = chaos
        return subprocess.run(
            [
                sys.executable, "-m", "dataplatform.streaming.runner",
                "--stream", workspace["dir"],
                "--table", TABLE,
                "--batch-size", str(batch_size),
                "--attempt", str(attempt),
            ] + (["--max-batches", str(max_batches)] if max_batches else []),
            env=env,
            capture_output=True,
            text=True,
        )

    def _verify(self, workspace):
        sink = _sink()
        return verify(
            workspace["manifest"], sink.rows(), committed_offset=sink.read_offsets().total()
        )

    def test_kill_inside_the_transaction_then_restart(self, workspace):
        killed = self._run(workspace, chaos="after_write:2")
        assert killed.returncode == CRASH_EXIT_CODE, killed.stderr

        restarted = self._run(workspace)
        assert restarted.returncode == 0, restarted.stderr

        report = self._verify(workspace)
        assert report.duplicate_rows == 0
        assert report.missing_total == 0
        assert report.ok, report.summary()

    def test_kill_right_after_commit_then_restart(self, workspace):
        killed = self._run(workspace, chaos="after_commit:2")
        assert killed.returncode == CRASH_EXIT_CODE, killed.stderr

        restarted = self._run(workspace)
        assert restarted.returncode == 0, restarted.stderr

        report = self._verify(workspace)
        assert report.duplicate_rows == 0
        assert report.missing_total == 0

    def test_kill_between_poll_and_commit_then_restart(self, workspace):
        killed = self._run(workspace, chaos="after_poll:3")
        assert killed.returncode == CRASH_EXIT_CODE, killed.stderr

        restarted = self._run(workspace)
        assert restarted.returncode == 0, restarted.stderr

        assert self._verify(workspace).ok

    def test_repeated_kills_still_converge(self, workspace):
        for point in ("after_write:1", "after_commit:1", "before_poll:2", "after_poll:1"):
            result = self._run(workspace, chaos=point, batch_size=17)
            assert result.returncode == CRASH_EXIT_CODE, (point, result.stderr)

        assert self._run(workspace, batch_size=17).returncode == 0
        report = self._verify(workspace)
        assert report.ok, report.summary()

    def test_a_fenced_out_process_writes_nothing_new(self, workspace):
        # Attempt 7 takes the stream partway, so there is still work left for
        # the stale writer to try to commit.
        assert self._run(workspace, attempt=7, batch_size=25, max_batches=2).returncode == 0
        before = len(_sink().rows())
        assert before < workspace["total"]

        stale = self._run(workspace, attempt=6)

        assert stale.returncode == 2
        assert len(_sink().rows()) == before

    def test_a_fenced_out_process_with_no_backlog_still_refuses(self, workspace):
        # The whole stream is drained, so the stale writer has nothing to
        # commit -- it must still report that it was superseded rather than
        # exiting as though it had done its job.
        assert self._run(workspace, attempt=7).returncode == 0

        stale = self._run(workspace, attempt=6)

        assert stale.returncode == 2
