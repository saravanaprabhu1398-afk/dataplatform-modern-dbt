"""Lease and claim behaviour on PostgreSQL.

The SQLite path and the PostgreSQL path take different claim strategies --
candidate-walking versus ``FOR UPDATE SKIP LOCKED`` -- so the guarantee has to
be tested on both.  These tests are skipped unless a database is provided:

    docker run -d --rm -e POSTGRES_PASSWORD=dp -e POSTGRES_USER=dp \\
        -e POSTGRES_DB=dp -p 55432:5432 postgres:16-alpine

    DATAPLATFORM_TEST_POSTGRES_URL=postgresql+psycopg2://dp:dp@localhost:55432/dp \\
        pytest tests/test_queue_leases_postgres.py
"""
import os
import threading
from datetime import datetime, timedelta

import pytest

POSTGRES_URL = os.environ.get("DATAPLATFORM_TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    not POSTGRES_URL, reason="set DATAPLATFORM_TEST_POSTGRES_URL to run PostgreSQL tests"
)


@pytest.fixture()
def pg_db(monkeypatch):
    """Point the metadata store at PostgreSQL and start from a clean queue."""
    monkeypatch.setenv("POSTGRES_URL", POSTGRES_URL)
    import dataplatform.core.database as db

    db._initialized = False
    db._engine = None
    db.init_db()
    with db._get_conn() as conn:
        conn.execute(db.text("DELETE FROM pipeline_queue"))
        conn.commit()

    yield db

    with db._get_conn() as conn:
        conn.execute(db.text("DELETE FROM pipeline_queue"))
        conn.commit()
    db._initialized = False
    db._engine = None


def _expire_lease(db, run_id, seconds_ago=60):
    past = (datetime.utcnow() - timedelta(seconds=seconds_ago)).isoformat() + "Z"
    with db._get_conn() as conn:
        conn.execute(
            db.text("UPDATE pipeline_queue SET lease_expires_at = :p WHERE run_id = :r"),
            {"p": past, "r": run_id},
        )
        conn.commit()


class TestPostgresClaim:
    def test_uses_the_skip_locked_path(self, pg_db):
        assert pg_db._is_postgres()

    def test_claim_stamps_lease_and_attempt(self, pg_db):
        pg_db.enqueue_run("pg-1", "pipe", "p.yaml")

        claimed = pg_db.claim_next_queued_run(worker_id="w1", lease_seconds=30)

        assert claimed["run_id"] == "pg-1"
        assert claimed["worker_id"] == "w1"
        assert claimed["attempt"] == 1
        assert claimed["lease_expires_at"] > pg_db._now_iso()

    def test_claim_returns_none_when_empty(self, pg_db):
        assert pg_db.claim_next_queued_run(worker_id="w1") is None

    def test_concurrent_claims_take_different_rows(self, pg_db):
        # The point of SKIP LOCKED: 12 workers against 12 runs must produce 12
        # distinct claims, not a pile of losers returning None.
        total = 12
        for index in range(total):
            pg_db.enqueue_run("pg-c{0}".format(index), "pipe", "p.yaml")

        claims = []
        misses = []
        lock = threading.Lock()
        barrier = threading.Barrier(total)

        def claim(worker):
            barrier.wait()
            result = pg_db.claim_next_queued_run(worker_id=worker)
            with lock:
                (claims if result else misses).append(result)

        threads = [
            threading.Thread(target=claim, args=("w{0}".format(i),)) for i in range(total)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert misses == [], "no worker should come away empty while work exists"
        assert len({c["run_id"] for c in claims}) == total
        assert all(c["attempt"] == 1 for c in claims)

    def test_reaper_requeues_and_fences(self, pg_db):
        pg_db.enqueue_run("pg-2", "pipe", "p.yaml")
        pg_db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease(pg_db, "pg-2")

        assert pg_db.reap_expired_leases() == {"requeued": 1, "dead_lettered": 0}

        second = pg_db.claim_next_queued_run(worker_id="w2")
        assert second["attempt"] == 2
        assert pg_db.complete_run_with_lease("pg-2", "w1", 1, "completed") is False
        assert pg_db.get_queue_run("pg-2")["status"] == "running"

    def test_recovery_skips_externally_owned_runs(self, pg_db):
        pg_db.enqueue_run("pg-3", "pipe", "p.yaml")
        pg_db.claim_next_queued_run(worker_id="w1", lease_seconds=300)

        assert pg_db.recover_orphaned_runs(stale_after_seconds=0) == 0
        assert pg_db.get_queue_run("pg-3")["status"] == "running"


class TestPostgresTransactionalSink:
    """The sink's fence takes a row lock on PostgreSQL; SQLite never runs it."""

    def _stream(self, tmp_path):
        from dataplatform.streaming.generator import GeneratorConfig, generate

        stream = generate(GeneratorConfig(keys=6, events_per_key=20, seed=5))
        path = tmp_path / "events.jsonl"
        stream.write(str(path), str(tmp_path / "manifest.json"))
        return stream, str(path)

    def _wire(self, path):
        from dataplatform.streaming.sinks import SqlTransactionalSink
        from dataplatform.streaming.sources import JsonlSource

        return JsonlSource(path, partitions=4), SqlTransactionalSink(
            table="pg_events_sink", stream="pg-events"
        )

    @pytest.fixture(autouse=True)
    def clean_sink(self, pg_db):
        with pg_db._get_conn() as conn:
            conn.execute(pg_db.text("DROP TABLE IF EXISTS pg_events_sink"))
            conn.execute(pg_db.text("DELETE FROM stream_state WHERE stream = 'pg-events'"))
            conn.commit()
        yield
        with pg_db._get_conn() as conn:
            conn.execute(pg_db.text("DROP TABLE IF EXISTS pg_events_sink"))
            conn.execute(pg_db.text("DELETE FROM stream_state WHERE stream = 'pg-events'"))
            conn.commit()

    def test_drain_is_exactly_once(self, pg_db, tmp_path):
        from dataplatform.plugins.base import Fence
        from dataplatform.streaming.runner import run_stream
        from dataplatform.streaming.verifier import verify

        stream, path = self._stream(tmp_path)
        source, sink = self._wire(path)

        stats = run_stream(source, sink, Fence("pg-events", 1), batch_size=25)
        report = verify(
            stream.manifest, sink.rows(), committed_offset=sink.read_offsets().total()
        )

        assert stats.records == stream.manifest.total_unique
        assert report.ok, report.summary()

    def test_stale_writer_is_fenced_under_row_lock(self, pg_db, tmp_path):
        from dataplatform.plugins.base import Fence, StaleFence

        stream, path = self._stream(tmp_path)
        source, sink = self._wire(path)

        batch, offsets = source.poll(10)
        sink.commit(batch, offsets, Fence("pg-events", 4))
        source.commit_local(offsets)
        newer, newer_offsets = source.poll(10)
        sink.commit(newer, newer_offsets, Fence("pg-events", 5))

        source.commit_local(newer_offsets)
        stale, stale_offsets = source.poll(10)
        with pytest.raises(StaleFence):
            sink.commit(stale, stale_offsets, Fence("pg-events", 4))

        assert len(sink.rows()) == 20
        assert sink.committed_attempt() == 5

    def test_replay_is_idempotent(self, pg_db, tmp_path):
        from dataplatform.plugins.base import Fence

        stream, path = self._stream(tmp_path)
        source, sink = self._wire(path)
        batch, offsets = source.poll(30)

        sink.commit(batch, offsets, Fence("pg-events", 1))
        sink.commit(batch, offsets, Fence("pg-events", 1))

        assert len(sink.rows()) == 30


class TestPostgresWindowing:
    """Window aggregates use arithmetic in ON CONFLICT DO UPDATE."""

    @pytest.fixture(autouse=True)
    def clean(self, pg_db):
        for table in ("pg_win_sink",):
            with pg_db._get_conn() as conn:
                conn.execute(pg_db.text("DROP TABLE IF EXISTS {0}".format(table)))
                conn.commit()
        with pg_db._get_conn() as conn:
            for table in ("stream_state", "stream_windows", "window_corrections", "late_events"):
                conn.execute(pg_db.text("DELETE FROM {0} WHERE stream = 'pg-win'".format(table)))
            conn.commit()
        yield

    def test_accounting_closes_on_postgres(self, pg_db, tmp_path):
        from dataplatform.plugins.base import Fence
        from dataplatform.streaming.generator import GeneratorConfig, generate
        from dataplatform.streaming.runner import run_stream
        from dataplatform.streaming.sinks import SqlTransactionalSink
        from dataplatform.streaming.sources import JsonlSource, partition_for
        from dataplatform.streaming.verifier import verify, verify_window_accounting
        from dataplatform.streaming.windows import WindowPolicy

        stream = generate(
            GeneratorConfig(
                keys=6, events_per_key=300, seed=13, event_interval_seconds=144,
                late_fraction=0.1, late_delay_seconds=5400,
                very_late_fraction=0.03, very_late_delay_seconds=172800,
            )
        )
        stream.write(str(tmp_path / "events.jsonl"), str(tmp_path / "manifest.json"))

        source = JsonlSource(str(tmp_path / "events.jsonl"), partitions=4)
        sink = SqlTransactionalSink(
            table="pg_win_sink", stream="pg-win", windowing=WindowPolicy(),
            partition_of=lambda record: partition_for(str(record["key"]), 4),
        )
        run_stream(source, sink, Fence("pg-win", 1), batch_size=200)

        rows = sink.rows()
        accounting = verify_window_accounting(
            stream.manifest, sink.window_rows(), sink.late_rows(), sink_rows=rows
        )

        assert verify(stream.manifest, rows).ok
        assert accounting.ok, accounting.summary()
        assert accounting.late_counted > 0 and accounting.late_side_output > 0
        assert any(row["revision"] > 0 for row in sink.window_rows())
        assert any(row["closed_at"] for row in sink.window_rows())
