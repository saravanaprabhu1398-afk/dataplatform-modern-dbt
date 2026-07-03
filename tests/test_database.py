"""Tests for the SQLite-backed metadata store (database.py)."""
import os
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Point DATABASE_PATH at a temp file so each test gets a fresh DB."""
    db_file = str(tmp_path / "test_platform.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    # Reset the _initialized flag so init_db() always runs fresh
    import dataplatform.core.database as db_module
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module._engine = None  # force engine recreation with new path
    yield
    db_module._initialized = False
    db_module._engine = None


import dataplatform.core.database as db


class TestInitDb:
    def test_creates_pipeline_runs_table(self, tmp_path):
        db.init_db()
        from sqlalchemy import inspect as _inspect
        tables = _inspect(db._get_engine()).get_table_names()
        assert "pipeline_runs" in tables

    def test_creates_users_table(self):
        db.init_db()
        from sqlalchemy import inspect as _inspect
        tables = _inspect(db._get_engine()).get_table_names()
        assert "users" in tables

    def test_idempotent(self):
        db.init_db()
        db.init_db()  # second call should be a no-op


class TestRunHistory:
    def setup_method(self):
        db.init_db()

    def test_save_and_retrieve_latest(self):
        db.save_run_status("pipe_a", "run-1", "completed", "done", {"x": 1})
        latest = db.get_latest_run("pipe_a")
        assert latest is not None
        assert latest["status"] == "completed"
        assert latest["run_id"] == "run-1"
        assert latest["details"]["x"] == 1

    def test_latest_returns_none_for_unknown_pipeline(self):
        assert db.get_latest_run("no_such_pipeline") is None

    def test_multiple_statuses_same_run(self):
        db.save_run_status("pipe_b", "run-2", "started", "starting")
        db.save_run_status("pipe_b", "run-2", "running", "in progress")
        db.save_run_status("pipe_b", "run-2", "completed", "done")
        latest = db.get_latest_run("pipe_b")
        assert latest["status"] == "completed"

    def test_history_returns_last_n_runs(self):
        for i in range(7):
            db.save_run_status("pipe_c", f"run-{i}", "completed", f"run {i}")
        history = db.get_run_history("pipe_c", limit=5)
        assert len(history) == 5

    def test_history_empty_for_unknown_pipeline(self):
        assert db.get_run_history("no_pipe") == []

    def test_details_deserialized_from_json(self):
        db.save_run_status("pipe_d", "run-x", "failed", "oops", {"tasks": ["a", "b"]})
        latest = db.get_latest_run("pipe_d")
        assert isinstance(latest["details"], dict)
        assert latest["details"]["tasks"] == ["a", "b"]

    def test_get_all_pipeline_names(self):
        db.save_run_status("alpha", "r1", "completed", "ok")
        db.save_run_status("beta", "r2", "failed", "err")
        names = db.get_all_pipeline_names()
        assert "alpha" in names
        assert "beta" in names


class TestUserCrud:
    def setup_method(self):
        db.init_db()

    def test_create_and_get_user(self):
        ok = db.create_user("alice", "hash:abc123", role="editor", team="data")
        assert ok is True
        user = db.get_user("alice")
        assert user is not None
        assert user["username"] == "alice"
        assert user["role"] == "editor"
        assert user["team"] == "data"

    def test_create_duplicate_returns_false(self):
        db.create_user("bob", "hash:xyz", role="viewer")
        ok = db.create_user("bob", "hash:xyz", role="viewer")
        assert ok is False

    def test_get_nonexistent_user_returns_none(self):
        assert db.get_user("nobody") is None

    def test_list_users(self):
        db.create_user("carol", "hash:1", role="admin")
        db.create_user("dave", "hash:2", role="viewer")
        users = db.list_users()
        names = [u["username"] for u in users]
        assert "carol" in names
        assert "dave" in names
        # password_hash must NOT be in the response
        for u in users:
            assert "password_hash" not in u

    def test_update_role(self):
        db.create_user("eve", "hash:3", role="viewer")
        ok = db.update_user_role("eve", "editor")
        assert ok is True
        assert db.get_user("eve")["role"] == "editor"

    def test_update_role_nonexistent_returns_false(self):
        assert db.update_user_role("nobody", "admin") is False

    def test_delete_user(self):
        db.create_user("frank", "hash:4", role="viewer")
        ok = db.delete_user("frank")
        assert ok is True
        assert db.get_user("frank") is None

    def test_delete_nonexistent_returns_false(self):
        assert db.delete_user("nobody") is False


class TestPipelineQueue:
    def setup_method(self):
        db.init_db()

    def test_enqueue_creates_queued_entry(self):
        db.enqueue_run("run-q1", "pipe_x", "pipelines/pipe_x.yaml", actor="alice")
        runs = db.get_queue_runs()
        assert any(r["run_id"] == "run-q1" and r["status"] == "queued" for r in runs)

    def test_set_running_updates_status_and_started_at(self):
        db.enqueue_run("run-q2", "pipe_y", "pipelines/pipe_y.yaml")
        db.set_run_status_in_queue("run-q2", "running")
        runs = db.get_queue_runs(status="running")
        r = next(x for x in runs if x["run_id"] == "run-q2")
        assert r["status"] == "running"
        assert r["started_at"] is not None

    def test_set_completed_updates_status_and_completed_at(self):
        db.enqueue_run("run-q3", "pipe_z", "pipelines/pipe_z.yaml")
        db.set_run_status_in_queue("run-q3", "completed")
        runs = db.get_queue_runs(status="completed")
        r = next(x for x in runs if x["run_id"] == "run-q3")
        assert r["status"] == "completed"
        assert r["completed_at"] is not None

    def test_set_failed_stores_error(self):
        db.enqueue_run("run-q4", "pipe_a", "pipelines/pipe_a.yaml")
        db.set_run_status_in_queue("run-q4", "failed", error="task crashed")
        runs = db.get_queue_runs(status="failed")
        r = next(x for x in runs if x["run_id"] == "run-q4")
        assert r["error"] == "task crashed"

    def test_recover_orphaned_runs_marks_as_failed(self):
        db.enqueue_run("run-orphan1", "pipe_b", "p.yaml")
        db.enqueue_run("run-orphan2", "pipe_c", "p.yaml")
        db.set_run_status_in_queue("run-orphan2", "running")
        recovered = db.recover_orphaned_runs()
        assert recovered == 2
        runs = db.get_queue_runs(status="failed")
        ids = [r["run_id"] for r in runs]
        assert "run-orphan1" in ids
        assert "run-orphan2" in ids
        for r in runs:
            if r["run_id"] in ("run-orphan1", "run-orphan2"):
                assert r["error"] == "Server restarted"

    def test_get_queue_runs_filtered_by_status(self):
        db.enqueue_run("run-filt1", "pipe_d", "p.yaml")
        db.enqueue_run("run-filt2", "pipe_e", "p.yaml")
        db.set_run_status_in_queue("run-filt2", "running")
        queued = db.get_queue_runs(status="queued")
        running = db.get_queue_runs(status="running")
        assert any(r["run_id"] == "run-filt1" for r in queued)
        assert any(r["run_id"] == "run-filt2" for r in running)

    def test_recover_skips_completed_and_failed(self):
        db.enqueue_run("run-done1", "pipe_f", "p.yaml")
        db.set_run_status_in_queue("run-done1", "completed")
        db.enqueue_run("run-done2", "pipe_g", "p.yaml")
        db.set_run_status_in_queue("run-done2", "failed", error="already failed")
        recovered = db.recover_orphaned_runs()
        assert recovered == 0


class TestSchedulerSchedules:
    def setup_method(self):
        db.init_db()

    def test_save_and_list_schedule(self):
        db.save_schedule("daily_etl", "pipelines/daily_etl.yaml", {"hour": "6", "minute": "0"})
        rows = db.list_schedules()
        assert any(r["pipeline_name"] == "daily_etl" for r in rows)
        row = next(r for r in rows if r["pipeline_name"] == "daily_etl")
        assert row["config_path"] == "pipelines/daily_etl.yaml"
        assert row["schedule"]["hour"] == "6"

    def test_upsert_updates_existing(self):
        db.save_schedule("pipe_x", "p.yaml", {"hour": "1"})
        db.save_schedule("pipe_x", "p.yaml", {"hour": "3"})
        rows = [r for r in db.list_schedules() if r["pipeline_name"] == "pipe_x"]
        assert len(rows) == 1
        assert rows[0]["schedule"]["hour"] == "3"

    def test_delete_schedule(self):
        db.save_schedule("pipe_y", "p.yaml", {"hour": "2"})
        ok = db.delete_schedule("pipe_y")
        assert ok is True
        assert all(r["pipeline_name"] != "pipe_y" for r in db.list_schedules())

    def test_delete_nonexistent_returns_false(self):
        assert db.delete_schedule("no_such_pipe") is False

    def test_list_empty_returns_empty_list(self):
        assert db.list_schedules() == []
