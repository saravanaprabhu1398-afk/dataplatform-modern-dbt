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
    yield
    db_module._initialized = False


import dataplatform.core.database as db


class TestInitDb:
    def test_creates_pipeline_runs_table(self, tmp_path):
        db.init_db()
        conn = db._get_conn()
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
        assert "pipeline_runs" in tables

    def test_creates_users_table(self):
        db.init_db()
        conn = db._get_conn()
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
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
