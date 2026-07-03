"""Tests for RBAC enforcement on API endpoints and admin user management."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from dataplatform.core import api
from dataplatform.core.api import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Fresh DB + reset env admin for every test."""
    db_file = str(tmp_path / "rbac_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    monkeypatch.setenv("DATAPLATFORM_USERNAME", "admin")
    monkeypatch.setenv("DATAPLATFORM_PASSWORD", "admin")
    import dataplatform.core.database as db_module
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    yield
    db_module._initialized = False


@pytest.fixture(autouse=True)
def suppress_scheduler(monkeypatch):
    mock_scheduler = MagicMock()
    mock_scheduler.start = MagicMock()
    monkeypatch.setattr(api, "get_scheduler", lambda: mock_scheduler)


@pytest.fixture
def client():
    return TestClient(app)


def _set_role(monkeypatch, role: str, username: str = "testuser"):
    """Patch _get_current_user to return a user with the given role."""
    monkeypatch.setattr(
        api,
        "_get_current_user",
        lambda request: {"username": username, "role": role, "team": None},
    )


# ---------------------------------------------------------------------------
# /me endpoint
# ---------------------------------------------------------------------------

class TestMeEndpoint:
    def test_returns_user_info(self, client, monkeypatch):
        _set_role(monkeypatch, "editor", "alice")
        resp = client.get("/me")
        assert resp.status_code == 200
        body = resp.json()
        assert body["username"] == "alice"
        assert body["role"] == "editor"

    def test_unauthenticated_returns_401(self, client, monkeypatch):
        monkeypatch.setattr(api, "_get_current_user", lambda r: None)
        resp = client.get("/me")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Write endpoints require editor+
# ---------------------------------------------------------------------------

class TestEditorPermissions:
    @pytest.mark.parametrize("method,path,body", [
        ("POST", "/generate-pipeline", {"input_text": "load from postgres"}),
        ("POST", "/save-pipeline", {"yaml_content": "x: y", "filename": "p.yaml"}),
        ("POST", "/validate", {"config_path": "pipelines/x.yaml"}),
        ("POST", "/run", {"config_path": "pipelines/x.yaml"}),
        ("POST", "/run/sync", {"config_path": "pipelines/x.yaml"}),
        ("POST", "/schedule", {"config_path": "pipelines/x.yaml"}),
    ])
    def test_viewer_gets_403(self, client, monkeypatch, method, path, body):
        _set_role(monkeypatch, "viewer")
        resp = client.request(method, path, json=body)
        assert resp.status_code == 403

    def test_editor_can_generate(self, client, monkeypatch):
        _set_role(monkeypatch, "editor")
        with patch("dataplatform.core.api.generate_pipeline_yaml_from_text") as mock_gen:
            mock_gen.return_value = {
                "yaml_content": "pipeline_name: test\ntasks: []",
                "parsed_config": {},
                "warnings": [],
                "detected_language": "en",
            }
            resp = client.post("/generate-pipeline", json={"input_text": "load from postgres"})
        assert resp.status_code == 200

    def test_editor_can_validate(self, client, monkeypatch):
        _set_role(monkeypatch, "editor")
        from dataplatform.core.config import PipelineConfig, Task
        config = PipelineConfig(
            pipeline_name="test", tasks=[Task(name="t1", type="executor", plugin="python")]
        )
        with patch("dataplatform.core.api.load_config", return_value=config), \
             patch("dataplatform.core.executor.TaskExecutor.load_plugin", return_value=MagicMock()):
            resp = client.post("/validate", json={"config_path": "p.yaml"})
        assert resp.status_code == 200
        assert resp.json()["is_valid"] is True


# ---------------------------------------------------------------------------
# Admin endpoints require admin role
# ---------------------------------------------------------------------------

class TestAdminUserEndpoints:
    def test_viewer_cannot_list_users(self, client, monkeypatch):
        _set_role(monkeypatch, "viewer")
        resp = client.get("/admin/users")
        assert resp.status_code == 403

    def test_editor_cannot_list_users(self, client, monkeypatch):
        _set_role(monkeypatch, "editor")
        resp = client.get("/admin/users")
        assert resp.status_code == 403

    def test_admin_can_list_users(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        resp = client.get("/admin/users")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_admin_can_create_user(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        resp = client.post(
            "/admin/users",
            json={"username": "newuser", "password": "pass123", "role": "viewer"},
        )
        assert resp.status_code == 201
        assert "created" in resp.json()["message"]

    def test_duplicate_user_returns_409(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        client.post("/admin/users", json={"username": "dup", "password": "x", "role": "viewer"})
        resp = client.post("/admin/users", json={"username": "dup", "password": "x", "role": "viewer"})
        assert resp.status_code == 409

    def test_invalid_role_returns_400(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        resp = client.post(
            "/admin/users",
            json={"username": "bad", "password": "x", "role": "superuser"},
        )
        assert resp.status_code == 400

    def test_admin_can_update_role(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        client.post("/admin/users", json={"username": "tom", "password": "x", "role": "viewer"})
        resp = client.patch("/admin/users/tom/role", json={"role": "editor"})
        assert resp.status_code == 200

    def test_update_role_for_nonexistent_user_is_404(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        resp = client.patch("/admin/users/nobody/role", json={"role": "editor"})
        assert resp.status_code == 404

    def test_admin_can_delete_user(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        client.post("/admin/users", json={"username": "todelete", "password": "x", "role": "viewer"})
        resp = client.delete("/admin/users/todelete")
        assert resp.status_code == 200

    def test_delete_nonexistent_user_is_404(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        resp = client.delete("/admin/users/nobody")
        assert resp.status_code == 404

    def test_cannot_delete_env_admin(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        resp = client.delete("/admin/users/admin")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Login / session
# ---------------------------------------------------------------------------

class TestLogin:
    def test_valid_credentials_return_200(self, client):
        resp = client.post("/login", json={"username": "admin", "password": "admin"})
        assert resp.status_code == 200
        assert resp.json()["role"] == "admin"

    def test_invalid_credentials_return_401(self, client):
        resp = client.post("/login", json={"username": "admin", "password": "wrong"})
        assert resp.status_code == 401

    def test_db_user_can_login(self, client, monkeypatch):
        _set_role(monkeypatch, "admin")
        client.post("/admin/users", json={"username": "newbie", "password": "pass!", "role": "editor"})
        monkeypatch.undo()  # restore real _get_current_user
        resp = client.post("/login", json={"username": "newbie", "password": "pass!"})
        assert resp.status_code == 200
        assert resp.json()["role"] == "editor"
