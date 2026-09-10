"""Tests for the deployment control plane API."""
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from dataplatform.core import api
from dataplatform.core.api import app


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "deployments_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    monkeypatch.setenv("DATAPLATFORM_USERNAME", "admin")
    monkeypatch.setenv("DATAPLATFORM_PASSWORD", "admin")
    monkeypatch.setenv("DATAPLATFORM_OBSERVABILITY_AUTO_COLLECT", "false")
    monkeypatch.setenv("PIPELINES_PATH", str(tmp_path))
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    import dataplatform.core.database as db_module
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module._engine = None
    yield
    db_module._initialized = False
    db_module._engine = None


@pytest.fixture(autouse=True)
def suppress_scheduler(monkeypatch):
    mock_scheduler = MagicMock()
    mock_scheduler.start = MagicMock()
    monkeypatch.setattr(api, "get_scheduler", lambda: mock_scheduler)


@pytest.fixture
def client():
    return TestClient(app)


def _set_role(monkeypatch, role: str, username: str = "deploy-user"):
    monkeypatch.setattr(
        api,
        "_get_current_user",
        lambda request: {"username": username, "role": role, "team": None},
    )


def _pipeline_file(tmp_path: Path, name: str = "deploy_pipe") -> Path:
    path = tmp_path / f"{name}.yaml"
    path.write_text(
        f"""
pipeline_name: {name}
execution:
  profile: local
  deployment_target: local
tasks:
  - name: ingest
    type: executor
    plugin: python
    execution_layer: ingest
  - name: publish
    type: executor
    plugin: shell
    execution_layer: serve
    depends_on:
      - ingest
""".strip(),
        encoding="utf-8",
    )
    return path


def test_viewer_can_list_but_not_deploy(client, monkeypatch, tmp_path):
    _set_role(monkeypatch, "viewer")
    config_path = _pipeline_file(tmp_path)

    connections_resp = client.get("/deployment-connections")
    assert connections_resp.status_code == 200
    connections = connections_resp.json()["connections"]
    assert any(c["connection_id"] == "local-default" for c in connections)

    list_resp = client.get("/deployments/records")
    assert list_resp.status_code == 200
    assert list_resp.json()["deployments"] == []

    deploy_resp = client.post(
        "/deployments/deploy",
        json={"config_path": str(config_path), "environment_profile": "prod"},
    )
    assert deploy_resp.status_code == 403


def test_editor_can_validate_and_deploy(client, monkeypatch, tmp_path):
    _set_role(monkeypatch, "editor", username="alice")
    config_path = _pipeline_file(tmp_path)
    monkeypatch.setattr(api.TaskExecutor, "load_plugin", lambda self, plugin, plugin_type: object())

    validate_resp = client.post(
        "/deployments/validate",
        json={
            "config_path": str(config_path),
            "environment_profile": "prod",
            "target_id": "kubernetes",
        },
    )
    assert validate_resp.status_code == 200
    assert validate_resp.json()["validation"]["is_valid"] is True

    deploy_resp = client.post(
        "/deployments/deploy",
        json={
            "config_path": str(config_path),
            "environment_profile": "prod",
            "target_id": "kubernetes",
            "notes": "release candidate",
        },
    )
    assert deploy_resp.status_code == 201
    deployment = deploy_resp.json()["deployment"]
    assert deployment["status"] == "deployed"
    assert deployment["active"] is True
    assert deployment["actor"] == "alice"
    assert deployment["target_id"] == "kubernetes"
    assert deployment["version_id"]

    active_resp = client.get("/deployments/records?active_only=true")
    assert active_resp.status_code == 200
    assert active_resp.json()["total"] == 1


def test_editor_can_create_connection_and_deploy_to_it(client, monkeypatch, tmp_path):
    _set_role(monkeypatch, "editor", username="alice")
    config_path = _pipeline_file(tmp_path, name="cluster_pipe")
    monkeypatch.setattr(api.TaskExecutor, "load_plugin", lambda self, plugin, plugin_type: object())

    create_resp = client.post(
        "/deployment-connections",
        json={
            "connection_id": "eks-prod-a",
            "name": "EKS Prod A",
            "target_id": "kubernetes",
            "provider": "eks",
            "endpoint": "https://eks.example.com",
            "region": "us-east-1",
            "namespace": "data-platform",
            "status": "connected",
            "credentials_ref": "env:AWS_PROFILE",
        },
    )
    assert create_resp.status_code == 201
    connection = create_resp.json()["connection"]
    assert connection["target_id"] == "kubernetes"
    assert connection["status"] == "connected"

    deploy_resp = client.post(
        "/deployments/deploy",
        json={
            "config_path": str(config_path),
            "environment_profile": "prod",
            "target_id": "kubernetes",
            "connection_id": "eks-prod-a",
        },
    )
    assert deploy_resp.status_code == 201
    deployment = deploy_resp.json()["deployment"]
    assert deployment["connection_id"] == "eks-prod-a"
    assert deployment["connection_name"] == "EKS Prod A"
    assert deployment["connection_provider"] == "eks"
    assert deployment["manifest"]["connection"]["connection_id"] == "eks-prod-a"

    filtered = client.get("/deployments/records?connection_id=eks-prod-a")
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 1


def test_connection_target_mismatch_is_rejected(client, monkeypatch, tmp_path):
    _set_role(monkeypatch, "editor")
    config_path = _pipeline_file(tmp_path)
    monkeypatch.setattr(api.TaskExecutor, "load_plugin", lambda self, plugin, plugin_type: object())

    client.post(
        "/deployment-connections",
        json={
            "connection_id": "gcp-prod",
            "name": "GCP Prod",
            "target_id": "cloud",
            "provider": "gcp",
            "status": "connected",
        },
    )
    resp = client.post(
        "/deployments/validate",
        json={
            "config_path": str(config_path),
            "environment_profile": "prod",
            "target_id": "kubernetes",
            "connection_id": "gcp-prod",
        },
    )
    assert resp.status_code == 400
    assert "belongs to target" in resp.json()["detail"]


def test_failed_validation_creates_failed_deployment_record(client, monkeypatch, tmp_path):
    _set_role(monkeypatch, "editor")
    config_path = _pipeline_file(tmp_path)

    def _fail_plugin(self, plugin, plugin_type):
        raise ValueError("plugin not available")

    monkeypatch.setattr(api.TaskExecutor, "load_plugin", _fail_plugin)
    deploy_resp = client.post(
        "/deployments/deploy",
        json={"config_path": str(config_path), "environment_profile": "dev"},
    )
    assert deploy_resp.status_code == 400
    assert deploy_resp.json()["detail"]["validation"]["is_valid"] is False

    failed_resp = client.get("/deployments/records?status=failed")
    assert failed_resp.status_code == 200
    failed = failed_resp.json()["deployments"]
    assert len(failed) == 1
    assert failed[0]["status"] == "failed"
    assert failed[0]["active"] is False


def test_rollback_restores_previous_deployment(client, monkeypatch, tmp_path):
    _set_role(monkeypatch, "editor")
    config_path = _pipeline_file(tmp_path, name="rollback_pipe")
    monkeypatch.setattr(api.TaskExecutor, "load_plugin", lambda self, plugin, plugin_type: object())

    first = client.post(
        "/deployments/deploy",
        json={"config_path": str(config_path), "environment_profile": "prod"},
    ).json()["deployment"]
    second = client.post(
        "/deployments/deploy",
        json={"config_path": str(config_path), "environment_profile": "prod"},
    ).json()["deployment"]

    assert first["active"] is True
    assert second["active"] is True

    rollback_resp = client.post(
        f"/deployments/records/{second['deployment_id']}/rollback",
        json={"reason": "test rollback"},
    )
    assert rollback_resp.status_code == 200
    body = rollback_resp.json()
    assert body["rolled_back"]["status"] == "rolled_back"
    assert body["restored"]["deployment_id"] == first["deployment_id"]
    assert body["restored"]["active"] is True

    active_resp = client.get("/deployments/records?active_only=true")
    active = active_resp.json()["deployments"]
    assert len(active) == 1
    assert active[0]["deployment_id"] == first["deployment_id"]
