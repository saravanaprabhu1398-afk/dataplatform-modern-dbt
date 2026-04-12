"""Tests for the POST /validate pipeline validation endpoint."""
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from dataplatform.core import api
from dataplatform.core.api import app
from dataplatform.core.config import PipelineConfig, Task


# Bypass authentication and grant admin role for all validation endpoint tests
@pytest.fixture(autouse=True)
def bypass_auth(monkeypatch):
    monkeypatch.setattr(api, "_is_authenticated", lambda request: True)
    monkeypatch.setattr(
        api,
        "_get_current_user",
        lambda request: {"username": "admin", "role": "admin", "team": None},
    )


# Suppress scheduler startup during tests
@pytest.fixture(autouse=True)
def suppress_scheduler(monkeypatch):
    mock_scheduler = MagicMock()
    mock_scheduler.start = MagicMock()
    monkeypatch.setattr(api, "get_scheduler", lambda: mock_scheduler)


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


def _make_config(tasks=None):
    if tasks is None:
        tasks = [Task(name="step1", type="executor", plugin="python")]
    return PipelineConfig(pipeline_name="test_pipeline", tasks=tasks)


class TestValidateEndpointHappyPath:
    def test_valid_pipeline_returns_is_valid_true(self, client):
        config = _make_config()
        with patch("dataplatform.core.api.load_config", return_value=config), \
             patch("dataplatform.core.executor.TaskExecutor.load_plugin", return_value=MagicMock()):
            response = client.post("/validate", json={"config_path": "pipelines/test.yaml"})
        assert response.status_code == 200
        body = response.json()
        assert body["is_valid"] is True
        assert body["pipeline_name"] == "test_pipeline"
        assert body["task_count"] == 1
        assert body["errors"] == []

    def test_task_results_per_task(self, client):
        tasks = [
            Task(name="t1", type="executor", plugin="python"),
            Task(name="t2", type="executor", plugin="shell", depends_on=["t1"]),
        ]
        config = _make_config(tasks)
        with patch("dataplatform.core.api.load_config", return_value=config), \
             patch("dataplatform.core.executor.TaskExecutor.load_plugin", return_value=MagicMock()):
            response = client.post("/validate", json={"config_path": "pipelines/test.yaml"})
        body = response.json()
        assert body["task_count"] == 2
        names = [r["task_name"] for r in body["task_results"]]
        assert "t1" in names
        assert "t2" in names


class TestValidateEndpointErrorCases:
    def test_config_not_found_returns_404(self, client):
        with patch("dataplatform.core.api.load_config", side_effect=FileNotFoundError("not found")):
            response = client.post("/validate", json={"config_path": "missing.yaml"})
        assert response.status_code == 404

    def test_invalid_config_returns_400(self, client):
        with patch("dataplatform.core.api.load_config", side_effect=ValueError("bad yaml")):
            response = client.post("/validate", json={"config_path": "bad.yaml"})
        assert response.status_code == 400

    def test_unloadable_plugin_marks_task_invalid(self, client):
        config = _make_config()
        with patch("dataplatform.core.api.load_config", return_value=config), \
             patch("dataplatform.core.executor.TaskExecutor.load_plugin",
                   side_effect=ValueError("plugin not found")):
            response = client.post("/validate", json={"config_path": "pipelines/test.yaml"})
        body = response.json()
        assert body["is_valid"] is False
        assert len(body["errors"]) > 0
        assert body["task_results"][0]["plugin_loadable"] is False
        assert body["task_results"][0]["error"] is not None

    def test_dag_cycle_detected_in_errors(self, client):
        tasks = [
            Task(name="a", type="executor", plugin="python", depends_on=["b"]),
            Task(name="b", type="executor", plugin="python", depends_on=["a"]),
        ]
        bad_config = MagicMock()
        bad_config.pipeline_name = "cyclic"
        bad_config.tasks = tasks
        with patch("dataplatform.core.api.load_config", return_value=bad_config), \
             patch("dataplatform.core.api.DAGBuilder") as mock_dag_cls, \
             patch("dataplatform.core.executor.TaskExecutor.load_plugin", return_value=MagicMock()):
            mock_dag_cls.return_value.build.side_effect = ValueError("Circular dependency")
            response = client.post("/validate", json={"config_path": "cyclic.yaml"})
        body = response.json()
        assert body["is_valid"] is False
        assert any("DAG error" in e for e in body["errors"])
