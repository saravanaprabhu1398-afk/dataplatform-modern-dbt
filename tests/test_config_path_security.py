"""Regression tests for pipeline config path containment."""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dataplatform.core import api
from dataplatform.core.api import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    monkeypatch.setenv("PIPELINES_PATH", str(pipelines_dir))
    monkeypatch.setattr(
        api,
        "_get_current_user",
        lambda request: {"username": "admin", "role": "admin", "team": None},
    )
    return TestClient(app)


def test_config_inside_pipeline_root_is_allowed(client, tmp_path):
    config_path = tmp_path / "pipelines" / "valid.yaml"
    config_path.write_text(
        "pipeline_name: valid\ntasks:\n  - name: task\n    type: executor\n    plugin: python\n",
        encoding="utf-8",
    )

    response = client.get("/pipeline-config", params={"config_path": str(config_path)})

    assert response.status_code == 200
    assert response.json()["parsed_config"]["pipeline_name"] == "valid"


def test_config_path_traversal_is_rejected(client, tmp_path):
    outside_path = tmp_path / "outside.yaml"
    outside_path.write_text("pipeline_name: outside\ntasks: []\n", encoding="utf-8")
    traversal_path = tmp_path / "pipelines" / ".." / "outside.yaml"

    response = client.get("/pipeline-config", params={"config_path": str(traversal_path)})

    assert response.status_code == 400
    assert "inside the configured pipelines directory" in response.json()["detail"]


def test_unrelated_absolute_config_path_is_rejected(client, tmp_path):
    outside_path = tmp_path / "outside.yaml"
    outside_path.write_text("pipeline_name: outside\ntasks: []\n", encoding="utf-8")

    response = client.get("/pipeline-config", params={"config_path": str(outside_path)})

    assert response.status_code == 400
    assert "inside the configured pipelines directory" in response.json()["detail"]
