"""API tests for operational metrics and alert rules."""
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from dataplatform.core import api
from dataplatform.core.api import app


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "observability_api_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    monkeypatch.setenv("DATAPLATFORM_USERNAME", "admin")
    monkeypatch.setenv("DATAPLATFORM_PASSWORD", "admin")
    monkeypatch.setenv("DATAPLATFORM_OBSERVABILITY_AUTO_COLLECT", "true")
    monkeypatch.setenv("DATAPLATFORM_OBSERVABILITY_INTERVAL_SECONDS", "60")
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


def _set_role(monkeypatch, role: str, username: str = "testuser"):
    monkeypatch.setattr(
        api,
        "_get_current_user",
        lambda request: {"username": username, "role": role, "team": None},
    )


def test_viewer_can_read_metric_catalog(client, monkeypatch):
    _set_role(monkeypatch, "viewer")
    resp = client.get("/metrics/catalog")
    assert resp.status_code == 200
    names = {metric["metric_name"] for metric in resp.json()["metrics"]}
    assert "queue_depth" in names


def test_viewer_cannot_create_alert_rule(client, monkeypatch):
    _set_role(monkeypatch, "viewer")
    resp = client.post(
        "/alert-rules",
        json={
            "name": "Queue high",
            "metric_name": "queue_depth",
            "comparator": ">",
            "threshold": 0,
        },
    )
    assert resp.status_code == 403


def test_editor_can_create_rule_and_collect_metrics(client, monkeypatch):
    _set_role(monkeypatch, "editor")
    create_resp = client.post(
        "/alert-rules",
        json={
            "name": "Queue high",
            "metric_name": "queue_depth",
            "comparator": ">",
            "threshold": 0,
            "severity": "critical",
        },
    )
    assert create_resp.status_code == 201
    assert create_resp.json()["rule"]["name"] == "Queue high"

    collect_resp = client.post(
        "/observability/collect",
        json={"range_hours": 24, "store": True, "evaluate": True, "notify": False},
    )
    assert collect_resp.status_code == 200
    assert collect_resp.json()["sample_count"] > 0

    list_resp = client.get("/alert-rules")
    assert list_resp.status_code == 200
    assert len(list_resp.json()["rules"]) == 1


def test_health_reports_observability_status(client, monkeypatch):
    _set_role(monkeypatch, "viewer")
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["observability"]["auto_collect_enabled"] is True
    assert body["observability"]["interval_seconds"] >= 5


def test_editor_can_manage_notification_channel(client, monkeypatch):
    _set_role(monkeypatch, "editor")
    create_resp = client.post(
        "/notification-channels",
        json={
            "channel_id": "platform-slack",
            "name": "Platform Slack",
            "channel_type": "webhook",
            "destination": "https://hooks.example.com/test",
            "severities": ["critical", "warning"],
        },
    )
    assert create_resp.status_code == 201
    assert create_resp.json()["channel"]["enabled"] is True

    patch_resp = client.patch(
        "/notification-channels/platform-slack",
        json={"enabled": False},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["channel"]["enabled"] is False

    list_resp = client.get("/notification-channels")
    assert list_resp.status_code == 200
    assert list_resp.json()["total"] == 1


def test_viewer_can_read_delivery_history(client, monkeypatch):
    _set_role(monkeypatch, "viewer")
    resp = client.get("/notification-deliveries")
    assert resp.status_code == 200
    assert resp.json()["deliveries"] == []
