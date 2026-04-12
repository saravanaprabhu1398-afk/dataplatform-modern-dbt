"""Tests for the SLA alert module (alerts.py)."""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "alerts_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    yield
    db_module._initialized = False


from dataplatform.core.config import SLAConfig, AlertConfig
from dataplatform.core.database import init_db, get_sla_violations
from dataplatform.core.alerts import (
    check_sla_and_alert,
    send_webhook,
    send_email,
)


class TestCheckSlaAndAlert:
    def setup_method(self):
        init_db()

    def test_no_violation_within_sla(self):
        sla = SLAConfig(max_duration_minutes=10)
        violated = check_sla_and_alert("my_pipe", "run-1", 300.0, sla)  # 5 min < 10 min
        assert violated is False

    def test_violation_detected(self):
        sla = SLAConfig(max_duration_minutes=1)
        violated = check_sla_and_alert("slow_pipe", "run-2", 120.0, sla)  # 2 min > 1 min
        assert violated is True

    def test_violation_persisted_to_db(self):
        sla = SLAConfig(max_duration_minutes=1)
        check_sla_and_alert("persist_pipe", "run-3", 200.0, sla)
        violations = get_sla_violations("persist_pipe")
        assert len(violations) == 1
        assert violations[0]["pipeline_name"] == "persist_pipe"
        assert violations[0]["duration_seconds"] == 200.0
        assert violations[0]["limit_seconds"] == 60.0

    def test_no_violation_not_persisted(self):
        sla = SLAConfig(max_duration_minutes=60)
        check_sla_and_alert("fast_pipe", "run-4", 10.0, sla)
        assert get_sla_violations("fast_pipe") == []

    def test_webhook_alert_called_on_violation(self):
        sla = SLAConfig(
            max_duration_minutes=1,
            alert=AlertConfig(type="webhook", webhook_url="http://hooks.example.com/test"),
        )
        with patch("dataplatform.core.alerts.send_webhook", return_value=True) as mock_wh:
            check_sla_and_alert("wh_pipe", "run-5", 200.0, sla)
        mock_wh.assert_called_once()
        payload = mock_wh.call_args[0][1]
        assert payload["alert"] == "sla_violation"
        assert payload["pipeline"] == "wh_pipe"

    def test_email_alert_called_on_violation(self):
        sla = SLAConfig(
            max_duration_minutes=1,
            alert=AlertConfig(type="email", email="oncall@example.com"),
        )
        with patch("dataplatform.core.alerts.send_email", return_value=True) as mock_em:
            check_sla_and_alert("em_pipe", "run-6", 200.0, sla)
        mock_em.assert_called_once()
        assert mock_em.call_args[0][0] == "oncall@example.com"

    def test_no_alert_config_still_records_violation(self):
        sla = SLAConfig(max_duration_minutes=1)  # no alert block
        check_sla_and_alert("quiet_pipe", "run-7", 200.0, sla)
        assert len(get_sla_violations("quiet_pipe")) == 1

    def test_boundary_exactly_at_limit_not_violated(self):
        sla = SLAConfig(max_duration_minutes=2)
        # exactly 120 seconds = 2 minutes: NOT a violation (<=)
        violated = check_sla_and_alert("boundary_pipe", "run-8", 120.0, sla)
        assert violated is False


class TestSendWebhook:
    def test_success_returns_true(self):
        mock_response = MagicMock()
        with patch("urllib.request.urlopen", return_value=mock_response):
            result = send_webhook("http://example.com/hook", {"key": "value"})
        assert result is True

    def test_network_error_returns_false(self):
        with patch("urllib.request.urlopen", side_effect=Exception("connection refused")):
            result = send_webhook("http://bad-host/hook", {})
        assert result is False

    def test_sends_json_content_type(self):
        captured = {}
        def fake_urlopen(req, timeout):
            captured["content_type"] = req.get_header("Content-type")

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            send_webhook("http://example.com", {"x": 1})
        assert captured.get("content_type") == "application/json"


class TestSendEmail:
    def test_missing_credentials_returns_false(self, monkeypatch):
        monkeypatch.delenv("ALERT_SMTP_USER", raising=False)
        monkeypatch.delenv("ALERT_SMTP_PASSWORD", raising=False)
        result = send_email("to@example.com", "subject", "body")
        assert result is False

    def test_smtp_error_returns_false(self, monkeypatch):
        monkeypatch.setenv("ALERT_SMTP_USER", "sender@example.com")
        monkeypatch.setenv("ALERT_SMTP_PASSWORD", "password")
        with patch("smtplib.SMTP", side_effect=Exception("SMTP error")):
            result = send_email("to@example.com", "subject", "body")
        assert result is False

    def test_success_returns_true(self, monkeypatch):
        monkeypatch.setenv("ALERT_SMTP_USER", "sender@example.com")
        monkeypatch.setenv("ALERT_SMTP_PASSWORD", "password")
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)
        with patch("smtplib.SMTP", return_value=mock_smtp):
            result = send_email("to@example.com", "Test Subject", "Test body")
        assert result is True
        mock_smtp.sendmail.assert_called_once()
