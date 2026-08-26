"""Tests for operational metric collection and alert evaluation."""
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "observability_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    import dataplatform.core.database as db_module
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module._engine = None
    yield
    db_module._initialized = False
    db_module._engine = None


from dataplatform.core import database as db
from dataplatform.core.observability import (
    collect_once_with_defaults,
    collect_platform_metrics,
    ensure_default_alert_rules,
    list_metric_catalog,
    validate_alert_rule_fields,
)


class TestObservabilityCollector:
    def setup_method(self):
        db.init_db()

    def test_metric_catalog_includes_core_runtime_metrics(self):
        names = {metric["metric_name"] for metric in list_metric_catalog()}
        assert "pipeline_failure_rate" in names
        assert "queue_depth" in names
        assert "sla_violations_total" in names

    def test_collects_and_persists_runtime_metric_samples(self):
        db.enqueue_run("run-ok", "pipe_a", "pipe_a.yaml")
        db.set_run_status_in_queue("run-ok", "completed")
        db.enqueue_run("run-fail", "pipe_a", "pipe_a.yaml")
        db.set_run_status_in_queue("run-fail", "failed", error="boom")

        result = collect_platform_metrics(range_hours=24, store=True, evaluate=False)

        assert result["sample_count"] > 0
        samples = db.get_metric_samples(metric_name="pipeline_failure_rate", pipeline_name="pipe_a")
        assert samples
        assert samples[0]["value"] == 50.0
        assert samples[0]["labels"]["pipeline"] == "pipe_a"

    def test_alert_rule_fires_once_then_resolves_when_condition_clears(self):
        db.create_alert_rule(
            rule_id="queue-depth",
            name="Queue depth high",
            metric_name="queue_depth",
            comparator=">",
            threshold=0,
            severity="critical",
        )
        db.enqueue_run("queued-run", "pipe_a", "pipe_a.yaml")

        first = collect_platform_metrics(range_hours=24, store=True, evaluate=True, notify=False)
        second = collect_platform_metrics(range_hours=24, store=True, evaluate=True, notify=False)

        assert len(first["evaluation"]["fired"]) == 1
        assert len(second["evaluation"]["fired"]) == 0
        assert len(second["evaluation"]["updated"]) == 1
        active = db.list_alert_incidents(status="firing")
        assert len(active) == 1

        db.set_run_status_in_queue("queued-run", "completed")
        cleared = collect_platform_metrics(range_hours=24, store=True, evaluate=True, notify=False)

        assert cleared["evaluation"]["resolved"] == 1
        assert db.list_alert_incidents(status="firing") == []
        resolved = db.list_alert_incidents(status="resolved")
        assert len(resolved) == 1

    def test_default_alert_rules_are_seeded_once(self):
        first = ensure_default_alert_rules(enabled=True)
        second = ensure_default_alert_rules(enabled=True)

        assert first["created_count"] >= 5
        assert second["created_count"] == 0
        rule_ids = {rule["rule_id"] for rule in db.list_alert_rules()}
        assert "default:queue-depth-high" in rule_ids
        assert "default:pipeline-stale" in rule_ids

    def test_collect_once_with_defaults_seeds_and_collects(self):
        result = collect_once_with_defaults(
            range_hours=24,
            evaluate=True,
            notify=False,
            seed_default_rules=True,
        )

        assert result["default_rules"]["created_count"] >= 5
        assert result["sample_count"] > 0

    def test_wildcard_alert_rule_evaluates_each_pipeline_sample(self):
        db.enqueue_run("old-a", "pipe_a", "pipe_a.yaml")
        db.enqueue_run("old-b", "pipe_b", "pipe_b.yaml")
        old_timestamp = "2026-01-01T00:00:00Z"
        with db._get_conn() as conn:
            conn.execute(
                db.text("UPDATE pipeline_queue SET queued_at = :old_ts"),
                {"old_ts": old_timestamp},
            )
            conn.commit()
        db.create_alert_rule(
            rule_id="stale",
            name="Pipeline stale",
            metric_name="pipeline_last_run_age_minutes",
            comparator=">",
            threshold=1,
            severity="warning",
            pipeline_name="*",
        )

        result = collect_platform_metrics(range_hours=24, store=True, evaluate=True, notify=False)

        fired_pipelines = {incident["pipeline_name"] for incident in result["evaluation"]["fired"]}
        assert fired_pipelines == {"pipe_a", "pipe_b"}

    def test_notification_channel_delivery_is_recorded(self, monkeypatch):
        monkeypatch.setattr("dataplatform.core.observability.send_webhook", lambda *_args, **_kwargs: True)
        db.create_notification_channel(
            channel_id="slack",
            name="Slack",
            channel_type="webhook",
            destination="https://hooks.example.com/test",
            severities=["critical"],
        )
        db.create_alert_rule(
            rule_id="queue-depth-notify",
            name="Queue depth high",
            metric_name="queue_depth",
            comparator=">",
            threshold=0,
            severity="critical",
        )
        db.enqueue_run("queued-run", "pipe_a", "pipe_a.yaml")

        result = collect_platform_metrics(range_hours=24, store=True, evaluate=True, notify=True)

        assert len(result["evaluation"]["fired"]) == 1
        assert len(result["evaluation"]["deliveries"]) == 1
        deliveries = db.list_notification_deliveries()
        assert deliveries[0]["status"] == "sent"
        assert deliveries[0]["channel_id"] == "slack"

    def test_notification_delivery_skipped_when_no_route(self):
        db.create_alert_rule(
            rule_id="queue-depth-no-route",
            name="Queue depth high",
            metric_name="queue_depth",
            comparator=">",
            threshold=0,
            severity="critical",
        )
        db.enqueue_run("queued-run", "pipe_a", "pipe_a.yaml")

        result = collect_platform_metrics(range_hours=24, store=True, evaluate=True, notify=True)

        assert len(result["evaluation"]["deliveries"]) == 1
        assert result["evaluation"]["deliveries"][0]["status"] == "skipped"


class TestAlertRuleValidation:
    def test_rejects_invalid_comparator(self):
        with pytest.raises(ValueError, match="comparator"):
            validate_alert_rule_fields(
                {
                    "name": "Bad",
                    "metric_name": "queue_depth",
                    "comparator": "contains",
                    "threshold": 1,
                }
            )

    def test_normalizes_optional_fields(self):
        fields = validate_alert_rule_fields(
            {
                "name": " Freshness ",
                "metric_name": "pipeline_last_run_age_minutes",
                "comparator": ">",
                "threshold": "120",
                "severity": "WARNING",
                "pipeline_name": " ",
            }
        )
        assert fields["name"] == "Freshness"
        assert fields["threshold"] == 120.0
        assert fields["severity"] == "warning"
        assert fields["pipeline_name"] is None
