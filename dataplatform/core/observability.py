"""Operational metrics collection and alert rule evaluation.

This module turns the platform metadata tables into dashboard-ready metric
samples and lightweight alert incidents. It complements the Prometheus text
endpoint: Prometheus can scrape counters, while the built-in UI can persist
snapshots and manage rules without external infrastructure.
"""
from __future__ import annotations

import logging
import math
import operator
import threading
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from dataplatform.core.alerts import send_email, send_webhook
from dataplatform.core.database import (
    create_alert_incident,
    create_alert_rule,
    get_active_alert_incident,
    get_all_pipeline_names,
    get_alert_rule,
    get_queue_runs,
    get_queue_runs_since,
    get_quality_results_since,
    get_sla_violations_since,
    list_alert_incidents,
    list_alert_rules,
    list_notification_channels,
    init_db,
    resolve_active_alert_incidents_for_rule,
    save_notification_delivery,
    save_metric_samples,
    update_alert_incident_observation,
)

logger = logging.getLogger(__name__)

ALLOWED_COMPARATORS = {">", ">=", "<", "<=", "==", "!="}
ALLOWED_SEVERITIES = {"critical", "warning", "info"}
ALLOWED_NOTIFICATION_TYPES = {"email", "webhook"}

METRIC_CATALOG: Dict[str, Dict[str, str]] = {
    "pipeline_runs_total": {
        "label": "Pipeline runs",
        "unit": "count",
        "description": "Runs queued in the selected collection window.",
    },
    "pipeline_completed_total": {
        "label": "Completed runs",
        "unit": "count",
        "description": "Runs completed in the selected collection window.",
    },
    "pipeline_failed_total": {
        "label": "Failed runs",
        "unit": "count",
        "description": "Runs failed in the selected collection window.",
    },
    "pipeline_success_rate": {
        "label": "Success rate",
        "unit": "percent",
        "description": "Completed terminal runs divided by completed plus failed runs.",
    },
    "pipeline_failure_rate": {
        "label": "Failure rate",
        "unit": "percent",
        "description": "Failed terminal runs divided by completed plus failed runs.",
    },
    "pipeline_p95_duration_seconds": {
        "label": "P95 duration",
        "unit": "seconds",
        "description": "95th percentile completed run duration.",
    },
    "pipeline_last_run_age_minutes": {
        "label": "Last run age",
        "unit": "minutes",
        "description": "Minutes since the latest queued run for a pipeline.",
    },
    "queue_depth": {
        "label": "Queue depth",
        "unit": "count",
        "description": "Runs currently waiting in the persistent queue.",
    },
    "active_runs": {
        "label": "Active runs",
        "unit": "count",
        "description": "Queued plus running runs.",
    },
    "running_runs": {
        "label": "Running runs",
        "unit": "count",
        "description": "Runs currently marked running.",
    },
    "sla_violations_total": {
        "label": "SLA violations",
        "unit": "count",
        "description": "SLA violations recorded in the selected collection window.",
    },
    "quality_failure_rate": {
        "label": "Quality failure rate",
        "unit": "percent",
        "description": "Failed quality checks divided by all quality checks in the window.",
    },
}

_COMPARATORS: Dict[str, Callable[[float, float], bool]] = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}

DEFAULT_ALERT_RULES: List[Dict[str, Any]] = [
    {
        "rule_id": "default:queue-depth-high",
        "name": "Queue depth high",
        "metric_name": "queue_depth",
        "comparator": ">",
        "threshold": 100,
        "severity": "warning",
        "window_minutes": 60,
        "pipeline_name": None,
    },
    {
        "rule_id": "default:failure-rate-high",
        "name": "Failure rate high",
        "metric_name": "pipeline_failure_rate",
        "comparator": ">",
        "threshold": 25,
        "severity": "critical",
        "window_minutes": 60,
        "pipeline_name": None,
    },
    {
        "rule_id": "default:p95-duration-high",
        "name": "P95 duration high",
        "metric_name": "pipeline_p95_duration_seconds",
        "comparator": ">",
        "threshold": 900,
        "severity": "warning",
        "window_minutes": 60,
        "pipeline_name": None,
    },
    {
        "rule_id": "default:sla-violations",
        "name": "SLA violation detected",
        "metric_name": "sla_violations_total",
        "comparator": ">",
        "threshold": 0,
        "severity": "critical",
        "window_minutes": 60,
        "pipeline_name": None,
    },
    {
        "rule_id": "default:quality-failure-rate",
        "name": "Quality failure rate high",
        "metric_name": "quality_failure_rate",
        "comparator": ">",
        "threshold": 0,
        "severity": "warning",
        "window_minutes": 60,
        "pipeline_name": None,
    },
    {
        "rule_id": "default:pipeline-stale",
        "name": "Pipeline activity stale",
        "metric_name": "pipeline_last_run_age_minutes",
        "comparator": ">",
        "threshold": 720,
        "severity": "warning",
        "window_minutes": 60,
        "pipeline_name": "*",
    },
]


def list_metric_catalog() -> List[Dict[str, str]]:
    """Return metric definitions supported by the built-in collector."""
    return [
        {"metric_name": metric_name, **definition}
        for metric_name, definition in sorted(METRIC_CATALOG.items())
    ]


def list_default_alert_rules() -> List[Dict[str, Any]]:
    """Return the built-in alert rule templates."""
    return [dict(rule) for rule in DEFAULT_ALERT_RULES]


def ensure_default_alert_rules(enabled: bool = True) -> Dict[str, Any]:
    """Create built-in alert rules that are missing.

    Existing rules are left untouched so operators can tune defaults after
    bootstrapping.
    """
    init_db()
    created: List[Dict[str, Any]] = []
    existing: List[Dict[str, Any]] = []
    for rule in DEFAULT_ALERT_RULES:
        rule_id = rule["rule_id"]
        current = get_alert_rule(rule_id)
        if current:
            existing.append(current)
            continue

        payload = dict(rule)
        payload.pop("rule_id")
        payload["enabled"] = enabled
        created.append(create_alert_rule(rule_id=rule_id, **payload))

    return {
        "created": created,
        "existing": existing,
        "created_count": len(created),
        "existing_count": len(existing),
        "total_defaults": len(DEFAULT_ALERT_RULES),
    }


def collect_platform_metrics(
    range_hours: int = 24,
    store: bool = True,
    evaluate: bool = True,
    notify: bool = True,
    collected_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Collect operational metrics from the metadata store.

    Returns a payload with the samples and alert-evaluation result. When
    ``store`` is true, samples are appended to ``metric_samples``.
    """
    init_db()
    safe_hours = max(1, min(int(range_hours or 24), 24 * 30))
    now = _utc_now()
    collected_at = collected_at or _iso(now)
    since = _iso(now - timedelta(hours=safe_hours))

    queue_runs = get_queue_runs_since(since, limit=10000)
    recent_or_all_queue = get_queue_runs(limit=10000)
    quality_rows = get_quality_results_since(since, limit=10000)
    sla_rows = get_sla_violations_since(since, limit=10000)

    samples = _build_samples(
        queue_runs=queue_runs,
        recent_or_all_queue=recent_or_all_queue,
        quality_rows=quality_rows,
        sla_rows=sla_rows,
        range_hours=safe_hours,
        now=now,
        collected_at=collected_at,
    )
    stored_samples = save_metric_samples(samples) if store else []
    evaluation = evaluate_alert_rules(samples, notify=notify) if evaluate else _empty_evaluation()

    return {
        "range_hours": safe_hours,
        "collected_at": collected_at,
        "stored": store,
        "samples": stored_samples or samples,
        "sample_count": len(samples),
        "evaluation": evaluation,
        "metric_catalog": list_metric_catalog(),
        "alert_summary": _alert_summary(),
    }


def collect_once_with_defaults(
    range_hours: int = 24,
    store: bool = True,
    evaluate: bool = True,
    notify: bool = True,
    seed_default_rules: bool = True,
) -> Dict[str, Any]:
    """Seed default rules if requested, then collect one metric snapshot."""
    defaults = (
        ensure_default_alert_rules(enabled=True)
        if seed_default_rules
        else {"created_count": 0, "existing_count": 0, "total_defaults": 0}
    )
    result = collect_platform_metrics(
        range_hours=range_hours,
        store=store,
        evaluate=evaluate,
        notify=notify,
    )
    result["default_rules"] = defaults
    return result


def run_observability_collector_loop(
    interval_seconds: float = 60.0,
    range_hours: int = 24,
    notify: bool = True,
    seed_default_rules: bool = True,
    stop_event: Optional[threading.Event] = None,
    once: bool = False,
) -> None:
    """Run the blocking observability collector loop.

    Used by the CLI and suitable for a dedicated worker process.
    """
    safe_interval = max(5.0, float(interval_seconds or 60.0))
    stop_event = stop_event or threading.Event()
    if seed_default_rules:
        defaults = ensure_default_alert_rules(enabled=True)
        if defaults["created_count"]:
            logger.info("Seeded %d default observability alert rule(s)", defaults["created_count"])

    while not stop_event.is_set():
        try:
            result = collect_platform_metrics(
                range_hours=range_hours,
                store=True,
                evaluate=True,
                notify=notify,
            )
            logger.info(
                "Collected %d observability metric sample(s); evaluated %d alert rule(s)",
                result["sample_count"],
                result["evaluation"]["rules_evaluated"],
            )
        except Exception as exc:
            logger.error("Observability collector iteration failed: %s", exc, exc_info=True)

        if once:
            return
        stop_event.wait(safe_interval)


def build_observability_dashboard(range_hours: int = 24) -> Dict[str, Any]:
    """Build a fresh dashboard payload without persisting samples."""
    payload = collect_platform_metrics(
        range_hours=range_hours,
        store=False,
        evaluate=False,
        notify=False,
    )
    payload["rules"] = list_alert_rules()
    payload["incidents"] = list_alert_incidents(limit=50)
    payload["alert_summary"] = _alert_summary(payload["incidents"])
    return payload


def evaluate_alert_rules(
    samples: List[Dict[str, Any]],
    notify: bool = True,
) -> Dict[str, Any]:
    """Evaluate enabled alert rules against freshly collected samples."""
    init_db()
    rules = list_alert_rules(enabled_only=True)

    fired: List[Dict[str, Any]] = []
    updated: List[Dict[str, Any]] = []
    deliveries: List[Dict[str, Any]] = []
    resolved_count = 0
    skipped: List[Dict[str, str]] = []

    for rule in rules:
        matching_samples = _matching_samples_for_rule(rule, samples)
        if not matching_samples:
            skipped.append({"rule_id": rule["rule_id"], "reason": "no matching sample"})
            continue

        comparator = rule.get("comparator")
        compare = _COMPARATORS.get(comparator)
        if compare is None:
            skipped.append({"rule_id": rule["rule_id"], "reason": "invalid comparator"})
            continue

        for sample in matching_samples:
            observed_value = float(sample["value"])
            threshold = float(rule["threshold"])
            sample_pipeline = sample.get("pipeline_name")
            is_firing = compare(observed_value, threshold)

            if is_firing:
                message = _format_alert_message(rule, sample)
                active = get_active_alert_incident(rule["rule_id"], sample_pipeline)
                if active:
                    incident = update_alert_incident_observation(
                        active["incident_id"],
                        observed_value=observed_value,
                        message=message,
                        labels=sample.get("labels") or {},
                    )
                    if incident:
                        updated.append(incident)
                    continue

                incident = create_alert_incident(
                    incident_id=str(uuid.uuid4()),
                    rule_id=rule["rule_id"],
                    rule_name=rule["name"],
                    metric_name=rule["metric_name"],
                    pipeline_name=sample_pipeline,
                    severity=rule["severity"],
                    observed_value=observed_value,
                    threshold=threshold,
                    message=message,
                    labels=sample.get("labels") or {},
                )
                fired.append(incident)
                if notify:
                    deliveries.extend(_notify_rule(rule, incident))
                continue

            resolved_count += resolve_active_alert_incidents_for_rule(
                rule["rule_id"],
                sample_pipeline,
            )

    return {
        "rules_evaluated": len(rules),
        "fired": fired,
        "updated": updated,
        "deliveries": deliveries,
        "resolved": resolved_count,
        "skipped": skipped,
    }


def validate_alert_rule_fields(fields: Dict[str, Any], partial: bool = False) -> Dict[str, Any]:
    """Normalize and validate API payload fields for alert rules."""
    allowed = {
        "name",
        "metric_name",
        "comparator",
        "threshold",
        "severity",
        "window_minutes",
        "pipeline_name",
        "enabled",
        "destination_type",
        "destination",
    }
    normalized = {k: v for k, v in fields.items() if k in allowed and v is not None}
    required = {"name", "metric_name", "comparator", "threshold"}
    missing = required - set(normalized) if not partial else set()
    if missing:
        raise ValueError(f"missing required alert rule fields: {', '.join(sorted(missing))}")

    if "name" in normalized:
        normalized["name"] = str(normalized["name"]).strip()
        if not normalized["name"]:
            raise ValueError("alert rule name must not be empty")
    if "metric_name" in normalized:
        normalized["metric_name"] = str(normalized["metric_name"]).strip()
        if not normalized["metric_name"]:
            raise ValueError("metric_name must not be empty")
    if "comparator" in normalized:
        comparator = str(normalized["comparator"]).strip()
        if comparator not in ALLOWED_COMPARATORS:
            raise ValueError(f"comparator must be one of: {', '.join(sorted(ALLOWED_COMPARATORS))}")
        normalized["comparator"] = comparator
    if "threshold" in normalized:
        threshold = float(normalized["threshold"])
        if not math.isfinite(threshold):
            raise ValueError("threshold must be finite")
        normalized["threshold"] = threshold
    if "severity" in normalized:
        severity = str(normalized["severity"]).strip().lower()
        if severity not in ALLOWED_SEVERITIES:
            raise ValueError(f"severity must be one of: {', '.join(sorted(ALLOWED_SEVERITIES))}")
        normalized["severity"] = severity
    elif not partial:
        normalized["severity"] = "warning"
    if "window_minutes" in normalized:
        window = int(normalized["window_minutes"])
        if window < 1 or window > 60 * 24 * 30:
            raise ValueError("window_minutes must be between 1 and 43200")
        normalized["window_minutes"] = window
    elif not partial:
        normalized["window_minutes"] = 60
    if "pipeline_name" in normalized:
        pipeline_name = str(normalized["pipeline_name"]).strip()
        normalized["pipeline_name"] = pipeline_name or None
    if "enabled" in normalized:
        normalized["enabled"] = bool(normalized["enabled"])
    elif not partial:
        normalized["enabled"] = True
    if "destination_type" in normalized:
        destination_type = str(normalized["destination_type"]).strip().lower()
        if destination_type and destination_type not in ALLOWED_NOTIFICATION_TYPES:
            raise ValueError("destination_type must be email or webhook")
        normalized["destination_type"] = destination_type or None
    if "destination" in normalized:
        destination = str(normalized["destination"]).strip()
        normalized["destination"] = destination or None
    return normalized


def validate_notification_channel_fields(
    fields: Dict[str, Any],
    partial: bool = False,
) -> Dict[str, Any]:
    """Normalize and validate API payload fields for notification channels."""
    allowed = {"name", "channel_type", "destination", "severities", "enabled"}
    normalized = {key: value for key, value in fields.items() if key in allowed and value is not None}
    required = {"name", "channel_type", "destination"}
    missing = required - set(normalized) if not partial else set()
    if missing:
        raise ValueError(f"missing required notification channel fields: {', '.join(sorted(missing))}")

    if "name" in normalized:
        name = str(normalized["name"]).strip()
        if not name:
            raise ValueError("notification channel name must not be empty")
        normalized["name"] = name
    if "channel_type" in normalized:
        channel_type = str(normalized["channel_type"]).strip().lower()
        if channel_type not in ALLOWED_NOTIFICATION_TYPES:
            raise ValueError("channel_type must be email or webhook")
        normalized["channel_type"] = channel_type
    if "destination" in normalized:
        destination = str(normalized["destination"]).strip()
        if not destination:
            raise ValueError("destination must not be empty")
        normalized["destination"] = destination
    if "severities" in normalized:
        severities = normalized["severities"]
        if isinstance(severities, str):
            severities = [part.strip().lower() for part in severities.split(",")]
        if not isinstance(severities, list):
            raise ValueError("severities must be a list")
        cleaned = []
        for severity in severities:
            value = str(severity).strip().lower()
            if value not in ALLOWED_SEVERITIES and value != "all":
                raise ValueError("severities must contain critical, warning, info, or all")
            if value not in cleaned:
                cleaned.append(value)
        normalized["severities"] = cleaned or ["critical", "warning"]
    elif not partial:
        normalized["severities"] = ["critical", "warning"]
    if "enabled" in normalized:
        normalized["enabled"] = bool(normalized["enabled"])
    elif not partial:
        normalized["enabled"] = True
    return normalized


def _build_samples(
    queue_runs: List[Dict[str, Any]],
    recent_or_all_queue: List[Dict[str, Any]],
    quality_rows: List[Dict[str, Any]],
    sla_rows: List[Dict[str, Any]],
    range_hours: int,
    now: datetime,
    collected_at: str,
) -> List[Dict[str, Any]]:
    status_counts = Counter(row.get("status") for row in queue_runs)
    terminal = status_counts["completed"] + status_counts["failed"]
    success_rate = _rate(status_counts["completed"], terminal)
    failure_rate = _rate(status_counts["failed"], terminal)
    active_runs = status_counts["queued"] + status_counts["running"]

    durations_by_pipeline: Dict[str, List[float]] = defaultdict(list)
    runs_by_pipeline: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in queue_runs:
        pipeline = row.get("pipeline_name") or "unknown"
        runs_by_pipeline[pipeline].append(row)
        duration = _duration_seconds(row)
        if duration is not None and row.get("status") == "completed":
            durations_by_pipeline[pipeline].append(duration)

    quality_failures = sum(1 for row in quality_rows if not row.get("passed"))
    quality_rate = _rate(quality_failures, len(quality_rows))
    global_durations = [duration for values in durations_by_pipeline.values() for duration in values]

    samples = [
        _sample("pipeline_runs_total", len(queue_runs), "count", collected_at, range_hours=range_hours),
        _sample("pipeline_completed_total", status_counts["completed"], "count", collected_at, range_hours=range_hours),
        _sample("pipeline_failed_total", status_counts["failed"], "count", collected_at, range_hours=range_hours),
        _sample("pipeline_success_rate", success_rate, "percent", collected_at, range_hours=range_hours),
        _sample("pipeline_failure_rate", failure_rate, "percent", collected_at, range_hours=range_hours),
        _sample("pipeline_p95_duration_seconds", _p95(global_durations), "seconds", collected_at, range_hours=range_hours),
        _sample("queue_depth", status_counts["queued"], "count", collected_at, range_hours=range_hours),
        _sample("active_runs", active_runs, "count", collected_at, range_hours=range_hours),
        _sample("running_runs", status_counts["running"], "count", collected_at, range_hours=range_hours),
        _sample("sla_violations_total", len(sla_rows), "count", collected_at, range_hours=range_hours),
        _sample("quality_failure_rate", quality_rate, "percent", collected_at, range_hours=range_hours),
    ]

    latest_queue_by_pipeline: Dict[str, Dict[str, Any]] = {}
    for row in recent_or_all_queue:
        pipeline = row.get("pipeline_name")
        if pipeline and pipeline not in latest_queue_by_pipeline:
            latest_queue_by_pipeline[pipeline] = row
    known_pipelines = (
        set(get_all_pipeline_names())
        | set(runs_by_pipeline.keys())
        | set(latest_queue_by_pipeline.keys())
    )

    for pipeline in sorted(known_pipelines):
        pipeline_runs = runs_by_pipeline.get(pipeline, [])
        p_status = Counter(row.get("status") for row in pipeline_runs)
        p_terminal = p_status["completed"] + p_status["failed"]
        samples.extend([
            _sample("pipeline_runs_total", len(pipeline_runs), "count", collected_at, pipeline, range_hours),
            _sample("pipeline_success_rate", _rate(p_status["completed"], p_terminal), "percent", collected_at, pipeline, range_hours),
            _sample("pipeline_failure_rate", _rate(p_status["failed"], p_terminal), "percent", collected_at, pipeline, range_hours),
            _sample("pipeline_p95_duration_seconds", _p95(durations_by_pipeline.get(pipeline, [])), "seconds", collected_at, pipeline, range_hours),
        ])

        latest = latest_queue_by_pipeline.get(pipeline)
        if latest and latest.get("queued_at"):
            age = _age_minutes(latest["queued_at"], now)
            if age is not None:
                samples.append(
                    _sample("pipeline_last_run_age_minutes", age, "minutes", collected_at, pipeline, range_hours)
                )

    return samples


def _sample(
    metric_name: str,
    value: float,
    unit: str,
    collected_at: str,
    pipeline_name: Optional[str] = None,
    range_hours: int = 24,
) -> Dict[str, Any]:
    labels: Dict[str, Any] = {"range_hours": range_hours, "scope": "pipeline" if pipeline_name else "global"}
    if pipeline_name:
        labels["pipeline"] = pipeline_name
    return {
        "metric_name": metric_name,
        "value": round(float(value), 4),
        "unit": unit,
        "pipeline_name": pipeline_name,
        "labels": labels,
        "collected_at": collected_at,
    }


def _matching_samples_for_rule(
    rule: Dict[str, Any],
    samples: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    metric_name = rule.get("metric_name")
    pipeline_name = rule.get("pipeline_name")
    metric_samples = [sample for sample in samples if sample.get("metric_name") == metric_name]

    if pipeline_name == "*":
        return [sample for sample in metric_samples if sample.get("pipeline_name")]
    if pipeline_name:
        return [
            sample
            for sample in metric_samples
            if sample.get("pipeline_name") == pipeline_name
        ]
    return [
        sample
        for sample in metric_samples
        if sample.get("pipeline_name") is None
    ]


def _rate(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 4)


def _p95(values: List[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return round(float(ordered[idx]), 4)


def _duration_seconds(row: Dict[str, Any]) -> Optional[float]:
    if not row.get("started_at") or not row.get("completed_at"):
        return None
    try:
        start = _parse_iso(row["started_at"])
        end = _parse_iso(row["completed_at"])
        return max(0.0, (end - start).total_seconds())
    except Exception:
        return None


def _age_minutes(value: str, now: datetime) -> Optional[float]:
    try:
        return round(max(0.0, (now - _parse_iso(value)).total_seconds() / 60.0), 4)
    except Exception:
        return None


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_alert_message(rule: Dict[str, Any], sample: Dict[str, Any]) -> str:
    sample_pipeline = sample.get("pipeline_name")
    scope = f" for {sample_pipeline}" if sample_pipeline else ""
    unit = sample.get("unit") or METRIC_CATALOG.get(rule["metric_name"], {}).get("unit") or ""
    unit_suffix = f" {unit}" if unit else ""
    return (
        f"{rule['name']} is firing{scope}: "
        f"{rule['metric_name']}={sample['value']}{unit_suffix} "
        f"{rule['comparator']} {rule['threshold']}."
    )


def _notify_rule(rule: Dict[str, Any], incident: Dict[str, Any]) -> List[Dict[str, Any]]:
    targets = _notification_targets_for_rule(rule, incident)
    if not targets:
        return [
            save_notification_delivery(
                delivery_id=str(uuid.uuid4()),
                incident_id=incident["incident_id"],
                rule_id=rule["rule_id"],
                destination_type="none",
                destination="",
                status="skipped",
                error="No rule destination or matching notification channel configured",
            )
        ]

    payload = _notification_payload(rule, incident)
    deliveries: List[Dict[str, Any]] = []
    for target in targets:
        destination_type = target["destination_type"]
        destination = target["destination"]
        status = "failed"
        error = None
        try:
            if destination_type == "webhook":
                status = "sent" if send_webhook(destination, payload) else "failed"
                if status == "failed":
                    error = "Webhook delivery returned false"
            elif destination_type == "email":
                subject = f"[DataPlatform] {incident['severity'].upper()} alert: {rule['name']}"
                status = "sent" if send_email(destination, subject, incident["message"]) else "failed"
                if status == "failed":
                    error = "Email delivery returned false"
            else:
                status = "skipped"
                error = f"Unsupported destination type: {destination_type}"
        except Exception as exc:
            status = "failed"
            error = str(exc)

        deliveries.append(
            save_notification_delivery(
                delivery_id=str(uuid.uuid4()),
                incident_id=incident["incident_id"],
                rule_id=rule["rule_id"],
                channel_id=target.get("channel_id"),
                channel_name=target.get("channel_name"),
                destination_type=destination_type,
                destination=destination,
                status=status,
                error=error,
            )
        )
    return deliveries


def _notification_targets_for_rule(
    rule: Dict[str, Any],
    incident: Dict[str, Any],
) -> List[Dict[str, Any]]:
    destination_type = rule.get("destination_type")
    destination = rule.get("destination")
    if destination_type and destination:
        return [
            {
                "channel_id": None,
                "channel_name": "Rule destination",
                "destination_type": destination_type,
                "destination": destination,
            }
        ]

    channels = list_notification_channels(
        enabled_only=True,
        severity=incident.get("severity"),
    )
    return [
        {
            "channel_id": channel["channel_id"],
            "channel_name": channel["name"],
            "destination_type": channel["channel_type"],
            "destination": channel["destination"],
        }
        for channel in channels
    ]


def _notification_payload(rule: Dict[str, Any], incident: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "alert": "metric_rule_firing",
        "incident_id": incident["incident_id"],
        "rule_id": rule["rule_id"],
        "rule_name": rule["name"],
        "metric_name": rule["metric_name"],
        "pipeline_name": incident.get("pipeline_name"),
        "severity": incident["severity"],
        "observed_value": incident["observed_value"],
        "threshold": incident["threshold"],
        "message": incident["message"],
    }


def _alert_summary(incidents: Optional[List[Dict[str, Any]]] = None) -> Dict[str, int]:
    rows = incidents if incidents is not None else list_alert_incidents(limit=500)
    counts = Counter(row.get("status") for row in rows)
    return {
        "firing": counts["firing"],
        "acknowledged": counts["acknowledged"],
        "resolved": counts["resolved"],
        "active": counts["firing"] + counts["acknowledged"],
        "total": len(rows),
    }


def _empty_evaluation() -> Dict[str, Any]:
    return {
        "rules_evaluated": 0,
        "fired": [],
        "updated": [],
        "deliveries": [],
        "resolved": 0,
        "skipped": [],
    }
