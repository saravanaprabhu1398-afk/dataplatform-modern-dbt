"""Prometheus-compatible metrics endpoint.

Generates Prometheus text format output from the SQLite metadata store.
No external ``prometheus-client`` dependency required — the format is simple
enough to produce directly.

Exposed metrics
---------------
dp_pipeline_runs_total{pipeline, status}
    Counter — total number of pipeline run status events per pipeline+status.

dp_quality_check_results_total{pipeline, task, result}
    Counter — total quality check executions, labelled passed/failed.

dp_sla_violations_total{pipeline}
    Counter — total SLA violations recorded per pipeline.

Usage::

    from dataplatform.core.metrics import generate_prometheus_text
    text = generate_prometheus_text()
    # return as plain text response with Content-Type: text/plain; version=0.0.4
"""
import logging
from typing import List

from dataplatform.core.database import (
    get_quality_counts,
    get_run_counts_by_status,
    get_sla_violation_counts,
)

logger = logging.getLogger(__name__)

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def generate_prometheus_text() -> str:
    """Return a Prometheus text-format string built from the metadata DB."""
    lines: List[str] = []

    # ------------------------------------------------------------------
    # dp_pipeline_runs_total
    # ------------------------------------------------------------------
    lines.append("# HELP dp_pipeline_runs_total Total pipeline run status events per pipeline and status.")
    lines.append("# TYPE dp_pipeline_runs_total counter")
    try:
        for row in get_run_counts_by_status():
            label = f'pipeline="{_esc(row["pipeline_name"])}",status="{_esc(row["status"])}"'
            lines.append(f"dp_pipeline_runs_total{{{label}}} {row['count']}")
    except Exception as exc:
        logger.warning("metrics: failed to read pipeline run counts: %s", exc)

    lines.append("")

    # ------------------------------------------------------------------
    # dp_quality_check_results_total
    # ------------------------------------------------------------------
    lines.append("# HELP dp_quality_check_results_total Total quality check executions per pipeline, task, and result.")
    lines.append("# TYPE dp_quality_check_results_total counter")
    try:
        for row in get_quality_counts():
            result_label = "passed" if row["passed"] else "failed"
            label = (
                f'pipeline="{_esc(row["pipeline_name"])}",'
                f'task="{_esc(row["task_name"])}",'
                f'result="{result_label}"'
            )
            lines.append(f"dp_quality_check_results_total{{{label}}} {row['count']}")
    except Exception as exc:
        logger.warning("metrics: failed to read quality check counts: %s", exc)

    lines.append("")

    # ------------------------------------------------------------------
    # dp_sla_violations_total
    # ------------------------------------------------------------------
    lines.append("# HELP dp_sla_violations_total Total SLA violations recorded per pipeline.")
    lines.append("# TYPE dp_sla_violations_total counter")
    try:
        for row in get_sla_violation_counts():
            label = f'pipeline="{_esc(row["pipeline_name"])}"'
            lines.append(f"dp_sla_violations_total{{{label}}} {row['count']}")
    except Exception as exc:
        logger.warning("metrics: failed to read SLA violation counts: %s", exc)

    lines.append("")
    return "\n".join(lines)


def _esc(value: str) -> str:
    """Escape label values for Prometheus text format."""
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
