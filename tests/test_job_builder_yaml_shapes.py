"""Round-trip tests for the YAML shapes the Job Builder emits.

These cover the metadata, lineage, quality, trigger, and SLA fields exposed by
the Phase B/C builder UI. We exercise the full pipeline_generator.save_generated_pipeline
→ load_config path because that is exactly what /save-pipeline runs at request time.
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from dataplatform.core.config import PipelineConfig, load_config
from dataplatform.core.dag import DAGBuilder
from dataplatform.core.pipeline_generator import save_generated_pipeline


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _roundtrip(yaml_text: str, name: str) -> PipelineConfig:
    """Persist via save_generated_pipeline then reload — mirrors /save-pipeline."""
    saved = save_generated_pipeline(yaml_text, name)
    try:
        return load_config(saved)
    finally:
        os.remove(saved)


# ---------------------------------------------------------------------------
# Ownership: team + tags
# ---------------------------------------------------------------------------

def test_team_and_tags_persist_through_save_and_load():
    cfg = _roundtrip(
        """
pipeline_name: ownership_demo
team: "data-platform"
tags:
  - "finance"
  - "daily"
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    config: { code: "print(1)" }
""",
        "ownership_demo",
    )
    assert cfg.team == "data-platform"
    assert cfg.tags == ["finance", "daily"]


# ---------------------------------------------------------------------------
# SLA: email + webhook variants
# ---------------------------------------------------------------------------

def test_sla_with_email_alert_persists():
    cfg = _roundtrip(
        """
pipeline_name: sla_email
sla:
  max_duration_minutes: 45
  alert:
    type: email
    email: "oncall@example.com"
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    config: { code: "print(1)" }
""",
        "sla_email",
    )
    assert cfg.sla is not None
    assert cfg.sla.max_duration_minutes == 45
    assert cfg.sla.alert is not None
    assert cfg.sla.alert.type == "email"
    assert cfg.sla.alert.email == "oncall@example.com"


def test_sla_with_webhook_alert_persists():
    cfg = _roundtrip(
        """
pipeline_name: sla_webhook
sla:
  max_duration_minutes: 10
  alert:
    type: webhook
    webhook_url: "https://hooks.example.com/abc"
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    config: { code: "print(1)" }
""",
        "sla_webhook",
    )
    assert cfg.sla.alert.type == "webhook"
    assert cfg.sla.alert.webhook_url == "https://hooks.example.com/abc"


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------

def test_triggers_file_sensor_and_completion_persist():
    cfg = _roundtrip(
        """
pipeline_name: triggers_demo
triggers:
  - type: file_sensor
    watch_path: "/data/drops/orders.csv"
    poll_interval_seconds: 60
  - type: pipeline_completion
    upstream_pipeline: "extract_orders"
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    config: { code: "print(1)" }
""",
        "triggers_demo",
    )
    assert cfg.triggers is not None
    assert len(cfg.triggers) == 2
    fs = cfg.triggers[0]
    assert fs.type == "file_sensor"
    assert fs.watch_path == "/data/drops/orders.csv"
    assert fs.poll_interval_seconds == 60
    comp = cfg.triggers[1]
    assert comp.type == "pipeline_completion"
    assert comp.upstream_pipeline == "extract_orders"


def test_file_sensor_without_watch_path_is_rejected():
    with pytest.raises(Exception):
        _roundtrip(
            """
pipeline_name: bad_trigger
triggers:
  - type: file_sensor
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    config: { code: "print(1)" }
""",
            "bad_trigger",
        )


# ---------------------------------------------------------------------------
# Per-task lineage
# ---------------------------------------------------------------------------

def test_per_task_lineage_persists():
    cfg = _roundtrip(
        """
pipeline_name: lineage_demo
tasks:
  - name: extract
    id: task_1
    type: executor
    plugin: python
    lineage:
      reads_from:
        - "postgres://db/public.orders"
        - "s3://raw/orders/*.csv"
      writes_to:
        - "duckdb://local/staging.orders"
    config: { code: "print(1)" }
""",
        "lineage_demo",
    )
    t = cfg.tasks[0]
    assert t.lineage is not None
    assert t.lineage.reads_from == ["postgres://db/public.orders", "s3://raw/orders/*.csv"]
    assert t.lineage.writes_to == ["duckdb://local/staging.orders"]


# ---------------------------------------------------------------------------
# Per-task quality
# ---------------------------------------------------------------------------

def test_per_task_quality_checks_persist_on_any_task():
    cfg = _roundtrip(
        """
pipeline_name: quality_demo
tasks:
  - name: load
    id: task_1
    type: executor
    plugin: python
    quality:
      checks:
        - name: row_count_positive
          sql: "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM data"
          expect: 0
        - name: order_total_range
          sql: "SELECT MAX(total) FROM data"
          expect_min: 1
          expect_max: 1000000
    config: { code: "print(1)" }
""",
        "quality_demo",
    )
    checks = cfg.tasks[0].quality.checks
    assert len(checks) == 2
    assert checks[0].name == "row_count_positive"
    assert checks[0].expect == 0
    assert checks[1].expect_min == 1
    assert checks[1].expect_max == 1000000


# ---------------------------------------------------------------------------
# Kitchen-sink + DAG build
# ---------------------------------------------------------------------------

def test_kitchen_sink_pipeline_loads_and_builds_dag():
    cfg = _roundtrip(
        """
pipeline_name: kitchen_sink
description: "everything the builder can emit"
team: "data-platform"
tags: ["finance", "daily"]

sla:
  max_duration_minutes: 30
  alert: { type: email, email: "oncall@example.com" }

triggers:
  - type: file_sensor
    watch_path: "/drops/orders.csv"
    poll_interval_seconds: 30

schedule:
  minute: "0"
  hour: "6"
  day: "*"
  month: "*"
  day_of_week: "*"

tasks:
  - name: sql_1
    id: task_1
    type: executor
    plugin: duckdb
    lineage:
      reads_from: ["postgres://db/public.orders"]
      writes_to:  ["duckdb://local/staging.orders"]
    quality:
      checks:
        - name: nn
          sql: "SELECT COUNT(*) FROM data WHERE id IS NULL"
          expect: 0
    config:
      sql: "SELECT 1"

  - name: py_1
    id: task_2
    type: executor
    plugin: python
    depends_on: [task_1]
    config: { code: "print(1)" }
""",
        "kitchen_sink",
    )
    assert cfg.team == "data-platform"
    assert cfg.tags == ["finance", "daily"]
    assert cfg.sla is not None and cfg.sla.alert.type == "email"
    assert len(cfg.triggers) == 1 and cfg.triggers[0].type == "file_sensor"
    assert cfg.schedule["hour"] == "6"
    assert cfg.tasks[0].lineage.reads_from[0].startswith("postgres://")
    assert len(cfg.tasks[0].quality.checks) == 1
    builder = DAGBuilder(cfg.tasks)
    builder.build()
    waves = builder.get_execution_waves()
    # waves is [['task_1'], ['task_2']] because depends_on uses ids
    assert waves == [["task_1"], ["task_2"]]


# ---------------------------------------------------------------------------
# Removed surfaces must STILL be rejected (regression guard for C3/C4)
# ---------------------------------------------------------------------------

def test_top_level_variables_block_is_still_rejected():
    with pytest.raises(Exception):
        _roundtrip(
            """
pipeline_name: legacy_variables
variables:
  foo: bar
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    config: { code: "print(1)" }
""",
            "legacy_variables",
        )


def test_task_depends_on_failure_is_still_rejected():
    with pytest.raises(Exception):
        _roundtrip(
            """
pipeline_name: legacy_failure
tasks:
  - name: t1
    id: task_1
    type: executor
    plugin: python
    depends_on_failure: [task_0]
    config: { code: "print(1)" }
""",
            "legacy_failure",
        )
