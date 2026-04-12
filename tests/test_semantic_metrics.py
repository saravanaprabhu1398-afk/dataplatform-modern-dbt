"""Tests for the semantic metrics layer (semantic_metrics.py)."""
import pytest
from pathlib import Path
from unittest.mock import patch


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "metrics_sm_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module.init_db()
    yield
    db_module._initialized = False


from dataplatform.core.semantic_metrics import (
    load_metric,
    list_metrics,
    compute_metric,
    get_history,
    _METRICS_DIR,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_metric_yaml(directory: Path, filename: str, content: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    p = directory / filename
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# load_metric
# ---------------------------------------------------------------------------

class TestLoadMetric:
    def test_valid_metric_loaded(self, tmp_path):
        p = write_metric_yaml(tmp_path, "rev.yaml", """
metric_name: revenue
sql: SELECT 42
description: Test metric
owner: eng_team
""")
        m = load_metric(str(p))
        assert m["metric_name"] == "revenue"
        assert m["sql"] == "SELECT 42"
        assert m["description"] == "Test metric"
        assert m["owner"] == "eng_team"

    def test_missing_metric_name_raises(self, tmp_path):
        p = write_metric_yaml(tmp_path, "bad.yaml", "sql: SELECT 1\n")
        with pytest.raises(ValueError, match="metric_name"):
            load_metric(str(p))

    def test_missing_sql_raises(self, tmp_path):
        p = write_metric_yaml(tmp_path, "bad2.yaml", "metric_name: foo\n")
        with pytest.raises(ValueError, match="sql"):
            load_metric(str(p))

    def test_optional_fields_default_empty(self, tmp_path):
        p = write_metric_yaml(tmp_path, "min.yaml", "metric_name: m\nsql: SELECT 1\n")
        m = load_metric(str(p))
        assert m["description"] == ""
        assert m["owner"] == ""

    def test_file_path_included(self, tmp_path):
        p = write_metric_yaml(tmp_path, "fp.yaml", "metric_name: m\nsql: SELECT 1\n")
        m = load_metric(str(p))
        assert str(p) in m["file_path"]


# ---------------------------------------------------------------------------
# list_metrics
# ---------------------------------------------------------------------------

class TestListMetrics:
    def test_empty_when_metrics_dir_missing(self, monkeypatch):
        monkeypatch.setattr(
            "dataplatform.core.semantic_metrics._METRICS_DIR",
            Path("/nonexistent_dir_12345"),
        )
        assert list_metrics() == []

    def test_lists_valid_metrics(self, tmp_path, monkeypatch):
        monkeypatch.setattr("dataplatform.core.semantic_metrics._METRICS_DIR", tmp_path)
        write_metric_yaml(tmp_path, "m1.yaml", "metric_name: alpha\nsql: SELECT 1\n")
        write_metric_yaml(tmp_path, "m2.yaml", "metric_name: beta\nsql: SELECT 2\n")

        metrics = list_metrics()
        names = [m["metric_name"] for m in metrics]
        assert "alpha" in names
        assert "beta" in names

    def test_skips_invalid_yaml_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr("dataplatform.core.semantic_metrics._METRICS_DIR", tmp_path)
        write_metric_yaml(tmp_path, "good.yaml", "metric_name: ok\nsql: SELECT 1\n")
        write_metric_yaml(tmp_path, "bad.yaml", "not_valid: true\n")  # missing required keys

        metrics = list_metrics()
        assert len(metrics) == 1
        assert metrics[0]["metric_name"] == "ok"

    def test_no_sql_in_listing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("dataplatform.core.semantic_metrics._METRICS_DIR", tmp_path)
        write_metric_yaml(tmp_path, "nosql.yaml", "metric_name: m\nsql: SELECT SECRET\n")
        metrics = list_metrics()
        assert "sql" not in metrics[0]


# ---------------------------------------------------------------------------
# compute_metric
# ---------------------------------------------------------------------------

class TestComputeMetric:
    def test_simple_scalar_returns_value(self):
        result = compute_metric("test_metric", "SELECT 42")
        assert result["value"] == 42.0
        assert result["error"] is None
        assert result["metric_name"] == "test_metric"

    def test_null_result_stored_as_none(self):
        result = compute_metric("null_metric", "SELECT NULL")
        assert result["value"] is None
        assert result["error"] is None

    def test_sql_error_stored_as_error(self):
        result = compute_metric("bad_metric", "SELECT * FROM nonexistent_xyz")
        assert result["value"] is None
        assert result["error"] is not None

    def test_computed_at_is_set(self):
        result = compute_metric("ts_metric", "SELECT 1")
        assert result["computed_at"].endswith("Z")

    def test_float_coercion(self):
        result = compute_metric("float_metric", "SELECT 3.14")
        assert abs(result["value"] - 3.14) < 1e-6

    def test_result_persisted_to_db(self):
        compute_metric("db_metric", "SELECT 99")
        history = get_history("db_metric")
        assert len(history) == 1
        assert history[0]["value"] == 99.0

    def test_multiple_computes_all_stored(self):
        for _ in range(3):
            compute_metric("multi_metric", "SELECT 1")
        history = get_history("multi_metric")
        assert len(history) == 3


# ---------------------------------------------------------------------------
# get_history
# ---------------------------------------------------------------------------

class TestGetHistory:
    def test_empty_history_for_unknown_metric(self):
        assert get_history("no_such_metric") == []

    def test_history_newest_first(self):
        compute_metric("ordered_metric", "SELECT 1")
        compute_metric("ordered_metric", "SELECT 2")
        history = get_history("ordered_metric")
        assert history[0]["computed_at"] >= history[1]["computed_at"]

    def test_limit_respected(self):
        for i in range(10):
            compute_metric("limited_metric", f"SELECT {i}")
        history = get_history("limited_metric", limit=4)
        assert len(history) == 4

    def test_history_has_expected_keys(self):
        compute_metric("key_metric", "SELECT 7")
        row = get_history("key_metric")[0]
        assert "metric_name" in row
        assert "value" in row
        assert "error" in row
        assert "computed_at" in row
