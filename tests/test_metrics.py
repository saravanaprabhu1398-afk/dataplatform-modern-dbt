"""Tests for the Prometheus metrics generator."""
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "metrics_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    yield
    db_module._initialized = False


from dataplatform.core.database import (
    init_db, save_run_status, save_quality_result, save_sla_violation
)
from dataplatform.core.metrics import generate_prometheus_text, _esc


class TestPrometheusTextFormat:
    def setup_method(self):
        init_db()

    def test_output_is_string(self):
        text = generate_prometheus_text()
        assert isinstance(text, str)

    def test_help_and_type_lines_present(self):
        text = generate_prometheus_text()
        assert "# HELP dp_pipeline_runs_total" in text
        assert "# TYPE dp_pipeline_runs_total counter" in text
        assert "# HELP dp_quality_check_results_total" in text
        assert "# HELP dp_sla_violations_total" in text

    def test_pipeline_run_counter_included(self):
        save_run_status("pipe_a", "r1", "completed", "done")
        save_run_status("pipe_a", "r2", "failed", "err")
        text = generate_prometheus_text()
        assert 'pipeline="pipe_a"' in text
        assert 'status="completed"' in text
        assert 'status="failed"' in text

    def test_run_count_value_correct(self):
        save_run_status("count_pipe", "r1", "completed", "ok")
        save_run_status("count_pipe", "r2", "completed", "ok")
        save_run_status("count_pipe", "r3", "completed", "ok")
        text = generate_prometheus_text()
        # Find the line with count_pipe + completed
        line = next(
            (l for l in text.splitlines()
             if 'count_pipe' in l and 'completed' in l and l.startswith("dp_pipeline_runs_total")),
            None
        )
        assert line is not None
        count = int(line.split()[-1])
        assert count == 3

    def test_quality_metrics_included(self):
        save_quality_result("r1", "qpipe", "t1", "check1", True, "42", "== 42")
        save_quality_result("r2", "qpipe", "t1", "check1", False, "0", "== 42")
        text = generate_prometheus_text()
        assert 'pipeline="qpipe"' in text
        assert 'result="passed"' in text
        assert 'result="failed"' in text

    def test_sla_violations_metric_included(self):
        save_sla_violation("r1", "sla_pipe", 120.0, 60.0, True)
        save_sla_violation("r2", "sla_pipe", 90.0, 60.0, False)
        text = generate_prometheus_text()
        assert 'pipeline="sla_pipe"' in text
        line = next(
            (l for l in text.splitlines()
             if 'sla_pipe' in l and l.startswith("dp_sla_violations_total")),
            None
        )
        assert line is not None
        assert int(line.split()[-1]) == 2

    def test_empty_db_produces_only_headers(self):
        text = generate_prometheus_text()
        # No data lines (no pipeline/quality/sla rows), just HELP/TYPE lines
        data_lines = [l for l in text.splitlines()
                      if l and not l.startswith("#") and l.strip()]
        assert data_lines == []


class TestLabelEscaping:
    def test_plain_string_unchanged(self):
        assert _esc("hello") == "hello"

    def test_double_quote_escaped(self):
        assert _esc('say "hi"') == 'say \\"hi\\"'

    def test_backslash_escaped(self):
        assert _esc("a\\b") == "a\\\\b"

    def test_newline_escaped(self):
        assert _esc("a\nb") == "a\\nb"
