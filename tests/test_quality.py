"""Tests for the DuckDB-based quality check runner."""
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "quality_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    yield
    db_module._initialized = False


from dataplatform.core.config import QualityCheck
from dataplatform.core.database import init_db, get_quality_results
from dataplatform.core.quality import run_quality_checks, get_pipeline_quality_history, _evaluate


class TestEvaluate:
    def test_exact_match_passes(self):
        check = QualityCheck(name="x", sql="", expect=0)
        assert _evaluate(check, 0) is True

    def test_exact_match_fails(self):
        check = QualityCheck(name="x", sql="", expect=0)
        assert _evaluate(check, 1) is False

    def test_numeric_coercion(self):
        check = QualityCheck(name="x", sql="", expect=0)
        assert _evaluate(check, 0.0) is True

    def test_expect_min_passes(self):
        check = QualityCheck(name="x", sql="", expect_min=10)
        assert _evaluate(check, 15) is True

    def test_expect_min_fails(self):
        check = QualityCheck(name="x", sql="", expect_min=10)
        assert _evaluate(check, 5) is False

    def test_expect_max_passes(self):
        check = QualityCheck(name="x", sql="", expect_max=100)
        assert _evaluate(check, 50) is True

    def test_expect_max_fails(self):
        check = QualityCheck(name="x", sql="", expect_max=100)
        assert _evaluate(check, 200) is False

    def test_range_both_bounds(self):
        check = QualityCheck(name="x", sql="", expect_min=1, expect_max=10)
        assert _evaluate(check, 5) is True
        assert _evaluate(check, 0) is False
        assert _evaluate(check, 11) is False

    def test_no_expectation_any_value_passes(self):
        check = QualityCheck(name="x", sql="")
        assert _evaluate(check, 42) is True
        assert _evaluate(check, None) is True

    def test_none_actual_fails_numeric_check(self):
        check = QualityCheck(name="x", sql="", expect_min=1)
        assert _evaluate(check, None) is False


class TestRunQualityChecks:
    def setup_method(self):
        init_db()

    def test_passing_scalar_check(self):
        checks = [QualityCheck(name="one_plus_one", sql="SELECT 1 + 1", expect=2)]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        assert len(results) == 1
        assert results[0]["passed"] is True
        assert results[0]["actual"] == 2

    def test_failing_exact_check(self):
        checks = [QualityCheck(name="wrong", sql="SELECT 99", expect=0)]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        assert results[0]["passed"] is False
        assert results[0]["actual"] == 99

    def test_passing_min_check(self):
        checks = [QualityCheck(name="big_enough", sql="SELECT 100", expect_min=50)]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        assert results[0]["passed"] is True

    def test_failing_min_check(self):
        checks = [QualityCheck(name="too_small", sql="SELECT 5", expect_min=50)]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        assert results[0]["passed"] is False

    def test_sql_error_returns_failed_with_error(self):
        checks = [QualityCheck(name="bad_sql", sql="SELECT * FROM nonexistent_table_xyz")]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        assert results[0]["passed"] is False
        assert results[0]["error"] is not None

    def test_multiple_checks_independent(self):
        checks = [
            QualityCheck(name="pass1", sql="SELECT 1", expect=1),
            QualityCheck(name="fail1", sql="SELECT 2", expect=99),
            QualityCheck(name="pass2", sql="SELECT 3", expect=3),
        ]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        statuses = {r["name"]: r["passed"] for r in results}
        assert statuses["pass1"] is True
        assert statuses["fail1"] is False
        assert statuses["pass2"] is True

    def test_results_persisted_to_db(self):
        checks = [QualityCheck(name="persist_me", sql="SELECT 42", expect=42)]
        run_quality_checks(checks, "r1", "my_pipe", "my_task")
        db_results = get_quality_results("my_pipe")
        assert any(r["check_name"] == "persist_me" for r in db_results)

    def test_passed_flag_in_db(self):
        checks = [
            QualityCheck(name="ok", sql="SELECT 1", expect=1),
            QualityCheck(name="bad", sql="SELECT 2", expect=999),
        ]
        run_quality_checks(checks, "r1", "pipe2", "t1")
        db_results = get_quality_results("pipe2")
        by_name = {r["check_name"]: r for r in db_results}
        assert by_name["ok"]["passed"] is True
        assert by_name["bad"]["passed"] is False

    def test_string_equality_check(self):
        checks = [QualityCheck(name="str_eq", sql="SELECT 'hello'", expect="hello")]
        results = run_quality_checks(checks, "r1", "pipe", "task")
        assert results[0]["passed"] is True


class TestGetPipelineQualityHistory:
    def setup_method(self):
        init_db()

    def test_returns_results_for_pipeline(self):
        checks = [QualityCheck(name="c1", sql="SELECT 1", expect=1)]
        run_quality_checks(checks, "r1", "hist_pipe", "t1")
        history = get_pipeline_quality_history("hist_pipe")
        assert len(history) >= 1

    def test_returns_empty_for_unknown_pipeline(self):
        assert get_pipeline_quality_history("no_such_pipe") == []
