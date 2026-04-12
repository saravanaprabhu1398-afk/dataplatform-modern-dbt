"""Tests for cost attribution (costs.py)."""
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "costs_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module.init_db()
    yield
    db_module._initialized = False


from dataplatform.core.costs import (
    record_run_cost,
    get_cost_summary,
    get_team_cost_summary,
    get_pipeline_cost_history,
    _COST_PER_TASK_SECOND,
)


class TestRecordRunCost:
    def test_returns_dict_with_expected_keys(self):
        result = record_run_cost("run-1", "pipe_a", "team_a", 3, 10.0)
        assert "run_id" in result
        assert "pipeline_name" in result
        assert "estimated_cost_usd" in result
        assert "task_count" in result
        assert "duration_seconds" in result

    def test_cost_calculation(self):
        result = record_run_cost("run-1", "pipe_a", None, 4, 100.0)
        expected = round(4 * 100.0 * _COST_PER_TASK_SECOND, 6)
        assert result["estimated_cost_usd"] == expected

    def test_zero_duration(self):
        result = record_run_cost("run-0", "pipe_a", None, 5, 0.0)
        assert result["estimated_cost_usd"] == 0.0

    def test_team_is_stored(self):
        record_run_cost("run-t", "pipe_x", "analytics", 2, 30.0)
        history = get_pipeline_cost_history("pipe_x")
        assert history[0]["team"] == "analytics"

    def test_none_team_allowed(self):
        result = record_run_cost("run-nt", "pipe_y", None, 1, 5.0)
        assert result["team"] is None

    def test_multiple_runs_stored(self):
        for i in range(3):
            record_run_cost(f"run-{i}", "repeated_pipe", "eng", 2, float(i * 10))
        history = get_pipeline_cost_history("repeated_pipe")
        assert len(history) == 3


class TestGetCostSummary:
    def test_empty_when_no_runs(self):
        assert get_cost_summary() == []

    def test_aggregates_by_pipeline(self):
        record_run_cost("r1", "pipe_a", "team_a", 2, 10.0)
        record_run_cost("r2", "pipe_a", "team_a", 2, 20.0)
        summary = get_cost_summary()
        row = next(r for r in summary if r["pipeline_name"] == "pipe_a")
        assert row["run_count"] == 2
        assert row["total_duration_seconds"] == pytest.approx(30.0)

    def test_sorted_by_cost_descending(self):
        record_run_cost("ra", "cheap_pipe", None, 1, 1.0)
        record_run_cost("rb", "expensive_pipe", None, 10, 1000.0)
        summary = get_cost_summary()
        names = [r["pipeline_name"] for r in summary]
        assert names.index("expensive_pipe") < names.index("cheap_pipe")


class TestGetTeamCostSummary:
    def test_empty_when_no_runs(self):
        assert get_team_cost_summary() == []

    def test_groups_by_team(self):
        record_run_cost("r1", "pipe_a", "team_eng", 2, 10.0)
        record_run_cost("r2", "pipe_b", "team_data", 3, 20.0)
        record_run_cost("r3", "pipe_c", "team_eng", 1, 5.0)
        summary = get_team_cost_summary()
        teams = {r["team"] for r in summary}
        assert "team_eng" in teams
        assert "team_data" in teams

    def test_none_team_becomes_unassigned(self):
        record_run_cost("rx", "pipe_no_team", None, 1, 5.0)
        summary = get_team_cost_summary()
        teams = [r["team"] for r in summary]
        assert "unassigned" in teams

    def test_pipeline_count_per_team(self):
        record_run_cost("r1", "pipe_a", "eng", 1, 1.0)
        record_run_cost("r2", "pipe_b", "eng", 1, 1.0)
        record_run_cost("r3", "pipe_a", "eng", 1, 1.0)  # same pipe_a again
        summary = get_team_cost_summary()
        row = next(r for r in summary if r["team"] == "eng")
        assert row["pipeline_count"] == 2  # pipe_a and pipe_b


class TestGetPipelineCostHistory:
    def test_empty_for_unknown_pipeline(self):
        assert get_pipeline_cost_history("ghost_pipe") == []

    def test_returns_history_newest_first(self):
        record_run_cost("ra", "ordered_pipe", None, 1, 10.0)
        record_run_cost("rb", "ordered_pipe", None, 1, 20.0)
        history = get_pipeline_cost_history("ordered_pipe")
        assert history[0]["recorded_at"] >= history[1]["recorded_at"]

    def test_limit_respected(self):
        for i in range(10):
            record_run_cost(f"r{i}", "limited_pipe", None, 1, float(i))
        history = get_pipeline_cost_history("limited_pipe", limit=4)
        assert len(history) == 4

    def test_has_expected_keys(self):
        record_run_cost("rk", "key_pipe", "team", 2, 5.0)
        row = get_pipeline_cost_history("key_pipe")[0]
        for key in ("run_id", "pipeline_name", "team", "task_count",
                    "duration_seconds", "estimated_cost_usd", "recorded_at"):
            assert key in row
