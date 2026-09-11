"""Tests for backfilling past windows.

The property that matters is not "N runs were queued" but "each run processes
its own period, once". A backfill that re-runs today N times, or that silently
re-runs a period that already succeeded, is worse than no backfill at all.
"""
from pathlib import Path

import pytest

import dataplatform.core.database as db
from dataplatform.core.backfill import (
    DEFAULT_MAX_RUNS,
    SKIP_ALREADY_COVERED,
    plan_backfill,
    submit_backfill,
)
from dataplatform.core.intervals import Interval
from dataplatform.core.parameters import RUN_PARAMETERS_KEY


_DEFAULT_SCHEDULE = {"hour": "6", "minute": "0"}
_UNSET = object()


class FakeConfig:
    def __init__(self, name="daily_revenue", schedule=_UNSET, file_path="pipelines/daily.yaml"):
        self.pipeline_name = name
        # A sentinel, not None-means-default: otherwise "this pipeline has no
        # schedule" is impossible to express and its test cannot fail.
        self.schedule = _DEFAULT_SCHEDULE if schedule is _UNSET else schedule
        self.file_path = file_path


@pytest.fixture()
def store(tmp_path, monkeypatch):
    db_file = tmp_path / "platform.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_file))
    db._initialized = False
    db._DB_PATH = Path(str(db_file))
    db._engine = None
    db.init_db()
    yield db
    db._initialized = False
    db._engine = None


class TestPlanning:
    def test_one_run_per_window_not_n_runs_of_today(self, store):
        plan = plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")

        starts = [run.interval.start for run in plan.to_run]
        assert starts == [
            "2026-01-01T06:00:00Z", "2026-01-02T06:00:00Z", "2026-01-03T06:00:00Z"
        ]
        assert len(set(starts)) == len(starts), "each run must own a distinct window"

    def test_a_pipeline_without_a_schedule_is_refused(self, store):
        with pytest.raises(ValueError) as exc:
            plan_backfill(FakeConfig(schedule=None), "2026-01-01T00:00:00Z",
                          "2026-01-05T00:00:00Z")
        assert "no schedule" in str(exc.value)

    def test_an_oversized_range_is_refused_before_queueing(self, store):
        with pytest.raises(ValueError):
            plan_backfill(FakeConfig(schedule={"minute": "*"}),
                          "2026-01-01T00:00:00Z", "2026-06-01T00:00:00Z", max_runs=50)

    def test_planning_queues_nothing(self, store):
        plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")
        assert store.get_queue_runs() == []

    def test_the_plan_is_printable_before_committing_to_it(self, store):
        plan = plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")
        summary = plan.summary()
        assert "3 window(s)" in summary and "will run  3" in summary


class TestIdempotency:
    def test_rerunning_the_same_backfill_queues_nothing(self, store):
        config = FakeConfig()
        first = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")
        submit_backfill(first)

        second = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")

        assert second.to_run == []
        assert len(second.skipped) == 3
        assert second.skipped[0].skip_reason == SKIP_ALREADY_COVERED

    def test_force_re_runs_a_covered_window(self, store):
        config = FakeConfig()
        submit_backfill(plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z"))

        forced = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z",
                               force=True)

        assert len(forced.to_run) == 3
        assert forced.skipped == []

    def test_a_partially_covered_range_fills_only_the_gap(self, store):
        config = FakeConfig()
        submit_backfill(plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z"))

        wider = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")

        assert [r.interval.ds for r in wider.to_run] == ["2026-01-02", "2026-01-03"]
        assert [r.interval.ds for r in wider.skipped] == ["2026-01-01"]

    def test_a_failed_window_is_still_treated_as_covered(self, store):
        # Deliberate: silently re-running a failure hides it. --force is how
        # you re-run it, and then it is a decision rather than a side effect.
        config = FakeConfig()
        result = submit_backfill(plan_backfill(config, "2026-01-01T00:00:00Z",
                                               "2026-01-03T00:00:00Z"))
        store.set_run_status_in_queue(result.run_ids[0], "failed", error="boom")

        again = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z")

        assert again.to_run == []
        assert again.skipped[0].existing_status == "failed"


class TestSubmission:
    def test_runs_carry_their_window_into_the_queue(self, store):
        result = submit_backfill(
            plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
        )
        rows = store.get_backfill_runs(result.backfill_id)

        assert len(rows) == 2
        assert rows[0]["logical_start"] == "2026-01-01T06:00:00Z"
        assert rows[0]["logical_end"] == "2026-01-02T06:00:00Z"

    def test_runs_are_queued_oldest_window_first(self, store):
        result = submit_backfill(
            plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-06T00:00:00Z")
        )
        starts = [row["logical_start"] for row in store.get_backfill_runs(result.backfill_id)]

        assert starts == sorted(starts)

    def test_the_group_is_identifiable(self, store):
        result = submit_backfill(
            plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
        )
        assert result.backfill_id.startswith("bf-")
        assert all(row["backfill_id"] == result.backfill_id
                   for row in store.get_backfill_runs(result.backfill_id))

    def test_two_backfills_do_not_share_a_group(self, store):
        config = FakeConfig()
        first = submit_backfill(plan_backfill(config, "2026-01-01T00:00:00Z",
                                              "2026-01-04T00:00:00Z"))
        second = submit_backfill(plan_backfill(config, "2026-02-01T00:00:00Z",
                                               "2026-02-04T00:00:00Z"))

        assert first.backfill_id != second.backfill_id
        assert len(store.get_backfill_runs(first.backfill_id)) == 2

    def test_submitting_an_empty_plan_queues_nothing(self, store):
        config = FakeConfig()
        submit_backfill(plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z"))
        empty = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")

        result = submit_backfill(empty)

        assert result.run_ids == []
        assert result.skipped == 2

    def test_a_plan_without_a_config_path_is_refused(self, store):
        plan = plan_backfill(FakeConfig(file_path=""), "2026-01-01T00:00:00Z",
                             "2026-01-04T00:00:00Z")
        with pytest.raises(ValueError):
            submit_backfill(plan)


class TestTheWindowReachesTheTask:
    """The whole point: a backfilled run must process its own period."""

    def test_the_worker_turns_a_queued_window_into_an_interval(self, store):
        from dataplatform.core.queue_worker import _interval_of

        result = submit_backfill(
            plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
        )
        row = store.get_backfill_runs(result.backfill_id)[0]

        interval = _interval_of(row)

        assert interval == Interval("2026-01-01T06:00:00Z", "2026-01-02T06:00:00Z")
        assert interval.ds == "2026-01-01"

    def test_an_adhoc_run_has_no_window_rather_than_a_fabricated_one(self, store):
        from dataplatform.core.queue_worker import _interval_of

        store.enqueue_run("adhoc", "daily_revenue", "p.yaml")
        assert _interval_of(store.get_queue_run("adhoc")) is None

    def test_the_interval_renders_into_a_task_config(self, store):
        from dataplatform.core.config import Task
        from dataplatform.core.executor import TaskExecutor
        from dataplatform.core.parameters import available_parameters

        interval = Interval("2026-01-01T06:00:00Z", "2026-01-02T06:00:00Z")
        task = Task(name="load", type="executor", plugin="duckdb",
                    config={"sql": "SELECT * FROM orders WHERE d >= '{{ logical_start }}' "
                                   "AND d < '{{ logical_end }}'"})
        captured = {}

        class FakePlugin:
            def execute(self, config):
                captured.update(config)
                return True, {}

        executor = TaskExecutor()
        executor.load_plugin = lambda *a, **k: FakePlugin()
        executor.execute_task(task, config={
            RUN_PARAMETERS_KEY: available_parameters(interval=interval)
        })

        assert captured["sql"] == (
            "SELECT * FROM orders WHERE d >= '2026-01-01T06:00:00Z' "
            "AND d < '2026-01-02T06:00:00Z'"
        )

    def test_two_backfilled_runs_render_different_periods(self, store):
        # The failure this guards: N runs that all process today.
        from dataplatform.core.parameters import available_parameters, render

        plan = plan_backfill(FakeConfig(), "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")
        rendered = [
            render("{{ ds }}", available_parameters(interval=run.interval))
            for run in plan.to_run
        ]

        assert rendered == ["2026-01-01", "2026-01-02", "2026-01-03"]
        assert len(set(rendered)) == 3
