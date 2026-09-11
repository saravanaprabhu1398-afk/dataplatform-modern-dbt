"""Tests for logical intervals and parameter substitution.

These two together are what a backfill stands on: a run has to know which
period it is for, and the task has to be able to see it. Either one missing and
"backfill" degrades to running today repeatedly.
"""
import pytest

from dataplatform.core.intervals import (
    Interval,
    fire_times,
    from_iso,
    intervals_between,
    to_iso,
)
from dataplatform.core.parameters import (
    RUN_PARAMETERS_KEY,
    UnknownParameter,
    available_parameters,
    placeholders_in,
    render,
    render_task_config,
)

DAILY_6AM = {"hour": "6", "minute": "0"}
HOURLY = {"minute": "0"}


class TestIntervals:
    def test_daily_schedule_yields_day_long_windows(self):
        windows = intervals_between(DAILY_6AM, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")

        assert [w.start for w in windows] == [
            "2026-01-01T06:00:00Z", "2026-01-02T06:00:00Z", "2026-01-03T06:00:00Z"
        ]
        assert windows[0].end == windows[1].start, "windows must not leave a gap"

    def test_windows_are_half_open(self):
        # Consecutive runs must not both own the same instant.
        windows = intervals_between(DAILY_6AM, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
        assert all(a.end == b.start for a, b in zip(windows, windows[1:]))

    def test_ds_is_the_start_date(self):
        window = intervals_between(DAILY_6AM, "2026-03-01T00:00:00Z", "2026-03-03T00:00:00Z")[0]
        assert window.ds == "2026-03-01"
        assert window.ds_end == "2026-03-02"

    def test_hourly_schedule(self):
        # Both endpoints fire, so 00:00..05:00 is six fire times and five
        # windows -- the last fire time starts a window the range does not close.
        windows = intervals_between(HOURLY, "2026-01-01T00:00:00Z", "2026-01-01T05:00:00Z")
        assert len(windows) == 5
        assert windows[0].start == "2026-01-01T00:00:00Z"
        assert windows[-1].end == "2026-01-01T05:00:00Z"

    def test_a_range_with_one_fire_time_has_no_window(self):
        # One fire time cannot bound a window; a backfill of it is a no-op
        # rather than a half-open interval running to infinity.
        assert intervals_between(DAILY_6AM, "2026-01-01T00:00:00Z", "2026-01-01T12:00:00Z") == []

    def test_empty_range_is_empty(self):
        assert intervals_between(DAILY_6AM, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z") == []

    def test_reversed_range_is_rejected(self):
        with pytest.raises(ValueError):
            intervals_between(DAILY_6AM, "2026-02-01T00:00:00Z", "2026-01-01T00:00:00Z")

    def test_a_schedule_without_cron_fields_is_rejected(self):
        with pytest.raises(ValueError):
            intervals_between({}, "2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")

    def test_an_enormous_range_is_refused_not_enumerated(self):
        # Quietly queueing half a million runs is an outage, not a backfill.
        with pytest.raises(ValueError) as exc:
            intervals_between({"minute": "*"}, "2026-01-01T00:00:00Z", "2026-12-31T00:00:00Z",
                              limit=100)
        assert "narrow it" in str(exc.value)

    def test_iso_round_trip_tolerates_microseconds(self):
        # Other tables store microseconds; intervals must still parse them.
        assert to_iso(from_iso("2026-01-01T06:00:00.123456Z")) == "2026-01-01T06:00:00Z"


class TestParameterAvailability:
    def test_interval_values_are_offered(self):
        values = available_parameters(interval=Interval("2026-01-01T06:00:00Z",
                                                        "2026-01-02T06:00:00Z"))
        assert values["ds"] == "2026-01-01"
        assert values["logical_end"] == "2026-01-02T06:00:00Z"

    def test_user_parameters_are_offered_bare_and_namespaced(self):
        values = available_parameters(runtime_parameters={"region": "EU"})
        assert values["region"] == "EU"
        assert values["params.region"] == "EU"

    def test_platform_values_win_over_a_colliding_user_parameter(self):
        # A pipeline with its own "ds" must not silently redefine the interval.
        values = available_parameters(
            interval=Interval("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
            runtime_parameters={"ds": "not-the-interval"},
        )
        assert values["ds"] == "2026-01-01"
        assert values["params.ds"] == "not-the-interval"

    def test_values_are_stringified(self):
        values = available_parameters(runtime_parameters={"n": 5, "flag": True, "none": None})
        assert values["n"] == "5" and values["flag"] == "True" and values["none"] == ""

    def test_nothing_available_without_a_run(self):
        assert available_parameters() == {}


class TestRendering:
    PARAMS = {"ds": "2026-01-01", "logical_end": "2026-01-02T06:00:00Z", "params.region": "EU"}

    def test_substitutes_into_a_string(self):
        assert render("date = '{{ ds }}'", self.PARAMS) == "date = '2026-01-01'"

    def test_tolerates_whitespace_variants(self):
        assert render("{{ds}} {{  ds  }}", self.PARAMS) == "2026-01-01 2026-01-01"

    def test_recurses_into_dicts_and_lists(self):
        rendered = render({"a": ["{{ ds }}", 3], "b": {"c": "{{ params.region }}"}}, self.PARAMS)
        assert rendered == {"a": ["2026-01-01", 3], "b": {"c": "EU"}}

    def test_non_strings_keep_their_type(self):
        assert render({"retries": 3, "on": True, "x": None}, self.PARAMS) == {
            "retries": 3, "on": True, "x": None
        }

    def test_unknown_parameter_raises_rather_than_blanking(self):
        # Blank substitution turns WHERE d >= '' into a query that succeeds and
        # returns the wrong rows.
        with pytest.raises(UnknownParameter) as exc:
            render("{{ nope }}", self.PARAMS)
        assert "nope" in str(exc.value)
        assert "ds" in str(exc.value), "the error should say what is available"

    def test_config_without_placeholders_is_returned_untouched(self):
        config = {"sql": "SELECT 1"}
        assert render_task_config(config, self.PARAMS) is config

    def test_placeholders_are_discoverable_for_validation(self):
        found = placeholders_in({"sql": "{{ ds }} {{ params.region }}", "n": 1})
        assert sorted(found) == ["ds", "params.region"]

    def test_a_literal_brace_is_left_alone(self):
        assert render("{ not a placeholder }", self.PARAMS) == "{ not a placeholder }"


class TestExecutorWiring:
    def test_the_executor_renders_a_task_config(self):
        from dataplatform.core.config import Task
        from dataplatform.core.executor import TaskExecutor

        task = Task(name="t", type="executor", plugin="python",
                    config={"sql": "SELECT '{{ ds }}'"})
        executor = TaskExecutor()
        captured = {}

        class FakePlugin:
            def execute(self, config):
                captured.update(config)
                return True, {"ok": True}

        executor.load_plugin = lambda *a, **k: FakePlugin()
        executor.execute_task(task, config={RUN_PARAMETERS_KEY: {"ds": "2026-01-01"}})

        assert captured["sql"] == "SELECT '2026-01-01'"
        assert RUN_PARAMETERS_KEY not in captured, "platform context must not reach the plugin"
