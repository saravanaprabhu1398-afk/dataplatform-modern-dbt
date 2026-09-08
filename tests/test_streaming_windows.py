"""Tests for event-time windowing, watermarks and late data (Week 3).

Two things are being pinned down here.  First, that lateness is decided by the
watermark and nothing else -- not arrival order, not batch boundaries.  Second,
that a record the pipeline declines to count is *recorded* rather than lost:
windows plus side output must always reconcile to the manifest.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import dataplatform.core.database as db
from dataplatform.core.config import StreamingWindows
from dataplatform.core.durations import parse_duration
from dataplatform.plugins.base import Fence, Offsets
from dataplatform.streaming.generator import GeneratorConfig, generate
from dataplatform.streaming.model import to_iso
from dataplatform.streaming.sinks import SqlTransactionalSink
from dataplatform.streaming.sources import JsonlSource, partition_for
from dataplatform.streaming.runner import run_stream
from dataplatform.streaming.verifier import verify, verify_window_accounting
from dataplatform.streaming.windows import (
    ON_LATE_SIDE_OUTPUT,
    ON_LATE_UPDATE,
    REASON_BEYOND_LATENESS,
    REASON_CORRECTION,
    WatermarkTracker,
    WindowPlanner,
    WindowPolicy,
)

BASE = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
STREAM = "wtest"
TABLE = "wtest_sink"


def at(**delta):
    """An ISO event time offset from a fixed base."""
    return to_iso(BASE + timedelta(**delta))


def record(key="k1", seq=0, event_time=None, amount=100):
    return {
        "key": key,
        "seq": seq,
        "event_time": event_time or at(minutes=1),
        "ingest_time": at(minutes=1),
        "amount_cents": amount,
        "payload": "p",
        "checksum": "c",
    }


class FrozenClock:
    def __init__(self, start=None):
        self.now = start or BASE

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now = self.now + timedelta(seconds=seconds)


POLICY = WindowPolicy(
    window_seconds=3600,
    out_of_orderness_seconds=300,
    allowed_lateness_seconds=21600,
    idle_partition_timeout_seconds=60,
)


class TestDurations:
    @pytest.mark.parametrize(
        "value,expected",
        [("30s", 30), ("90m", 5400), ("6h", 21600), ("2d", 172800), (45, 45), ("45", 45)],
    )
    def test_parses(self, value, expected):
        assert parse_duration(value) == expected

    @pytest.mark.parametrize("value", ["banana", "6 hours", "", -1, True, None])
    def test_rejects_nonsense(self, value):
        with pytest.raises((ValueError, TypeError)):
            parse_duration(value)


class TestConfigModel:
    def test_accepts_duration_strings(self):
        config = StreamingWindows(window="1h", out_of_orderness="5m", allowed_lateness="6h")
        assert (config.window, config.out_of_orderness, config.allowed_lateness) == (
            3600, 300, 21600
        )

    def test_rejects_zero_window(self):
        with pytest.raises(Exception):
            StreamingWindows(window="0s")

    def test_policy_reads_from_config(self):
        policy = WindowPolicy.from_config(StreamingWindows(window="2h", on_late="update"))
        assert policy.window_seconds == 7200
        assert policy.on_late == ON_LATE_UPDATE


class TestWatermark:
    def test_none_before_any_record(self):
        assert WatermarkTracker(POLICY).watermark() is None

    def test_lags_the_frontier_by_out_of_orderness(self):
        tracker = WatermarkTracker(POLICY, now_fn=FrozenClock())
        tracker.observe("0", at(hours=2))
        assert tracker.watermark() == at(hours=2, minutes=-5)

    def test_takes_the_minimum_across_partitions(self):
        tracker = WatermarkTracker(POLICY, now_fn=FrozenClock())
        tracker.observe("0", at(hours=5))
        tracker.observe("1", at(hours=2))
        assert tracker.watermark() == at(hours=2, minutes=-5)

    def test_idle_partition_stops_holding_the_watermark_back(self):
        clock = FrozenClock()
        tracker = WatermarkTracker(POLICY, now_fn=clock)
        tracker.observe("0", at(hours=1))
        tracker.observe("1", at(hours=5))

        assert tracker.watermark() == at(hours=1, minutes=-5)

        clock.advance(120)          # partition 0 has gone quiet
        tracker.observe("1", at(hours=6))

        assert tracker.idle_partitions() == {"0"}
        assert tracker.watermark() == at(hours=6, minutes=-5)

    def test_all_partitions_idle_does_not_advance_the_watermark(self):
        # Silence is not evidence that time passed in the stream.
        clock = FrozenClock()
        tracker = WatermarkTracker(POLICY, now_fn=clock)
        tracker.observe("0", at(hours=1))
        tracker.observe("1", at(hours=5))
        clock.advance(600)

        assert tracker.idle_partitions() == {"0", "1"}
        assert tracker.watermark() == at(hours=1, minutes=-5)

    def test_restore_rehydrates_progress(self):
        tracker = WatermarkTracker(POLICY, now_fn=FrozenClock())
        tracker.restore({"0": at(hours=3), "1": at(hours=4)})
        assert tracker.watermark() == at(hours=3, minutes=-5)


class TestPlanner:
    def _planner(self, policy=POLICY, clock=None):
        tracker = WatermarkTracker(policy, now_fn=clock or FrozenClock())
        return WindowPlanner(policy, tracker), tracker

    def test_on_time_records_only_produce_deltas(self):
        planner, _ = self._planner()
        plan = planner.plan([record(seq=i, event_time=at(minutes=i)) for i in range(5)],
                            partition_of=lambda r: "0")

        assert plan.deltas == {at(hours=0): (5, 500)}
        assert plan.corrections == []
        assert plan.late_records == []

    def test_lateness_is_judged_at_the_start_of_the_batch(self):
        # A record must not be late because of a watermark its own batch
        # advanced -- otherwise batch boundaries would change the answer.
        planner, _ = self._planner()
        batch = [
            record(seq=0, event_time=at(hours=20)),   # jumps the frontier
            record(seq=1, event_time=at(minutes=5)),  # would look ancient after it
        ]
        plan = planner.plan(batch, partition_of=lambda r: "0")

        assert plan.late_records == []
        assert plan.deltas[at(hours=0)] == (1, 100)

    def test_late_within_allowed_lateness_is_corrected(self):
        planner, _ = self._planner()
        planner.plan([record(seq=0, event_time=at(hours=3))], partition_of=lambda r: "0")

        plan = planner.plan([record(seq=1, event_time=at(minutes=30))],
                            partition_of=lambda r: "0")

        assert plan.deltas == {at(hours=0): (1, 100)}
        assert [c.reason for c in plan.corrections] == [REASON_CORRECTION]
        assert len(plan.late_records) == 1
        assert plan.late_records[0].counted is True
        assert plan.late_records[0].lateness_seconds > 0

    def test_beyond_allowed_lateness_goes_to_side_output(self):
        planner, _ = self._planner()
        planner.plan([record(seq=0, event_time=at(hours=48))], partition_of=lambda r: "0")

        plan = planner.plan([record(seq=1, event_time=at(minutes=30))],
                            partition_of=lambda r: "0")

        assert plan.deltas == {}
        assert plan.corrections == []
        assert plan.late_records[0].counted is False
        assert plan.late_records[0].reason == REASON_BEYOND_LATENESS

    def test_on_late_update_still_counts_very_late_records(self):
        policy = WindowPolicy(
            window_seconds=3600, out_of_orderness_seconds=300,
            allowed_lateness_seconds=21600, on_late=ON_LATE_UPDATE,
        )
        planner, _ = self._planner(policy)
        planner.plan([record(seq=0, event_time=at(hours=48))], partition_of=lambda r: "0")

        plan = planner.plan([record(seq=1, event_time=at(minutes=30))],
                            partition_of=lambda r: "0")

        assert plan.deltas == {at(hours=0): (1, 100)}
        assert plan.late_records[0].counted is True

    def test_reports_windows_the_watermark_closed(self):
        planner, _ = self._planner()
        plan = planner.plan(
            [record(seq=0, event_time=at(minutes=10)), record(seq=1, event_time=at(hours=3))],
            partition_of=lambda r: "0",
        )
        assert at(hours=0) in plan.closed_windows
        assert at(hours=3) not in plan.closed_windows

    def test_default_policy_is_side_output(self):
        assert POLICY.on_late == ON_LATE_SIDE_OUTPUT


@pytest.fixture()
def sink_env(tmp_path, monkeypatch):
    db_file = tmp_path / "platform.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_file))
    db._initialized = False
    db._DB_PATH = Path(str(db_file))
    db._engine = None
    db.init_db()
    yield tmp_path
    db._initialized = False
    db._engine = None


class TestSinkWindowState:
    def _sink(self, policy=POLICY):
        return SqlTransactionalSink(
            table=TABLE, stream=STREAM, windowing=policy,
            partition_of=lambda r: "0",
        )

    def test_windows_accumulate_and_close(self, sink_env):
        sink = self._sink()
        sink.commit(
            [record(seq=i, event_time=at(minutes=i)) for i in range(4)],
            Offsets({"0": 4}), Fence(STREAM, 1),
        )
        sink.commit([record(seq=9, event_time=at(hours=4))], Offsets({"0": 5}), Fence(STREAM, 1))

        windows = {row["window_start"]: row for row in sink.window_rows()}
        assert windows[at(hours=0)]["event_count"] == 4
        assert windows[at(hours=0)]["closed_at"] is not None
        assert windows[at(hours=4)]["closed_at"] is None

    def test_correction_bumps_revision_and_records_the_delta(self, sink_env):
        sink = self._sink()
        sink.commit([record(seq=0, event_time=at(hours=3))], Offsets({"0": 1}), Fence(STREAM, 1))
        sink.commit(
            [record(seq=1, event_time=at(minutes=30), amount=250)],
            Offsets({"0": 2}), Fence(STREAM, 1),
        )

        window = next(r for r in sink.window_rows() if r["window_start"] == at(hours=0))
        corrections = sink.correction_rows()

        assert window["revision"] == 1
        assert window["event_count"] == 1
        assert corrections[0]["delta_count"] == 1
        assert corrections[0]["delta_amount_cents"] == 250
        assert corrections[0]["reason"] == REASON_CORRECTION

    def test_side_output_is_recorded_not_counted(self, sink_env):
        sink = self._sink()
        sink.commit([record(seq=0, event_time=at(hours=48))], Offsets({"0": 1}), Fence(STREAM, 1))
        sink.commit([record(seq=1, event_time=at(minutes=30))], Offsets({"0": 2}), Fence(STREAM, 1))

        late = sink.late_rows()
        assert len(late) == 1
        assert late[0]["counted"] == 0
        assert not any(r["window_start"] == at(hours=0) for r in sink.window_rows())

    def test_replayed_batch_does_not_double_count(self, sink_env):
        # Records upsert idempotently, but window deltas are additive, so a
        # commit that advances no offset must not touch the aggregates.
        sink = self._sink()
        batch = [record(seq=i, event_time=at(minutes=i)) for i in range(3)]

        sink.commit(batch, Offsets({"0": 3}), Fence(STREAM, 1))
        sink.commit(batch, Offsets({"0": 3}), Fence(STREAM, 1))

        window = next(r for r in sink.window_rows() if r["window_start"] == at(hours=0))
        assert window["event_count"] == 3
        assert len(sink.rows()) == 3

    def test_watermark_survives_a_restart(self, sink_env):
        sink = self._sink()
        sink.commit([record(seq=0, event_time=at(hours=6))], Offsets({"0": 1}), Fence(STREAM, 1))
        before = sink.watermark

        revived = self._sink()

        assert before is not None
        assert revived.watermark == before

    def test_failed_transaction_leaves_window_state_untouched(self, sink_env, monkeypatch):
        sink = self._sink()
        sink.commit([record(seq=0, event_time=at(minutes=5))], Offsets({"0": 1}), Fence(STREAM, 1))
        before_windows = sink.window_rows()
        before_watermark = sink.watermark

        def boom(point):
            if point == "before_commit":
                raise RuntimeError("killed before commit")

        monkeypatch.setattr("dataplatform.streaming.sinks.maybe_crash", boom)
        with pytest.raises(RuntimeError):
            sink.commit(
                [record(seq=1, event_time=at(hours=9))], Offsets({"0": 2}), Fence(STREAM, 1)
            )

        assert sink.window_rows() == before_windows
        # The in-memory watermark was rolled back to what the database says.
        assert sink.watermark == before_watermark

    def test_windowing_off_leaves_the_tables_empty(self, sink_env):
        sink = SqlTransactionalSink(table=TABLE, stream=STREAM)
        sink.commit([record(seq=0)], Offsets({"0": 1}), Fence(STREAM, 1))

        assert sink.window_rows() == []
        assert sink.late_rows() == []
        assert sink.watermark is None


class TestAccountingClosesEndToEnd:
    def _stream(self, tmp_path, seed=7):
        # 300 events at 144s spans 12 hours, which is the only way a window
        # can outlive a 6h allowed lateness and reach the side output.
        config = GeneratorConfig(
            keys=8, events_per_key=300, seed=seed, event_interval_seconds=144,
            late_fraction=0.1, late_delay_seconds=5400,
            very_late_fraction=0.03, very_late_delay_seconds=172800,
        )
        stream = generate(config)
        stream.write(str(tmp_path / "events.jsonl"), str(tmp_path / "manifest.json"))
        return stream

    def test_every_record_is_in_a_window_or_the_side_output(self, sink_env):
        stream = self._stream(sink_env)
        source = JsonlSource(str(sink_env / "events.jsonl"), partitions=4)
        sink = SqlTransactionalSink(
            table=TABLE, stream=STREAM, windowing=POLICY,
            partition_of=lambda r: partition_for(str(r["key"]), 4),
        )

        run_stream(source, sink, Fence(STREAM, 1), batch_size=50)

        rows = sink.rows()
        assert verify(stream.manifest, rows).ok

        accounting = verify_window_accounting(
            stream.manifest, sink.window_rows(), sink.late_rows(), sink_rows=rows
        )
        assert accounting.ok, accounting.summary()
        assert accounting.late_side_output > 0, "the 48h stragglers should be side output"
        assert accounting.late_counted > 0, "the 1.5h arrivals should be corrections"
        assert accounting.missing_from_both == 0

    def test_side_output_records_are_absent_from_window_totals(self, sink_env):
        stream = self._stream(sink_env, seed=11)
        source = JsonlSource(str(sink_env / "events.jsonl"), partitions=4)
        sink = SqlTransactionalSink(
            table=TABLE, stream=STREAM, windowing=POLICY,
            partition_of=lambda r: partition_for(str(r["key"]), 4),
        )
        run_stream(source, sink, Fence(STREAM, 1), batch_size=50)

        windowed = sum(row["event_count"] for row in sink.window_rows())
        side_output = sum(1 for row in sink.late_rows() if not row["counted"])

        assert windowed + side_output == stream.manifest.total_unique
        assert len(sink.rows()) == stream.manifest.total_unique


class TestWindowStateSurvivesAKill:
    """Window aggregates are additive, so a crash is where they would drift."""

    def _generate(self, tmp_path):
        stream = generate(
            GeneratorConfig(
                keys=8, events_per_key=300, seed=31, event_interval_seconds=144,
                late_fraction=0.1, late_delay_seconds=5400,
                very_late_fraction=0.03, very_late_delay_seconds=172800,
            )
        )
        stream.write(str(tmp_path / "events.jsonl"), str(tmp_path / "manifest.json"))
        return stream

    def _run(self, tmp_path, chaos=None):
        import os
        import subprocess
        import sys

        env = dict(os.environ)
        env["DATABASE_PATH"] = str(tmp_path / "platform.db")
        env.pop("DATAPLATFORM_CHAOS", None)
        if chaos:
            env["DATAPLATFORM_CHAOS"] = chaos
        return subprocess.run(
            [
                sys.executable, "-m", "dataplatform.streaming.runner",
                "--stream", str(tmp_path),
                "--table", TABLE,
                "--stream-name", STREAM,
                "--batch-size", "250",
            ],
            env=env, capture_output=True, text=True,
        )

    def test_kill_mid_batch_does_not_double_count_windows(self, sink_env):
        from dataplatform.core.chaos import CRASH_EXIT_CODE

        stream = self._generate(sink_env)

        killed = self._run(sink_env, chaos="after_write:3")
        assert killed.returncode == CRASH_EXIT_CODE, killed.stderr
        assert self._run(sink_env).returncode == 0

        sink = SqlTransactionalSink(
            table=TABLE, stream=STREAM, windowing=POLICY,
            partition_of=lambda r: partition_for(str(r["key"]), 4),
        )
        rows = sink.rows()
        accounting = verify_window_accounting(
            stream.manifest, sink.window_rows(), sink.late_rows(), sink_rows=rows
        )

        assert verify(stream.manifest, rows).ok
        assert accounting.ok, accounting.summary()
        windowed = sum(row["event_count"] for row in sink.window_rows())
        side_output = sum(1 for row in sink.late_rows() if not row["counted"])
        assert windowed + side_output == stream.manifest.total_unique
