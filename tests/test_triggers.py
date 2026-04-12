"""Tests for event-driven triggers (TriggerManager)."""
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from dataplatform.core.triggers import TriggerManager


class TestFileSensor:
    def test_fires_callback_on_mtime_change(self, tmp_path):
        """Callback is invoked when a watched file's mtime changes."""
        watch_file = tmp_path / "input.csv"
        watch_file.write_text("v1")

        fired = threading.Event()
        manager = TriggerManager()

        manager.register_file_sensor(
            trigger_id="t1",
            watch_path=str(watch_file),
            callback=lambda: fired.set(),
            poll_interval=0,  # poll as fast as possible
        )

        # Give sensor time to detect initial state without firing
        time.sleep(0.05)
        # Modify file to trigger callback
        watch_file.write_text("v2")

        assert fired.wait(timeout=2.0), "Callback was not fired after file change"
        manager.stop_all()

    def test_no_fire_on_initial_detection(self, tmp_path):
        """Pre-existing file does not trigger callback on first poll."""
        watch_file = tmp_path / "input.csv"
        watch_file.write_text("existing content")

        fired = threading.Event()
        manager = TriggerManager()

        manager.register_file_sensor(
            trigger_id="t2",
            watch_path=str(watch_file),
            callback=lambda: fired.set(),
            poll_interval=0,
        )

        time.sleep(0.1)
        manager.stop_all()
        assert not fired.is_set(), "Callback fired on initial file detection (should not)"

    def test_no_fire_on_nonexistent_file(self, tmp_path):
        """No crash or fire when watched path does not exist."""
        fired = threading.Event()
        manager = TriggerManager()

        manager.register_file_sensor(
            trigger_id="t3",
            watch_path=str(tmp_path / "missing.csv"),
            callback=lambda: fired.set(),
            poll_interval=0,
        )

        time.sleep(0.1)
        manager.stop_all()
        assert not fired.is_set()

    def test_callback_error_does_not_crash_sensor(self, tmp_path):
        """A callback that raises does not kill the sensor thread."""
        watch_file = tmp_path / "data.csv"
        watch_file.write_text("v1")

        call_count = [0]

        def bad_callback():
            call_count[0] += 1
            raise RuntimeError("callback exploded")

        manager = TriggerManager()
        manager.register_file_sensor(
            trigger_id="t4",
            watch_path=str(watch_file),
            callback=bad_callback,
            poll_interval=0,
        )

        time.sleep(0.05)
        watch_file.write_text("v2")
        time.sleep(0.1)
        manager.stop_all()
        # Sensor should have survived; no exception propagated
        assert call_count[0] >= 1

    def test_unregister_stops_sensor(self, tmp_path):
        """After unregister(), file changes no longer fire the callback."""
        watch_file = tmp_path / "data.csv"
        watch_file.write_text("v1")

        fired_after = threading.Event()
        manager = TriggerManager()

        manager.register_file_sensor(
            trigger_id="t5",
            watch_path=str(watch_file),
            callback=lambda: fired_after.set(),
            poll_interval=0,
        )

        time.sleep(0.05)  # let it see initial state
        found = manager.unregister("t5")
        assert found is True

        watch_file.write_text("v2")
        time.sleep(0.1)
        assert not fired_after.is_set(), "Callback fired after unregister()"


class TestCompletionTrigger:
    def test_fires_on_notify(self):
        """Completion callback fires when notify_pipeline_completed is called."""
        received = []
        manager = TriggerManager()

        manager.register_completion_trigger(
            trigger_id="ct1",
            upstream_pipeline="upstream_pipe",
            callback=lambda pname, rid: received.append((pname, rid)),
        )

        manager.notify_pipeline_completed("upstream_pipe", "run-123")
        assert len(received) == 1
        assert received[0] == ("upstream_pipe", "run-123")

    def test_no_fire_for_different_pipeline(self):
        """Callback is not invoked for a different pipeline name."""
        received = []
        manager = TriggerManager()

        manager.register_completion_trigger(
            trigger_id="ct2",
            upstream_pipeline="pipe_a",
            callback=lambda pname, rid: received.append(pname),
        )

        manager.notify_pipeline_completed("pipe_b", "run-456")
        assert received == []

    def test_multiple_callbacks_same_upstream(self):
        """Multiple callbacks registered on the same upstream all fire."""
        log = []
        manager = TriggerManager()

        manager.register_completion_trigger("c1", "shared_upstream", lambda p, r: log.append("c1"))
        manager.register_completion_trigger("c2", "shared_upstream", lambda p, r: log.append("c2"))

        manager.notify_pipeline_completed("shared_upstream", "run-789")
        assert set(log) == {"c1", "c2"}

    def test_callback_error_does_not_stop_others(self):
        """Error in one completion callback does not prevent others from firing."""
        log = []
        manager = TriggerManager()

        manager.register_completion_trigger("bad", "pipe", lambda p, r: (_ for _ in ()).throw(RuntimeError("boom")))
        manager.register_completion_trigger("good", "pipe", lambda p, r: log.append("good"))

        manager.notify_pipeline_completed("pipe", "run-x")
        assert "good" in log

    def test_unregister_completion_trigger(self):
        """After unregister(), completion callback no longer fires."""
        received = []
        manager = TriggerManager()

        manager.register_completion_trigger(
            "ct3", "upstream", lambda p, r: received.append(r)
        )
        manager.unregister("ct3")
        manager.notify_pipeline_completed("upstream", "run-zzz")
        assert received == []

    def test_unregister_nonexistent_returns_false(self):
        manager = TriggerManager()
        assert manager.unregister("no-such-id") is False


class TestListActive:
    def test_list_active_shows_registered_sensors(self, tmp_path):
        watch_file = tmp_path / "f.csv"
        watch_file.write_text("x")

        manager = TriggerManager()
        manager.register_file_sensor("fs1", str(watch_file), lambda: None, poll_interval=60)

        active = manager.list_active()
        assert "fs1" in active["file_sensors"]
        manager.stop_all()

    def test_list_active_shows_completion_triggers(self):
        manager = TriggerManager()
        manager.register_completion_trigger("ct99", "pipe_x", lambda p, r: None)

        active = manager.list_active()
        assert "ct99" in active["completion_triggers"].get("pipe_x", [])

    def test_stop_all_clears_state(self, tmp_path):
        watch_file = tmp_path / "g.csv"
        watch_file.write_text("x")

        manager = TriggerManager()
        manager.register_file_sensor("fs2", str(watch_file), lambda: None, poll_interval=60)
        manager.register_completion_trigger("ct100", "pipe_y", lambda p, r: None)

        manager.stop_all()
        active = manager.list_active()
        assert active["file_sensors"] == []
        assert active["completion_triggers"] == {}
