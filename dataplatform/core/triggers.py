"""Event-driven pipeline triggers: file sensor, webhook, and pipeline_completion.

Design:
- TriggerManager is a singleton that holds in-memory state for active triggers.
- File sensors poll a path in daemon threads; callback fires when mtime changes.
- Completion triggers register callbacks keyed by upstream pipeline name;
  callers invoke notify_pipeline_completed() after a successful run.
- Trigger metadata is persisted in the DB so triggers survive restarts.
"""
import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# TriggerManager
# ---------------------------------------------------------------------------

class TriggerManager:
    """In-memory registry for all active event-driven triggers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # file_sensor: trigger_id -> threading.Event (set to stop)
        self._stop_events: Dict[str, threading.Event] = {}
        # pipeline_completion: upstream_pipeline -> [(trigger_id, callback)]
        self._completion_callbacks: Dict[str, List] = {}

    # ------------------------------------------------------------------ #
    # File sensor                                                           #
    # ------------------------------------------------------------------ #

    def register_file_sensor(
        self,
        trigger_id: str,
        watch_path: str,
        callback: Callable[[], None],
        poll_interval: int = 30,
    ) -> None:
        """Poll *watch_path* every *poll_interval* seconds.

        Fires *callback* when the file is modified (mtime changes).
        The first detection of an existing file is recorded but does NOT
        fire the callback — only subsequent changes do.
        """
        stop_event = threading.Event()

        def _poll() -> None:
            last_mtime: Optional[float] = None
            while not stop_event.is_set():
                try:
                    p = Path(watch_path)
                    if p.exists():
                        mtime = p.stat().st_mtime
                        if last_mtime is not None and mtime != last_mtime:
                            logger.info("File sensor triggered: %s", watch_path)
                            try:
                                callback()
                            except Exception as exc:
                                logger.error(
                                    "File sensor callback error for %s: %s", watch_path, exc
                                )
                        last_mtime = mtime
                except Exception as exc:
                    logger.warning("File sensor poll error for %s: %s", watch_path, exc)
                stop_event.wait(poll_interval)

        with self._lock:
            self._stop_events[trigger_id] = stop_event

        t = threading.Thread(
            target=_poll, name=f"file-sensor-{trigger_id}", daemon=True
        )
        t.start()
        logger.info("File sensor trigger %s watching %s", trigger_id, watch_path)

    # ------------------------------------------------------------------ #
    # Pipeline completion                                                   #
    # ------------------------------------------------------------------ #

    def register_completion_trigger(
        self,
        trigger_id: str,
        upstream_pipeline: str,
        callback: Callable[[str, str], None],
    ) -> None:
        """Fire *callback(pipeline_name, run_id)* when *upstream_pipeline* succeeds."""
        with self._lock:
            self._completion_callbacks.setdefault(upstream_pipeline, [])
            self._completion_callbacks[upstream_pipeline].append((trigger_id, callback))
        logger.info(
            "Completion trigger %s registered on upstream '%s'",
            trigger_id, upstream_pipeline,
        )

    def notify_pipeline_completed(self, pipeline_name: str, run_id: str) -> None:
        """Called after a pipeline finishes successfully. Fires all registered callbacks."""
        with self._lock:
            callbacks = list(self._completion_callbacks.get(pipeline_name, []))
        for trigger_id, cb in callbacks:
            try:
                logger.info(
                    "Firing completion trigger %s (upstream=%s)", trigger_id, pipeline_name
                )
                cb(pipeline_name, run_id)
            except Exception as exc:
                logger.error("Completion trigger %s callback failed: %s", trigger_id, exc)

    # ------------------------------------------------------------------ #
    # Lifecycle                                                             #
    # ------------------------------------------------------------------ #

    def unregister(self, trigger_id: str) -> bool:
        """Stop and remove a trigger. Returns True if the trigger was found."""
        found = False
        with self._lock:
            if trigger_id in self._stop_events:
                self._stop_events[trigger_id].set()
                del self._stop_events[trigger_id]
                found = True
            for upstream, callbacks in self._completion_callbacks.items():
                before = len(callbacks)
                self._completion_callbacks[upstream] = [
                    (tid, cb) for tid, cb in callbacks if tid != trigger_id
                ]
                if len(self._completion_callbacks[upstream]) < before:
                    found = True
        return found

    def stop_all(self) -> None:
        """Stop all file sensor threads and clear all callbacks."""
        with self._lock:
            for stop_event in self._stop_events.values():
                stop_event.set()
            self._stop_events.clear()
            self._completion_callbacks.clear()

    def list_active(self) -> Dict[str, Any]:
        """Return a summary of active in-memory triggers."""
        with self._lock:
            return {
                "file_sensors": list(self._stop_events.keys()),
                "completion_triggers": {
                    upstream: [tid for tid, _ in cbs]
                    for upstream, cbs in self._completion_callbacks.items()
                },
            }


# ---------------------------------------------------------------------------
# Helpers for wiring triggers from DB records
# ---------------------------------------------------------------------------

def _make_pipeline_runner(config_path: str, trigger_id: str) -> Callable[[], None]:
    """Return a zero-arg callable that runs the pipeline at *config_path*."""

    def _run() -> None:
        import uuid as _uuid
        from dataplatform.core.config import load_config
        from dataplatform.core.dag import DAGBuilder
        from dataplatform.core.executor import PipelineExecutor
        from dataplatform.core.database import save_run_status, update_trigger_last_fired

        update_trigger_last_fired(trigger_id)
        try:
            config = load_config(config_path)
            run_id = str(_uuid.uuid4())
            save_run_status(config.pipeline_name, run_id, "started", f"Triggered by {trigger_id}")
            dag = DAGBuilder(config.tasks)
            dag.build()
            waves = dag.get_execution_waves()
            executor = PipelineExecutor()
            success, results, errors = executor.execute_pipeline_parallel(
                tasks={t.name: t for t in config.tasks},
                execution_waves=waves,
                config={"file_path": config.file_path},
                pipeline_name=config.pipeline_name,
                run_id=run_id,
            )
            status = "completed" if success else "failed"
            save_run_status(config.pipeline_name, run_id, status, f"Trigger run {status}")
        except Exception as exc:
            logger.error("Trigger-initiated run failed for %s: %s", config_path, exc)

    return _run


def restore_triggers_from_db(manager: "TriggerManager") -> None:
    """Re-register all enabled triggers from the DB on server startup."""
    try:
        from dataplatform.core.database import get_triggers, init_db
        init_db()
        for row in get_triggers(enabled_only=True):
            trigger_id = row["trigger_id"]
            trigger_type = row["trigger_type"]
            config_path = row["config_path"]
            pipeline_name = row["pipeline_name"]
            tc = row.get("trigger_config") or {}

            if trigger_type == "file_sensor":
                watch_path = tc.get("watch_path", "")
                poll_interval = int(tc.get("poll_interval_seconds", 30))
                if watch_path:
                    manager.register_file_sensor(
                        trigger_id,
                        watch_path,
                        _make_pipeline_runner(config_path, trigger_id),
                        poll_interval,
                    )
            elif trigger_type == "pipeline_completion":
                upstream = tc.get("upstream_pipeline", "")
                if upstream:
                    manager.register_completion_trigger(
                        trigger_id,
                        upstream,
                        lambda pname, rid, _cp=config_path, _tid=trigger_id: _make_pipeline_runner(_cp, _tid)(),
                    )
    except Exception as exc:
        logger.warning("Failed to restore triggers from DB: %s", exc)


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_trigger_manager = TriggerManager()


def get_trigger_manager() -> TriggerManager:
    """Return the global TriggerManager singleton."""
    return _trigger_manager
