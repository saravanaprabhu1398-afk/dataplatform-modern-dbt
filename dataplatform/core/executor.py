import importlib
import inspect
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataplatform.core.config import Task
from dataplatform.plugins.base import Plugin
from dataplatform.core.logging_config import log_task_start, log_task_success, log_task_failure
from dataplatform.core.secrets import resolve_secrets

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self):
        self.plugins: Dict[Tuple[str, str], Plugin] = {}
        self._lock = threading.Lock()

    def load_plugin(self, plugin_name: str, plugin_type: str) -> Plugin:
        """Dynamically load a plugin."""
        cache_key = (plugin_type, plugin_name)
        if cache_key in self.plugins:
            return self.plugins[cache_key]

        with self._lock:
            # Re-check inside lock to avoid double-loading
            if cache_key in self.plugins:
                return self.plugins[cache_key]

        try:
            module_name = f"dataplatform.plugins.{plugin_type}s.{plugin_name}_plugin"
            module = importlib.import_module(module_name)

            plugin_class = None
            for attribute in module.__dict__.values():
                if (
                    isinstance(attribute, type)
                    and issubclass(attribute, Plugin)
                    and attribute is not Plugin
                    and not inspect.isabstract(attribute)
                    and attribute.__module__ == module.__name__
                ):
                    plugin_class = attribute
                    break

            if plugin_class is None:
                for attribute in module.__dict__.values():
                    if (
                        isinstance(attribute, type)
                        and attribute.__module__ == module.__name__
                        and callable(getattr(attribute, "execute", None))
                        and not inspect.isabstract(attribute)
                    ):
                        plugin_class = attribute
                        break

            if plugin_class is None:
                raise AttributeError(f"Plugin class not found in module {module_name}")

            plugin = plugin_class()
            self.plugins[cache_key] = plugin
            return plugin
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Failed to load plugin {plugin_name}: {e}")

    @staticmethod
    def _extract_error_details(result: Any) -> str:
        if isinstance(result, dict):
            return str(result.get("error") or result.get("stderr") or result)
        if isinstance(result, list):
            return f"Plugin returned failure result with {len(result)} item(s)"
        if result is None:
            return "Plugin returned failure status"
        return str(result)

    @staticmethod
    def _utc_now() -> str:
        return datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _build_task_run(
        task: Task,
        status: str,
        started_at: str,
        completed_at: str,
        duration_seconds: float,
        attempt_history: List[Dict[str, Any]],
        error_details: str = "",
    ) -> Dict[str, Any]:
        return {
            "task_name": task.name,
            "status": status,
            "plugin": task.plugin,
            "type": task.type,
            "operation": task.operation,
            "depends_on": task.depends_on or [],
            "timeout_seconds": task.timeout,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": round(duration_seconds, 3),
            "attempts": len(attempt_history),
            "max_attempts": task.retries + 1,
            "retry_count": max(len(attempt_history) - 1, 0),
            "attempt_history": attempt_history,
            "error": error_details or None,
        }

    def execute_task(
        self,
        task: Task,
        config: Dict[str, Any] = None,
        previous_data: Any = None,
        collect_timeline: bool = False,
    ):
        """Execute a task with retries and return result data and error details."""
        if config is None:
            config = {}

        # Merge task-level config with global config
        # Task-level config takes precedence
        task_config = {**config}
        if task.config:
            task_config.update(task.config)
        
        # Pass task name for logging and context
        task_config["task_name"] = task.name
        # Add operation if specified
        if task.operation:
            task_config["operation"] = task.operation
        
        # Pass previous task data if available
        if previous_data is not None:
            task_config["previous_data"] = previous_data

        # Resolve ${ENV_VAR} and ${vault:path:key} tokens before handing
        # the config to the plugin (keeps secrets out of plain YAML files).
        task_config = resolve_secrets(task_config)

        error_details = ""
        task_started_monotonic = time.time()
        task_started_at = self._utc_now()
        attempt_history: List[Dict[str, Any]] = []
        
        for attempt in range(task.retries + 1):
            start_time = time.time()
            attempt_started_at = self._utc_now()
            log_task_start(task.name, attempt + 1)

            try:
                plugin = self.load_plugin(task.plugin, task.type)

                if task.timeout:
                    with ThreadPoolExecutor(max_workers=1) as _tpool:
                        _fut = _tpool.submit(plugin.execute, task_config)
                        try:
                            result = _fut.result(timeout=task.timeout)
                        except FutureTimeoutError:
                            raise TimeoutError(
                                f"Task '{task.name}' timed out after {task.timeout}s"
                            )
                else:
                    result = plugin.execute(task_config)
                
                # Handle different return types from plugins
                if isinstance(result, tuple):
                    success, data = result
                else:
                    success = result
                    data = None

                duration = time.time() - start_time
                completed_at = self._utc_now()

                if success:
                    log_task_success(task.name, duration)
                    attempt_history.append({
                        "attempt": attempt + 1,
                        "status": "success",
                        "started_at": attempt_started_at,
                        "completed_at": completed_at,
                        "duration_seconds": round(duration, 3),
                        "error": None,
                    })
                    task_run = self._build_task_run(
                        task,
                        "success",
                        task_started_at,
                        completed_at,
                        time.time() - task_started_monotonic,
                        attempt_history,
                    )
                    if collect_timeline:
                        return True, data, "", task_run
                    return True, data, ""
                else:
                    error_msg = self._extract_error_details(data)
                    error_details = error_msg
                    attempt_history.append({
                        "attempt": attempt + 1,
                        "status": "failed",
                        "started_at": attempt_started_at,
                        "completed_at": completed_at,
                        "duration_seconds": round(duration, 3),
                        "error": error_msg,
                    })
                    log_task_failure(task.name, error_msg, attempt + 1, task.retries + 1)
                    if attempt < task.retries:
                        logger.info(f"Retrying task {task.name} in {attempt + 1} seconds...")
                        time.sleep(attempt + 1)  # Exponential backoff

            except Exception as e:
                duration = time.time() - start_time
                completed_at = self._utc_now()
                error_msg = str(e)
                error_details = error_msg
                attempt_history.append({
                    "attempt": attempt + 1,
                    "status": "failed",
                    "started_at": attempt_started_at,
                    "completed_at": completed_at,
                    "duration_seconds": round(duration, 3),
                    "error": error_msg,
                })
                log_task_failure(task.name, error_msg, attempt + 1, task.retries + 1)
                logger.error(f"Task {task.name} exception: {e}", exc_info=True)

                if attempt < task.retries:
                    logger.info(f"Retrying task {task.name} in {attempt + 1} seconds...")
                    time.sleep(attempt + 1)  # Exponential backoff

        logger.error(f"Task {task.name} failed after {task.retries + 1} attempts")
        if collect_timeline:
            task_run = self._build_task_run(
                task,
                "failed",
                task_started_at,
                self._utc_now(),
                time.time() - task_started_monotonic,
                attempt_history,
                error_details,
            )
            return False, None, error_details, task_run
        return False, None, error_details


class PipelineExecutor:
    def __init__(self):
        self.task_executor = TaskExecutor()

    def execute_pipeline(self, tasks: Dict[str, Task], execution_order: list, config: Dict[str, Any] = None):
        """Execute pipeline tasks in order."""
        results = {}
        task_data = {}  # Store data from each task
        errors = {}  # Store error details per task

        for task_name in execution_order:
            task = tasks[task_name]
            
            # Get data from previous tasks that this task depends on
            previous_data = None
            if task.depends_on:
                # For now, pass data from the first dependency
                # In a more complex setup, you might want to merge data from multiple dependencies
                dependency_data = []
                for dep in task.depends_on:
                    if dep in task_data and task_data[dep] is not None:
                        dependency_data.append(task_data[dep])
                
                if dependency_data:
                    # If multiple dependencies have data, you might want to handle this differently
                    previous_data = dependency_data[0] if len(dependency_data) == 1 else dependency_data
            
            success, data, error_details = self.task_executor.execute_task(task, config, previous_data)
            results[task_name] = success
            task_data[task_name] = data
            if not success and error_details:
                errors[task_name] = error_details

            if not success:
                logger.error(f"Pipeline failed at task {task_name}: {error_details}")
                return False, results, errors

        logger.info("Pipeline completed successfully")
        return True, results, errors

    def _collect_dependency_data(self, task: Task, task_data: Dict[str, Any]) -> Any:
        """Return output data from a task's dependencies to pass as previous_data."""
        if not task.depends_on:
            return None
        dependency_data = [
            task_data[dep]
            for dep in task.depends_on
            if dep in task_data and task_data[dep] is not None
        ]
        if not dependency_data:
            return None
        return dependency_data[0] if len(dependency_data) == 1 else dependency_data

    def execute_pipeline_parallel(
        self,
        tasks: Dict[str, Task],
        execution_waves: List[List[str]],
        config: Dict[str, Any] = None,
        max_workers: int = 4,
        pipeline_name: str = "",
        run_id: str = "",
        return_task_runs: bool = False,
    ) -> Tuple[Any, ...]:
        """Execute pipeline tasks in parallel waves.

        Tasks within the same wave have no mutual dependencies and run
        concurrently via ThreadPoolExecutor. Each wave completes before the
        next starts, preserving the DAG ordering guarantee.
        """
        results: Dict[str, bool] = {}
        task_data: Dict[str, Any] = {}
        errors: Dict[str, str] = {}
        task_runs: List[Dict[str, Any]] = []

        for wave_index, wave in enumerate(execution_waves):
            with ThreadPoolExecutor(max_workers=min(max_workers, len(wave))) as pool:
                future_to_name = {}
                for task_name in wave:
                    task = tasks[task_name]
                    previous_data = self._collect_dependency_data(task, task_data)
                    if return_task_runs:
                        future = pool.submit(
                            self.task_executor.execute_task,
                            task,
                            config,
                            previous_data,
                            True,
                        )
                    else:
                        future = pool.submit(
                            self.task_executor.execute_task,
                            task,
                            config,
                            previous_data,
                        )
                    future_to_name[future] = task_name

                for future in as_completed(future_to_name):
                    task_name = future_to_name[future]
                    task = tasks[task_name]
                    task_run = None
                    try:
                        task_result = future.result()
                        if return_task_runs and len(task_result) == 4:
                            success, data, error_details, task_run = task_result
                        else:
                            success, data, error_details = task_result
                    except Exception as exc:
                        success, data, error_details = False, None, str(exc)
                        if return_task_runs:
                            now = datetime.utcnow().isoformat() + "Z"
                            task_run = {
                                "task_name": task_name,
                                "status": "failed",
                                "plugin": task.plugin,
                                "type": task.type,
                                "operation": task.operation,
                                "depends_on": task.depends_on or [],
                                "timeout_seconds": task.timeout,
                                "started_at": now,
                                "completed_at": now,
                                "duration_seconds": 0,
                                "attempts": 0,
                                "max_attempts": task.retries + 1,
                                "retry_count": 0,
                                "attempt_history": [],
                                "error": str(exc),
                            }

                    # --- Lineage recording (non-blocking, never fails the pipeline) ---
                    if success and task.lineage and pipeline_name:
                        try:
                            from dataplatform.core.lineage import record_task_lineage
                            record_task_lineage(run_id, pipeline_name, task_name, task.lineage)
                        except Exception as exc:
                            logger.warning("Lineage recording skipped for '%s': %s", task_name, exc)

                    # --- Quality checks (failures propagate as task failure) ---
                    if success and task.quality and task.quality.checks:
                        try:
                            from dataplatform.core.quality import run_quality_checks
                            check_results = run_quality_checks(
                                task.quality.checks, run_id, pipeline_name, task_name
                            )
                            failed = [r["name"] for r in check_results if not r["passed"]]
                            if failed:
                                success = False
                                error_details = f"Quality checks failed: {failed}"
                                if task_run:
                                    task_run["status"] = "failed"
                                    task_run["error"] = error_details
                        except Exception as exc:
                            logger.error("Quality check runner raised an exception for '%s': %s", task_name, exc)
                            success = False
                            error_details = f"Quality check error: {exc}"
                            if task_run:
                                task_run["status"] = "failed"
                                task_run["error"] = error_details

                    results[task_name] = success
                    task_data[task_name] = data
                    if not success and error_details:
                        errors[task_name] = error_details
                    if return_task_runs and task_run:
                        task_run["status"] = "success" if success else "failed"
                        if error_details:
                            task_run["error"] = error_details
                        task_runs.append(task_run)

            wave_failed = [name for name in wave if not results.get(name, False)]
            if wave_failed:
                logger.error(f"Pipeline failed in wave at tasks: {wave_failed}")
                if return_task_runs:
                    reported_tasks = {run["task_name"] for run in task_runs}
                    now = datetime.utcnow().isoformat() + "Z"
                    for remaining_wave in execution_waves[wave_index + 1:]:
                        for skipped_name in remaining_wave:
                            if skipped_name in reported_tasks:
                                continue
                            skipped_task = tasks[skipped_name]
                            task_runs.append({
                                "task_name": skipped_name,
                                "status": "skipped",
                                "plugin": skipped_task.plugin,
                                "type": skipped_task.type,
                                "operation": skipped_task.operation,
                                "depends_on": skipped_task.depends_on or [],
                                "timeout_seconds": skipped_task.timeout,
                                "started_at": None,
                                "completed_at": now,
                                "duration_seconds": None,
                                "attempts": 0,
                                "max_attempts": skipped_task.retries + 1,
                                "retry_count": 0,
                                "attempt_history": [],
                                "error": "Skipped because an upstream task failed",
                            })
                    return False, results, errors, task_runs
                return False, results, errors

        logger.info("Pipeline completed successfully (parallel)")
        if return_task_runs:
            return True, results, errors, task_runs
        return True, results, errors
