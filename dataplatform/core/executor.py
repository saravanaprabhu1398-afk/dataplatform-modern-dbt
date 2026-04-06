import importlib
import inspect
import logging
import time
from typing import Dict, Any, Tuple
from dataplatform.core.config import Task
from dataplatform.plugins.base import Plugin
from dataplatform.core.logging_config import log_task_start, log_task_success, log_task_failure

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self):
        self.plugins: Dict[Tuple[str, str], Plugin] = {}

    def load_plugin(self, plugin_name: str, plugin_type: str) -> Plugin:
        """Dynamically load a plugin."""
        cache_key = (plugin_type, plugin_name)
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

    def execute_task(self, task: Task, config: Dict[str, Any] = None, previous_data: Any = None) -> tuple[bool, Any, str]:
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
        
        error_details = ""
        
        for attempt in range(task.retries + 1):
            start_time = time.time()
            log_task_start(task.name, attempt + 1)

            try:
                plugin = self.load_plugin(task.plugin, task.type)
                result = plugin.execute(task_config)
                
                # Handle different return types from plugins
                if isinstance(result, tuple):
                    success, data = result
                else:
                    success = result
                    data = None

                duration = time.time() - start_time

                if success:
                    log_task_success(task.name, duration)
                    return True, data, ""
                else:
                    error_msg = self._extract_error_details(data)
                    error_details = error_msg
                    log_task_failure(task.name, error_msg, attempt + 1, task.retries + 1)
                    if attempt < task.retries:
                        logger.info(f"Retrying task {task.name} in {attempt + 1} seconds...")
                        time.sleep(attempt + 1)  # Exponential backoff

            except Exception as e:
                duration = time.time() - start_time
                error_msg = str(e)
                error_details = error_msg
                log_task_failure(task.name, error_msg, attempt + 1, task.retries + 1)
                logger.error(f"Task {task.name} exception: {e}", exc_info=True)

                if attempt < task.retries:
                    logger.info(f"Retrying task {task.name} in {attempt + 1} seconds...")
                    time.sleep(attempt + 1)  # Exponential backoff

        logger.error(f"Task {task.name} failed after {task.retries + 1} attempts")
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
