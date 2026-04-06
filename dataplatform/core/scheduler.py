import logging
import json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from typing import Dict, Any, Optional
from pathlib import Path
from dataplatform.core.config import load_config
from dataplatform.core.dag import DAGBuilder
from dataplatform.core.executor import PipelineExecutor
from dataplatform.core.logging_config import log_pipeline_start, log_pipeline_success, log_pipeline_failure
import time

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Scheduler for running pipelines based on cron schedules."""

    def __init__(self):
        self.state_file = Path(__file__).parent.parent.parent / "data" / "scheduler_state.json"
        self.scheduler = BackgroundScheduler(
            jobstores={
                'default': MemoryJobStore()
            },
            executors={
                'default': ThreadPoolExecutor(max_workers=5)
            },
            job_defaults={
                'coalesce': True,
                'max_instances': 1,
                'misfire_grace_time': 30
            }
        )
        self.scheduled_pipelines = {}

    def _persist_schedules(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        persisted = {
            name: {
                "config_path": info["config_path"],
                "schedule": info["schedule"],
            }
            for name, info in self.scheduled_pipelines.items()
        }
        self.state_file.write_text(json.dumps(persisted, indent=2), encoding="utf-8")

    def _restore_schedules(self) -> None:
        if not self.state_file.exists():
            return

        try:
            restored = json.loads(self.state_file.read_text(encoding="utf-8"))
            if not isinstance(restored, dict):
                logger.warning("Ignoring invalid scheduler state file format")
                return

            for pipeline_name, payload in restored.items():
                config_path = payload.get("config_path")
                schedule = payload.get("schedule")
                if not config_path or not isinstance(schedule, dict):
                    logger.warning(f"Skipping invalid scheduler state entry for {pipeline_name}")
                    continue
                self.schedule_pipeline(config_path, custom_schedule=schedule)
        except Exception as e:
            logger.error(f"Failed to restore persisted schedules: {e}")

    def schedule_pipeline(self, config_path: str, custom_schedule: dict = None) -> bool:
        """Schedule a pipeline based on its cron configuration or custom schedule."""
        try:
            config = load_config(config_path)

            # Use custom schedule if provided, otherwise use config schedule
            if custom_schedule:
                schedule = custom_schedule
            else:
                if not config.schedule:
                    logger.warning(f"No schedule defined for pipeline {config.pipeline_name}")
                    return False
                schedule = config.schedule

            cron_trigger = CronTrigger(
                minute=schedule.get('minute', '*'),
                hour=schedule.get('hour', '*'),
                day=schedule.get('day', '*'),
                month=schedule.get('month', '*'),
                day_of_week=schedule.get('day_of_week', '*')
            )

            # Add job to scheduler
            job = self.scheduler.add_job(
                func=self._run_scheduled_pipeline,
                trigger=cron_trigger,
                args=[config_path],
                id=f"pipeline_{config.pipeline_name}",
                name=f"Scheduled pipeline: {config.pipeline_name}",
                replace_existing=True
            )

            self.scheduled_pipelines[config.pipeline_name] = {
                'job': job,
                'config_path': config_path,
                'schedule': schedule
            }

            self._persist_schedules()
            logger.info(f"Scheduled pipeline {config.pipeline_name} with cron: {schedule}")
            return True

        except Exception as e:
            logger.error(f"Failed to schedule pipeline from {config_path}: {e}")
            return False

    def unschedule_pipeline(self, pipeline_name: str) -> bool:
        """Remove a pipeline from the schedule."""
        if pipeline_name in self.scheduled_pipelines:
            try:
                self.scheduled_pipelines[pipeline_name]['job'].remove()
                del self.scheduled_pipelines[pipeline_name]
                self._persist_schedules()
                logger.info(f"Unscheduled pipeline {pipeline_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to unschedule pipeline {pipeline_name}: {e}")
                return False
        else:
            logger.warning(f"Pipeline {pipeline_name} is not scheduled")
            return False

    def list_scheduled_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get list of all scheduled pipelines."""
        return {
            name: {
                'config_path': info['config_path'],
                'schedule': info['schedule'],
                'next_run': info['job'].next_run_time.isoformat() if info['job'].next_run_time else None
            }
            for name, info in self.scheduled_pipelines.items()
        }

    def start(self):
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            self._restore_schedules()
            logger.info("Pipeline scheduler started")

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Pipeline scheduler stopped")

    def _run_scheduled_pipeline(self, config_path: str):
        """Execute a scheduled pipeline run."""
        try:
            config = load_config(config_path)
            logger.info(f"Running scheduled pipeline: {config.pipeline_name}")

            # Log pipeline start
            log_pipeline_start(config.pipeline_name, len(config.tasks))
            start_time = time.time()

            dag_builder = DAGBuilder(config.tasks)
            dag = dag_builder.build()
            execution_order = dag_builder.get_execution_order()

            executor = PipelineExecutor()
            success, results, errors = executor.execute_pipeline(
                tasks={task.name: task for task in config.tasks},
                execution_order=execution_order,
                config={"file_path": config.file_path}
            )

            duration = time.time() - start_time

            if success:
                log_pipeline_success(config.pipeline_name, duration)
            else:
                # Log which task failed
                failed_tasks = [task for task, result in results.items() if not result]
                error_msg = f"Failed at tasks: {failed_tasks}"
                if errors:
                    error_msg += f" - Errors: {errors}"
                log_pipeline_failure(config.pipeline_name, error_msg, duration)

        except Exception as e:
            logger.error(f"Scheduled pipeline execution failed for {config_path}: {e}", exc_info=True)


# Global scheduler instance
scheduler = PipelineScheduler()


def get_scheduler() -> PipelineScheduler:
    """Get the global scheduler instance."""
    return scheduler
