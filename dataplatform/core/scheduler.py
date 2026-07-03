import logging
import os
import uuid
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from typing import Dict, Any, Optional
from dataplatform.core.config import load_config
from dataplatform.core.dag import DAGBuilder
from dataplatform.core.executor import PipelineExecutor
from dataplatform.core.logging_config import log_pipeline_start, log_pipeline_success, log_pipeline_failure
from dataplatform.core.database import save_run_status, save_schedule, list_schedules, delete_schedule
from dataplatform.core.alerts import check_sla_and_alert
import time

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Scheduler for running pipelines based on cron schedules."""

    def __init__(self):
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
        """Write all active schedules to the DB (upserts each pipeline)."""
        for name, info in self.scheduled_pipelines.items():
            try:
                save_schedule(name, info["config_path"], info["schedule"])
            except Exception as exc:
                logger.warning("Failed to persist schedule for '%s': %s", name, exc)

    def _restore_schedules(self) -> None:
        """Reload schedules from the DB on startup."""
        try:
            rows = list_schedules()
        except Exception as exc:
            logger.error("Failed to load persisted schedules from DB: %s", exc)
            return

        for row in rows:
            pipeline_name = row["pipeline_name"]
            config_path = row.get("config_path")
            schedule = row.get("schedule")
            if not config_path or not isinstance(schedule, dict):
                logger.warning("Skipping invalid schedule record for '%s'", pipeline_name)
                continue
            try:
                self.schedule_pipeline(config_path, custom_schedule=schedule)
            except Exception as exc:
                logger.warning("Failed to restore schedule for '%s': %s", pipeline_name, exc)

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
                delete_schedule(pipeline_name)
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
            run_id = str(uuid.uuid4())
            logger.info(f"Running scheduled pipeline: {config.pipeline_name} (run_id={run_id})")

            log_pipeline_start(config.pipeline_name, len(config.tasks))
            save_run_status(config.pipeline_name, run_id, "started", "Scheduled run started")
            start_time = time.time()

            dag_builder = DAGBuilder(config.tasks)
            dag_builder.build()
            execution_waves = dag_builder.get_execution_waves()
            execution_order = [t for wave in execution_waves for t in wave]

            executor = PipelineExecutor()
            success, results, errors = executor.execute_pipeline_parallel(
                tasks={task.name: task for task in config.tasks},
                execution_waves=execution_waves,
                config={"file_path": config.file_path},
                pipeline_name=config.pipeline_name,
                run_id=run_id,
            )

            duration = time.time() - start_time

            sla_violated = False
            if config.sla:
                sla_violated = check_sla_and_alert(config.pipeline_name, run_id, duration, config.sla)

            if success:
                log_pipeline_success(config.pipeline_name, duration)
                save_run_status(config.pipeline_name, run_id, "completed", "Scheduled run completed",
                                {"duration_seconds": round(duration, 2), "sla_violated": sla_violated})
            else:
                failed_tasks = [t for t, r in results.items() if not r]
                error_msg = f"Failed at tasks: {failed_tasks}"
                if errors:
                    error_msg += f" - Errors: {errors}"
                log_pipeline_failure(config.pipeline_name, error_msg, duration)
                save_run_status(config.pipeline_name, run_id, "failed", error_msg,
                                {"duration_seconds": round(duration, 2), "sla_violated": sla_violated})

        except Exception as e:
            logger.error(f"Scheduled pipeline execution failed for {config_path}: {e}", exc_info=True)


# Global scheduler instance
scheduler = PipelineScheduler()


def get_scheduler() -> PipelineScheduler:
    """Get the global scheduler instance."""
    return scheduler
