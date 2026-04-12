"""Cost attribution: estimated compute cost per pipeline run.

Cost model: task_count × duration_seconds × COST_PER_TASK_SECOND (USD).
The rate defaults to $0.0001/task-second and is configurable via the
``COST_PER_TASK_SECOND`` environment variable.

Public API:
    record_run_cost(run_id, pipeline_name, team, task_count, duration_seconds) -> Dict
    get_cost_summary() -> List[Dict]          — grouped by pipeline + team
    get_team_cost_summary() -> List[Dict]     — grouped by team only
    get_pipeline_cost_history(pipeline_name, limit) -> List[Dict]
"""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from dataplatform.core.database import (
    init_db,
    save_pipeline_cost,
    get_cost_by_pipeline,
    get_cost_by_team,
    get_pipeline_cost_history as _db_cost_history,
)

logger = logging.getLogger(__name__)

# Dollars per task-second of execution time
_COST_PER_TASK_SECOND = float(os.getenv("COST_PER_TASK_SECOND", "0.0001"))


def record_run_cost(
    run_id: str,
    pipeline_name: str,
    team: Optional[str],
    task_count: int,
    duration_seconds: float,
) -> Dict[str, Any]:
    """Calculate and persist estimated cost for a pipeline run.

    Cost = task_count × duration_seconds × COST_PER_TASK_SECOND
    Errors are logged but never bubble up — cost recording must not fail a run.
    """
    init_db()
    estimated_cost_usd = round(task_count * duration_seconds * _COST_PER_TASK_SECOND, 6)
    now = datetime.utcnow().isoformat() + "Z"
    try:
        save_pipeline_cost(
            run_id, pipeline_name, team, task_count, duration_seconds,
            estimated_cost_usd, now,
        )
    except Exception as exc:
        logger.warning("Cost recording failed for run %s: %s", run_id, exc)

    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "team": team,
        "task_count": task_count,
        "duration_seconds": round(duration_seconds, 3),
        "estimated_cost_usd": estimated_cost_usd,
        "recorded_at": now,
    }


def get_cost_summary(limit: int = 100) -> List[Dict[str, Any]]:
    """Return total cost grouped by pipeline + team, sorted by cost descending."""
    init_db()
    return get_cost_by_pipeline(limit=limit)


def get_team_cost_summary() -> List[Dict[str, Any]]:
    """Return total cost grouped by team, sorted by cost descending."""
    init_db()
    return get_cost_by_team()


def get_pipeline_cost_history(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Return per-run cost history for a pipeline (newest first)."""
    init_db()
    return _db_cost_history(pipeline_name, limit=limit)
