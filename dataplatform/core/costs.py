"""Cost attribution: estimated compute units per pipeline run.

Cost model: task_count × duration_seconds × COST_PER_TASK_SECOND.
The rate defaults to 0.0001/task-second and is configurable via the
``COST_PER_TASK_SECOND`` environment variable.

NOTE: The cost figure is a dimensionless compute-unit estimate based on
task count × duration. It does not represent real USD.

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

# Compute-unit weight per task-second of execution time (dimensionless, not USD)
_COST_PER_TASK_SECOND = float(os.getenv("COST_PER_TASK_SECOND", "0.0001"))


def record_run_cost(
    run_id: str,
    pipeline_name: str,
    team: Optional[str],
    task_count: int,
    duration_seconds: float,
) -> Dict[str, Any]:
    """Calculate and persist estimated compute units for a pipeline run.

    compute_units = task_count × duration_seconds × COST_PER_TASK_SECOND

    NOTE: The cost figure is a dimensionless compute-unit estimate based on
    task count × duration. It does not represent real USD.

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
    """Return total compute units grouped by pipeline + team, sorted descending.

    Each record includes ``"unit": "compute_units"`` to make clear the figure
    is not real USD.
    """
    init_db()
    rows = get_cost_by_pipeline(limit=limit)
    for row in rows:
        row["unit"] = "compute_units"
    return rows


def get_team_cost_summary() -> List[Dict[str, Any]]:
    """Return total compute units grouped by team, sorted descending.

    Each record includes ``"unit": "compute_units"`` to make clear the figure
    is not real USD.
    """
    init_db()
    rows = get_cost_by_team()
    for row in rows:
        row["unit"] = "compute_units"
    return rows


def get_pipeline_cost_history(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Return per-run compute-unit history for a pipeline (newest first).

    Each record includes ``"unit": "compute_units"`` to make clear the figure
    is not real USD.
    """
    init_db()
    rows = _db_cost_history(pipeline_name, limit=limit)
    for row in rows:
        row["unit"] = "compute_units"
    return rows
