"""Semantic metrics layer: named SQL metric definitions with compute history.

Metric definitions live in YAML files under the project-root ``metrics/``
directory.  Each file declares a single metric:

    metric_name: daily_revenue
    sql: SELECT SUM(amount) FROM orders WHERE date = CURRENT_DATE
    description: Total revenue for today
    owner: analytics_team

Computed values are persisted in the ``metric_results`` DB table so callers
can retrieve trend history without re-running SQL.

Public API:
    load_metric(metric_path) -> Dict[str, Any]
    list_metrics() -> List[Dict[str, Any]]
    compute_metric(metric_name, sql) -> Dict[str, Any]
    get_history(metric_name, limit) -> List[Dict[str, Any]]
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataplatform.core.database import (
    init_db,
    save_metric_result as _db_save_metric,
    get_metric_history as _db_get_history,
)

logger = logging.getLogger(__name__)

# Project-root metrics/ directory
_METRICS_DIR = Path(__file__).resolve().parent.parent.parent / "metrics"

# Required YAML keys for a metric definition
_REQUIRED_KEYS = {"metric_name", "sql"}


def load_metric(metric_path: str) -> Dict[str, Any]:
    """Load and validate a metric definition from a YAML file.

    Raises ValueError if required fields are missing.
    """
    import yaml

    path = Path(metric_path)
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Metric file {metric_path} must contain a YAML mapping")

    missing = _REQUIRED_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"Metric YAML missing required fields: {sorted(missing)}")

    return {
        "metric_name": str(data["metric_name"]).strip(),
        "sql": str(data["sql"]).strip(),
        "description": str(data.get("description", "")).strip(),
        "owner": str(data.get("owner", "")).strip(),
        "file_path": str(path),
    }


def list_metrics() -> List[Dict[str, Any]]:
    """Discover all metric YAML files in the ``metrics/`` directory.

    Returns metadata only (no SQL) sorted by metric_name.
    Silently skips files that fail to load.
    """
    if not _METRICS_DIR.exists():
        return []

    results: List[Dict[str, Any]] = []
    for path in sorted(_METRICS_DIR.glob("*.yaml")):
        try:
            m = load_metric(str(path))
            results.append({
                "metric_name": m["metric_name"],
                "description": m["description"],
                "owner": m["owner"],
                "file_path": m["file_path"],
            })
        except Exception as exc:
            logger.warning("Failed to load metric from %s: %s", path, exc)

    return results


def compute_metric(metric_name: str, sql: str) -> Dict[str, Any]:
    """Execute *sql* via DuckDB in-memory and persist the scalar result.

    The metric SQL must return a single row with a single numeric (or NULL)
    value in the first column.  Errors are caught and stored as the ``error``
    field so callers can surface them without crashing.

    Returns a dict with keys: metric_name, value, error, computed_at.
    """
    import duckdb

    init_db()
    value: Optional[float] = None
    error: Optional[str] = None

    try:
        with duckdb.connect(":memory:") as con:
            row = con.execute(sql).fetchone()
            if row is not None and row[0] is not None:
                value = float(row[0])
    except Exception as exc:
        error = str(exc)
        logger.warning("Metric '%s' compute failed: %s", metric_name, exc)

    now = datetime.utcnow().isoformat() + "Z"
    _db_save_metric(metric_name, value, error, now)

    return {
        "metric_name": metric_name,
        "value": value,
        "error": error,
        "computed_at": now,
    }


def get_history(metric_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent computed values for a named metric (newest first)."""
    init_db()
    return _db_get_history(metric_name, limit)
