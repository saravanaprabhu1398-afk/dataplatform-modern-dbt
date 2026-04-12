"""SQLite-backed metadata store for pipeline run history and user management.

Uses Python's built-in sqlite3 — no extra dependencies required.
Point DATABASE_PATH at a different file to move the DB, or swap the
underlying engine later by replacing _get_conn().

WAL journal mode is enabled so multiple readers don't block writes.
"""
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DB_PATH = Path(
    os.getenv(
        "DATABASE_PATH",
        str(Path(__file__).resolve().parent.parent.parent / "data" / "platform.db"),
    )
)

_init_lock = threading.Lock()
_initialized = False


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    """Open a new SQLite connection to the platform DB."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create all tables if they don't exist. Safe to call multiple times."""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        conn = _get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id        TEXT    NOT NULL,
                    pipeline_name TEXT    NOT NULL,
                    status        TEXT    NOT NULL,
                    message       TEXT,
                    details       TEXT,
                    started_at    TEXT    NOT NULL,
                    updated_at    TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runs_pipeline
                    ON pipeline_runs(pipeline_name, updated_at DESC);

                CREATE TABLE IF NOT EXISTS users (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    username      TEXT    UNIQUE NOT NULL,
                    password_hash TEXT    NOT NULL,
                    role          TEXT    NOT NULL DEFAULT 'viewer',
                    team          TEXT,
                    created_at    TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS lineage_records (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id        TEXT    NOT NULL,
                    pipeline_name TEXT    NOT NULL,
                    task_name     TEXT    NOT NULL,
                    direction     TEXT    NOT NULL,
                    asset_uri     TEXT    NOT NULL,
                    recorded_at   TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_lineage_asset
                    ON lineage_records(asset_uri);

                CREATE INDEX IF NOT EXISTS idx_lineage_pipeline
                    ON lineage_records(pipeline_name);

                CREATE TABLE IF NOT EXISTS quality_results (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id         TEXT    NOT NULL,
                    pipeline_name  TEXT    NOT NULL,
                    task_name      TEXT    NOT NULL,
                    check_name     TEXT    NOT NULL,
                    passed         INTEGER NOT NULL,
                    actual_value   TEXT,
                    expected_value TEXT,
                    error          TEXT,
                    checked_at     TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_quality_pipeline
                    ON quality_results(pipeline_name, checked_at DESC);

                CREATE TABLE IF NOT EXISTS sla_violations (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id            TEXT    NOT NULL,
                    pipeline_name     TEXT    NOT NULL,
                    duration_seconds  REAL    NOT NULL,
                    limit_seconds     REAL    NOT NULL,
                    alerted           INTEGER NOT NULL DEFAULT 0,
                    violated_at       TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS triggers (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger_id      TEXT    UNIQUE NOT NULL,
                    trigger_type    TEXT    NOT NULL,
                    pipeline_name   TEXT    NOT NULL,
                    config_path     TEXT    NOT NULL,
                    trigger_config  TEXT    NOT NULL,
                    enabled         INTEGER NOT NULL DEFAULT 1,
                    created_at      TEXT    NOT NULL,
                    last_fired_at   TEXT
                );

                CREATE TABLE IF NOT EXISTS pipeline_versions (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id    TEXT    NOT NULL,
                    pipeline_name TEXT    NOT NULL,
                    version_hash  TEXT    NOT NULL,
                    content       TEXT    NOT NULL,
                    saved_by      TEXT,
                    saved_at      TEXT    NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_versions_pipeline_hash
                    ON pipeline_versions(pipeline_name, version_hash);

                CREATE INDEX IF NOT EXISTS idx_versions_pipeline_time
                    ON pipeline_versions(pipeline_name, saved_at DESC);

                CREATE TABLE IF NOT EXISTS metric_results (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name  TEXT    NOT NULL,
                    value        REAL,
                    error        TEXT,
                    computed_at  TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_metric_results_name
                    ON metric_results(metric_name, computed_at DESC);

                CREATE TABLE IF NOT EXISTS pipeline_costs (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id               TEXT    NOT NULL,
                    pipeline_name        TEXT    NOT NULL,
                    team                 TEXT,
                    task_count           INTEGER NOT NULL,
                    duration_seconds     REAL    NOT NULL,
                    estimated_cost_usd   REAL    NOT NULL,
                    recorded_at          TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_costs_pipeline
                    ON pipeline_costs(pipeline_name, recorded_at DESC);

                CREATE INDEX IF NOT EXISTS idx_costs_team
                    ON pipeline_costs(team, recorded_at DESC);
            """)
        finally:
            conn.close()
        _initialized = True
        logger.info(f"Database initialised at {_DB_PATH}")


# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------

def save_run_status(
    pipeline_name: str,
    run_id: str,
    status: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Append a status record for a pipeline run. Returns the record as a dict."""
    now = datetime.utcnow().isoformat() + "Z"
    details_json = json.dumps(details or {})

    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, pipeline_name, status, message, details, started_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, pipeline_name, status, message, details_json, now, now),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "run_id": run_id,
        "status": status,
        "message": message,
        "details": details or {},
        "updated_at": now,
    }


def get_latest_run(pipeline_name: str) -> Optional[Dict[str, Any]]:
    """Return the most recent status entry for a pipeline, or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE pipeline_name = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (pipeline_name,),
        ).fetchone()
    finally:
        conn.close()
    return _row_to_dict(row) if row else None


def get_run_history(pipeline_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Return the final status entry for each of the last `limit` distinct runs."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT r.*
            FROM pipeline_runs r
            INNER JOIN (
                SELECT run_id, MAX(updated_at) AS max_ts
                FROM pipeline_runs
                WHERE pipeline_name = ?
                GROUP BY run_id
                ORDER BY max_ts DESC
                LIMIT ?
            ) latest
              ON r.run_id = latest.run_id
             AND r.updated_at = latest.max_ts
            ORDER BY r.updated_at DESC
            """,
            (pipeline_name, limit),
        ).fetchall()
    finally:
        conn.close()
    return [_row_to_dict(r) for r in rows]


def get_all_pipeline_names() -> List[str]:
    """Return distinct pipeline names that have at least one run."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name"
        ).fetchall()
    finally:
        conn.close()
    return [r["pipeline_name"] for r in rows]


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------

def create_user(
    username: str,
    password_hash: str,
    role: str = "viewer",
    team: Optional[str] = None,
) -> bool:
    """Insert a new user row. Returns False if the username already exists."""
    now = datetime.utcnow().isoformat() + "Z"
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, team, created_at) VALUES (?, ?, ?, ?, ?)",
            (username, password_hash, role, team, now),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_user(username: str) -> Optional[Dict[str, Any]]:
    """Return a user record by username, or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT id, username, password_hash, role, team, created_at FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def list_users() -> List[Dict[str, Any]]:
    """Return all users without password hashes."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT id, username, role, team, created_at FROM users ORDER BY username"
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def update_user_role(username: str, new_role: str) -> bool:
    """Update a user's role. Returns False if the user was not found."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "UPDATE users SET role = ? WHERE username = ?", (new_role, username)
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete_user(username: str) -> bool:
    """Delete a user by username. Returns False if not found."""
    conn = _get_conn()
    try:
        cur = conn.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

def save_lineage_record(
    run_id: str,
    pipeline_name: str,
    task_name: str,
    direction: str,
    asset_uri: str,
) -> None:
    """Append a single lineage edge (reads_from or writes_to)."""
    now = datetime.utcnow().isoformat() + "Z"
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO lineage_records
                (run_id, pipeline_name, task_name, direction, asset_uri, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, pipeline_name, task_name, direction, asset_uri, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_lineage_for_asset(asset_uri: str) -> List[Dict[str, Any]]:
    """Return all lineage records involving a specific asset URI."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM lineage_records WHERE asset_uri = ? ORDER BY recorded_at DESC",
            (asset_uri,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_full_lineage_graph() -> List[Dict[str, Any]]:
    """Return all unique lineage edges (most recent occurrence per edge)."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name, task_name, direction, asset_uri,
                   MAX(recorded_at) AS last_seen
            FROM lineage_records
            GROUP BY pipeline_name, task_name, direction, asset_uri
            ORDER BY last_seen DESC
            """,
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Quality results
# ---------------------------------------------------------------------------

def save_quality_result(
    run_id: str,
    pipeline_name: str,
    task_name: str,
    check_name: str,
    passed: bool,
    actual_value: Optional[str] = None,
    expected_value: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Persist a single quality check result."""
    now = datetime.utcnow().isoformat() + "Z"
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO quality_results
                (run_id, pipeline_name, task_name, check_name, passed,
                 actual_value, expected_value, error, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, pipeline_name, task_name, check_name, int(passed),
             actual_value, expected_value, error, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_quality_results(pipeline_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return the most recent quality check results for a pipeline."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT * FROM quality_results
            WHERE pipeline_name = ?
            ORDER BY checked_at DESC
            LIMIT ?
            """,
            (pipeline_name, limit),
        ).fetchall()
    finally:
        conn.close()
    results = []
    for r in rows:
        d = dict(r)
        d["passed"] = bool(d["passed"])
        results.append(d)
    return results


# ---------------------------------------------------------------------------
# SLA violations
# ---------------------------------------------------------------------------

def save_sla_violation(
    run_id: str,
    pipeline_name: str,
    duration_seconds: float,
    limit_seconds: float,
    alerted: bool = False,
) -> None:
    """Record an SLA violation event."""
    now = datetime.utcnow().isoformat() + "Z"
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO sla_violations
                (run_id, pipeline_name, duration_seconds, limit_seconds, alerted, violated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, pipeline_name, duration_seconds, limit_seconds, int(alerted), now),
        )
        conn.commit()
    finally:
        conn.close()


def get_sla_violations(pipeline_name: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """Return recent SLA violations, optionally filtered by pipeline."""
    conn = _get_conn()
    try:
        if pipeline_name:
            rows = conn.execute(
                "SELECT * FROM sla_violations WHERE pipeline_name = ? ORDER BY violated_at DESC LIMIT ?",
                (pipeline_name, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM sla_violations ORDER BY violated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Metrics aggregates
# ---------------------------------------------------------------------------

def get_run_counts_by_status() -> List[Dict[str, Any]]:
    """Return {pipeline_name, status, count} for Prometheus metrics."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name, status, COUNT(*) AS count
            FROM pipeline_runs
            GROUP BY pipeline_name, status
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_quality_counts() -> List[Dict[str, Any]]:
    """Return {pipeline_name, task_name, passed, count} for Prometheus metrics."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name, task_name, passed, COUNT(*) AS count
            FROM quality_results
            GROUP BY pipeline_name, task_name, passed
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_sla_violation_counts() -> List[Dict[str, Any]]:
    """Return {pipeline_name, count} of total SLA violations."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name, COUNT(*) AS count
            FROM sla_violations
            GROUP BY pipeline_name
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------

def save_trigger(
    trigger_id: str,
    trigger_type: str,
    pipeline_name: str,
    config_path: str,
    trigger_config: Dict[str, Any],
) -> bool:
    """Persist a trigger definition. Returns False if trigger_id already exists."""
    now = datetime.utcnow().isoformat() + "Z"
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO triggers
                (trigger_id, trigger_type, pipeline_name, config_path, trigger_config, enabled, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
            """,
            (trigger_id, trigger_type, pipeline_name, config_path,
             json.dumps(trigger_config), now),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_triggers(enabled_only: bool = False) -> List[Dict[str, Any]]:
    """Return all triggers, optionally filtered to enabled ones."""
    conn = _get_conn()
    try:
        if enabled_only:
            rows = conn.execute(
                "SELECT * FROM triggers WHERE enabled = 1 ORDER BY created_at ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM triggers ORDER BY created_at ASC"
            ).fetchall()
    finally:
        conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["trigger_config"] = json.loads(d["trigger_config"])
        except Exception:
            pass
        result.append(d)
    return result


def get_trigger(trigger_id: str) -> Optional[Dict[str, Any]]:
    """Return a single trigger record, or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM triggers WHERE trigger_id = ?", (trigger_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    d = dict(row)
    try:
        d["trigger_config"] = json.loads(d["trigger_config"])
    except Exception:
        pass
    return d


def delete_trigger(trigger_id: str) -> bool:
    """Delete a trigger by ID. Returns False if not found."""
    conn = _get_conn()
    try:
        cur = conn.execute("DELETE FROM triggers WHERE trigger_id = ?", (trigger_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def update_trigger_last_fired(trigger_id: str) -> None:
    """Stamp last_fired_at for a trigger."""
    now = datetime.utcnow().isoformat() + "Z"
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE triggers SET last_fired_at = ? WHERE trigger_id = ?", (now, trigger_id)
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Pipeline versions
# ---------------------------------------------------------------------------

def save_pipeline_version(
    version_id: str,
    pipeline_name: str,
    version_hash: str,
    content: str,
    saved_by: Optional[str],
    saved_at: str,
) -> bool:
    """Persist a pipeline version. Returns False if hash already exists for this pipeline."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO pipeline_versions
                (version_id, pipeline_name, version_hash, content, saved_by, saved_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (version_id, pipeline_name, version_hash, content, saved_by, saved_at),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # duplicate hash
    finally:
        conn.close()


def get_pipeline_versions(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """List pipeline versions (no content), newest first."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT version_id, pipeline_name, version_hash, saved_by, saved_at
            FROM pipeline_versions
            WHERE pipeline_name = ?
            ORDER BY saved_at DESC
            LIMIT ?
            """,
            (pipeline_name, limit),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_pipeline_version_content(pipeline_name: str, version_id: str) -> Optional[str]:
    """Return the YAML content for a specific version, or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT content FROM pipeline_versions WHERE pipeline_name = ? AND version_id = ?",
            (pipeline_name, version_id),
        ).fetchone()
    finally:
        conn.close()
    return row["content"] if row else None


# ---------------------------------------------------------------------------
# Metric results
# ---------------------------------------------------------------------------

def save_metric_result(
    metric_name: str,
    value: Optional[float],
    error: Optional[str],
    computed_at: str,
) -> None:
    """Persist a computed metric value."""
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT INTO metric_results (metric_name, value, error, computed_at) VALUES (?, ?, ?, ?)",
            (metric_name, value, error, computed_at),
        )
        conn.commit()
    finally:
        conn.close()


def get_metric_history(metric_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent computed values for a named metric."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT metric_name, value, error, computed_at
            FROM metric_results
            WHERE metric_name = ?
            ORDER BY computed_at DESC
            LIMIT ?
            """,
            (metric_name, limit),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Cost attribution
# ---------------------------------------------------------------------------

def save_pipeline_cost(
    run_id: str,
    pipeline_name: str,
    team: Optional[str],
    task_count: int,
    duration_seconds: float,
    estimated_cost_usd: float,
    recorded_at: str,
) -> None:
    """Persist a cost record for a pipeline run."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO pipeline_costs
                (run_id, pipeline_name, team, task_count, duration_seconds,
                 estimated_cost_usd, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, pipeline_name, team, task_count, duration_seconds,
             estimated_cost_usd, recorded_at),
        )
        conn.commit()
    finally:
        conn.close()


def get_cost_by_pipeline(limit: int = 100) -> List[Dict[str, Any]]:
    """Aggregate cost grouped by pipeline and team."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name, team,
                   COUNT(*) AS run_count,
                   SUM(duration_seconds) AS total_duration_seconds,
                   SUM(estimated_cost_usd) AS total_cost_usd,
                   MAX(recorded_at) AS last_run_at
            FROM pipeline_costs
            GROUP BY pipeline_name, team
            ORDER BY total_cost_usd DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_cost_by_team() -> List[Dict[str, Any]]:
    """Aggregate cost grouped by team."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT COALESCE(team, 'unassigned') AS team,
                   COUNT(*) AS run_count,
                   COUNT(DISTINCT pipeline_name) AS pipeline_count,
                   SUM(duration_seconds) AS total_duration_seconds,
                   SUM(estimated_cost_usd) AS total_cost_usd
            FROM pipeline_costs
            GROUP BY team
            ORDER BY total_cost_usd DESC
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_pipeline_cost_history(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Per-run cost history for a pipeline."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT run_id, pipeline_name, team, task_count, duration_seconds,
                   estimated_cost_usd, recorded_at
            FROM pipeline_costs
            WHERE pipeline_name = ?
            ORDER BY recorded_at DESC
            LIMIT ?
            """,
            (pipeline_name, limit),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Data catalog helpers (derived from lineage_records)
# ---------------------------------------------------------------------------

def get_catalog_assets(query: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Return data assets with usage stats, optionally filtered by URI substring."""
    conn = _get_conn()
    try:
        if query:
            rows = conn.execute(
                """
                SELECT asset_uri,
                       COUNT(DISTINCT pipeline_name) AS pipeline_count,
                       SUM(CASE WHEN direction='reads_from' THEN 1 ELSE 0 END) AS read_count,
                       SUM(CASE WHEN direction='writes_to'  THEN 1 ELSE 0 END) AS write_count,
                       MAX(recorded_at) AS last_seen_at
                FROM lineage_records
                WHERE asset_uri LIKE ?
                GROUP BY asset_uri
                ORDER BY pipeline_count DESC, last_seen_at DESC
                LIMIT ?
                """,
                (f"%{query}%", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT asset_uri,
                       COUNT(DISTINCT pipeline_name) AS pipeline_count,
                       SUM(CASE WHEN direction='reads_from' THEN 1 ELSE 0 END) AS read_count,
                       SUM(CASE WHEN direction='writes_to'  THEN 1 ELSE 0 END) AS write_count,
                       MAX(recorded_at) AS last_seen_at
                FROM lineage_records
                GROUP BY asset_uri
                ORDER BY pipeline_count DESC, last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_catalog_asset_detail(asset_uri: str) -> List[Dict[str, Any]]:
    """Return per-pipeline usage records for one asset."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name, task_name, direction, MAX(recorded_at) AS last_seen
            FROM lineage_records
            WHERE asset_uri = ?
            GROUP BY pipeline_name, task_name, direction
            ORDER BY last_seen DESC
            """,
            (asset_uri,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    if "details" in d and isinstance(d["details"], str):
        try:
            d["details"] = json.loads(d["details"])
        except Exception:
            pass
    return d
