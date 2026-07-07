"""Metadata store for pipeline run history and user management.

Supports both SQLite (default) and PostgreSQL (when POSTGRES_URL is set).
Uses SQLAlchemy Core so DDL and queries are portable across both backends.

WAL journal mode is enabled for SQLite so multiple readers don't block writes.
"""
import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.exc import IntegrityError

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
# SQLAlchemy metadata / table definitions
# ---------------------------------------------------------------------------

_metadata = MetaData()

_pipeline_runs = Table(
    "pipeline_runs",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("message", Text),
    Column("details", Text),
    Column("started_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
)

_users = Table(
    "users",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String, unique=True, nullable=False),
    Column("password_hash", String, nullable=False),
    Column("role", String, nullable=False, server_default="viewer"),
    Column("team", String),
    Column("created_at", String, nullable=False),
)

_lineage_records = Table(
    "lineage_records",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("task_name", String, nullable=False),
    Column("direction", String, nullable=False),
    Column("asset_uri", String, nullable=False),
    Column("recorded_at", String, nullable=False),
)

_quality_results = Table(
    "quality_results",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("task_name", String, nullable=False),
    Column("check_name", String, nullable=False),
    Column("passed", Integer, nullable=False),
    Column("actual_value", Text),
    Column("expected_value", Text),
    Column("error", Text),
    Column("checked_at", String, nullable=False),
)

_sla_violations = Table(
    "sla_violations",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("duration_seconds", Float, nullable=False),
    Column("limit_seconds", Float, nullable=False),
    Column("alerted", Integer, nullable=False, server_default="0"),
    Column("violated_at", String, nullable=False),
)

_triggers = Table(
    "triggers",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("trigger_id", String, unique=True, nullable=False),
    Column("trigger_type", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("config_path", String, nullable=False),
    Column("trigger_config", Text, nullable=False),
    Column("enabled", Integer, nullable=False, server_default="1"),
    Column("created_at", String, nullable=False),
    Column("last_fired_at", String),
)

_pipeline_versions = Table(
    "pipeline_versions",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("version_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("version_hash", String, nullable=False),
    Column("content", Text, nullable=False),
    Column("saved_by", String),
    Column("saved_at", String, nullable=False),
    UniqueConstraint("pipeline_name", "version_hash", name="uq_versions_pipeline_hash"),
)

_metric_results = Table(
    "metric_results",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("metric_name", String, nullable=False),
    Column("value", Float),
    Column("error", Text),
    Column("computed_at", String, nullable=False),
)

_pipeline_costs = Table(
    "pipeline_costs",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("team", String),
    Column("task_count", Integer, nullable=False),
    Column("duration_seconds", Float, nullable=False),
    Column("estimated_cost_usd", Float, nullable=False),
    Column("recorded_at", String, nullable=False),
)

_git_remotes = Table(
    "git_remotes",
    _metadata,
    Column("id", String, primary_key=True),
    Column("name", String, unique=True, nullable=False),
    Column("remote_url", String, nullable=False),
    Column("auth_type", String, nullable=False, server_default="token"),
    Column("token", String),
    Column("branch", String, nullable=False, server_default="main"),
    Column("pipelines_path", String, nullable=False, server_default="pipelines"),
    Column("clone_path", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("created_by", String),
)

_git_push_log = Table(
    "git_push_log",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("remote_id", String, nullable=False),
    Column("remote_name", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("commit_sha", String),
    Column("commit_message", Text),
    Column("pushed_by", String),
    Column("status", String, nullable=False),
    Column("error", Text),
    Column("pushed_at", String, nullable=False),
)

_audit_log = Table(
    "audit_log",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", String, nullable=False, unique=True),
    Column("event_type", String, nullable=False),
    Column("actor", String),
    Column("resource", String),
    Column("action", String, nullable=False),
    Column("details", Text),
    Column("occurred_at", String, nullable=False),
)

_scheduler_schedules = Table(
    "scheduler_schedules",
    _metadata,
    Column("pipeline_name", String, primary_key=True),
    Column("config_path", String, nullable=False),
    Column("schedule", Text, nullable=False),  # JSON-encoded cron dict
    Column("updated_at", String, nullable=False),
)

_pipeline_queue = Table(
    "pipeline_queue",
    _metadata,
    Column("run_id", String, primary_key=True),
    Column("pipeline_name", String, nullable=False),
    Column("config_path", String, nullable=False),
    Column("status", String, nullable=False),  # queued/running/completed/failed/cancelled
    Column("actor", String),
    Column("queued_at", String, nullable=False),
    Column("started_at", String),
    Column("completed_at", String),
    Column("error", Text),
)

# Indexes (defined after tables so they can reference column objects)
Index("idx_runs_pipeline", _pipeline_runs.c.pipeline_name, _pipeline_runs.c.updated_at)
Index("idx_lineage_asset", _lineage_records.c.asset_uri)
Index("idx_lineage_pipeline", _lineage_records.c.pipeline_name)
Index("idx_quality_pipeline", _quality_results.c.pipeline_name, _quality_results.c.checked_at)
Index(
    "idx_versions_pipeline_hash",
    _pipeline_versions.c.pipeline_name,
    _pipeline_versions.c.version_hash,
    unique=True,
)
Index("idx_versions_pipeline_time", _pipeline_versions.c.pipeline_name, _pipeline_versions.c.saved_at)
Index("idx_metric_results_name", _metric_results.c.metric_name, _metric_results.c.computed_at)
Index("idx_costs_pipeline", _pipeline_costs.c.pipeline_name, _pipeline_costs.c.recorded_at)
Index("idx_costs_team", _pipeline_costs.c.team, _pipeline_costs.c.recorded_at)
Index("idx_git_remotes_name", _git_remotes.c.name)
Index("idx_git_push_log_remote", _git_push_log.c.remote_id, _git_push_log.c.pushed_at)
Index("idx_git_push_log_pipeline", _git_push_log.c.pipeline_name, _git_push_log.c.pushed_at)
Index("idx_audit_event_type", _audit_log.c.event_type, _audit_log.c.occurred_at)
Index("idx_audit_actor", _audit_log.c.actor, _audit_log.c.occurred_at)
Index("idx_queue_status", _pipeline_queue.c.status, _pipeline_queue.c.queued_at)
Index("idx_queue_pipeline", _pipeline_queue.c.pipeline_name, _pipeline_queue.c.queued_at)

# ---------------------------------------------------------------------------
# Engine / connection helpers
# ---------------------------------------------------------------------------

_engine = None
_engine_db_path: Optional[Path] = None  # tracks which path the current engine points to


def _get_engine():
    global _engine, _engine_db_path

    postgres_url = os.getenv("POSTGRES_URL")
    if postgres_url:
        # For PostgreSQL, create once and reuse
        if _engine is not None:
            return _engine
        _engine = create_engine(postgres_url, pool_pre_ping=True)
        return _engine

    # SQLite: recreate the engine if _DB_PATH changed (e.g., test fixtures swap it)
    if _engine is not None and _engine_db_path == _DB_PATH:
        return _engine

    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    _engine = create_engine(
        f"sqlite:///{_DB_PATH}",
        connect_args={"check_same_thread": False},
    )
    _engine_db_path = _DB_PATH

    @event.listens_for(_engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return _engine


def _get_conn():
    """Return a new SQLAlchemy connection (use as context manager)."""
    return _get_engine().connect()


def init_db() -> None:
    """Create all tables if they don't exist. Safe to call multiple times."""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        engine = _get_engine()
        _metadata.create_all(engine)
        _initialized = True
        logger.info("Database initialised (engine: %s)", engine.url.get_backend_name())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    if "details" in d and isinstance(d["details"], str):
        try:
            d["details"] = json.loads(d["details"])
        except Exception:
            pass
    return d


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

    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_runs
                    (run_id, pipeline_name, status, message, details, started_at, updated_at)
                VALUES (:run_id, :pipeline_name, :status, :message, :details, :started_at, :updated_at)
                """
            ),
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "status": status,
                "message": message,
                "details": details_json,
                "started_at": now,
                "updated_at": now,
            },
        )
        conn.commit()

    return {
        "run_id": run_id,
        "status": status,
        "message": message,
        "details": details or {},
        "updated_at": now,
    }


def get_latest_run(pipeline_name: str) -> Optional[Dict[str, Any]]:
    """Return the most recent status entry for a pipeline, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text(
                """
                SELECT * FROM pipeline_runs
                WHERE pipeline_name = :pipeline_name
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {"pipeline_name": pipeline_name},
        ).fetchone()
    return _row_to_dict(row) if row else None


def get_run_by_id(run_id: str) -> Optional[Dict[str, Any]]:
    """Return the most recent status entry for a specific run_id, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text(
                """
                SELECT * FROM pipeline_runs
                WHERE run_id = :run_id
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {"run_id": run_id},
        ).fetchone()
    return _row_to_dict(row) if row else None


def get_run_history(pipeline_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Return the final status entry for each of the last `limit` distinct runs."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT r.*
                FROM pipeline_runs r
                INNER JOIN (
                    SELECT run_id, MAX(updated_at) AS max_ts
                    FROM pipeline_runs
                    WHERE pipeline_name = :pipeline_name
                    GROUP BY run_id
                    ORDER BY max_ts DESC
                    LIMIT :lim
                ) latest
                  ON r.run_id = latest.run_id
                 AND r.updated_at = latest.max_ts
                ORDER BY r.updated_at DESC
                """
            ),
            {"pipeline_name": pipeline_name, "lim": limit},
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_all_pipeline_names() -> List[str]:
    """Return distinct pipeline names that have at least one run."""
    with _get_conn() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name")
        ).fetchall()
    return [r._mapping["pipeline_name"] for r in rows]


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
    try:
        with _get_conn() as conn:
            conn.execute(
                text(
                    "INSERT INTO users (username, password_hash, role, team, created_at) "
                    "VALUES (:username, :password_hash, :role, :team, :created_at)"
                ),
                {
                    "username": username,
                    "password_hash": password_hash,
                    "role": role,
                    "team": team,
                    "created_at": now,
                },
            )
            conn.commit()
        return True
    except IntegrityError:
        return False


def get_user(username: str) -> Optional[Dict[str, Any]]:
    """Return a user record by username, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text(
                "SELECT id, username, password_hash, role, team, created_at "
                "FROM users WHERE username = :username"
            ),
            {"username": username},
        ).fetchone()
    return dict(row._mapping) if row else None


def list_users() -> List[Dict[str, Any]]:
    """Return all users without password hashes."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                "SELECT id, username, role, team, created_at FROM users ORDER BY username"
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def update_user_role(username: str, new_role: str) -> bool:
    """Update a user's role. Returns False if the user was not found."""
    with _get_conn() as conn:
        result = conn.execute(
            text("UPDATE users SET role = :role WHERE username = :username"),
            {"role": new_role, "username": username},
        )
        conn.commit()
    return result.rowcount > 0


def delete_user(username: str) -> bool:
    """Delete a user by username. Returns False if not found."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM users WHERE username = :username"),
            {"username": username},
        )
        conn.commit()
    return result.rowcount > 0


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
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO lineage_records
                    (run_id, pipeline_name, task_name, direction, asset_uri, recorded_at)
                VALUES (:run_id, :pipeline_name, :task_name, :direction, :asset_uri, :recorded_at)
                """
            ),
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "direction": direction,
                "asset_uri": asset_uri,
                "recorded_at": now,
            },
        )
        conn.commit()


def get_lineage_for_asset(asset_uri: str) -> List[Dict[str, Any]]:
    """Return all lineage records involving a specific asset URI."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM lineage_records WHERE asset_uri = :asset_uri "
                "ORDER BY recorded_at DESC"
            ),
            {"asset_uri": asset_uri},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_full_lineage_graph() -> List[Dict[str, Any]]:
    """Return all unique lineage edges (most recent occurrence per edge)."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pipeline_name, task_name, direction, asset_uri,
                       MAX(recorded_at) AS last_seen
                FROM lineage_records
                GROUP BY pipeline_name, task_name, direction, asset_uri
                ORDER BY last_seen DESC
                """
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


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
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO quality_results
                    (run_id, pipeline_name, task_name, check_name, passed,
                     actual_value, expected_value, error, checked_at)
                VALUES (:run_id, :pipeline_name, :task_name, :check_name, :passed,
                        :actual_value, :expected_value, :error, :checked_at)
                """
            ),
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "check_name": check_name,
                "passed": int(passed),
                "actual_value": actual_value,
                "expected_value": expected_value,
                "error": error,
                "checked_at": now,
            },
        )
        conn.commit()


def get_quality_results(pipeline_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return the most recent quality check results for a pipeline."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM quality_results
                WHERE pipeline_name = :pipeline_name
                ORDER BY checked_at DESC
                LIMIT :lim
                """
            ),
            {"pipeline_name": pipeline_name, "lim": limit},
        ).fetchall()
    results = []
    for r in rows:
        d = dict(r._mapping)
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
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO sla_violations
                    (run_id, pipeline_name, duration_seconds, limit_seconds, alerted, violated_at)
                VALUES (:run_id, :pipeline_name, :duration_seconds, :limit_seconds, :alerted, :violated_at)
                """
            ),
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "duration_seconds": duration_seconds,
                "limit_seconds": limit_seconds,
                "alerted": int(alerted),
                "violated_at": now,
            },
        )
        conn.commit()


def get_sla_violations(pipeline_name: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """Return recent SLA violations, optionally filtered by pipeline."""
    with _get_conn() as conn:
        if pipeline_name:
            rows = conn.execute(
                text(
                    "SELECT * FROM sla_violations WHERE pipeline_name = :pipeline_name "
                    "ORDER BY violated_at DESC LIMIT :lim"
                ),
                {"pipeline_name": pipeline_name, "lim": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text("SELECT * FROM sla_violations ORDER BY violated_at DESC LIMIT :lim"),
                {"lim": limit},
            ).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Metrics aggregates
# ---------------------------------------------------------------------------

def get_run_counts_by_status() -> List[Dict[str, Any]]:
    """Return {pipeline_name, status, count} for Prometheus metrics."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pipeline_name, status, COUNT(*) AS count
                FROM pipeline_runs
                GROUP BY pipeline_name, status
                """
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_quality_counts() -> List[Dict[str, Any]]:
    """Return {pipeline_name, task_name, passed, count} for Prometheus metrics."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pipeline_name, task_name, passed, COUNT(*) AS count
                FROM quality_results
                GROUP BY pipeline_name, task_name, passed
                """
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_sla_violation_counts() -> List[Dict[str, Any]]:
    """Return {pipeline_name, count} of total SLA violations."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pipeline_name, COUNT(*) AS count
                FROM sla_violations
                GROUP BY pipeline_name
                """
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


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
    try:
        with _get_conn() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO triggers
                        (trigger_id, trigger_type, pipeline_name, config_path,
                         trigger_config, enabled, created_at)
                    VALUES (:trigger_id, :trigger_type, :pipeline_name, :config_path,
                            :trigger_config, 1, :created_at)
                    """
                ),
                {
                    "trigger_id": trigger_id,
                    "trigger_type": trigger_type,
                    "pipeline_name": pipeline_name,
                    "config_path": config_path,
                    "trigger_config": json.dumps(trigger_config),
                    "created_at": now,
                },
            )
            conn.commit()
        return True
    except IntegrityError:
        return False


def get_triggers(enabled_only: bool = False) -> List[Dict[str, Any]]:
    """Return all triggers, optionally filtered to enabled ones."""
    with _get_conn() as conn:
        if enabled_only:
            rows = conn.execute(
                text("SELECT * FROM triggers WHERE enabled = 1 ORDER BY created_at ASC")
            ).fetchall()
        else:
            rows = conn.execute(
                text("SELECT * FROM triggers ORDER BY created_at ASC")
            ).fetchall()
    result = []
    for r in rows:
        d = dict(r._mapping)
        try:
            d["trigger_config"] = json.loads(d["trigger_config"])
        except Exception:
            pass
        result.append(d)
    return result


def get_trigger(trigger_id: str) -> Optional[Dict[str, Any]]:
    """Return a single trigger record, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM triggers WHERE trigger_id = :trigger_id"),
            {"trigger_id": trigger_id},
        ).fetchone()
    if row is None:
        return None
    d = dict(row._mapping)
    try:
        d["trigger_config"] = json.loads(d["trigger_config"])
    except Exception:
        pass
    return d


def delete_trigger(trigger_id: str) -> bool:
    """Delete a trigger by ID. Returns False if not found."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM triggers WHERE trigger_id = :trigger_id"),
            {"trigger_id": trigger_id},
        )
        conn.commit()
    return result.rowcount > 0


def update_trigger_last_fired(trigger_id: str) -> None:
    """Stamp last_fired_at for a trigger."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                "UPDATE triggers SET last_fired_at = :now WHERE trigger_id = :trigger_id"
            ),
            {"now": now, "trigger_id": trigger_id},
        )
        conn.commit()


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
    try:
        with _get_conn() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO pipeline_versions
                        (version_id, pipeline_name, version_hash, content, saved_by, saved_at)
                    VALUES (:version_id, :pipeline_name, :version_hash, :content, :saved_by, :saved_at)
                    """
                ),
                {
                    "version_id": version_id,
                    "pipeline_name": pipeline_name,
                    "version_hash": version_hash,
                    "content": content,
                    "saved_by": saved_by,
                    "saved_at": saved_at,
                },
            )
            conn.commit()
        return True
    except IntegrityError:
        return False  # duplicate hash


def get_pipeline_versions(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """List pipeline versions (no content), newest first."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT version_id, pipeline_name, version_hash, saved_by, saved_at
                FROM pipeline_versions
                WHERE pipeline_name = :pipeline_name
                ORDER BY saved_at DESC
                LIMIT :lim
                """
            ),
            {"pipeline_name": pipeline_name, "lim": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_pipeline_version_content(pipeline_name: str, version_id: str) -> Optional[str]:
    """Return the YAML content for a specific version, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text(
                "SELECT content FROM pipeline_versions "
                "WHERE pipeline_name = :pipeline_name AND version_id = :version_id"
            ),
            {"pipeline_name": pipeline_name, "version_id": version_id},
        ).fetchone()
    return row._mapping["content"] if row else None


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
    with _get_conn() as conn:
        conn.execute(
            text(
                "INSERT INTO metric_results (metric_name, value, error, computed_at) "
                "VALUES (:metric_name, :value, :error, :computed_at)"
            ),
            {
                "metric_name": metric_name,
                "value": value,
                "error": error,
                "computed_at": computed_at,
            },
        )
        conn.commit()


def get_metric_history(metric_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent computed values for a named metric."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT metric_name, value, error, computed_at
                FROM metric_results
                WHERE metric_name = :metric_name
                ORDER BY computed_at DESC
                LIMIT :lim
                """
            ),
            {"metric_name": metric_name, "lim": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


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
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_costs
                    (run_id, pipeline_name, team, task_count, duration_seconds,
                     estimated_cost_usd, recorded_at)
                VALUES (:run_id, :pipeline_name, :team, :task_count, :duration_seconds,
                        :estimated_cost_usd, :recorded_at)
                """
            ),
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "team": team,
                "task_count": task_count,
                "duration_seconds": duration_seconds,
                "estimated_cost_usd": estimated_cost_usd,
                "recorded_at": recorded_at,
            },
        )
        conn.commit()


def get_cost_by_pipeline(limit: int = 100) -> List[Dict[str, Any]]:
    """Aggregate cost grouped by pipeline and team."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pipeline_name, team,
                       COUNT(*) AS run_count,
                       SUM(duration_seconds) AS total_duration_seconds,
                       SUM(estimated_cost_usd) AS total_cost_usd,
                       MAX(recorded_at) AS last_run_at
                FROM pipeline_costs
                GROUP BY pipeline_name, team
                ORDER BY total_cost_usd DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_cost_by_team() -> List[Dict[str, Any]]:
    """Aggregate cost grouped by team."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
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
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_pipeline_cost_history(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Per-run cost history for a pipeline."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT run_id, pipeline_name, team, task_count, duration_seconds,
                       estimated_cost_usd, recorded_at
                FROM pipeline_costs
                WHERE pipeline_name = :pipeline_name
                ORDER BY recorded_at DESC
                LIMIT :lim
                """
            ),
            {"pipeline_name": pipeline_name, "lim": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Data catalog helpers (derived from lineage_records)
# ---------------------------------------------------------------------------

def get_catalog_assets(query: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Return data assets with usage stats, optionally filtered by URI substring."""
    with _get_conn() as conn:
        if query:
            rows = conn.execute(
                text(
                    """
                    SELECT asset_uri,
                           COUNT(DISTINCT pipeline_name) AS pipeline_count,
                           SUM(CASE WHEN direction='reads_from' THEN 1 ELSE 0 END) AS read_count,
                           SUM(CASE WHEN direction='writes_to'  THEN 1 ELSE 0 END) AS write_count,
                           MAX(recorded_at) AS last_seen_at
                    FROM lineage_records
                    WHERE asset_uri LIKE :pattern
                    GROUP BY asset_uri
                    ORDER BY pipeline_count DESC, last_seen_at DESC
                    LIMIT :lim
                    """
                ),
                {"pattern": f"%{query}%", "lim": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT asset_uri,
                           COUNT(DISTINCT pipeline_name) AS pipeline_count,
                           SUM(CASE WHEN direction='reads_from' THEN 1 ELSE 0 END) AS read_count,
                           SUM(CASE WHEN direction='writes_to'  THEN 1 ELSE 0 END) AS write_count,
                           MAX(recorded_at) AS last_seen_at
                    FROM lineage_records
                    GROUP BY asset_uri
                    ORDER BY pipeline_count DESC, last_seen_at DESC
                    LIMIT :lim
                    """
                ),
                {"lim": limit},
            ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_catalog_asset_detail(asset_uri: str) -> List[Dict[str, Any]]:
    """Return per-pipeline usage records for one asset."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT pipeline_name, task_name, direction, MAX(recorded_at) AS last_seen
                FROM lineage_records
                WHERE asset_uri = :asset_uri
                GROUP BY pipeline_name, task_name, direction
                ORDER BY last_seen DESC
                """
            ),
            {"asset_uri": asset_uri},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Git remotes
# ---------------------------------------------------------------------------

def save_git_remote(
    remote_id: str,
    name: str,
    remote_url: str,
    auth_type: str,
    token: Optional[str],
    branch: str,
    pipelines_path: str,
    clone_path: str,
    created_by: Optional[str],
) -> bool:
    """Insert a git remote. Returns False if name already exists."""
    now = datetime.utcnow().isoformat() + "Z"
    try:
        with _get_conn() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO git_remotes
                        (id, name, remote_url, auth_type, token, branch,
                         pipelines_path, clone_path, created_at, created_by)
                    VALUES (:id, :name, :remote_url, :auth_type, :token, :branch,
                            :pipelines_path, :clone_path, :created_at, :created_by)
                    """
                ),
                {
                    "id": remote_id,
                    "name": name,
                    "remote_url": remote_url,
                    "auth_type": auth_type,
                    "token": token,
                    "branch": branch,
                    "pipelines_path": pipelines_path,
                    "clone_path": clone_path,
                    "created_at": now,
                    "created_by": created_by,
                },
            )
            conn.commit()
        return True
    except IntegrityError:
        return False


def list_git_remotes() -> List[Dict[str, Any]]:
    """Return all git remotes."""
    with _get_conn() as conn:
        rows = conn.execute(
            text("SELECT * FROM git_remotes ORDER BY created_at ASC")
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_git_remote(remote_id: str) -> Optional[Dict[str, Any]]:
    """Return a single git remote by id, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM git_remotes WHERE id = :id"),
            {"id": remote_id},
        ).fetchone()
    return dict(row._mapping) if row else None


def delete_git_remote(remote_id: str) -> bool:
    """Delete a git remote by id. Returns False if not found."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM git_remotes WHERE id = :id"),
            {"id": remote_id},
        )
        conn.commit()
    return result.rowcount > 0


def save_git_push_log(
    remote_id: str,
    remote_name: str,
    pipeline_name: str,
    commit_sha: Optional[str],
    commit_message: Optional[str],
    pushed_by: Optional[str],
    status: str,
    error: Optional[str],
) -> None:
    """Append an entry to the git push history log."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO git_push_log
                    (remote_id, remote_name, pipeline_name, commit_sha, commit_message,
                     pushed_by, status, error, pushed_at)
                VALUES (:remote_id, :remote_name, :pipeline_name, :commit_sha, :commit_message,
                        :pushed_by, :status, :error, :pushed_at)
                """
            ),
            {
                "remote_id": remote_id,
                "remote_name": remote_name,
                "pipeline_name": pipeline_name,
                "commit_sha": commit_sha,
                "commit_message": commit_message,
                "pushed_by": pushed_by,
                "status": status,
                "error": error,
                "pushed_at": now,
            },
        )
        conn.commit()


def list_git_push_log(remote_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Return recent push log entries for a remote, newest first."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM git_push_log
                WHERE remote_id = :remote_id
                ORDER BY pushed_at DESC
                LIMIT :lim
                """
            ),
            {"remote_id": remote_id, "lim": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def append_audit_event(
    event_type: str,
    action: str,
    actor: Optional[str] = None,
    resource: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> str:
    """Append an immutable audit event. Returns the new event_id."""
    import uuid
    event_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO audit_log (event_id, event_type, actor, resource, action, details, occurred_at)
                VALUES (:event_id, :event_type, :actor, :resource, :action, :details, :occurred_at)
                """
            ),
            {
                "event_id": event_id,
                "event_type": event_type,
                "actor": actor,
                "resource": resource,
                "action": action,
                "details": json.dumps(details or {}),
                "occurred_at": now,
            },
        )
        conn.commit()
    return event_id


def get_audit_log(
    limit: int = 100,
    actor: Optional[str] = None,
    resource: Optional[str] = None,
    event_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return recent audit log entries, newest first. Supports optional filters."""
    conditions = []
    params: Dict[str, Any] = {}
    if actor:
        conditions.append("actor = :actor")
        params["actor"] = actor
    if resource:
        conditions.append("resource = :resource")
        params["resource"] = resource
    if event_type:
        conditions.append("event_type = :event_type")
        params["event_type"] = event_type
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params["lim"] = limit
    with _get_conn() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM audit_log {where} ORDER BY occurred_at DESC LIMIT :lim"),
            params,
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r._mapping)
        try:
            d["details"] = json.loads(d["details"])
        except Exception:
            pass
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Scheduler schedules (replaces scheduler_state.json)
# ---------------------------------------------------------------------------

def save_schedule(pipeline_name: str, config_path: str, schedule: Dict[str, Any]) -> None:
    """Upsert a pipeline schedule record."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO scheduler_schedules (pipeline_name, config_path, schedule, updated_at)
                VALUES (:name, :config_path, :schedule, :now)
                ON CONFLICT (pipeline_name) DO UPDATE
                  SET config_path = excluded.config_path,
                      schedule    = excluded.schedule,
                      updated_at  = excluded.updated_at
                """
            ),
            {
                "name": pipeline_name,
                "config_path": config_path,
                "schedule": json.dumps(schedule),
                "now": now,
            },
        )
        conn.commit()


def list_schedules() -> List[Dict[str, Any]]:
    """Return all persisted pipeline schedules."""
    with _get_conn() as conn:
        rows = conn.execute(
            text("SELECT * FROM scheduler_schedules ORDER BY pipeline_name ASC")
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r._mapping)
        try:
            d["schedule"] = json.loads(d["schedule"])
        except Exception:
            pass
        result.append(d)
    return result


def delete_schedule(pipeline_name: str) -> bool:
    """Remove a pipeline schedule. Returns False if not found."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM scheduler_schedules WHERE pipeline_name = :name"),
            {"name": pipeline_name},
        )
        conn.commit()
    return result.rowcount > 0


# ---------------------------------------------------------------------------
# Persistent run queue
# ---------------------------------------------------------------------------

def enqueue_run(
    run_id: str,
    pipeline_name: str,
    config_path: str,
    actor: Optional[str] = None,
) -> None:
    """Record a new pipeline run as 'queued' in the persistent queue."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_queue
                    (run_id, pipeline_name, config_path, status, actor, queued_at)
                VALUES (:run_id, :pipeline_name, :config_path, :status, :actor, :queued_at)
                """
            ),
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "config_path": config_path,
                "status": "queued",
                "actor": actor,
                "queued_at": now,
            },
        )
        conn.commit()


def set_run_status_in_queue(
    run_id: str,
    status: str,
    error: Optional[str] = None,
) -> None:
    """Transition a queued run to running / completed / failed / cancelled."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        if status == "running":
            conn.execute(
                text(
                    "UPDATE pipeline_queue SET status = :status, started_at = :now WHERE run_id = :run_id"
                ),
                {"status": status, "now": now, "run_id": run_id},
            )
        else:
            conn.execute(
                text(
                    """
                    UPDATE pipeline_queue
                    SET status = :status, completed_at = :now, error = :error
                    WHERE run_id = :run_id
                    """
                ),
                {"status": status, "now": now, "error": error, "run_id": run_id},
            )
        conn.commit()


def get_queue_runs(
    status: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Return queue entries, newest first. Pass status= to filter (e.g. 'queued','running')."""
    params: Dict[str, Any] = {"lim": limit}
    where = ""
    if status:
        where = "WHERE status = :status"
        params["status"] = status
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                f"SELECT * FROM pipeline_queue {where} ORDER BY queued_at DESC LIMIT :lim"
            ),
            params,
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_queue_run(run_id: str) -> Optional[Dict[str, Any]]:
    """Return one persistent queue entry by run_id, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM pipeline_queue WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).fetchone()
    return dict(row._mapping) if row else None


def recover_orphaned_runs() -> int:
    """Mark any run stuck in queued/running state as failed on server restart.

    Returns the number of runs recovered.
    """
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                UPDATE pipeline_queue
                SET status = 'failed', completed_at = :now, error = 'Server restarted'
                WHERE status IN ('queued', 'running')
                """
            ),
            {"now": now},
        )
        conn.commit()
    recovered = result.rowcount
    if recovered:
        logger.warning(
            "Recovered %d orphaned run(s) from previous server instance", recovered
        )
    return recovered


# ---------------------------------------------------------------------------
# Timeseries metrics (for monitoring charts)
# ---------------------------------------------------------------------------

def get_run_timeseries(range_hours: int = 24) -> dict:
    """Return bucketed run counts and per-pipeline stats for the monitoring charts.

    Queries pipeline_queue (has timestamps) + pipeline_runs (has per-pipeline
    history).  All heavy bucketing is done in Python to stay DB-agnostic.
    """
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=range_hours)
    since_str = since.isoformat()

    with _get_engine().connect() as conn:
        rows = conn.execute(
            text(
                "SELECT pipeline_name, status, queued_at, started_at, completed_at "
                "FROM pipeline_queue "
                "WHERE queued_at >= :since "
                "ORDER BY queued_at ASC"
            ),
            {"since": since_str},
        ).fetchall()

    runs = [dict(r._mapping) for r in rows]

    # ── 24 h exec-volume & error-rate (2-hour buckets, 12 slots) ────────────
    bucket_hours = max(1, range_hours // 12)
    buckets: dict[int, dict] = {i: {"total": 0, "failed": 0} for i in range(12)}

    for r in runs:
        try:
            ts = datetime.fromisoformat(r["queued_at"].replace("Z", "+00:00"))
            age_hours = (now - ts).total_seconds() / 3600
            slot = min(11, int(age_hours // bucket_hours))
            idx = 11 - slot   # most-recent bucket last
            buckets[idx]["total"] += 1
            if r["status"] == "failed":
                buckets[idx]["failed"] += 1
        except Exception:
            pass

    exec_volume = [buckets[i]["total"]  for i in range(12)]
    error_count = [buckets[i]["failed"] for i in range(12)]

    # ── Hour labels ──────────────────────────────────────────────────────────
    if range_hours <= 24:
        labels = [
            (now - timedelta(hours=(11 - i) * bucket_hours)).strftime("%H:%M")
            for i in range(12)
        ]
    else:
        labels = [
            (now - timedelta(hours=(11 - i) * bucket_hours)).strftime("%d/%m")
            for i in range(12)
        ]

    # ── 7-day throughput (daily buckets) ────────────────────────────────────
    day_labels, throughput_ok, throughput_fail = [], [], []
    for d in range(6, -1, -1):
        day_start = (now - timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end   = day_start + timedelta(days=1)
        ok   = sum(1 for r in runs
                   if r["status"] == "completed"
                   and day_start.isoformat() <= r.get("queued_at","") < day_end.isoformat())
        fail = sum(1 for r in runs
                   if r["status"] == "failed"
                   and day_start.isoformat() <= r.get("queued_at","") < day_end.isoformat())
        day_labels.append(day_start.strftime("%a"))
        throughput_ok.append(ok)
        throughput_fail.append(fail)

    # ── Per-pipeline P95 durations (seconds) ────────────────────────────────
    from collections import defaultdict
    pipe_durations: dict[str, list] = defaultdict(list)
    for r in runs:
        if r.get("started_at") and r.get("completed_at") and r["status"] == "completed":
            try:
                s = datetime.fromisoformat(r["started_at"].replace("Z", "+00:00"))
                e = datetime.fromisoformat(r["completed_at"].replace("Z", "+00:00"))
                pipe_durations[r["pipeline_name"]].append((e - s).total_seconds())
            except Exception:
                pass

    # Get all known pipeline names from pipeline_runs for completeness
    with _get_engine().connect() as conn:
        name_rows = conn.execute(
            text("SELECT DISTINCT pipeline_name FROM pipeline_runs LIMIT 10")
        ).fetchall()
    pipeline_names = [r[0] for r in name_rows] or list(pipe_durations.keys())[:6]

    def p95(vals):
        if not vals:
            return 0
        s = sorted(vals)
        idx = max(0, int(len(s) * 0.95) - 1)
        return round(s[idx])

    p95_durations = [p95(pipe_durations.get(n, [])) for n in pipeline_names]

    # ── SLA compliance (from sla_violations table if it exists, else from queue) ─
    sla_total = len(runs)
    sla_ok    = sum(1 for r in runs if r["status"] == "completed")

    # ── 7-day health map ─────────────────────────────────────────────────────
    health_map = []
    for name in pipeline_names[:6]:
        row = []
        for d in range(6, -1, -1):
            day_start = (now - timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
            day_end   = day_start + timedelta(days=1)
            day_runs  = [r for r in runs
                         if r["pipeline_name"] == name
                         and day_start.isoformat() <= r.get("queued_at","") < day_end.isoformat()]
            if not day_runs:
                row.append("idle")
            elif any(r["status"] == "failed" for r in day_runs):
                row.append("fail")
            else:
                row.append("ok")
        health_map.append(row)

    return {
        "range_hours":     range_hours,
        "hour_labels":     labels,
        "exec_volume":     exec_volume,
        "error_count":     error_count,
        "day_labels":      day_labels,
        "throughput_ok":   throughput_ok,
        "throughput_fail": throughput_fail,
        "pipeline_names":  pipeline_names,
        "p95_durations":   p95_durations,
        "sla_ok":          sla_ok,
        "sla_total":       sla_total,
        "health_map":      health_map,
    }
