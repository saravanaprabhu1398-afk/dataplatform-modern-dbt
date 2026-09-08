"""Metadata store for pipeline run history and user management.

Supports both SQLite (default) and PostgreSQL (when POSTGRES_URL is set).
Uses SQLAlchemy Core so DDL and queries are portable across both backends.

WAL journal mode is enabled for SQLite so multiple readers don't block writes.
"""
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timedelta
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
    inspect,
    text,
)
from sqlalchemy.exc import IntegrityError

from dataplatform.core.leases import Lease, current_lease

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
    # Lease + fencing: who holds this run, until when, and on which attempt.
    # ``attempt`` only ever increases for a run_id, so it doubles as a fencing
    # token -- a worker whose lease expired can be rejected on write.
    Column("worker_id", String),
    Column("lease_expires_at", String),
    Column("heartbeat_at", String),
    Column("attempt", Integer, nullable=False, server_default="0"),
)

_stream_state = Table(
    "stream_state",
    _metadata,
    Column("stream", String, primary_key=True),
    Column("partition", String, primary_key=True),
    Column("next_offset", Integer, nullable=False),
    # The fencing token of the writer that last advanced this partition.
    Column("attempt", Integer, nullable=False, server_default="0"),
    Column("watermark", String),
    Column("updated_at", String, nullable=False),
)

_metric_samples = Table(
    "metric_samples",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("metric_name", String, nullable=False),
    Column("value", Float, nullable=False),
    Column("unit", String),
    Column("pipeline_name", String),
    Column("labels", Text),
    Column("collected_at", String, nullable=False),
)

_alert_rules = Table(
    "alert_rules",
    _metadata,
    Column("rule_id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("metric_name", String, nullable=False),
    Column("comparator", String, nullable=False),
    Column("threshold", Float, nullable=False),
    Column("severity", String, nullable=False),
    Column("window_minutes", Integer, nullable=False, server_default="60"),
    Column("pipeline_name", String),
    Column("enabled", Integer, nullable=False, server_default="1"),
    Column("destination_type", String),
    Column("destination", Text),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
)

_alert_incidents = Table(
    "alert_incidents",
    _metadata,
    Column("incident_id", String, primary_key=True),
    Column("rule_id", String, nullable=False),
    Column("rule_name", String, nullable=False),
    Column("metric_name", String, nullable=False),
    Column("pipeline_name", String),
    Column("severity", String, nullable=False),
    Column("status", String, nullable=False),
    Column("observed_value", Float, nullable=False),
    Column("threshold", Float, nullable=False),
    Column("message", Text, nullable=False),
    Column("labels", Text),
    Column("fired_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Column("acknowledged_at", String),
    Column("acknowledged_by", String),
    Column("resolved_at", String),
)

_notification_channels = Table(
    "notification_channels",
    _metadata,
    Column("channel_id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("channel_type", String, nullable=False),
    Column("destination", Text, nullable=False),
    Column("severities", Text),
    Column("enabled", Integer, nullable=False, server_default="1"),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
)

_notification_deliveries = Table(
    "notification_deliveries",
    _metadata,
    Column("delivery_id", String, primary_key=True),
    Column("incident_id", String, nullable=False),
    Column("rule_id", String, nullable=False),
    Column("channel_id", String),
    Column("channel_name", String),
    Column("destination_type", String, nullable=False),
    Column("destination", Text, nullable=False),
    Column("status", String, nullable=False),
    Column("error", Text),
    Column("attempted_at", String, nullable=False),
)

_deployments = Table(
    "deployments",
    _metadata,
    Column("deployment_id", String, primary_key=True),
    Column("pipeline_name", String, nullable=False),
    Column("config_path", Text, nullable=False),
    Column("version_id", String),
    Column("version_hash", String),
    Column("environment_profile", String, nullable=False),
    Column("target_id", String, nullable=False),
    Column("target_name", String, nullable=False),
    Column("connection_id", String),
    Column("connection_name", String),
    Column("connection_provider", String),
    Column("status", String, nullable=False),
    Column("active", Integer, nullable=False, server_default="0"),
    Column("actor", String),
    Column("notes", Text),
    Column("validation_summary", Text),
    Column("execution_fabric", Text),
    Column("manifest", Text),
    Column("created_at", String, nullable=False),
    Column("deployed_at", String),
    Column("updated_at", String, nullable=False),
    Column("rolled_back_at", String),
    Column("rollback_of", String),
    Column("restored_deployment_id", String),
)

_deployment_connections = Table(
    "deployment_connections",
    _metadata,
    Column("connection_id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("target_id", String, nullable=False),
    Column("provider", String, nullable=False),
    Column("endpoint", Text),
    Column("region", String),
    Column("namespace", String),
    Column("status", String, nullable=False),
    Column("credentials_ref", Text),
    Column("labels", Text),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Column("last_checked_at", String),
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
Index("idx_queue_lease", _pipeline_queue.c.status, _pipeline_queue.c.lease_expires_at)
Index("idx_metric_samples_name_time", _metric_samples.c.metric_name, _metric_samples.c.collected_at)
Index("idx_metric_samples_pipeline_time", _metric_samples.c.pipeline_name, _metric_samples.c.collected_at)
Index("idx_alert_rules_enabled", _alert_rules.c.enabled, _alert_rules.c.metric_name)
Index("idx_alert_incidents_status", _alert_incidents.c.status, _alert_incidents.c.updated_at)
Index("idx_alert_incidents_rule", _alert_incidents.c.rule_id, _alert_incidents.c.status)
Index("idx_notification_channels_enabled", _notification_channels.c.enabled, _notification_channels.c.channel_type)
Index("idx_notification_deliveries_incident", _notification_deliveries.c.incident_id, _notification_deliveries.c.attempted_at)
Index("idx_notification_deliveries_status", _notification_deliveries.c.status, _notification_deliveries.c.attempted_at)
Index("idx_deployments_pipeline_env", _deployments.c.pipeline_name, _deployments.c.environment_profile, _deployments.c.target_id)
Index("idx_deployments_status", _deployments.c.status, _deployments.c.updated_at)
Index("idx_deployments_active", _deployments.c.active, _deployments.c.updated_at)
Index("idx_deployment_connections_target", _deployment_connections.c.target_id, _deployment_connections.c.status)

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
        _ensure_schema_migrations(engine)
        _initialized = True
        logger.info("Database initialised (engine: %s)", engine.url.get_backend_name())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_schema_migrations(engine: Any) -> None:
    """Apply additive schema changes for existing metadata stores."""
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    if "deployments" in table_names:
        deployment_columns = {column["name"] for column in inspector.get_columns("deployments")}
        optional_columns = {
            "connection_id": "VARCHAR",
            "connection_name": "VARCHAR",
            "connection_provider": "VARCHAR",
        }
        with engine.begin() as conn:
            for column_name, column_type in optional_columns.items():
                if column_name not in deployment_columns:
                    conn.execute(text(f"ALTER TABLE deployments ADD COLUMN {column_name} {column_type}"))

    if "pipeline_queue" in table_names:
        queue_columns = {column["name"] for column in inspector.get_columns("pipeline_queue")}
        lease_columns = {
            "worker_id": "VARCHAR",
            "lease_expires_at": "VARCHAR",
            "heartbeat_at": "VARCHAR",
            "attempt": "INTEGER DEFAULT 0",
        }
        with engine.begin() as conn:
            for column_name, column_type in lease_columns.items():
                if column_name not in queue_columns:
                    conn.execute(
                        text(f"ALTER TABLE pipeline_queue ADD COLUMN {column_name} {column_type}")
                    )
            # Rows written before the migration have a NULL attempt; the
            # fencing comparison needs a number, not a NULL.
            if "attempt" not in queue_columns:
                conn.execute(text("UPDATE pipeline_queue SET attempt = 0 WHERE attempt IS NULL"))


def _row_to_dict(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    if "details" in d and isinstance(d["details"], str):
        try:
            d["details"] = json.loads(d["details"])
        except Exception:
            pass
    return d


def _json_or_none(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


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


def get_quality_results_since(since: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """Return quality check results recorded at or after *since*."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM quality_results
                WHERE checked_at >= :since
                ORDER BY checked_at DESC
                LIMIT :lim
                """
            ),
            {"since": since, "lim": limit},
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


def get_sla_violations_since(since: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """Return SLA violations recorded at or after *since*."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM sla_violations
                WHERE violated_at >= :since
                ORDER BY violated_at DESC
                LIMIT :lim
                """
            ),
            {"since": since, "lim": limit},
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
# Observability metric samples and alerting
# ---------------------------------------------------------------------------

def save_metric_sample(
    metric_name: str,
    value: float,
    unit: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
    collected_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist one collected observability metric sample."""
    now = collected_at or datetime.utcnow().isoformat() + "Z"
    labels_json = json.dumps(labels or {}, sort_keys=True)
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO metric_samples
                    (metric_name, value, unit, pipeline_name, labels, collected_at)
                VALUES (:metric_name, :value, :unit, :pipeline_name, :labels, :collected_at)
                """
            ),
            {
                "metric_name": metric_name,
                "value": float(value),
                "unit": unit,
                "pipeline_name": pipeline_name,
                "labels": labels_json,
                "collected_at": now,
            },
        )
        conn.commit()
        row_id = result.lastrowid
    return {
        "id": row_id,
        "metric_name": metric_name,
        "value": float(value),
        "unit": unit,
        "pipeline_name": pipeline_name,
        "labels": labels or {},
        "collected_at": now,
    }


def save_metric_samples(samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Persist multiple observability metric samples."""
    return [
        save_metric_sample(
            metric_name=s["metric_name"],
            value=s["value"],
            unit=s.get("unit"),
            pipeline_name=s.get("pipeline_name"),
            labels=s.get("labels"),
            collected_at=s.get("collected_at"),
        )
        for s in samples
    ]


def get_metric_samples(
    metric_name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Return metric samples newest first."""
    clauses: List[str] = []
    params: Dict[str, Any] = {"lim": limit}
    if metric_name:
        clauses.append("metric_name = :metric_name")
        params["metric_name"] = metric_name
    if pipeline_name:
        clauses.append("pipeline_name = :pipeline_name")
        params["pipeline_name"] = pipeline_name
    if since:
        clauses.append("collected_at >= :since")
        params["since"] = since
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM metric_samples
                {where}
                ORDER BY collected_at DESC, id DESC
                LIMIT :lim
                """
            ),
            params,
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row._mapping)
        d["labels"] = _json_or_none(d.get("labels")) or {}
        result.append(d)
    return result


def get_latest_metric_samples(limit: int = 2000) -> List[Dict[str, Any]]:
    """Return the latest sample for each metric/pipeline combination."""
    latest: Dict[tuple, Dict[str, Any]] = {}
    for row in get_metric_samples(limit=limit):
        key = (row.get("metric_name"), row.get("pipeline_name"))
        if key not in latest:
            latest[key] = row
    return list(latest.values())


def create_alert_rule(
    rule_id: str,
    name: str,
    metric_name: str,
    comparator: str,
    threshold: float,
    severity: str = "warning",
    window_minutes: int = 60,
    pipeline_name: Optional[str] = None,
    enabled: bool = True,
    destination_type: Optional[str] = None,
    destination: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist an alert rule."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO alert_rules
                    (rule_id, name, metric_name, comparator, threshold, severity,
                     window_minutes, pipeline_name, enabled, destination_type,
                     destination, created_at, updated_at)
                VALUES
                    (:rule_id, :name, :metric_name, :comparator, :threshold,
                     :severity, :window_minutes, :pipeline_name, :enabled,
                     :destination_type, :destination, :created_at, :updated_at)
                """
            ),
            {
                "rule_id": rule_id,
                "name": name,
                "metric_name": metric_name,
                "comparator": comparator,
                "threshold": float(threshold),
                "severity": severity,
                "window_minutes": int(window_minutes),
                "pipeline_name": pipeline_name,
                "enabled": int(bool(enabled)),
                "destination_type": destination_type,
                "destination": destination,
                "created_at": now,
                "updated_at": now,
            },
        )
        conn.commit()
    return get_alert_rule(rule_id) or {}


def _alert_rule_row(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    d["enabled"] = bool(d["enabled"])
    return d


def get_alert_rule(rule_id: str) -> Optional[Dict[str, Any]]:
    """Return one alert rule by ID."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM alert_rules WHERE rule_id = :rule_id"),
            {"rule_id": rule_id},
        ).fetchone()
    return _alert_rule_row(row) if row else None


def list_alert_rules(enabled_only: bool = False) -> List[Dict[str, Any]]:
    """Return configured alert rules."""
    where = "WHERE enabled = 1" if enabled_only else ""
    with _get_conn() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM alert_rules {where} ORDER BY updated_at DESC")
        ).fetchall()
    return [_alert_rule_row(row) for row in rows]


def update_alert_rule(rule_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Patch an alert rule. Unknown fields are ignored."""
    allowed = {
        "name",
        "metric_name",
        "comparator",
        "threshold",
        "severity",
        "window_minutes",
        "pipeline_name",
        "enabled",
        "destination_type",
        "destination",
    }
    patch = {k: v for k, v in updates.items() if k in allowed}
    if not patch:
        return get_alert_rule(rule_id)

    if "enabled" in patch:
        patch["enabled"] = int(bool(patch["enabled"]))
    if "threshold" in patch and patch["threshold"] is not None:
        patch["threshold"] = float(patch["threshold"])
    if "window_minutes" in patch and patch["window_minutes"] is not None:
        patch["window_minutes"] = int(patch["window_minutes"])
    patch["updated_at"] = datetime.utcnow().isoformat() + "Z"
    patch["rule_id"] = rule_id

    assignments = ", ".join(f"{key} = :{key}" for key in patch if key != "rule_id")
    with _get_conn() as conn:
        result = conn.execute(
            text(f"UPDATE alert_rules SET {assignments} WHERE rule_id = :rule_id"),
            patch,
        )
        conn.commit()
    if result.rowcount == 0:
        return None
    return get_alert_rule(rule_id)


def delete_alert_rule(rule_id: str) -> bool:
    """Delete one alert rule."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM alert_rules WHERE rule_id = :rule_id"),
            {"rule_id": rule_id},
        )
        conn.commit()
    return result.rowcount > 0


def _alert_incident_row(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    d["labels"] = _json_or_none(d.get("labels")) or {}
    return d


def create_alert_incident(
    incident_id: str,
    rule_id: str,
    rule_name: str,
    metric_name: str,
    severity: str,
    observed_value: float,
    threshold: float,
    message: str,
    pipeline_name: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
    status: str = "firing",
    fired_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a new alert incident."""
    now = fired_at or datetime.utcnow().isoformat() + "Z"
    labels_json = json.dumps(labels or {}, sort_keys=True)
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO alert_incidents
                    (incident_id, rule_id, rule_name, metric_name, pipeline_name,
                     severity, status, observed_value, threshold, message, labels,
                     fired_at, updated_at)
                VALUES
                    (:incident_id, :rule_id, :rule_name, :metric_name, :pipeline_name,
                     :severity, :status, :observed_value, :threshold, :message,
                     :labels, :fired_at, :updated_at)
                """
            ),
            {
                "incident_id": incident_id,
                "rule_id": rule_id,
                "rule_name": rule_name,
                "metric_name": metric_name,
                "pipeline_name": pipeline_name,
                "severity": severity,
                "status": status,
                "observed_value": float(observed_value),
                "threshold": float(threshold),
                "message": message,
                "labels": labels_json,
                "fired_at": now,
                "updated_at": now,
            },
        )
        conn.commit()
    return get_alert_incident(incident_id) or {}


def get_alert_incident(incident_id: str) -> Optional[Dict[str, Any]]:
    """Return one alert incident by ID."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM alert_incidents WHERE incident_id = :incident_id"),
            {"incident_id": incident_id},
        ).fetchone()
    return _alert_incident_row(row) if row else None


def get_active_alert_incident(
    rule_id: str,
    pipeline_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return an unresolved incident for a rule and pipeline, if one exists."""
    with _get_conn() as conn:
        row = conn.execute(
            text(
                """
                SELECT * FROM alert_incidents
                WHERE rule_id = :rule_id
                  AND status IN ('firing', 'acknowledged')
                  AND (
                    (:pipeline_name IS NULL AND pipeline_name IS NULL)
                    OR pipeline_name = :pipeline_name
                  )
                ORDER BY fired_at DESC
                LIMIT 1
                """
            ),
            {"rule_id": rule_id, "pipeline_name": pipeline_name},
        ).fetchone()
    return _alert_incident_row(row) if row else None


def list_alert_incidents(
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return alert incidents newest first."""
    params: Dict[str, Any] = {"lim": limit}
    where = ""
    if status:
        where = "WHERE status = :status"
        params["status"] = status
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM alert_incidents
                {where}
                ORDER BY updated_at DESC
                LIMIT :lim
                """
            ),
            params,
        ).fetchall()
    return [_alert_incident_row(row) for row in rows]


def update_alert_incident_observation(
    incident_id: str,
    observed_value: float,
    message: str,
    labels: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Update the latest observed value for an active incident."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                UPDATE alert_incidents
                SET observed_value = :observed_value,
                    message = :message,
                    labels = :labels,
                    updated_at = :updated_at
                WHERE incident_id = :incident_id
                """
            ),
            {
                "incident_id": incident_id,
                "observed_value": float(observed_value),
                "message": message,
                "labels": json.dumps(labels or {}, sort_keys=True),
                "updated_at": now,
            },
        )
        conn.commit()
    if result.rowcount == 0:
        return None
    return get_alert_incident(incident_id)


def update_alert_incident_status(
    incident_id: str,
    status: str,
    actor: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Set an incident to firing, acknowledged, or resolved."""
    now = datetime.utcnow().isoformat() + "Z"
    fields = ["status = :status", "updated_at = :updated_at"]
    params: Dict[str, Any] = {
        "incident_id": incident_id,
        "status": status,
        "updated_at": now,
    }
    if status == "acknowledged":
        fields.extend(["acknowledged_at = :now", "acknowledged_by = :actor"])
        params["now"] = now
        params["actor"] = actor
    elif status == "resolved":
        fields.append("resolved_at = :now")
        params["now"] = now

    with _get_conn() as conn:
        result = conn.execute(
            text(
                f"""
                UPDATE alert_incidents
                SET {", ".join(fields)}
                WHERE incident_id = :incident_id
                """
            ),
            params,
        )
        conn.commit()
    if result.rowcount == 0:
        return None
    return get_alert_incident(incident_id)


def resolve_active_alert_incidents_for_rule(
    rule_id: str,
    pipeline_name: Optional[str] = None,
) -> int:
    """Auto-resolve active incidents for a rule once the condition clears."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                UPDATE alert_incidents
                SET status = 'resolved', resolved_at = :now, updated_at = :now
                WHERE rule_id = :rule_id
                  AND status IN ('firing', 'acknowledged')
                  AND (
                    (:pipeline_name IS NULL AND pipeline_name IS NULL)
                    OR pipeline_name = :pipeline_name
                  )
                """
            ),
            {"rule_id": rule_id, "pipeline_name": pipeline_name, "now": now},
        )
        conn.commit()
    return result.rowcount


def create_notification_channel(
    channel_id: str,
    name: str,
    channel_type: str,
    destination: str,
    severities: Optional[List[str]] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Persist a reusable alert notification channel."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO notification_channels
                    (channel_id, name, channel_type, destination, severities,
                     enabled, created_at, updated_at)
                VALUES
                    (:channel_id, :name, :channel_type, :destination, :severities,
                     :enabled, :created_at, :updated_at)
                """
            ),
            {
                "channel_id": channel_id,
                "name": name,
                "channel_type": channel_type,
                "destination": destination,
                "severities": json.dumps(severities or ["critical", "warning"], sort_keys=True),
                "enabled": int(bool(enabled)),
                "created_at": now,
                "updated_at": now,
            },
        )
        conn.commit()
    return get_notification_channel(channel_id) or {}


def _notification_channel_row(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    d["enabled"] = bool(d["enabled"])
    d["severities"] = _json_or_none(d.get("severities")) or []
    return d


def get_notification_channel(channel_id: str) -> Optional[Dict[str, Any]]:
    """Return one notification channel by ID."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM notification_channels WHERE channel_id = :channel_id"),
            {"channel_id": channel_id},
        ).fetchone()
    return _notification_channel_row(row) if row else None


def list_notification_channels(
    enabled_only: bool = False,
    severity: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return notification channels, optionally filtered by enabled/severity."""
    where = "WHERE enabled = 1" if enabled_only else ""
    with _get_conn() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM notification_channels {where} ORDER BY updated_at DESC")
        ).fetchall()
    channels = [_notification_channel_row(row) for row in rows]
    if severity:
        severity = severity.lower()
        channels = [
            channel
            for channel in channels
            if "all" in channel.get("severities", [])
            or severity in channel.get("severities", [])
        ]
    return channels


def update_notification_channel(
    channel_id: str,
    updates: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Patch a notification channel."""
    allowed = {"name", "channel_type", "destination", "severities", "enabled"}
    patch = {key: value for key, value in updates.items() if key in allowed}
    if not patch:
        return get_notification_channel(channel_id)
    if "enabled" in patch:
        patch["enabled"] = int(bool(patch["enabled"]))
    if "severities" in patch:
        patch["severities"] = json.dumps(patch["severities"] or [], sort_keys=True)
    patch["updated_at"] = datetime.utcnow().isoformat() + "Z"
    patch["channel_id"] = channel_id

    assignments = ", ".join(f"{key} = :{key}" for key in patch if key != "channel_id")
    with _get_conn() as conn:
        result = conn.execute(
            text(
                f"UPDATE notification_channels SET {assignments} "
                "WHERE channel_id = :channel_id"
            ),
            patch,
        )
        conn.commit()
    if result.rowcount == 0:
        return None
    return get_notification_channel(channel_id)


def delete_notification_channel(channel_id: str) -> bool:
    """Delete one notification channel."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM notification_channels WHERE channel_id = :channel_id"),
            {"channel_id": channel_id},
        )
        conn.commit()
    return result.rowcount > 0


def save_notification_delivery(
    delivery_id: str,
    incident_id: str,
    rule_id: str,
    destination_type: str,
    destination: str,
    status: str,
    channel_id: Optional[str] = None,
    channel_name: Optional[str] = None,
    error: Optional[str] = None,
    attempted_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist one alert notification delivery attempt."""
    now = attempted_at or datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO notification_deliveries
                    (delivery_id, incident_id, rule_id, channel_id, channel_name,
                     destination_type, destination, status, error, attempted_at)
                VALUES
                    (:delivery_id, :incident_id, :rule_id, :channel_id,
                     :channel_name, :destination_type, :destination, :status,
                     :error, :attempted_at)
                """
            ),
            {
                "delivery_id": delivery_id,
                "incident_id": incident_id,
                "rule_id": rule_id,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "destination_type": destination_type,
                "destination": destination,
                "status": status,
                "error": error,
                "attempted_at": now,
            },
        )
        conn.commit()
    return {
        "delivery_id": delivery_id,
        "incident_id": incident_id,
        "rule_id": rule_id,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "destination_type": destination_type,
        "destination": destination,
        "status": status,
        "error": error,
        "attempted_at": now,
    }


def list_notification_deliveries(
    incident_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return alert notification delivery attempts newest first."""
    clauses: List[str] = []
    params: Dict[str, Any] = {"lim": limit}
    if incident_id:
        clauses.append("incident_id = :incident_id")
        params["incident_id"] = incident_id
    if status:
        clauses.append("status = :status")
        params["status"] = status
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM notification_deliveries
                {where}
                ORDER BY attempted_at DESC
                LIMIT :lim
                """
            ),
            params,
        ).fetchall()
    return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# Deployment control plane
# ---------------------------------------------------------------------------

def _deployment_connection_row(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    d["labels"] = _json_or_none(d.get("labels")) if d.get("labels") is not None else {}
    return d


def create_deployment_connection(
    connection_id: str,
    name: str,
    target_id: str,
    provider: str,
    endpoint: Optional[str] = None,
    region: Optional[str] = None,
    namespace: Optional[str] = None,
    status: str = "connected",
    credentials_ref: Optional[str] = None,
    labels: Optional[Dict[str, Any]] = None,
    last_checked_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a selectable deployment target instance."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        conn.execute(
            text(
                """
                INSERT INTO deployment_connections
                    (connection_id, name, target_id, provider, endpoint, region,
                     namespace, status, credentials_ref, labels, created_at,
                     updated_at, last_checked_at)
                VALUES
                    (:connection_id, :name, :target_id, :provider, :endpoint,
                     :region, :namespace, :status, :credentials_ref, :labels,
                     :created_at, :updated_at, :last_checked_at)
                """
            ),
            {
                "connection_id": connection_id,
                "name": name,
                "target_id": target_id,
                "provider": provider,
                "endpoint": endpoint,
                "region": region,
                "namespace": namespace,
                "status": status,
                "credentials_ref": credentials_ref,
                "labels": json.dumps(labels or {}, sort_keys=True),
                "created_at": now,
                "updated_at": now,
                "last_checked_at": last_checked_at,
            },
        )
        conn.commit()
    return get_deployment_connection(connection_id) or {}


def ensure_default_deployment_connections() -> int:
    """Seed a local connected target instance for a new metadata store."""
    init_db()
    if get_deployment_connection("local-default"):
        return 0
    create_deployment_connection(
        connection_id="local-default",
        name="Local API Worker",
        target_id="local",
        provider="local",
        endpoint="127.0.0.1",
        status="connected",
        labels={"default": True, "scope": "embedded"},
        last_checked_at=datetime.utcnow().isoformat() + "Z",
    )
    return 1


def get_deployment_connection(connection_id: str) -> Optional[Dict[str, Any]]:
    """Return one deployment target instance by ID."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM deployment_connections WHERE connection_id = :connection_id"),
            {"connection_id": connection_id},
        ).fetchone()
    return _deployment_connection_row(row) if row else None


def list_deployment_connections(
    target_id: Optional[str] = None,
    status: Optional[str] = None,
    connected_only: bool = False,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Return selectable deployment target instances."""
    clauses: List[str] = []
    params: Dict[str, Any] = {"lim": limit}
    if target_id:
        clauses.append("target_id = :target_id")
        params["target_id"] = target_id
    if connected_only:
        clauses.append("status = 'connected'")
    elif status:
        clauses.append("status = :status")
        params["status"] = status
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM deployment_connections
                {where}
                ORDER BY target_id ASC, name ASC
                LIMIT :lim
                """
            ),
            params,
        ).fetchall()
    return [_deployment_connection_row(row) for row in rows]


def update_deployment_connection(
    connection_id: str,
    updates: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Patch a deployment target instance."""
    allowed = {
        "name", "target_id", "provider", "endpoint", "region", "namespace",
        "status", "credentials_ref", "labels", "last_checked_at",
    }
    patch = {key: value for key, value in updates.items() if key in allowed}
    if not patch:
        return get_deployment_connection(connection_id)
    if "labels" in patch:
        patch["labels"] = json.dumps(patch["labels"] or {}, sort_keys=True)
    patch["updated_at"] = datetime.utcnow().isoformat() + "Z"
    patch["connection_id"] = connection_id
    assignments = ", ".join(f"{key} = :{key}" for key in patch if key != "connection_id")
    with _get_conn() as conn:
        result = conn.execute(
            text(
                f"UPDATE deployment_connections SET {assignments} "
                "WHERE connection_id = :connection_id"
            ),
            patch,
        )
        conn.commit()
    if result.rowcount == 0:
        return None
    return get_deployment_connection(connection_id)


def delete_deployment_connection(connection_id: str) -> bool:
    """Delete one deployment target instance."""
    with _get_conn() as conn:
        result = conn.execute(
            text("DELETE FROM deployment_connections WHERE connection_id = :connection_id"),
            {"connection_id": connection_id},
        )
        conn.commit()
    return result.rowcount > 0


def _deployment_row(row: Any) -> Dict[str, Any]:
    d = dict(row._mapping)
    d["active"] = bool(d.get("active"))
    for key in ("validation_summary", "execution_fabric", "manifest"):
        d[key] = _json_or_none(d.get(key)) if d.get(key) is not None else None
    return d


def create_deployment(
    deployment_id: str,
    pipeline_name: str,
    config_path: str,
    environment_profile: str,
    target_id: str,
    target_name: str,
    status: str,
    connection_id: Optional[str] = None,
    connection_name: Optional[str] = None,
    connection_provider: Optional[str] = None,
    actor: Optional[str] = None,
    version_id: Optional[str] = None,
    version_hash: Optional[str] = None,
    notes: Optional[str] = None,
    validation_summary: Optional[Dict[str, Any]] = None,
    execution_fabric: Optional[Dict[str, Any]] = None,
    manifest: Optional[Dict[str, Any]] = None,
    active: bool = False,
) -> Dict[str, Any]:
    """Create a deployment record and mark it active when it is deployed."""
    now = datetime.utcnow().isoformat() + "Z"
    should_activate = bool(active and status == "deployed")
    with _get_conn() as conn:
        if should_activate:
            conn.execute(
                text(
                    """
                    UPDATE deployments
                    SET active = 0, updated_at = :now
                    WHERE pipeline_name = :pipeline_name
                      AND environment_profile = :environment_profile
                      AND target_id = :target_id
                      AND (
                        (:connection_id IS NULL AND connection_id IS NULL)
                        OR connection_id = :connection_id
                      )
                      AND active = 1
                    """
                ),
                {
                    "now": now,
                    "pipeline_name": pipeline_name,
                    "environment_profile": environment_profile,
                    "target_id": target_id,
                    "connection_id": connection_id,
                },
            )
        conn.execute(
            text(
                """
                INSERT INTO deployments
                    (deployment_id, pipeline_name, config_path, version_id, version_hash,
                     environment_profile, target_id, target_name, connection_id,
                     connection_name, connection_provider, status, active, actor, notes,
                     validation_summary, execution_fabric, manifest, created_at, deployed_at,
                     updated_at, rolled_back_at, rollback_of, restored_deployment_id)
                VALUES
                    (:deployment_id, :pipeline_name, :config_path, :version_id,
                     :version_hash, :environment_profile, :target_id, :target_name,
                     :connection_id, :connection_name, :connection_provider, :status,
                     :active, :actor, :notes, :validation_summary, :execution_fabric,
                     :manifest, :created_at, :deployed_at, :updated_at, NULL, NULL, NULL)
                """
            ),
            {
                "deployment_id": deployment_id,
                "pipeline_name": pipeline_name,
                "config_path": config_path,
                "version_id": version_id,
                "version_hash": version_hash,
                "environment_profile": environment_profile,
                "target_id": target_id,
                "target_name": target_name,
                "connection_id": connection_id,
                "connection_name": connection_name,
                "connection_provider": connection_provider,
                "status": status,
                "active": int(should_activate),
                "actor": actor,
                "notes": notes,
                "validation_summary": json.dumps(validation_summary or {}, sort_keys=True),
                "execution_fabric": json.dumps(execution_fabric or {}, sort_keys=True),
                "manifest": json.dumps(manifest or {}, sort_keys=True),
                "created_at": now,
                "deployed_at": now if status == "deployed" else None,
                "updated_at": now,
            },
        )
        conn.commit()
    return get_deployment(deployment_id) or {}


def get_deployment(deployment_id: str) -> Optional[Dict[str, Any]]:
    """Return one deployment record by ID."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM deployments WHERE deployment_id = :deployment_id"),
            {"deployment_id": deployment_id},
        ).fetchone()
    return _deployment_row(row) if row else None


def list_deployments(
    pipeline_name: Optional[str] = None,
    environment_profile: Optional[str] = None,
    target_id: Optional[str] = None,
    connection_id: Optional[str] = None,
    status: Optional[str] = None,
    active_only: bool = False,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return deployment history newest first."""
    clauses: List[str] = []
    params: Dict[str, Any] = {"lim": limit}
    if pipeline_name:
        clauses.append("pipeline_name = :pipeline_name")
        params["pipeline_name"] = pipeline_name
    if environment_profile:
        clauses.append("environment_profile = :environment_profile")
        params["environment_profile"] = environment_profile
    if target_id:
        clauses.append("target_id = :target_id")
        params["target_id"] = target_id
    if connection_id:
        clauses.append("connection_id = :connection_id")
        params["connection_id"] = connection_id
    if status:
        clauses.append("status = :status")
        params["status"] = status
    if active_only:
        clauses.append("active = 1")
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM deployments
                {where}
                ORDER BY updated_at DESC
                LIMIT :lim
                """
            ),
            params,
        ).fetchall()
    return [_deployment_row(row) for row in rows]


def rollback_deployment(deployment_id: str, actor: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Roll back an active deployment to the previous successful deployment."""
    now = datetime.utcnow().isoformat() + "Z"
    with _get_conn() as conn:
        current_row = conn.execute(
            text("SELECT * FROM deployments WHERE deployment_id = :deployment_id"),
            {"deployment_id": deployment_id},
        ).fetchone()
        if current_row is None:
            return None
        current = dict(current_row._mapping)
        if current.get("status") != "deployed" or int(current.get("active") or 0) != 1:
            raise ValueError("Only the active deployed record can be rolled back")

        previous_row = conn.execute(
            text(
                """
                SELECT * FROM deployments
                WHERE pipeline_name = :pipeline_name
                  AND environment_profile = :environment_profile
                  AND target_id = :target_id
                  AND (
                    (:connection_id IS NULL AND connection_id IS NULL)
                    OR connection_id = :connection_id
                  )
                  AND deployment_id != :deployment_id
                  AND status = 'deployed'
                ORDER BY deployed_at DESC, updated_at DESC
                LIMIT 1
                """
            ),
            {
                "pipeline_name": current["pipeline_name"],
                "environment_profile": current["environment_profile"],
                "target_id": current["target_id"],
                "connection_id": current.get("connection_id"),
                "deployment_id": deployment_id,
            },
        ).fetchone()
        if previous_row is None:
            raise ValueError("No previous successful deployment exists for this pipeline, profile, and target")
        previous = dict(previous_row._mapping)

        conn.execute(
            text(
                """
                UPDATE deployments
                SET status = 'rolled_back',
                    active = 0,
                    actor = COALESCE(:actor, actor),
                    rolled_back_at = :now,
                    updated_at = :now,
                    restored_deployment_id = :restored_deployment_id
                WHERE deployment_id = :deployment_id
                """
            ),
            {
                "actor": actor,
                "now": now,
                "restored_deployment_id": previous["deployment_id"],
                "deployment_id": deployment_id,
            },
        )
        conn.execute(
            text(
                """
                UPDATE deployments
                SET active = 1,
                    updated_at = :now,
                    rollback_of = :rollback_of
                WHERE deployment_id = :deployment_id
                """
            ),
            {
                "now": now,
                "rollback_of": deployment_id,
                "deployment_id": previous["deployment_id"],
            },
        )
        conn.commit()

    return {
        "rolled_back": get_deployment(deployment_id),
        "restored": get_deployment(previous["deployment_id"]),
    }


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
) -> bool:
    """Transition a queued run to running / completed / failed / cancelled.

    When the calling thread holds a lease on this run (see
    :mod:`dataplatform.core.leases`), the transition is fenced: it applies only
    while the lease is still ours.  Returns False when the write was fenced
    out, meaning another attempt now owns the run and this one must stop.
    """
    lease = current_lease(run_id)
    if lease is not None:
        return _set_run_status_fenced(run_id, status, error, lease)

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
                    SET status = :status,
                        completed_at = :now,
                        error = :error,
                        lease_expires_at = NULL
                    WHERE run_id = :run_id
                    """
                ),
                {"status": status, "now": now, "error": error, "run_id": run_id},
            )
        conn.commit()
    return True


def _set_run_status_fenced(
    run_id: str,
    status: str,
    error: Optional[str],
    lease: Lease,
) -> bool:
    """Apply a status transition only while *lease* is still the live attempt."""
    now = datetime.utcnow().isoformat() + "Z"
    terminal = status != "running"
    sql = """
        UPDATE pipeline_queue
           SET status = :status,
               {assignment}
         WHERE run_id = :run_id
           AND worker_id = :worker_id
           AND attempt = :attempt
    """.format(
        assignment=(
            "completed_at = :now, error = :error, lease_expires_at = NULL"
            if terminal
            else "started_at = :now"
        )
    )
    with _get_conn() as conn:
        result = conn.execute(
            text(sql),
            {
                "status": status,
                "now": now,
                "error": error,
                "run_id": run_id,
                "worker_id": lease.worker_id,
                "attempt": lease.attempt,
            },
        )
        conn.commit()

    if result.rowcount != 1:
        logger.warning(
            "Fenced out: worker=%s attempt=%s could not set run_id=%s to %s",
            lease.worker_id,
            lease.attempt,
            run_id,
            status,
        )
        return False
    return True


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


def get_queue_runs_since(since: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """Return queue entries queued at or after *since*, oldest first."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM pipeline_queue
                WHERE queued_at >= :since
                ORDER BY queued_at ASC
                LIMIT :lim
                """
            ),
            {"since": since, "lim": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def get_queue_counts_by_status() -> List[Dict[str, Any]]:
    """Return queue depth grouped by status."""
    with _get_conn() as conn:
        rows = conn.execute(
            text(
                """
                SELECT status, COUNT(*) AS count
                FROM pipeline_queue
                GROUP BY status
                """
            )
        ).fetchall()
    return [dict(row._mapping) for row in rows]


DEFAULT_LEASE_SECONDS = 60
DEFAULT_MAX_ATTEMPTS = 3

#: How many queued rows a claim will walk past before giving up.  Only used on
#: backends without ``SKIP LOCKED``; it bounds the work a losing racer does.
_CLAIM_SCAN_LIMIT = 20


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _iso_in(seconds: float) -> str:
    return (datetime.utcnow() + timedelta(seconds=seconds)).isoformat() + "Z"


def _is_postgres() -> bool:
    return _get_engine().dialect.name == "postgresql"


def generate_worker_id(prefix: str = "worker") -> str:
    """Return an id unique to one worker process."""
    return "{0}-{1}-{2}".format(prefix, os.getpid(), uuid.uuid4().hex[:8])


def claim_next_queued_run(
    worker_id: Optional[str] = None,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
) -> Optional[Dict[str, Any]]:
    """Atomically claim the oldest queued run and take a lease on it.

    The claim stamps the run with the claiming ``worker_id``, a lease deadline,
    and an incremented ``attempt``.  ``attempt`` is monotonic per run and acts
    as the fencing token: a worker that loses its lease can be rejected at
    write time by comparing attempts, rather than merely being unlikely to
    still be running.

    On PostgreSQL the claim is a single statement using ``FOR UPDATE SKIP
    LOCKED``, so concurrent workers take *different* rows instead of contending
    on the head of the queue.  Other backends (SQLite in development) have no
    ``SKIP LOCKED``; there the claim walks candidates and retries on a lost
    race, which is slower but never returns ``None`` while work is available.
    """
    worker = worker_id or generate_worker_id()
    now = _now_iso()
    lease = _iso_in(lease_seconds)
    params = {"worker_id": worker, "now": now, "lease": lease}

    if _is_postgres():
        with _get_conn() as conn:
            row = conn.execute(
                text(
                    """
                    UPDATE pipeline_queue
                       SET status = 'running',
                           worker_id = :worker_id,
                           attempt = attempt + 1,
                           started_at = :now,
                           heartbeat_at = :now,
                           lease_expires_at = :lease
                     WHERE run_id = (
                           SELECT run_id FROM pipeline_queue
                            WHERE status = 'queued'
                            ORDER BY queued_at
                              FOR UPDATE SKIP LOCKED
                            LIMIT 1)
                 RETURNING *
                    """
                ),
                params,
            ).fetchone()
            conn.commit()
        return dict(row._mapping) if row is not None else None

    # Backends without SKIP LOCKED: walk candidates, skipping rows another
    # worker claimed between our SELECT and our UPDATE.
    with _get_conn() as conn:
        candidates = conn.execute(
            text(
                """
                SELECT run_id FROM pipeline_queue
                 WHERE status = 'queued'
                 ORDER BY queued_at ASC
                 LIMIT :limit
                """
            ),
            {"limit": _CLAIM_SCAN_LIMIT},
        ).fetchall()

        for candidate in candidates:
            run_id = candidate._mapping["run_id"]
            result = conn.execute(
                text(
                    """
                    UPDATE pipeline_queue
                       SET status = 'running',
                           worker_id = :worker_id,
                           attempt = attempt + 1,
                           started_at = :now,
                           heartbeat_at = :now,
                           lease_expires_at = :lease
                     WHERE run_id = :run_id AND status = 'queued'
                    """
                ),
                dict(params, run_id=run_id),
            )
            if result.rowcount != 1:
                continue  # lost the race for this row -- try the next one

            conn.commit()
            row = conn.execute(
                text("SELECT * FROM pipeline_queue WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).fetchone()
            return dict(row._mapping) if row is not None else None

        conn.commit()
    return None


def renew_lease(
    run_id: str,
    worker_id: str,
    attempt: int,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
) -> bool:
    """Extend a lease. False means the caller no longer holds it.

    A worker that gets False here must stop working: the run has been reaped
    and possibly reclaimed by someone else.
    """
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                UPDATE pipeline_queue
                   SET lease_expires_at = :lease, heartbeat_at = :now
                 WHERE run_id = :run_id
                   AND worker_id = :worker_id
                   AND attempt = :attempt
                   AND status = 'running'
                """
            ),
            {
                "run_id": run_id,
                "worker_id": worker_id,
                "attempt": attempt,
                "now": _now_iso(),
                "lease": _iso_in(lease_seconds),
            },
        )
        conn.commit()
    return result.rowcount == 1


def holds_lease(run_id: str, worker_id: str, attempt: int) -> bool:
    """True when this worker still holds an unexpired lease on the run."""
    run = get_queue_run(run_id)
    if run is None or run["status"] != "running":
        return False
    if run.get("worker_id") != worker_id or int(run.get("attempt") or 0) != attempt:
        return False
    expires = run.get("lease_expires_at")
    return bool(expires) and expires > _now_iso()


def complete_run_with_lease(
    run_id: str,
    worker_id: str,
    attempt: int,
    status: str,
    error: Optional[str] = None,
) -> bool:
    """Finish a run, but only if the caller still holds the lease.

    This is the fence: a worker that stalled past its lease and woke up cannot
    overwrite the outcome of the attempt that replaced it.
    """
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                UPDATE pipeline_queue
                   SET status = :status,
                       completed_at = :now,
                       error = :error,
                       lease_expires_at = NULL
                 WHERE run_id = :run_id
                   AND worker_id = :worker_id
                   AND attempt = :attempt
                   AND status = 'running'
                """
            ),
            {
                "run_id": run_id,
                "worker_id": worker_id,
                "attempt": attempt,
                "status": status,
                "error": error,
                "now": _now_iso(),
            },
        )
        conn.commit()

    if result.rowcount != 1:
        logger.warning(
            "Fenced out: worker=%s attempt=%s could not finish run_id=%s",
            worker_id,
            attempt,
            run_id,
        )
    return result.rowcount == 1


def reap_expired_leases(
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> Dict[str, int]:
    """Requeue runs whose lease expired; dead-letter the ones that keep dying.

    This replaces "a run is dead if it started a while ago" with "a run is dead
    if nobody is renewing its lease", which is the difference between killing a
    slow job and detecting a lost worker.  Returns counts per action.
    """
    now = _now_iso()
    with _get_conn() as conn:
        dead = conn.execute(
            text(
                """
                UPDATE pipeline_queue
                   SET status = 'dead_letter',
                       completed_at = :now,
                       error = :error,
                       worker_id = NULL,
                       lease_expires_at = NULL
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at <= :now
                   AND attempt >= :max_attempts
                """
            ),
            {
                "now": now,
                "error": "Lease expired after {0} attempt(s)".format(max_attempts),
                "max_attempts": max_attempts,
            },
        )
        requeued = conn.execute(
            text(
                """
                UPDATE pipeline_queue
                   SET status = 'queued',
                       worker_id = NULL,
                       lease_expires_at = NULL,
                       started_at = NULL
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at <= :now
                   AND attempt < :max_attempts
                """
            ),
            {"now": now, "max_attempts": max_attempts},
        )
        conn.commit()

    counts = {"requeued": requeued.rowcount, "dead_lettered": dead.rowcount}
    if counts["requeued"] or counts["dead_lettered"]:
        logger.warning(
            "Reaped expired leases: %d requeued, %d dead-lettered",
            counts["requeued"],
            counts["dead_lettered"],
        )
    return counts


def get_queue_run(run_id: str) -> Optional[Dict[str, Any]]:
    """Return one persistent queue entry by run_id, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            text("SELECT * FROM pipeline_queue WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).fetchone()
    return dict(row._mapping) if row else None


def recover_orphaned_runs(stale_after_seconds: int = 3600) -> int:
    """Mark stale queued/running runs as failed on restart.

    This is the embedded-worker path: when the API process itself restarts, the
    runs it was executing in-process really are gone.

    Runs carrying a ``worker_id`` are skipped entirely, whether or not their
    lease is still live.  Those belong to an external worker, and their
    lifecycle is :func:`reap_expired_leases`'s job -- which *requeues* them
    instead of failing them.  Sweeping them here would both cause the
    split-brain this module exists to avoid and, for an already-expired lease,
    race the reaper and turn a recoverable run into a failed one.

    Returns the number of runs recovered.
    """
    now = datetime.utcnow().isoformat() + "Z"
    cutoff_iso = (datetime.utcnow() - timedelta(seconds=max(stale_after_seconds, 0))).isoformat() + "Z"
    with _get_conn() as conn:
        result = conn.execute(
            text(
                """
                UPDATE pipeline_queue
                SET status = 'failed', completed_at = :now, error = 'Server restarted'
                WHERE status IN ('queued', 'running')
                  AND COALESCE(started_at, queued_at) <= :cutoff
                  AND worker_id IS NULL
                """
            ),
            {"now": now, "cutoff": cutoff_iso},
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
