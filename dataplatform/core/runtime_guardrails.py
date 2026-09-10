"""Runtime safety checks for production deployments."""
from __future__ import annotations

import os
from typing import Optional


_PRODUCTION_ENVS = {"prod", "production"}
_DEFAULT_SESSION_SECRETS = {
    "dpflow-dev-secret-change-me",
    "change-me-in-production",
    "your-secret-key-here",
}


def _truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def is_production_environment(value: Optional[str] = None) -> bool:
    env = value if value is not None else os.getenv("DATAPLATFORM_ENV", os.getenv("ENVIRONMENT", ""))
    return str(env or "").strip().lower() in _PRODUCTION_ENVS


def normalize_execution_mode(value: Optional[str] = None) -> str:
    mode = value if value is not None else os.getenv("DATAPLATFORM_EXECUTION_MODE", "embedded")
    mode = str(mode or "embedded").strip().lower().replace("-", "_")
    aliases = {
        "in_process": "embedded",
        "inprocess": "embedded",
        "local": "embedded",
        "queue": "external",
        "worker": "external",
        "remote": "external",
    }
    return aliases.get(mode, mode)


def validate_runtime_settings(
    *,
    environment: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    session_secret: Optional[str] = None,
    execution_mode: Optional[str] = None,
    allow_embedded_worker: Optional[str] = None,
    postgres_url: Optional[str] = None,
) -> None:
    """Raise RuntimeError when production runtime settings are unsafe."""
    if not is_production_environment(environment):
        return

    username = username if username is not None else os.getenv("DATAPLATFORM_USERNAME", "admin")
    password = password if password is not None else os.getenv("DATAPLATFORM_PASSWORD", "admin")
    session_secret = (
        session_secret
        if session_secret is not None
        else os.getenv("DATAPLATFORM_SESSION_SECRET", "dpflow-dev-secret-change-me")
    )
    mode = normalize_execution_mode(execution_mode)
    allow_embedded = _truthy(
        allow_embedded_worker
        if allow_embedded_worker is not None
        else os.getenv("DATAPLATFORM_ALLOW_EMBEDDED_WORKER")
    )
    metadata_url = postgres_url if postgres_url is not None else os.getenv("POSTGRES_URL", "")

    errors = []
    if username == "admin" and password == "admin":
        errors.append("DATAPLATFORM_USERNAME/DATAPLATFORM_PASSWORD must not use admin/admin")
    if not session_secret or session_secret in _DEFAULT_SESSION_SECRETS or len(session_secret) < 32:
        errors.append("DATAPLATFORM_SESSION_SECRET must be unique and at least 32 characters")
    if mode != "external" and not allow_embedded:
        errors.append(
            "DATAPLATFORM_EXECUTION_MODE must be external in production "
            "or DATAPLATFORM_ALLOW_EMBEDDED_WORKER=true must be set explicitly"
        )

    if mode == "external" and not metadata_url:
        # The API and its workers coordinate only through the metadata store:
        # the run queue's leases and fencing tokens are enforced by database
        # transactions. Falling back to SQLite puts that file on a shared
        # volume, where locking is not dependable across processes -- so a
        # missing POSTGRES_URL silently removes the guarantee rather than
        # failing.
        errors.append(
            "POSTGRES_URL must be set when DATAPLATFORM_EXECUTION_MODE=external: "
            "run leases rely on transactional locking that SQLite cannot provide "
            "across processes on a shared volume"
        )

    if errors:
        joined = "; ".join(errors)
        raise RuntimeError(f"Unsafe production configuration: {joined}")


def validate_plugin_execution_allowed(plugin_name: str) -> None:
    """Block unsafe local-code executors by default in production."""
    plugin = str(plugin_name or "").strip().lower()
    if plugin not in {"python", "shell"}:
        return
    if not is_production_environment():
        return
    if _truthy(os.getenv("DATAPLATFORM_ALLOW_UNSAFE_EXECUTORS")):
        return
    raise RuntimeError(
        f"Plugin '{plugin}' executes local code and is disabled in production by default. "
        "Set DATAPLATFORM_ALLOW_UNSAFE_EXECUTORS=true only for trusted pipelines "
        "or run these tasks in an isolated worker sandbox."
    )
