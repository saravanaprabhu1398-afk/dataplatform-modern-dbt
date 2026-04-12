"""User authentication and role-based access control (RBAC).

Roles (least → most privileged):
  viewer  — read-only access (list pipelines, view history/status/DAG)
  editor  — viewer + can run, schedule, validate, generate, and save pipelines
  admin   — editor + can manage users and access admin endpoints

The environment-variable admin (DATAPLATFORM_USERNAME / DATAPLATFORM_PASSWORD)
always authenticates as role "admin" regardless of the users table, so the
platform is usable out of the box with no DB setup.
"""
import hashlib
import hmac
import logging
import os
import secrets as _secrets_lib
from typing import Any, Dict, Optional, Set

from dataplatform.core.database import (
    create_user as _db_create_user,
    delete_user as _db_delete_user,
    get_user,
    init_db,
    list_users,
    update_user_role as _db_update_user_role,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Role definitions
# ---------------------------------------------------------------------------

ROLES: Set[str] = {"admin", "editor", "viewer"}

# Each role inherits all permissions of roles below it in the hierarchy.
# "*" means unrestricted (admin only).
_ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "admin":  {"*"},
    "editor": {"run", "schedule", "validate", "generate", "save", "read"},
    "viewer": {"read"},
}


def has_permission(role: str, action: str) -> bool:
    """Return True if *role* is allowed to perform *action*."""
    perms = _ROLE_PERMISSIONS.get(role, set())
    return "*" in perms or action in perms


# ---------------------------------------------------------------------------
# Password hashing (stdlib only — no bcrypt dependency)
# ---------------------------------------------------------------------------

def _generate_salt() -> str:
    return _secrets_lib.token_hex(16)


def _hash_password(password: str, salt: str) -> str:
    return hmac.new(
        salt.encode("utf-8"),
        password.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def make_password_hash(password: str) -> str:
    """Return a storable 'salt:hash' string for *password*."""
    salt = _generate_salt()
    return f"{salt}:{_hash_password(password, salt)}"


def check_password(password: str, stored_hash: str) -> bool:
    """Return True if *password* matches the stored 'salt:hash' string."""
    if ":" not in stored_hash:
        return False
    salt, expected = stored_hash.split(":", 1)
    return hmac.compare_digest(expected, _hash_password(password, salt))


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def verify_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Verify credentials and return a user info dict, or None on failure.

    The env-var admin is checked first (always role='admin').
    Then the users table is checked.
    """
    env_user = os.getenv("DATAPLATFORM_USERNAME", "admin")
    env_pass = os.getenv("DATAPLATFORM_PASSWORD", "admin")

    if username == env_user and password == env_pass:
        return {"username": username, "role": "admin", "team": None}

    # DB users
    init_db()
    row = get_user(username)
    if row is None:
        return None
    if check_password(password, row["password_hash"]):
        return {"username": row["username"], "role": row["role"], "team": row["team"]}
    return None


# ---------------------------------------------------------------------------
# User management (delegated to database layer)
# ---------------------------------------------------------------------------

def create_user(
    username: str,
    password: str,
    role: str = "viewer",
    team: Optional[str] = None,
) -> bool:
    """Create a new user. Returns False if the username already exists."""
    if role not in ROLES:
        raise ValueError(f"Invalid role '{role}'. Must be one of: {', '.join(sorted(ROLES))}")

    env_user = os.getenv("DATAPLATFORM_USERNAME", "admin")
    if username == env_user:
        raise ValueError(f"Cannot create a DB user with the same name as the env-var admin ('{env_user}').")

    init_db()
    password_hash = make_password_hash(password)
    ok = _db_create_user(username, password_hash, role, team)
    if not ok:
        logger.warning(f"User '{username}' already exists.")
    return ok


def update_user_role(username: str, new_role: str) -> bool:
    """Update a user's role. Returns False if not found."""
    if new_role not in ROLES:
        raise ValueError(f"Invalid role '{new_role}'. Must be one of: {', '.join(sorted(ROLES))}")
    init_db()
    return _db_update_user_role(username, new_role)


def delete_user(username: str) -> bool:
    """Delete a user. Raises ValueError if trying to delete the env-var admin."""
    env_user = os.getenv("DATAPLATFORM_USERNAME", "admin")
    if username == env_user:
        raise ValueError(f"Cannot delete the environment-variable admin user '{env_user}'.")
    init_db()
    return _db_delete_user(username)


def get_all_users() -> list:
    """Return all DB users (no password hashes)."""
    init_db()
    return list_users()
