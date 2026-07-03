"""Secret interpolation for pipeline task configs.

Resolves ${TOKEN} patterns in any string value inside a config dict/list:

  ${MY_ENV_VAR}           → resolved from os.environ
  ${vault:path/to/secret:key} → resolved from HashiCorp Vault KV

Vault token reference format:
  ${vault:<mount>/<path>:<key>}
  e.g. ${vault:secret/myapp:db_password}

Auth methods (checked in order):
  1. Token:   VAULT_TOKEN env var
  2. AppRole: VAULT_ROLE_ID + VAULT_SECRET_ID env vars

Requires the ``hvac`` package (``pip install hvac``).  If hvac is not
installed or the Vault call fails the original token is left in place
and a warning is logged — pipelines are never silently broken.

The function is recursive — nested dicts and lists are fully resolved.
Non-string values (int, float, bool, None) are passed through unchanged.
"""
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ${SOME_VAR}  — plain environment variable reference
_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# ${vault:path/to/secret:key}  — Vault KV reference
_VAULT_RE = re.compile(r"\$\{vault:([^}]+)\}")

# ---------------------------------------------------------------------------
# Vault client cache — one client per (VAULT_ADDR, auth_hash) pair
# ---------------------------------------------------------------------------
_vault_client_cache: Optional[Any] = None


def _get_vault_client():
    """Return an authenticated hvac.Client, or None if unavailable."""
    global _vault_client_cache

    if _vault_client_cache is not None:
        return _vault_client_cache

    vault_addr = os.getenv("VAULT_ADDR")
    if not vault_addr:
        return None

    try:
        import hvac  # type: ignore[import]
    except ImportError:
        logger.warning(
            "VAULT_ADDR is set but the 'hvac' package is not installed. "
            "Run: pip install hvac"
        )
        return None

    client = hvac.Client(url=vault_addr)

    # Try token auth first
    vault_token = os.getenv("VAULT_TOKEN")
    if vault_token:
        client.token = vault_token
    else:
        # Try AppRole auth
        role_id = os.getenv("VAULT_ROLE_ID")
        secret_id = os.getenv("VAULT_SECRET_ID")
        if role_id and secret_id:
            try:
                client.auth.approle.login(role_id=role_id, secret_id=secret_id)
            except Exception as exc:
                logger.warning("Vault AppRole login failed: %s", exc)
                return None
        else:
            logger.warning(
                "VAULT_ADDR is set but no auth credentials found. "
                "Set VAULT_TOKEN or VAULT_ROLE_ID + VAULT_SECRET_ID."
            )
            return None

    if not client.is_authenticated():
        logger.warning("Vault client at %s failed authentication check.", vault_addr)
        return None

    _vault_client_cache = client
    logger.info("Vault client authenticated at %s", vault_addr)
    return client


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_secrets(config: Any) -> Any:
    """Recursively resolve ${...} tokens in *config*.

    Args:
        config: A dict, list, or scalar value (typically task.config).

    Returns:
        A new object of the same type with all resolvable tokens replaced.
    """
    if isinstance(config, str):
        return _resolve_string(config)
    if isinstance(config, dict):
        return {k: resolve_secrets(v) for k, v in config.items()}
    if isinstance(config, list):
        return [resolve_secrets(item) for item in config]
    return config  # int, float, bool, None — unchanged


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_string(value: str) -> str:
    """Resolve all ${...} tokens in a single string."""
    # Process Vault references first (they have a colon inside)
    value = _VAULT_RE.sub(_vault_replacer, value)
    # Then plain env-var references
    value = _ENV_RE.sub(_env_replacer, value)
    return value


def _env_replacer(match: re.Match) -> str:
    var_name = match.group(1)
    resolved = os.getenv(var_name)
    if resolved is None:
        logger.warning(
            "Secret reference '${%s}' is unresolved: environment variable not set. "
            "Set the variable or remove the reference from your pipeline config.",
            var_name,
        )
        return match.group(0)  # leave token in place — don't silently blank it
    return resolved


def _vault_replacer(match: re.Match) -> str:
    ref = match.group(1)
    vault_addr = os.getenv("VAULT_ADDR")

    if not vault_addr:
        logger.warning(
            "Vault reference '${vault:%s}' found but VAULT_ADDR is not set. "
            "Token left in place.",
            ref,
        )
        return match.group(0)

    # Parse  <mount>/<path>:<key>  or  <path>:<key>
    # Everything before the last ":" is the KV path; after is the key name.
    if ":" not in ref:
        logger.warning("Vault reference '${vault:%s}' has no key separator (expected path:key).", ref)
        return match.group(0)

    kv_path, key = ref.rsplit(":", 1)

    client = _get_vault_client()
    if client is None:
        return match.group(0)

    try:
        # Try KV v2 first (most common in modern Vault installs)
        # kv_path may include the mount point, e.g. "secret/myapp"
        parts = kv_path.split("/", 1)
        mount = parts[0] if len(parts) == 2 else "secret"
        path = parts[1] if len(parts) == 2 else kv_path
        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=path, mount_point=mount
            )
            secret_data = response["data"]["data"]
        except Exception:
            # Fall back to KV v1
            response = client.read(kv_path)
            secret_data = response["data"] if response else {}

        if key not in secret_data:
            logger.warning(
                "Vault secret at '%s' has no key '%s'. Token left in place.", kv_path, key
            )
            return match.group(0)

        return str(secret_data[key])

    except Exception as exc:
        logger.warning(
            "Vault read failed for '${vault:%s}': %s. Token left in place.", ref, exc
        )
        # Invalidate cached client in case the token expired
        global _vault_client_cache
        _vault_client_cache = None
        return match.group(0)
