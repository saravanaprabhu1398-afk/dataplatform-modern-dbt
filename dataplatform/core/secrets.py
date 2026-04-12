"""Secret interpolation for pipeline task configs.

Resolves ${TOKEN} patterns in any string value inside a config dict/list:

  ${MY_ENV_VAR}          → resolved from os.environ
  ${vault:secret/app:key} → resolved from HashiCorp Vault
                            (stub: logs a warning unless VAULT_ADDR is set)

Usage::

    from dataplatform.core.secrets import resolve_secrets

    safe_config = resolve_secrets(task.config)
    plugin.execute(safe_config)

The function is recursive — nested dicts and lists are fully resolved.
Non-string values (int, float, bool, None) are passed through unchanged.
If an environment variable is referenced but not set, the original
${VAR} token is left in place and a warning is logged (rather than
replacing with an empty string, which could silently break auth).
"""
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

# ${SOME_VAR}  — plain environment variable reference
_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# ${vault:path/to/secret:key}  — Vault KV reference
_VAULT_RE = re.compile(r"\$\{vault:([^}]+)\}")


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
            f"Secret reference '${{%s}}' is unresolved: environment variable not set. "
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
            "Returning empty string. Set VAULT_ADDR and VAULT_TOKEN to enable Vault integration.",
            ref,
        )
        return ""

    # Vault client stub — extend with hvac when Vault is available in the environment.
    # Example implementation once hvac is installed:
    #
    #   import hvac
    #   parts = ref.split(":")
    #   path, key = parts[0], parts[1] if len(parts) > 1 else "value"
    #   client = hvac.Client(url=vault_addr, token=os.getenv("VAULT_TOKEN"))
    #   secret = client.secrets.kv.read_secret_version(path=path)
    #   return secret["data"]["data"].get(key, "")
    #
    logger.warning(
        "VAULT_ADDR is set to %s but the Vault client (hvac) is not installed. "
        "Install hvac and implement _vault_replacer to enable Vault secrets.",
        vault_addr,
    )
    return ""
