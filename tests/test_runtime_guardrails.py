import pytest

from dataplatform.core.runtime_guardrails import (
    normalize_execution_mode,
    validate_plugin_execution_allowed,
    validate_runtime_settings,
)


def test_non_production_allows_development_defaults():
    validate_runtime_settings(
        environment="development",
        username="admin",
        password="admin",
        session_secret="dpflow-dev-secret-change-me",
        execution_mode="embedded",
    )


def test_production_rejects_default_credentials_and_secret_and_embedded_worker():
    with pytest.raises(RuntimeError) as exc:
        validate_runtime_settings(
            environment="production",
            username="admin",
            password="admin",
            session_secret="change-me-in-production",
            execution_mode="embedded",
        )

    message = str(exc.value)
    assert "admin/admin" in message
    assert "DATAPLATFORM_SESSION_SECRET" in message
    assert "DATAPLATFORM_EXECUTION_MODE" in message


def test_production_allows_strong_external_configuration():
    validate_runtime_settings(
        environment="production",
        username="platform-admin",
        password="not-the-default",
        session_secret="a-very-long-unique-session-secret-value",
        execution_mode="external",
    )


def test_execution_mode_aliases_normalize_to_external():
    assert normalize_execution_mode("queue") == "external"
    assert normalize_execution_mode("worker") == "external"
    assert normalize_execution_mode("in-process") == "embedded"


def test_production_blocks_unsafe_local_code_executors_by_default(monkeypatch):
    monkeypatch.setenv("DATAPLATFORM_ENV", "production")
    monkeypatch.delenv("DATAPLATFORM_ALLOW_UNSAFE_EXECUTORS", raising=False)

    with pytest.raises(RuntimeError, match="disabled in production"):
        validate_plugin_execution_allowed("python")
    with pytest.raises(RuntimeError, match="disabled in production"):
        validate_plugin_execution_allowed("shell")


def test_production_allows_unsafe_executors_with_explicit_override(monkeypatch):
    monkeypatch.setenv("DATAPLATFORM_ENV", "production")
    monkeypatch.setenv("DATAPLATFORM_ALLOW_UNSAFE_EXECUTORS", "true")

    validate_plugin_execution_allowed("python")
    validate_plugin_execution_allowed("shell")
