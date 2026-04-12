"""Tests for the secret interpolation module (secrets.py)."""
import os
import pytest
from unittest.mock import patch
from dataplatform.core.secrets import resolve_secrets


class TestEnvVarResolution:
    def test_resolves_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "supersecret")
        assert resolve_secrets("${MY_TOKEN}") == "supersecret"

    def test_resolves_env_var_inside_string(self, monkeypatch):
        monkeypatch.setenv("DB_PASS", "abc123")
        result = resolve_secrets("password=${DB_PASS}&host=localhost")
        assert result == "password=abc123&host=localhost"

    def test_multiple_vars_in_one_string(self, monkeypatch):
        monkeypatch.setenv("HOST", "db.example.com")
        monkeypatch.setenv("PORT", "5432")
        result = resolve_secrets("${HOST}:${PORT}")
        assert result == "db.example.com:5432"

    def test_unset_var_left_as_token(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        result = resolve_secrets("${MISSING_VAR}")
        assert result == "${MISSING_VAR}"  # not silently blanked

    def test_no_tokens_unchanged(self):
        assert resolve_secrets("plain string") == "plain string"


class TestVaultResolution:
    def test_vault_without_addr_returns_empty_and_warns(self, monkeypatch):
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        with patch("dataplatform.core.secrets.logger") as mock_log:
            result = resolve_secrets("${vault:secret/app:password}")
        assert result == ""
        mock_log.warning.assert_called_once()
        assert "VAULT_ADDR" in mock_log.warning.call_args[0][0]

    def test_vault_with_addr_but_no_client_returns_empty_and_warns(self, monkeypatch):
        monkeypatch.setenv("VAULT_ADDR", "http://vault:8200")
        with patch("dataplatform.core.secrets.logger") as mock_log:
            result = resolve_secrets("${vault:secret/app:key}")
        assert result == ""
        mock_log.warning.assert_called_once()
        warning_msg = mock_log.warning.call_args[0][0]
        assert "hvac" in warning_msg or "Vault" in warning_msg


class TestRecursiveResolution:
    def test_resolves_nested_dict(self, monkeypatch):
        monkeypatch.setenv("PGPASS", "secret123")
        config = {"connection": {"password": "${PGPASS}", "host": "localhost"}}
        result = resolve_secrets(config)
        assert result["connection"]["password"] == "secret123"
        assert result["connection"]["host"] == "localhost"

    def test_resolves_list_of_strings(self, monkeypatch):
        monkeypatch.setenv("BROKER", "kafka:9092")
        result = resolve_secrets(["${BROKER}", "other"])
        assert result[0] == "kafka:9092"
        assert result[1] == "other"

    def test_resolves_deeply_nested(self, monkeypatch):
        monkeypatch.setenv("SECRET_KEY", "xyz")
        config = {"a": {"b": {"c": "${SECRET_KEY}"}}}
        assert resolve_secrets(config)["a"]["b"]["c"] == "xyz"

    def test_non_string_values_pass_through(self):
        config = {"port": 5432, "enabled": True, "ratio": 1.5, "nothing": None}
        result = resolve_secrets(config)
        assert result == config

    def test_returns_new_dict_not_same_object(self, monkeypatch):
        monkeypatch.setenv("X", "1")
        original = {"key": "${X}"}
        result = resolve_secrets(original)
        assert result is not original

    def test_mixed_types_in_list(self, monkeypatch):
        monkeypatch.setenv("VAL", "resolved")
        result = resolve_secrets([1, "${VAL}", None, True])
        assert result == [1, "resolved", None, True]

    def test_scalars_pass_through_unchanged(self):
        assert resolve_secrets(42) == 42
        assert resolve_secrets(3.14) == 3.14
        assert resolve_secrets(True) is True
        assert resolve_secrets(None) is None
