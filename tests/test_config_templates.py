"""Tests for config template injection in the pipeline generator."""
import pytest
from dataplatform.core.pipeline_generator import (
    _get_config_template,
    generate_pipeline_yaml_from_text,
    PLUGIN_CONFIG_TEMPLATES,
)


class TestGetConfigTemplate:
    def test_duckdb_load_has_file_path(self):
        cfg = _get_config_template("duckdb", "load")
        assert "file_path" in cfg
        assert cfg["file_path"]

    def test_duckdb_validate_has_checks(self):
        cfg = _get_config_template("duckdb", "validate")
        assert "checks" in cfg
        assert isinstance(cfg["checks"], list)
        assert len(cfg["checks"]) > 0

    def test_duckdb_aggregate_has_metrics(self):
        cfg = _get_config_template("duckdb", "aggregate")
        assert "group_by" in cfg
        assert "metrics" in cfg

    def test_postgres_query_has_connection_block(self):
        cfg = _get_config_template("postgres", "query")
        assert "connection" in cfg
        assert "host" in cfg["connection"]
        assert "database" in cfg["connection"]

    def test_postgres_load_has_table_and_file(self):
        cfg = _get_config_template("postgres", "load")
        assert "table_name" in cfg
        assert "file_path" in cfg
        assert "connection" in cfg

    def test_snowflake_load_has_snowflake_config(self):
        cfg = _get_config_template("snowflake", "load_to_snowflake")
        assert "snowflake_config" in cfg
        assert "account" in cfg["snowflake_config"]
        assert "warehouse" in cfg["snowflake_config"]

    def test_bigquery_query_has_project(self):
        cfg = _get_config_template("bigquery", "query")
        assert "project_id" in cfg
        assert "sql" in cfg

    def test_api_get_has_url_and_headers(self):
        cfg = _get_config_template("api", "GET")
        assert "url" in cfg
        assert "headers" in cfg
        assert "method" in cfg

    def test_kafka_publish_has_brokers_and_topic(self):
        cfg = _get_config_template("kafka", "publish")
        assert "brokers" in cfg
        assert "topic" in cfg
        assert isinstance(cfg["brokers"], list)

    def test_python_execute_code_has_code_key(self):
        cfg = _get_config_template("python", "execute_code")
        assert "code" in cfg
        assert cfg["code"]

    def test_email_send_has_smtp_fields(self):
        cfg = _get_config_template("email", "send")
        assert "smtp_server" in cfg
        assert "smtp_port" in cfg
        assert "recipients" in cfg

    def test_dbt_run_has_project_dir(self):
        cfg = _get_config_template("dbt", "run")
        assert "project_dir" in cfg
        assert "profiles_dir" in cfg

    def test_unknown_plugin_returns_empty_dict(self):
        cfg = _get_config_template("nonexistent_plugin", "unknown_op")
        assert cfg == {}

    def test_known_plugin_unknown_op_falls_back_to_plugin_default(self):
        # When operation isn't in templates, _get_config_template falls back
        # to the first available template for that plugin rather than returning {}
        cfg = _get_config_template("postgres", "nonexistent_op")
        # Should return some non-empty template (plugin-level fallback)
        assert cfg != {}

    def test_completely_unknown_plugin_and_op_returns_empty_dict(self):
        cfg = _get_config_template("nonexistent_plugin_xyz", "nonexistent_op")
        assert cfg == {}

    def test_templates_return_copies(self):
        """Each call should return a fresh dict, not the same object."""
        cfg1 = _get_config_template("duckdb", "load")
        cfg2 = _get_config_template("duckdb", "load")
        assert cfg1 is not cfg2
        cfg1["file_path"] = "modified"
        assert cfg2["file_path"] != "modified"


class TestGeneratorIncludesTemplates:
    def test_postgres_task_has_connection_config(self):
        result = generate_pipeline_yaml_from_text(
            "Extract data from postgres database"
        )
        tasks = result["parsed_config"]["tasks"]
        assert len(tasks) >= 1
        postgres_task = next((t for t in tasks if t.get("plugin") == "postgres"), None)
        assert postgres_task is not None, "Expected a postgres task"
        config = postgres_task.get("config", {})
        assert "connection" in config or "sql" in config or config != {}

    def test_dbt_task_has_project_dir(self):
        result = generate_pipeline_yaml_from_text(
            "Run dbt models to transform the data"
        )
        tasks = result["parsed_config"]["tasks"]
        dbt_task = next((t for t in tasks if t.get("plugin") == "dbt"), None)
        assert dbt_task is not None, "Expected a dbt task"
        config = dbt_task.get("config", {})
        assert "project_dir" in config

    def test_task_structure_unchanged(self):
        """Ensure config injection doesn't break required task fields."""
        result = generate_pipeline_yaml_from_text(
            "Load csv file then validate with duckdb then send email"
        )
        for task in result["parsed_config"]["tasks"]:
            assert "name" in task
            assert "type" in task
            assert "plugin" in task
