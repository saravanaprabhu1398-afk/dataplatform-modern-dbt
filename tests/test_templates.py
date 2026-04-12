"""Tests for the pipeline templates marketplace (templates.py)."""
import pytest
from pathlib import Path
import yaml


@pytest.fixture
def templates_dir(tmp_path):
    return tmp_path / "templates"


@pytest.fixture
def pipelines_dir(tmp_path):
    return tmp_path / "pipelines"


@pytest.fixture
def patched_dirs(templates_dir, pipelines_dir, monkeypatch):
    """Point templates.py at tmp_path directories."""
    import dataplatform.core.templates as tmpl_module
    monkeypatch.setattr(tmpl_module, "_TEMPLATES_DIR", templates_dir)
    monkeypatch.setattr(tmpl_module, "_PIPELINES_DIR", pipelines_dir)
    return templates_dir, pipelines_dir


def write_template(directory: Path, stem: str, content: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    p = directory / f"{stem}.yaml"
    p.write_text(content, encoding="utf-8")
    return p


SIMPLE_TEMPLATE = """\
pipeline_name: simple_etl
description: A simple ETL template
tags:
  - etl
  - test
tasks:
  - name: extract
    type: executor
    plugin: python
    config: {}
  - name: load
    type: executor
    plugin: duckdb
    config: {}
    depends_on:
      - extract
"""


from dataplatform.core.templates import list_templates, get_template_content, use_template


class TestListTemplates:
    def test_empty_when_no_dir(self, patched_dirs):
        templates_dir, _ = patched_dirs
        # directory not yet created
        result = list_templates()
        assert result == []

    def test_returns_template_metadata(self, patched_dirs):
        templates_dir, _ = patched_dirs
        write_template(templates_dir, "my_template", SIMPLE_TEMPLATE)
        result = list_templates()
        assert len(result) == 1
        t = result[0]
        assert t["template_id"] == "my_template"
        assert t["pipeline_name"] == "simple_etl"
        assert t["description"] == "A simple ETL template"
        assert "etl" in t["tags"]
        assert t["task_count"] == 2

    def test_plugins_extracted(self, patched_dirs):
        templates_dir, _ = patched_dirs
        write_template(templates_dir, "t1", SIMPLE_TEMPLATE)
        result = list_templates()
        assert set(result[0]["plugins"]) == {"python", "duckdb"}

    def test_multiple_templates(self, patched_dirs):
        templates_dir, _ = patched_dirs
        write_template(templates_dir, "t1", SIMPLE_TEMPLATE)
        write_template(templates_dir, "t2", SIMPLE_TEMPLATE.replace("simple_etl", "another_etl"))
        result = list_templates()
        assert len(result) == 2

    def test_skips_invalid_yaml(self, patched_dirs):
        templates_dir, _ = patched_dirs
        write_template(templates_dir, "good", SIMPLE_TEMPLATE)
        (templates_dir / "bad.yaml").write_text(": invalid: yaml: [", encoding="utf-8")
        result = list_templates()
        assert len(result) == 1

    def test_no_file_path_in_result(self, patched_dirs):
        templates_dir, _ = patched_dirs
        write_template(templates_dir, "t", SIMPLE_TEMPLATE)
        result = list_templates()
        assert "file_path" not in result[0]


class TestGetTemplateContent:
    def test_returns_raw_yaml(self, patched_dirs):
        templates_dir, _ = patched_dirs
        write_template(templates_dir, "raw_t", SIMPLE_TEMPLATE)
        content = get_template_content("raw_t")
        assert content == SIMPLE_TEMPLATE

    def test_returns_none_for_missing(self, patched_dirs):
        result = get_template_content("does_not_exist")
        assert result is None


class TestUseTemplate:
    def test_creates_pipeline_file(self, patched_dirs):
        templates_dir, pipelines_dir = patched_dirs
        write_template(templates_dir, "base_t", SIMPLE_TEMPLATE)
        saved = use_template("base_t", "my_new_pipeline")
        assert Path(saved).exists()

    def test_pipeline_name_updated(self, patched_dirs):
        templates_dir, pipelines_dir = patched_dirs
        write_template(templates_dir, "base_t", SIMPLE_TEMPLATE)
        saved = use_template("base_t", "custom_name")
        data = yaml.safe_load(Path(saved).read_text())
        assert data["pipeline_name"] == "custom_name"

    def test_saved_to_pipelines_dir(self, patched_dirs):
        templates_dir, pipelines_dir = patched_dirs
        write_template(templates_dir, "base_t", SIMPLE_TEMPLATE)
        saved = use_template("base_t", "in_pipelines")
        assert Path(saved).parent == pipelines_dir

    def test_tags_removed_from_output(self, patched_dirs):
        templates_dir, pipelines_dir = patched_dirs
        write_template(templates_dir, "tagged_t", SIMPLE_TEMPLATE)
        saved = use_template("tagged_t", "no_tags_pipe")
        data = yaml.safe_load(Path(saved).read_text())
        assert "tags" not in data

    def test_raises_for_missing_template(self, patched_dirs):
        with pytest.raises(FileNotFoundError, match="not found"):
            use_template("ghost_template", "new_pipeline")

    def test_tasks_preserved(self, patched_dirs):
        templates_dir, pipelines_dir = patched_dirs
        write_template(templates_dir, "with_tasks", SIMPLE_TEMPLATE)
        saved = use_template("with_tasks", "tasks_preserved_pipe")
        data = yaml.safe_load(Path(saved).read_text())
        assert len(data["tasks"]) == 2
        assert data["tasks"][0]["name"] == "extract"


class TestBuiltinTemplates:
    """Smoke test the real template files shipped with the project."""

    def test_etl_postgres_to_duckdb_exists(self):
        content = get_template_content("etl_postgres_to_duckdb")
        assert content is not None
        data = yaml.safe_load(content)
        assert "tasks" in data
        assert len(data["tasks"]) >= 1

    def test_dbt_run_and_test_exists(self):
        content = get_template_content("dbt_run_and_test")
        assert content is not None

    def test_api_ingest_and_validate_exists(self):
        content = get_template_content("api_ingest_and_validate")
        assert content is not None

    def test_daily_python_etl_exists(self):
        content = get_template_content("daily_python_etl")
        assert content is not None

    def test_list_returns_all_builtin_templates(self):
        templates = list_templates()
        ids = {t["template_id"] for t in templates}
        assert "etl_postgres_to_duckdb" in ids
        assert "dbt_run_and_test" in ids
