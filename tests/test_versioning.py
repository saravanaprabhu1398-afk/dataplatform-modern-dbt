"""Tests for pipeline version history (versioning.py)."""
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "versioning_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module.init_db()
    yield
    db_module._initialized = False


from dataplatform.core.versioning import save_version, list_versions, get_version_content, diff_versions


YAML_V1 = """\
pipeline_name: my_pipeline
tasks:
  - name: extract
    type: executor
    plugin: python
    config: {}
"""

YAML_V2 = """\
pipeline_name: my_pipeline
tasks:
  - name: extract
    type: executor
    plugin: python
    config: {}
  - name: load
    type: executor
    plugin: duckdb
    config: {}
"""


class TestSaveVersion:
    def test_returns_version_id_for_new_content(self):
        vid = save_version("pipe_a", YAML_V1)
        assert vid is not None
        assert len(vid) == 36  # UUID

    def test_returns_none_for_identical_content(self):
        save_version("pipe_b", YAML_V1)
        vid2 = save_version("pipe_b", YAML_V1)
        assert vid2 is None

    def test_different_content_creates_new_version(self):
        v1 = save_version("pipe_c", YAML_V1)
        v2 = save_version("pipe_c", YAML_V2)
        assert v1 is not None
        assert v2 is not None
        assert v1 != v2

    def test_saved_by_is_stored(self):
        save_version("pipe_d", YAML_V1, saved_by="alice")
        versions = list_versions("pipe_d")
        assert versions[0]["saved_by"] == "alice"

    def test_same_content_different_pipelines_both_saved(self):
        v1 = save_version("pipe_x", YAML_V1)
        v2 = save_version("pipe_y", YAML_V1)
        assert v1 is not None
        assert v2 is not None
        assert v1 != v2


class TestListVersions:
    def test_returns_empty_for_unknown_pipeline(self):
        assert list_versions("nonexistent_pipe") == []

    def test_returns_versions_newest_first(self):
        save_version("ordered_pipe", YAML_V1)
        save_version("ordered_pipe", YAML_V2)
        versions = list_versions("ordered_pipe")
        assert len(versions) == 2
        assert versions[0]["saved_at"] >= versions[1]["saved_at"]

    def test_limit_respected(self):
        for i in range(5):
            save_version(f"limit_pipe_{i}", f"content: {i}")
        # Save all under the same pipeline with unique content
        for i in range(5):
            save_version("limited_pipe", f"content_{i}: {i}")
        versions = list_versions("limited_pipe", limit=3)
        assert len(versions) <= 3

    def test_no_content_in_listing(self):
        save_version("nocontent_pipe", YAML_V1)
        versions = list_versions("nocontent_pipe")
        assert "content" not in versions[0]


class TestGetVersionContent:
    def test_returns_correct_content(self):
        vid = save_version("content_pipe", YAML_V1)
        content = get_version_content("content_pipe", vid)
        assert content == YAML_V1

    def test_returns_none_for_wrong_pipeline(self):
        vid = save_version("content_pipe2", YAML_V1)
        result = get_version_content("wrong_pipeline", vid)
        assert result is None

    def test_returns_none_for_nonexistent_version(self):
        result = get_version_content("any_pipe", "00000000-0000-0000-0000-000000000000")
        assert result is None


class TestDiffVersions:
    def test_diff_between_two_versions(self):
        v1 = save_version("diff_pipe", YAML_V1)
        v2 = save_version("diff_pipe", YAML_V2)
        diff = diff_versions("diff_pipe", v1, v2)
        assert diff is not None
        assert "+" in diff or "-" in diff  # changes detected

    def test_diff_labels_contain_version_prefix(self):
        v1 = save_version("label_pipe", YAML_V1)
        v2 = save_version("label_pipe", YAML_V2)
        diff = diff_versions("label_pipe", v1, v2)
        assert "label_pipe@" in diff

    def test_diff_of_identical_content_is_empty(self):
        v1 = save_version("same_pipe", YAML_V1)
        # Force a second save with different content then back — but we can't
        # since the hash deduplicates. Instead diff v1 with itself via two separate pipes:
        # Actually: diff_versions expects two different version_ids for the same pipeline.
        # The dedup means we can't have two v_ids for identical content on the same pipe.
        # So test: diff returns empty string when a -> b content is same (impossible in normal flow).
        # We test via the same version_id twice — should still work.
        diff = diff_versions("same_pipe", v1, v1)
        assert diff == ""

    def test_returns_none_when_version_missing(self):
        v1 = save_version("missing_pipe", YAML_V1)
        result = diff_versions("missing_pipe", v1, "00000000-0000-0000-0000-000000000000")
        assert result is None

    def test_returns_none_when_both_missing(self):
        result = diff_versions(
            "any_pipe",
            "00000000-0000-0000-0000-000000000000",
            "11111111-1111-1111-1111-111111111111",
        )
        assert result is None
