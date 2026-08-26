"""Tests for full-repository Git workspace operations."""
import subprocess
from pathlib import Path

import pytest

from dataplatform.core import git_integration as git


def _run_git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


@pytest.fixture(autouse=True)
def isolated_git_workspace(tmp_path, monkeypatch):
    db_file = tmp_path / "git_workspace.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_file))

    import dataplatform.core.database as db_module

    db_module._initialized = False
    db_module._DB_PATH = db_file
    db_module._engine = None
    db_module._engine_db_path = None

    git._CLONES_BASE = tmp_path / "clones"
    git._PIPELINES_DIR = tmp_path / "pipelines"

    yield

    db_module._initialized = False
    db_module._engine = None
    db_module._engine_db_path = None


@pytest.fixture
def origin_repo(tmp_path):
    origin = tmp_path / "origin.git"
    source = tmp_path / "source"
    source.mkdir()

    _run_git(tmp_path, "init", "--bare", str(origin))
    _run_git(source, "init")
    _run_git(source, "checkout", "-b", "main")
    _run_git(source, "config", "user.email", "tests@example.com")
    _run_git(source, "config", "user.name", "Test User")

    (source / "pipelines").mkdir()
    (source / "src").mkdir()
    (source / "README.md").write_text("# Demo Repo\n", encoding="utf-8")
    (source / "pipelines" / "sample.yaml").write_text(
        "pipeline_name: sample\ntasks: []\n",
        encoding="utf-8",
    )
    (source / "src" / "job.py").write_text(
        "def run():\n    return 'hello'\n",
        encoding="utf-8",
    )
    _run_git(source, "add", ".")
    _run_git(source, "commit", "-m", "initial commit")
    _run_git(source, "remote", "add", "origin", str(origin))
    _run_git(source, "push", "-u", "origin", "main")

    return origin


def _register_local_remote(origin: Path) -> str:
    return git.register_remote(
        name="local-platform",
        remote_url=str(origin),
        auth_type="none",
        branch="main",
        pipelines_path="pipelines",
        created_by="tests",
    )


def test_workspace_browse_edit_commit_and_push_round_trip(tmp_path, origin_repo):
    remote_id = _register_local_remote(origin_repo)

    tree = git.list_repo_tree(remote_id)
    assert tree["ok"] is True
    paths = {entry["path"] for entry in tree["entries"]}
    assert {"README.md", "pipelines/sample.yaml", "src/job.py"}.issubset(paths)

    file_payload = git.read_repo_file(remote_id, "src/job.py")
    assert file_payload["ok"] is True
    assert "return 'hello'" in file_payload["content"]

    changed = "def run():\n    return 'hello from workspace'\n"
    assert git.write_repo_file(remote_id, "src/job.py", changed)["ok"] is True
    assert git.write_repo_file(remote_id, "docs/note.md", "Workspace note\n")["ok"] is True

    status = git.get_repo_status(remote_id)
    assert status["ok"] is True
    by_path = {change["path"]: change["status"] for change in status["changes"]}
    assert by_path["src/job.py"] == "modified"
    assert by_path["docs/note.md"] == "untracked"

    file_diff = git.get_repo_diff(remote_id, "src/job.py")
    assert file_diff["ok"] is True
    assert "hello from workspace" in file_diff["diff"]

    new_file_diff = git.get_repo_diff(remote_id, "docs/note.md")
    assert new_file_diff["ok"] is True
    assert "Workspace note" in new_file_diff["diff"]

    blocked_pull = git.pull_repo(remote_id)
    assert blocked_pull["ok"] is False
    assert "workspace changes" in blocked_pull["error"]

    commit_one = git.commit_repo_changes(
        remote_id,
        message="feat: update job",
        paths=["src/job.py"],
        push=False,
        actor="alice",
    )
    assert commit_one["ok"] is True
    assert commit_one["status"] == "committed"

    status_after_one = git.get_repo_status(remote_id)
    remaining = {change["path"]: change["status"] for change in status_after_one["changes"]}
    assert remaining == {"docs/note.md": "untracked"}
    assert status_after_one["ahead"] == 1

    push_one = git.push_repo(remote_id, pushed_by="alice")
    assert push_one["ok"] is True

    verify_one = tmp_path / "verify-one"
    _run_git(tmp_path, "clone", "--branch", "main", str(origin_repo), str(verify_one))
    assert (verify_one / "src" / "job.py").read_text(encoding="utf-8") == changed
    assert not (verify_one / "docs" / "note.md").exists()

    commit_two = git.commit_repo_changes(
        remote_id,
        message="docs: add workspace note",
        paths=["docs/note.md"],
        push=True,
        actor="alice",
    )
    assert commit_two["ok"] is True
    assert commit_two["push"]["ok"] is True

    verify_two = tmp_path / "verify-two"
    _run_git(tmp_path, "clone", "--branch", "main", str(origin_repo), str(verify_two))
    assert (verify_two / "docs" / "note.md").read_text(encoding="utf-8") == "Workspace note\n"


def test_workspace_rejects_paths_outside_worktree(origin_repo):
    remote_id = _register_local_remote(origin_repo)

    assert git.read_repo_file(remote_id, "../secret.txt")["ok"] is False
    assert git.write_repo_file(remote_id, ".git/config", "bad")["ok"] is False
    assert git.write_repo_file(remote_id, "nested/.git/config", "bad")["ok"] is False
    assert git.list_repo_tree(remote_id, ".git")["ok"] is False
    assert git.get_repo_diff(remote_id, "../secret.txt")["ok"] is False
    assert git.commit_repo_changes(
        remote_id,
        message="bad path",
        paths=["../secret.txt"],
    )["ok"] is False
