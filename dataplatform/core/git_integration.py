"""Git remote integration: push pipeline YAMLs to / pull from Git repositories.

Supports HTTPS (token auth) and SSH remotes. All git operations use subprocess
with explicit argument lists — no shell=True — to prevent command injection.

Env vars:
    GIT_CLONES_PATH   Where to store local repo clones (default: data/git-clones)
    PIPELINES_PATH    Local pipeline YAML directory    (default: pipelines)
"""
import logging
import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dataplatform.core.database import (
    delete_git_remote as _db_delete_remote,
    get_git_remote as _db_get_remote,
    init_db,
    list_git_remotes as _db_list_remotes,
    list_git_push_log as _db_list_push_log,
    save_git_push_log as _db_log_push,
    save_git_remote as _db_save_remote,
)

logger = logging.getLogger(__name__)

_CLONES_BASE = Path(os.getenv("GIT_CLONES_PATH", "data/git-clones"))
_PIPELINES_DIR = Path(os.getenv("PIPELINES_PATH", "pipelines"))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clone_dir(remote_id: str) -> Path:
    return _CLONES_BASE / remote_id


def _auth_url(remote_url: str, token: Optional[str]) -> str:
    """Rewrite HTTPS URL to embed token for authentication."""
    if token and remote_url.startswith("https://"):
        host_and_path = remote_url[len("https://"):]
        return f"https://oauth2:{token}@{host_and_path}"
    return remote_url


def _mask_url(url: str) -> str:
    """Replace embedded credentials in URL with *** for logging."""
    return re.sub(r"(https?://)([^@]+)@", r"\1***@", url)


def _git(
    args: List[str],
    cwd: Optional[Path] = None,
    extra_env: Optional[Dict[str, str]] = None,
) -> Tuple[int, str, str]:
    """Run a git command. Returns (returncode, stdout, stderr)."""
    env = {**os.environ, **(extra_env or {})}
    env["GIT_TERMINAL_PROMPT"] = "0"  # never block on interactive prompt

    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd) if cwd else None,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return 1, "", "git command timed out after 60s"
    except FileNotFoundError:
        return 1, "", "git not found in PATH — install git to use this feature"

    return result.returncode, result.stdout.strip(), result.stderr.strip()


def _ensure_clone(remote: Dict[str, Any]) -> Tuple[bool, str]:
    """Clone the repo if it doesn't exist locally, otherwise pull latest."""
    clone_path = Path(remote["clone_path"])
    auth_url = _auth_url(remote["remote_url"], remote.get("token"))
    branch = remote.get("branch", "main")

    if not clone_path.exists():
        clone_path.parent.mkdir(parents=True, exist_ok=True)
        rc, _, err = _git(["clone", "--branch", branch, "--single-branch", auth_url, str(clone_path)])
        if rc != 0:
            return False, f"Clone failed: {err}"
    else:
        # Keep remote URL current (token may have rotated)
        _git(["remote", "set-url", "origin", auth_url], cwd=clone_path)
        rc, _, err = _git(["pull", "--ff-only", "origin", branch], cwd=clone_path)
        if rc != 0:
            # Local diverged — fetch + hard reset to remote
            rc2, _, err2 = _git(["fetch", "origin", branch], cwd=clone_path)
            if rc2 != 0:
                return False, f"Fetch failed: {err2}"
            _git(["reset", "--hard", f"origin/{branch}"], cwd=clone_path)

    return True, ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def register_remote(
    name: str,
    remote_url: str,
    auth_type: str = "token",
    token: Optional[str] = None,
    branch: str = "main",
    pipelines_path: str = "pipelines",
    created_by: Optional[str] = None,
) -> str:
    """Register a new git remote. Returns the new remote_id."""
    init_db()
    remote_id = str(uuid.uuid4())
    ok = _db_save_remote(
        remote_id,
        name,
        remote_url.rstrip("/"),
        auth_type,
        token,
        branch,
        pipelines_path.strip("/"),
        str(_clone_dir(remote_id)),
        created_by,
    )
    if not ok:
        raise ValueError(f"A remote named '{name}' already exists")
    logger.info("Registered git remote '%s' id=%s", name, remote_id[:8])
    return remote_id


def list_remotes() -> List[Dict[str, Any]]:
    """Return all remotes with tokens masked."""
    init_db()
    remotes = _db_list_remotes()
    for r in remotes:
        if r.get("token"):
            r["token"] = "***"
    return remotes


def get_remote(remote_id: str) -> Optional[Dict[str, Any]]:
    init_db()
    r = _db_get_remote(remote_id)
    if r and r.get("token"):
        r["token"] = "***"
    return r


def delete_remote(remote_id: str) -> bool:
    """Delete a remote and remove its local clone directory."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return False
    clone_path = Path(remote["clone_path"])
    if clone_path.exists():
        shutil.rmtree(clone_path, ignore_errors=True)
    return _db_delete_remote(remote_id)


def test_connection(remote_id: str) -> Dict[str, Any]:
    """Verify connectivity to the remote using git ls-remote."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}

    auth_url = _auth_url(remote["remote_url"], remote.get("token"))
    rc, out, err = _git(["ls-remote", "--heads", auth_url])

    if rc != 0:
        return {"ok": False, "error": err or "Connection failed — check URL and credentials"}

    branches = [
        line.split("\t")[1].replace("refs/heads/", "")
        for line in out.splitlines()
        if "\t" in line
    ]
    return {"ok": True, "branches": branches, "remote": remote["name"]}


def push_pipeline(
    remote_id: str,
    pipeline_name: str,
    yaml_content: str,
    commit_message: Optional[str] = None,
    pushed_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Commit a pipeline YAML to the remote Git repo and push."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}

    ok, err = _ensure_clone(remote)
    if not ok:
        _db_log_push(remote_id, remote["name"], pipeline_name, None, None, pushed_by, "error", err)
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"])
    pipelines_dir = clone_path / remote.get("pipelines_path", "pipelines")
    pipelines_dir.mkdir(parents=True, exist_ok=True)

    yaml_file = pipelines_dir / f"{pipeline_name}.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")

    rel_path = str(yaml_file.relative_to(clone_path))
    rc, _, err = _git(["add", rel_path], cwd=clone_path)
    if rc != 0:
        _db_log_push(remote_id, remote["name"], pipeline_name, None, None, pushed_by, "error", err)
        return {"ok": False, "error": f"git add failed: {err}"}

    rc, status_out, _ = _git(["status", "--porcelain"], cwd=clone_path)
    if not status_out.strip():
        _db_log_push(remote_id, remote["name"], pipeline_name, None, "no changes", pushed_by, "noop", None)
        return {"ok": True, "commit_sha": None, "message": "Pipeline already up to date in remote — no changes pushed"}

    msg = commit_message or f"chore: update pipeline {pipeline_name} via dataplatform"
    author = f"{pushed_by or 'dataplatform'} <dataplatform@noreply>"

    rc, _, err = _git(["commit", "-m", msg, "--author", author], cwd=clone_path)
    if rc != 0:
        _db_log_push(remote_id, remote["name"], pipeline_name, None, msg, pushed_by, "error", err)
        return {"ok": False, "error": f"git commit failed: {err}"}

    _, sha, _ = _git(["rev-parse", "HEAD"], cwd=clone_path)

    auth_url = _auth_url(remote["remote_url"], remote.get("token"))
    branch = remote.get("branch", "main")
    rc, _, err = _git(["push", auth_url, f"HEAD:{branch}"], cwd=clone_path)
    if rc != 0:
        _db_log_push(remote_id, remote["name"], pipeline_name, sha, msg, pushed_by, "error", err)
        return {"ok": False, "error": f"git push failed: {err}"}

    short_sha = sha[:8] if sha else "?"
    _db_log_push(remote_id, remote["name"], pipeline_name, sha, msg, pushed_by, "success", None)
    logger.info("Pushed pipeline '%s' to remote '%s' commit=%s", pipeline_name, remote["name"], short_sha)
    return {
        "ok": True,
        "commit_sha": sha,
        "message": f"Pushed {pipeline_name}.yaml → {remote['name']} ({short_sha})",
    }


def pull_pipelines(
    remote_id: str,
    pulled_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Pull all pipeline YAMLs from the remote into the local pipelines/ directory."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}

    ok, err = _ensure_clone(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"])
    pipelines_dir = clone_path / remote.get("pipelines_path", "pipelines")

    if not pipelines_dir.exists():
        return {
            "ok": True,
            "imported": [],
            "errors": [],
            "message": f"No '{remote.get('pipelines_path', 'pipelines')}' directory found in remote",
        }

    _PIPELINES_DIR.mkdir(parents=True, exist_ok=True)
    imported: List[str] = []
    errors: List[Dict[str, str]] = []

    for yaml_file in sorted(pipelines_dir.glob("*.yaml")):
        try:
            content = yaml_file.read_text(encoding="utf-8")
            dest = _PIPELINES_DIR / yaml_file.name
            dest.write_text(content, encoding="utf-8")
            imported.append(yaml_file.stem)
        except Exception as exc:
            errors.append({"file": yaml_file.name, "error": str(exc)})

    logger.info("Pulled %d pipeline(s) from remote '%s'", len(imported), remote["name"])
    return {
        "ok": True,
        "imported": imported,
        "errors": errors,
        "message": f"Imported {len(imported)} pipeline(s) from {remote['name']}",
    }


def get_status(remote_id: str) -> Dict[str, Any]:
    """Compare local pipelines/ directory with the remote's pipelines path."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}

    ok, err = _ensure_clone(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"])
    pipelines_dir = clone_path / remote.get("pipelines_path", "pipelines")

    remote_names = {f.stem for f in pipelines_dir.glob("*.yaml")} if pipelines_dir.exists() else set()
    local_names = {f.stem for f in _PIPELINES_DIR.glob("*.yaml")} if _PIPELINES_DIR.exists() else set()

    only_local = sorted(local_names - remote_names)
    only_remote = sorted(remote_names - local_names)
    shared = local_names & remote_names

    modified_locally = []
    for name in shared:
        local_content = (_PIPELINES_DIR / f"{name}.yaml").read_text(encoding="utf-8")
        remote_content = (pipelines_dir / f"{name}.yaml").read_text(encoding="utf-8")
        if local_content != remote_content:
            modified_locally.append(name)

    _, log_out, _ = _git(["log", "--oneline", "-5"], cwd=clone_path)

    return {
        "ok": True,
        "only_local": only_local,
        "only_remote": only_remote,
        "modified_locally": sorted(modified_locally),
        "in_sync": sorted(shared - set(modified_locally)),
        "recent_commits": log_out.splitlines(),
    }


def get_push_log(remote_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Return push history for a remote."""
    init_db()
    return _db_list_push_log(remote_id, limit)
