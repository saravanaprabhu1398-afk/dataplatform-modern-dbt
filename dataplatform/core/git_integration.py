"""Git remote integration: push pipeline YAMLs to / pull from Git repositories.

Supports HTTPS (token auth) and SSH remotes. All git operations use subprocess
with explicit argument lists — no shell=True — to prevent command injection.

Env vars:
    GIT_CLONES_PATH   Where to store local repo clones (default: data/git-clones)
    GIT_WORKSPACE_MAX_FILE_BYTES  Max editable file size in the UI (default: 1048576)
    PIPELINES_PATH    Local pipeline YAML directory    (default: pipelines)
"""
import logging
import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
_MAX_FILE_BYTES = int(os.getenv("GIT_WORKSPACE_MAX_FILE_BYTES", str(1024 * 1024)))
_SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__"}


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
    strip_output: bool = True,
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

    stdout = result.stdout.strip() if strip_output else result.stdout
    stderr = result.stderr.strip() if strip_output else result.stderr
    return result.returncode, stdout, stderr


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


def _ensure_workspace(remote: Dict[str, Any]) -> Tuple[bool, str]:
    """Ensure a local clone exists without overwriting local workspace edits."""
    clone_path = Path(remote["clone_path"])
    auth_url = _auth_url(remote["remote_url"], remote.get("token"))
    branch = remote.get("branch", "main")

    if not clone_path.exists():
        clone_path.parent.mkdir(parents=True, exist_ok=True)
        rc, _, err = _git(["clone", "--branch", branch, "--single-branch", auth_url, str(clone_path)])
        if rc != 0:
            return False, f"Clone failed: {err}"
    else:
        _git(["remote", "set-url", "origin", auth_url], cwd=clone_path)
    return True, ""


def _safe_repo_path(clone_path: Path, rel_path: str = "") -> Path:
    """Resolve *rel_path* inside *clone_path* and reject traversal / .git access."""
    rel_path = (rel_path or "").strip().replace("\\", "/").lstrip("/")
    if rel_path in {"", "."}:
        return clone_path.resolve()
    parts = Path(rel_path).parts
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("Invalid repository path")
    if ".git" in parts:
        raise ValueError("The .git directory is not editable")
    candidate = (clone_path / rel_path).resolve()
    root = clone_path.resolve()
    if os.path.commonpath([str(root), str(candidate)]) != str(root):
        raise ValueError("Path escapes repository root")
    return candidate


def _repo_relpath(clone_path: Path, path: Path) -> str:
    return path.relative_to(clone_path).as_posix()


def _is_binary(data: bytes) -> bool:
    return b"\0" in data


def _status_label(xy: str) -> str:
    if "?" in xy:
        return "untracked"
    if "D" in xy:
        return "deleted"
    if "A" in xy:
        return "added"
    if "R" in xy:
        return "renamed"
    if "M" in xy:
        return "modified"
    return "changed"


def _parse_porcelain_z(raw: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    tokens = raw.split("\0")
    i = 0
    while i < len(tokens):
        token = tokens[i]
        i += 1
        if not token:
            continue
        xy = token[:2]
        path = token[3:] if len(token) > 3 else ""
        old_path = None
        if xy[0] in {"R", "C"} or xy[1] in {"R", "C"}:
            old_path = tokens[i] if i < len(tokens) else None
            i += 1
        entries.append({
            "path": path,
            "old_path": old_path,
            "index_status": xy[0],
            "worktree_status": xy[1],
            "status": _status_label(xy),
        })
    return entries


def _validate_paths(clone_path: Path, paths: Optional[Iterable[str]]) -> List[str]:
    safe_paths: List[str] = []
    for rel_path in paths or []:
        safe = _safe_repo_path(clone_path, rel_path)
        safe_paths.append(_repo_relpath(clone_path.resolve(), safe))
    return safe_paths


def _build_untracked_diff(clone_path: Path, paths: Optional[Iterable[str]] = None) -> str:
    """Render a compact diff for untracked text files."""
    requested = set(paths or [])
    rc, status_out, _ = _git(
        ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=clone_path,
        strip_output=False,
    )
    if rc != 0:
        return ""

    chunks: List[str] = []
    for change in _parse_porcelain_z(status_out):
        rel_path = change.get("path") or ""
        if change.get("status") != "untracked":
            continue
        if requested and rel_path not in requested:
            continue
        file_path = clone_path / rel_path
        if not file_path.is_file():
            continue
        data = file_path.read_bytes()
        if len(data) > _MAX_FILE_BYTES or _is_binary(data):
            continue
        rc_diff, diff_out, diff_err = _git(
            ["diff", "--no-index", "--", os.devnull, rel_path],
            cwd=clone_path,
        )
        if rc_diff in {0, 1} and diff_out:
            chunks.append(diff_out)
        elif rc_diff not in {0, 1}:
            chunks.append(f"# Could not diff {rel_path}: {diff_err}")
    return "\n".join(chunks)


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


# ---------------------------------------------------------------------------
# Repo workspace API
# ---------------------------------------------------------------------------

def list_repo_tree(remote_id: str, path: str = "", max_entries: int = 1000) -> Dict[str, Any]:
    """Return a browsable file tree for a remote's local clone."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"]).resolve()
    try:
        base = _safe_repo_path(clone_path, path)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    if not base.exists():
        return {"ok": False, "error": "Path not found"}
    if not base.is_dir():
        return {"ok": False, "error": "Path is not a directory"}

    entries: List[Dict[str, Any]] = []
    for root, dirs, files in os.walk(base):
        root_path = Path(root)
        dirs[:] = sorted([d for d in dirs if d not in _SKIP_DIRS])
        depth = len(root_path.relative_to(base).parts)
        if root_path != base:
            entries.append({
                "path": _repo_relpath(clone_path, root_path),
                "name": root_path.name,
                "type": "directory",
                "depth": depth - 1,
            })
        for filename in sorted(files):
            file_path = root_path / filename
            rel = _repo_relpath(clone_path, file_path)
            entries.append({
                "path": rel,
                "name": filename,
                "type": "file",
                "depth": depth,
                "size": file_path.stat().st_size,
            })
            if len(entries) >= max_entries:
                return {
                    "ok": True,
                    "remote": get_remote(remote_id),
                    "path": path,
                    "entries": entries,
                    "truncated": True,
                }
    return {
        "ok": True,
        "remote": get_remote(remote_id),
        "path": path,
        "entries": entries,
        "truncated": False,
    }


def read_repo_file(remote_id: str, path: str) -> Dict[str, Any]:
    """Read a text file from the remote's local clone."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"]).resolve()
    try:
        file_path = _safe_repo_path(clone_path, path)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    if not file_path.exists() or not file_path.is_file():
        return {"ok": False, "error": "File not found"}
    data = file_path.read_bytes()
    if len(data) > _MAX_FILE_BYTES:
        return {"ok": False, "error": f"File is larger than {_MAX_FILE_BYTES} bytes"}
    if _is_binary(data):
        return {"ok": False, "error": "Binary files cannot be edited in the workspace"}
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError:
        return {"ok": False, "error": "File is not UTF-8 text"}
    return {
        "ok": True,
        "path": _repo_relpath(clone_path, file_path),
        "content": content,
        "size": len(data),
    }


def write_repo_file(remote_id: str, path: str, content: str) -> Dict[str, Any]:
    """Write a text file inside the remote's local clone."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}
    if len(content.encode("utf-8")) > _MAX_FILE_BYTES:
        return {"ok": False, "error": f"File content is larger than {_MAX_FILE_BYTES} bytes"}

    clone_path = Path(remote["clone_path"]).resolve()
    try:
        file_path = _safe_repo_path(clone_path, path)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")
    rel = _repo_relpath(clone_path, file_path)
    return {"ok": True, "path": rel, "message": f"Saved {rel}"}


def get_repo_status(remote_id: str) -> Dict[str, Any]:
    """Return branch, changed files, recent commits, and ahead/behind counts."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"])
    branch = remote.get("branch", "main")
    _git(["fetch", "origin", branch], cwd=clone_path)
    _, current_branch, _ = _git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=clone_path)
    rc, status_out, err = _git(
        ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=clone_path,
        strip_output=False,
    )
    if rc != 0:
        return {"ok": False, "error": err or "git status failed"}
    changes = _parse_porcelain_z(status_out)
    rc, ahead_behind, _ = _git(["rev-list", "--left-right", "--count", f"HEAD...origin/{branch}"], cwd=clone_path)
    ahead, behind = 0, 0
    if rc == 0 and ahead_behind:
        parts = ahead_behind.split()
        if len(parts) == 2:
            ahead, behind = int(parts[0]), int(parts[1])
    _, log_out, _ = _git(["log", "--oneline", "-8"], cwd=clone_path)
    return {
        "ok": True,
        "branch": current_branch or branch,
        "remote_branch": branch,
        "ahead": ahead,
        "behind": behind,
        "clean": len(changes) == 0,
        "changes": changes,
        "recent_commits": log_out.splitlines() if log_out else [],
    }


def get_repo_diff(remote_id: str, path: Optional[str] = None) -> Dict[str, Any]:
    """Return the current diff for the workspace or one file."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"]).resolve()
    args = ["diff", "--"]
    safe_paths: List[str] = []
    if path:
        try:
            safe = _safe_repo_path(clone_path, path)
            safe_paths.append(_repo_relpath(clone_path, safe))
            args.extend(safe_paths)
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
    rc, out, err = _git(args, cwd=clone_path)
    if rc != 0:
        return {"ok": False, "error": err or "git diff failed"}
    rc_cached, cached_out, cached_err = _git(["diff", "--cached", "--"] + safe_paths, cwd=clone_path)
    if rc_cached != 0:
        return {"ok": False, "error": cached_err or "git cached diff failed"}
    untracked_out = _build_untracked_diff(clone_path, safe_paths if path else None)
    combined = "\n".join(part for part in [out, cached_out, untracked_out] if part)
    return {"ok": True, "path": path, "diff": combined}


def pull_repo(remote_id: str) -> Dict[str, Any]:
    """Pull the configured branch into the local clone when the workspace is clean."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"])
    rc, status_out, err = _git(["status", "--porcelain"], cwd=clone_path)
    if rc != 0:
        return {"ok": False, "error": err or "git status failed"}
    if status_out.strip():
        return {"ok": False, "error": "Commit or discard local workspace changes before pulling"}

    auth_url = _auth_url(remote["remote_url"], remote.get("token"))
    branch = remote.get("branch", "main")
    rc, out, err = _git(["pull", "--ff-only", auth_url, branch], cwd=clone_path)
    if rc != 0:
        return {"ok": False, "error": err or "git pull failed"}
    return {"ok": True, "message": out or "Already up to date"}


def push_repo(remote_id: str, pushed_by: Optional[str] = None) -> Dict[str, Any]:
    """Push committed workspace changes to the configured remote branch."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}

    clone_path = Path(remote["clone_path"])
    auth_url = _auth_url(remote["remote_url"], remote.get("token"))
    branch = remote.get("branch", "main")
    rc, _, err = _git(["push", auth_url, f"HEAD:{branch}"], cwd=clone_path)
    if rc != 0:
        _db_log_push(remote_id, remote["name"], "__repo__", None, "workspace push", pushed_by, "error", err)
        return {"ok": False, "error": f"git push failed: {err}"}
    _, sha, _ = _git(["rev-parse", "HEAD"], cwd=clone_path)
    _db_log_push(remote_id, remote["name"], "__repo__", sha, "workspace push", pushed_by, "success", None)
    return {"ok": True, "commit_sha": sha, "message": f"Pushed workspace to {remote['name']} ({sha[:8] if sha else '?'})"}


def commit_repo_changes(
    remote_id: str,
    message: str,
    paths: Optional[List[str]] = None,
    push: bool = False,
    actor: Optional[str] = None,
) -> Dict[str, Any]:
    """Commit workspace changes and optionally push them."""
    init_db()
    remote = _db_get_remote(remote_id)
    if not remote:
        return {"ok": False, "error": "Remote not found"}
    ok, err = _ensure_workspace(remote)
    if not ok:
        return {"ok": False, "error": err}
    message = (message or "").strip()
    if not message:
        return {"ok": False, "error": "Commit message is required"}

    clone_path = Path(remote["clone_path"]).resolve()
    try:
        safe_paths = _validate_paths(clone_path, paths)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    add_args = ["add", "--"] + safe_paths if safe_paths else ["add", "-A"]
    rc, _, err = _git(add_args, cwd=clone_path)
    if rc != 0:
        return {"ok": False, "error": f"git add failed: {err}"}

    rc, _, err = _git(["diff", "--cached", "--quiet", "--exit-code"], cwd=clone_path)
    if rc == 0:
        return {"ok": True, "status": "noop", "message": "No workspace changes to commit"}
    if rc not in {0, 1}:
        return {"ok": False, "error": err or "git staged diff failed"}

    author_name = actor or "dataplatform"
    author = f"{author_name} <dataplatform@noreply>"
    env = {
        "GIT_AUTHOR_NAME": author_name,
        "GIT_AUTHOR_EMAIL": "dataplatform@noreply",
        "GIT_COMMITTER_NAME": author_name,
        "GIT_COMMITTER_EMAIL": "dataplatform@noreply",
    }
    rc, _, err = _git(["commit", "-m", message, "--author", author], cwd=clone_path, extra_env=env)
    if rc != 0:
        return {"ok": False, "error": f"git commit failed: {err}"}
    _, sha, _ = _git(["rev-parse", "HEAD"], cwd=clone_path)

    result: Dict[str, Any] = {
        "ok": True,
        "status": "committed",
        "commit_sha": sha,
        "message": f"Committed workspace changes ({sha[:8] if sha else '?'})",
    }
    if push:
        push_result = push_repo(remote_id, pushed_by=actor)
        result["push"] = push_result
        if not push_result.get("ok"):
            result["ok"] = False
            result["error"] = push_result.get("error")
    return result
