try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import asyncio
import uvicorn
import logging
import os
import base64
import hashlib
import hmac
import inspect
import json
import tempfile
import time
from dataplatform.core.config import load_config, PipelineConfig
from dataplatform.core.dag import DAGBuilder
from dataplatform.core.executor import PipelineExecutor, TaskExecutor
from dataplatform.core.scheduler import get_scheduler
from dataplatform.core.logging_config import setup_logging
from dataplatform.core.pipeline_generator import (
    generate_pipeline_yaml_from_text,
    save_generated_pipeline,
)
from dataplatform.core.database import (
    init_db,
    save_run_status,
    get_latest_run,
    get_run_history,
    get_run_by_id,
    get_all_pipeline_names,
    get_sla_violations,
    append_audit_event,
    get_audit_log,
    enqueue_run,
    set_run_status_in_queue,
    get_queue_runs,
    get_queue_run,
    recover_orphaned_runs,
    get_run_timeseries,
)
from dataplatform.core.lineage import build_lineage_graph, get_asset_lineage
from dataplatform.core.metrics import generate_prometheus_text, _CONTENT_TYPE as _METRICS_CONTENT_TYPE
from dataplatform.core.quality import get_pipeline_quality_history
from dataplatform.core.alerts import check_sla_and_alert
from dataplatform.core.triggers import get_trigger_manager, restore_triggers_from_db
from dataplatform.core.versioning import save_version, list_versions, get_version_content, diff_versions
from dataplatform.core.semantic_metrics import list_metrics as list_metric_definitions, compute_metric, get_history as get_metric_history, load_metric
from dataplatform.core.costs import record_run_cost, get_cost_summary, get_team_cost_summary, get_pipeline_cost_history
from dataplatform.core.catalog import search_assets, get_asset_detail, get_pipeline_catalog
from dataplatform.core.templates import list_templates, get_template_content, use_template
from dataplatform.core.git_integration import (
    register_remote,
    list_remotes,
    get_remote,
    delete_remote,
    test_connection as git_test_connection,
    push_pipeline as git_push_pipeline,
    pull_pipelines as git_pull_pipelines,
    get_status as git_get_status,
    get_push_log as git_get_push_log,
)
from dataplatform.core.database import (
    save_trigger as db_save_trigger,
    get_triggers as db_get_triggers,
    get_trigger as db_get_trigger,
    delete_trigger as db_delete_trigger,
    update_trigger_last_fired,
)
from dataplatform.core.worker import get_worker_pool
from dataplatform.core.auth import (
    verify_user,
    create_user,
    update_user_role,
    delete_user,
    get_all_users,
    has_permission,
    ROLES,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    json_format=os.getenv("LOG_JSON", "false").lower() in ("1", "true", "yes"),
    log_file=os.getenv("LOG_FILE", "logs/pipeline.log"),
)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Data Platform API", version="0.2.0")


@app.on_event("startup")
async def startup_event():
    init_db()
    recovered = recover_orphaned_runs()
    if recovered:
        logger.warning("Startup: marked %d orphaned run(s) as failed", recovered)
    scheduler = get_scheduler()
    scheduler.start()
    restore_triggers_from_db(get_trigger_manager())
    logger.info("Database initialised. Pipeline scheduler and triggers started.")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
static_path = Path(__file__).resolve().parent.parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
    logger.info(f"Mounted static files from: {static_path}")
else:
    logger.warning(f"Static directory not found at: {static_path}")

# ---------------------------------------------------------------------------
# Session / auth constants
# ---------------------------------------------------------------------------
SESSION_COOKIE_NAME = "dpflow_session"
SESSION_DURATION_SECONDS = 60 * 60 * 12
AUTH_USERNAME = os.getenv("DATAPLATFORM_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("DATAPLATFORM_PASSWORD", "admin")
SESSION_SECRET = os.getenv("DATAPLATFORM_SESSION_SECRET", "dpflow-dev-secret-change-me")
PUBLIC_PATH_PREFIXES = ("/login", "/static", "/health")
PUBLIC_PATHS = {"/login", "/health", "/metrics"}

ENVIRONMENT_PROFILES: Dict[str, Dict[str, Any]] = {
    "local": {
        "id": "local",
        "name": "Local",
        "description": "Run on the local worker with local files and default plugins.",
        "compute": "local-worker",
        "badge": "Default",
    },
    "dev": {
        "id": "dev",
        "name": "Development",
        "description": "Development profile for isolated test inputs and dev credentials.",
        "compute": "dev-worker",
        "badge": "Dev",
    },
    "prod": {
        "id": "prod",
        "name": "Production",
        "description": "Production profile metadata for governed operational runs.",
        "compute": "prod-worker",
        "badge": "Prod",
    },
}

if AUTH_USERNAME == "admin" and AUTH_PASSWORD == "admin":
    logger.warning(
        "Using default DATAPLATFORM_USERNAME/DATAPLATFORM_PASSWORD credentials. "
        "Set environment variables before production use."
    )
if SESSION_SECRET == "dpflow-dev-secret-change-me":
    logger.warning(
        "Using default DATAPLATFORM_SESSION_SECRET. "
        "Set a strong secret before production use."
    )


# ---------------------------------------------------------------------------
# Session cookie helpers
# ---------------------------------------------------------------------------

def _sign_value(value: str) -> str:
    return hmac.new(SESSION_SECRET.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def _create_session_cookie(username: str, role: str = "admin", team: Optional[str] = None) -> str:
    payload = {
        "username": username,
        "role": role,
        "team": team,
        "exp": int(time.time()) + SESSION_DURATION_SECONDS,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    signature = _sign_value(encoded)
    return f"{encoded}.{signature}"


def _read_session_cookie(cookie_value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not cookie_value or "." not in cookie_value:
        return None

    encoded, signature = cookie_value.rsplit(".", 1)
    if not hmac.compare_digest(_sign_value(encoded), signature):
        return None

    try:
        payload = json.loads(base64.urlsafe_b64decode(encoded.encode("utf-8")).decode("utf-8"))
    except Exception:
        return None

    if int(payload.get("exp", 0)) < int(time.time()):
        return None

    return payload


def _get_current_user(request: Request) -> Optional[Dict[str, Any]]:
    """Return the authenticated user payload or None."""
    return _read_session_cookie(request.cookies.get(SESSION_COOKIE_NAME))


def _is_authenticated(request: Request) -> bool:
    return _get_current_user(request) is not None


def _set_session_cookie(response: Response, username: str, role: str = "admin", team: Optional[str] = None) -> None:
    response.set_cookie(
        SESSION_COOKIE_NAME,
        _create_session_cookie(username, role, team),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=SESSION_DURATION_SECONDS,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


def _is_browser_request(request: Request) -> bool:
    return "text/html" in request.headers.get("accept", "")


def _require_permission(request: Request, action: str) -> Dict[str, Any]:
    """Raise 401/403 if the current user lacks *action*. Returns user dict."""
    user = _get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not has_permission(user.get("role", "viewer"), action):
        raise HTTPException(
            status_code=403,
            detail=f"Forbidden: role '{user.get('role')}' cannot perform '{action}'",
        )
    return user


def _get_request_username(request: Request) -> Optional[str]:
    """Extract the authenticated username from the session cookie, or None."""
    user = _get_current_user(request)
    return user.get("username") if user else None


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    if request.method == "OPTIONS":
        return await call_next(request)

    if path in PUBLIC_PATHS or any(path.startswith(prefix) for prefix in PUBLIC_PATH_PREFIXES):
        return await call_next(request)

    if _is_authenticated(request):
        return await call_next(request)

    if _is_browser_request(request):
        return RedirectResponse(url="/login", status_code=303)

    return JSONResponse(status_code=401, content={"detail": "Authentication required"})


# ---------------------------------------------------------------------------
# Run history helpers (DB-backed, replaces in-memory dict + JSON file)
# ---------------------------------------------------------------------------

import uuid as _uuid


def update_pipeline_status(
    pipeline_name: str,
    status: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a pipeline status update to the DB and return the record."""
    if run_id is None:
        run_id = str(_uuid.uuid4())
    return save_run_status(pipeline_name, run_id, status, message, details)


def _pending_task_runs(config: PipelineConfig, execution_order: List[str]) -> List[Dict[str, Any]]:
    task_by_name = {task.name: task for task in config.tasks}
    task_runs: List[Dict[str, Any]] = []
    for task_name in execution_order:
        task = task_by_name.get(task_name)
        if not task:
            continue
        task_runs.append({
            "task_name": task.name,
            "status": "pending",
            "plugin": task.plugin,
            "type": task.type,
            "operation": task.operation,
            "depends_on": task.depends_on or [],
            "timeout_seconds": task.timeout,
            "started_at": None,
            "completed_at": None,
            "duration_seconds": None,
            "attempts": 0,
            "max_attempts": task.retries + 1,
            "retry_count": 0,
            "attempt_history": [],
            "error": None,
        })
    return task_runs


def _fallback_task_runs(
    tasks: Dict[str, Any],
    execution_waves: List[List[str]],
    results: Dict[str, bool],
    errors: Dict[str, str],
) -> List[Dict[str, Any]]:
    task_runs: List[Dict[str, Any]] = []
    for task_name in [name for wave in execution_waves for name in wave]:
        task = tasks.get(task_name)
        reported = task_name in results
        success = results.get(task_name)
        status = "success" if success is True else "failed" if success is False else "skipped"
        task_runs.append({
            "task_name": task_name,
            "status": status if reported else "skipped",
            "plugin": getattr(task, "plugin", None),
            "type": getattr(task, "type", None),
            "operation": getattr(task, "operation", None),
            "depends_on": getattr(task, "depends_on", None) or [],
            "timeout_seconds": getattr(task, "timeout", None),
            "started_at": None,
            "completed_at": None,
            "duration_seconds": None,
            "attempts": 1 if reported else 0,
            "max_attempts": getattr(task, "retries", 0) + 1 if task else None,
            "retry_count": 0,
            "attempt_history": [],
            "error": errors.get(task_name),
        })
    return task_runs


def _execute_parallel_with_task_runs(
    executor: Any,
    *,
    tasks: Dict[str, Any],
    execution_waves: List[List[str]],
    config: Dict[str, Any],
    pipeline_name: str,
    run_id: str,
) -> tuple[bool, Dict[str, bool], Dict[str, str], List[Dict[str, Any]]]:
    kwargs = {
        "tasks": tasks,
        "execution_waves": execution_waves,
        "config": config,
        "pipeline_name": pipeline_name,
        "run_id": run_id,
    }
    try:
        supports_task_runs = "return_task_runs" in inspect.signature(
            executor.execute_pipeline_parallel
        ).parameters
    except (TypeError, ValueError):
        supports_task_runs = False

    if supports_task_runs:
        success, results, errors, task_runs = executor.execute_pipeline_parallel(
            **kwargs,
            return_task_runs=True,
        )
    else:
        success, results, errors = executor.execute_pipeline_parallel(**kwargs)
        task_runs = _fallback_task_runs(tasks, execution_waves, results, errors)

    return success, results, errors, task_runs


def _normalize_environment_profile(profile_id: Optional[str]) -> Dict[str, Any]:
    key = (profile_id or "local").strip().lower()
    return ENVIRONMENT_PROFILES.get(key, ENVIRONMENT_PROFILES["local"])


def _runtime_context(
    *,
    config: PipelineConfig,
    parameters: Optional[Dict[str, Any]] = None,
    environment_profile: Optional[str] = "local",
) -> Dict[str, Any]:
    profile = _normalize_environment_profile(environment_profile)
    return {
        "file_path": config.file_path,
        "runtime_parameters": parameters or {},
        "parameters": parameters or {},
        "environment_profile": profile["id"],
        "environment": profile,
    }


def _run_detail_context(
    *,
    execution_order: List[str],
    task_runs: List[Dict[str, Any]],
    parameters: Optional[Dict[str, Any]] = None,
    environment_profile: Optional[str] = "local",
    repair: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    profile = _normalize_environment_profile(environment_profile)
    details: Dict[str, Any] = {
        "execution_order": execution_order,
        "task_runs": task_runs,
        "runtime_parameters": parameters or {},
        "environment_profile": profile["id"],
        "environment": profile,
    }
    if repair:
        details["repair"] = repair
    return details


def _resolve_config_path_for_run(run_record: Dict[str, Any]) -> str:
    queue_record = get_queue_run(run_record["run_id"])
    if queue_record and queue_record.get("config_path"):
        config_path = Path(queue_record["config_path"])
        if config_path.exists():
            return str(config_path)

    pipelines, _, _, _ = _discover_pipeline_files()
    matches = [
        pipeline for pipeline in pipelines
        if pipeline.get("display_name") == run_record.get("pipeline_name")
    ]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail=f"Could not locate config for pipeline '{run_record.get('pipeline_name')}'",
        )
    return matches[0]["file_path"]


def _first_failed_task(run_record: Dict[str, Any]) -> Optional[str]:
    details = run_record.get("details") or {}
    for task_run in details.get("task_runs") or []:
        if task_run.get("status") == "failed" and task_run.get("task_name"):
            return task_run["task_name"]
    errors = details.get("errors") or {}
    if isinstance(errors, dict) and errors:
        return next(iter(errors.keys()))
    results = details.get("results") or {}
    if isinstance(results, dict):
        for task_name, ok in results.items():
            if ok is False:
                return task_name
    return None


def _repair_execution_waves(
    config: PipelineConfig,
    from_task: str,
) -> List[List[str]]:
    task_names = {task.name for task in config.tasks}
    if from_task not in task_names:
        raise HTTPException(
            status_code=400,
            detail=f"Task '{from_task}' was not found in pipeline '{config.pipeline_name}'",
        )

    selected = {from_task}
    changed = True
    while changed:
        changed = False
        for task in config.tasks:
            dependencies = set(task.depends_on or [])
            if task.name not in selected and dependencies.intersection(selected):
                selected.add(task.name)
                changed = True

    dag_builder = DAGBuilder(config.tasks)
    dag_builder.build()
    full_waves = dag_builder.get_execution_waves()
    repair_waves = [
        [task_name for task_name in wave if task_name in selected]
        for wave in full_waves
    ]
    return [wave for wave in repair_waves if wave]


# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------

class PipelineRunRequest(BaseModel):
    config_path: str
    dry_run: bool = False
    parameters: Optional[Dict[str, Any]] = None
    environment_profile: str = "local"


class RepairRunRequest(BaseModel):
    from_task: Optional[str] = None


class VersionRestoreRequest(BaseModel):
    config_path: Optional[str] = None


class PipelineScheduleRequest(BaseModel):
    config_path: str
    schedule: Optional[Dict[str, str]] = None


class PipelineGenerationRequest(BaseModel):
    input_text: str


class LoginRequest(BaseModel):
    username: str
    password: str


class PipelineSaveRequest(BaseModel):
    yaml_content: str
    filename: str


class PipelineGenerationResponse(BaseModel):
    yaml_content: str
    parsed_config: Dict[str, Any]
    warnings: List[str]
    detected_language: str


class PipelineSaveResponse(BaseModel):
    file_path: str
    pipeline_name: str
    message: str


class PipelineResponse(BaseModel):
    pipeline_name: str
    status: str
    message: str
    run_id: Optional[str] = None
    execution_order: Optional[List[str]] = None
    results: Optional[Dict[str, bool]] = None


class ScheduledPipelineInfo(BaseModel):
    config_path: str
    schedule: Dict[str, str]
    next_run: Optional[str] = None


class PipelineStatusResponse(BaseModel):
    pipeline_name: str
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None
    updated_at: Optional[str] = None


class PipelineValidationRequest(BaseModel):
    config_path: str


class TaskValidationResult(BaseModel):
    task_name: str
    plugin: str
    plugin_type: str
    plugin_loadable: bool
    error: Optional[str] = None


class PipelineValidationResponse(BaseModel):
    config_path: str
    pipeline_name: str
    is_valid: bool
    task_count: int
    task_results: List[TaskValidationResult]
    errors: List[str]
    warnings: List[str]


# Admin user management models
class UserCreateRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"
    team: Optional[str] = None


class UserRoleUpdateRequest(BaseModel):
    role: str


class UserResponse(BaseModel):
    id: int
    username: str
    role: str
    team: Optional[str] = None
    created_at: str


# ---------------------------------------------------------------------------
# Pipeline discovery helper
# ---------------------------------------------------------------------------

def _discover_pipeline_files():
    workspace_root = Path(__file__).parent.parent.parent
    pipelines_dir = workspace_root / "pipelines"

    search_dirs: List[Path] = []
    if pipelines_dir.exists():
        search_dirs.append(pipelines_dir)
    search_dirs.append(workspace_root)

    yaml_files_set = set()
    for search_dir in search_dirs:
        for file_path in search_dir.glob("*.yaml"):
            if file_path.parent == workspace_root and (pipelines_dir / file_path.name).exists():
                continue
            yaml_files_set.add(file_path)

    pipelines: List[Dict[str, Any]] = []
    failed_pipelines: List[Dict[str, Any]] = []

    for yaml_file in sorted(yaml_files_set):
        try:
            config = load_config(str(yaml_file))
            pipelines.append({
                "name": yaml_file.name,
                "display_name": config.pipeline_name,
                "description": getattr(config, "description", "No description"),
                "team": getattr(config, "team", None),
                "file_path": str(yaml_file),
                "task_count": len(config.tasks),
                "status": "loaded",
                "error": None,
            })
        except Exception as e:
            failed_pipelines.append({
                "name": yaml_file.name,
                "file_path": str(yaml_file),
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
            })

    return pipelines, failed_pipelines, workspace_root, pipelines_dir if pipelines_dir.exists() else None


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

@app.get("/login")
async def login_page(request: Request):
    if _is_authenticated(request):
        return RedirectResponse(url="/", status_code=303)
    login_file = Path(__file__).resolve().parent.parent / "static" / "login.html"
    if login_file.exists(): return _page(login_file)
    raise HTTPException(status_code=404, detail=f"Login page not found at {login_file}")


@app.post("/login")
async def login(request: LoginRequest):
    username = request.username
    user = verify_user(username, request.password)
    if user is None:
        try:
            append_audit_event("auth", "login_failed", actor=username)
        except Exception as _ae:
            logger.warning("Audit log failed (login_failed): %s", _ae)
        raise HTTPException(status_code=401, detail="Invalid username or password")
    try:
        append_audit_event("auth", "login_success", actor=username)
    except Exception as _ae:
        logger.warning("Audit log failed (login_success): %s", _ae)
    response = JSONResponse({"message": "Login successful", "role": user["role"]})
    _set_session_cookie(response, user["username"], user["role"], user.get("team"))
    return response


@app.post("/logout")
async def logout():
    response = JSONResponse({"message": "Logged out"})
    _clear_session_cookie(response)
    return response


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

_NO_CACHE = {"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"}


def _page(path: Path) -> FileResponse:
    return FileResponse(str(path), media_type="text/html", headers=_NO_CACHE)


@app.get("/")
async def root():
    landing_page = Path(__file__).resolve().parent.parent / "static" / "landing.html"
    if landing_page.exists():
        return _page(landing_page)
    return {
        "message": "Data Platform API",
        "version": "0.2.0",
        "dashboard": "/dashboard",
        "generator": "/generator",
        "docs": "/docs",
    }


@app.get("/dashboard")
async def dashboard():
    static_index = Path(__file__).resolve().parent.parent / "static" / "index.html"
    if static_index.exists():
        return _page(static_index)
    raise HTTPException(status_code=404, detail=f"Dashboard not found at {static_index}")


@app.get("/generator")
async def generator_page():
    generator_index = Path(__file__).resolve().parent.parent / "static" / "generator.html"
    if generator_index.exists():
        return _page(generator_index)
    raise HTTPException(status_code=404, detail=f"Generator page not found at {generator_index}")


# ---------------------------------------------------------------------------
# Health / info
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "0.2.0",
    }


@app.get("/info")
async def get_info():
    workspace_root = Path(__file__).parent.parent.parent
    return {
        "api_version": "0.2.0",
        "workspace_root": str(workspace_root),
        "workspace_exists": workspace_root.exists(),
        "static_dir": str(Path(__file__).parent.parent / "static"),
        "static_exists": (Path(__file__).parent.parent / "static").exists(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@app.get("/environment-profiles")
async def list_environment_profiles(request: Request):
    _require_permission(request, "read")
    return {"profiles": list(ENVIRONMENT_PROFILES.values())}


# ---------------------------------------------------------------------------
# Pipeline config / discovery
# ---------------------------------------------------------------------------

@app.get("/pipeline-config")
async def get_pipeline_config(config_path: str):
    try:
        config_file = Path(config_path)
        if not config_file.exists():
            raise HTTPException(status_code=404, detail=f"Config file not found: {config_path}")
        raw_content = config_file.read_text(encoding="utf-8")
        parsed_config = load_config(str(config_file))
        return {
            "config_path": str(config_file),
            "raw_content": raw_content,
            "parsed_config": parsed_config.model_dump(exclude_none=True),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to load pipeline config preview: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/pipelines")
async def list_pipelines():
    try:
        pipelines, failed_pipelines, workspace_root, pipelines_dir = _discover_pipeline_files()
        return {
            "pipelines": pipelines,
            "failed_pipelines": failed_pipelines,
            "workspace_root": str(workspace_root),
            "pipelines_folder": str(pipelines_dir) if pipelines_dir and pipelines_dir.exists() else None,
        }
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list pipelines: {str(e)}")


# ---------------------------------------------------------------------------
# Pipeline generation (editor+)
# ---------------------------------------------------------------------------

@app.post("/generate-pipeline", response_model=PipelineGenerationResponse)
async def generate_pipeline(request_body: PipelineGenerationRequest, request: Request):
    _require_permission(request, "generate")
    try:
        return PipelineGenerationResponse(**generate_pipeline_yaml_from_text(request_body.input_text))
    except Exception as e:
        logger.error(f"Failed to generate pipeline YAML: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/save-pipeline", response_model=PipelineSaveResponse)
async def save_pipeline(request_body: PipelineSaveRequest, request: Request):
    user = _require_permission(request, "save")
    try:
        file_path = save_generated_pipeline(request_body.yaml_content, request_body.filename)
        config = load_config(file_path)
        # Auto-snapshot for version history
        try:
            save_version(config.pipeline_name, request_body.yaml_content, saved_by=user.get("username"))
        except Exception as exc:
            logger.warning("Version snapshot failed for '%s': %s", config.pipeline_name, exc)
        return PipelineSaveResponse(
            file_path=file_path,
            pipeline_name=config.pipeline_name,
            message="Pipeline saved successfully",
        )
    except Exception as e:
        logger.error(f"Failed to save generated pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------------
# Validation (editor+)
# ---------------------------------------------------------------------------

@app.post("/validate", response_model=PipelineValidationResponse)
async def validate_pipeline(request_body: PipelineValidationRequest, request: Request):
    _require_permission(request, "validate")

    errors: List[str] = []
    warnings: List[str] = []
    task_results: List[TaskValidationResult] = []
    pipeline_name = "unknown"

    try:
        config = load_config(request_body.config_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Config not found: {request_body.config_path}")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid config: {exc}")

    pipeline_name = config.pipeline_name

    try:
        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
    except ValueError as exc:
        errors.append(f"DAG error: {exc}")

    task_executor = TaskExecutor()
    for task in config.tasks:
        try:
            task_executor.load_plugin(task.plugin, task.type)
            task_results.append(TaskValidationResult(
                task_name=task.name,
                plugin=task.plugin,
                plugin_type=task.type,
                plugin_loadable=True,
            ))
        except Exception as exc:
            error_msg = str(exc)
            errors.append(f"Task '{task.name}': {error_msg}")
            task_results.append(TaskValidationResult(
                task_name=task.name,
                plugin=task.plugin,
                plugin_type=task.type,
                plugin_loadable=False,
                error=error_msg,
            ))

    return PipelineValidationResponse(
        config_path=request_body.config_path,
        pipeline_name=pipeline_name,
        is_valid=len(errors) == 0,
        task_count=len(config.tasks),
        task_results=task_results,
        errors=errors,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Pipeline execution (editor+)
# ---------------------------------------------------------------------------

@app.post("/run/dry-run")
async def dry_run_pipeline(request_body: PipelineRunRequest, request: Request):
    """Validate config and preview execution without running any tasks."""
    _require_permission(request, "run")
    try:
        config = load_config(request_body.config_path)
        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
        execution_waves = dag_builder.get_execution_waves()
        execution_order = [t for wave in execution_waves for t in wave]

        task_executor = TaskExecutor()
        task_previews = []
        plugin_errors = []

        for task in config.tasks:
            preview = {
                "name": task.name,
                "plugin": task.plugin,
                "type": task.type,
                "depends_on": task.depends_on or [],
                "timeout": task.timeout,
                "retries": task.retries,
                "status": "would_run",
                "plugin_loadable": False,
                "error": None,
            }
            try:
                task_executor.load_plugin(task.plugin, task.type)
                preview["plugin_loadable"] = True
            except Exception as e:
                preview["status"] = "plugin_error"
                preview["error"] = str(e)
                plugin_errors.append({"task": task.name, "error": str(e)})
            task_previews.append(preview)

        return {
            "pipeline_name": config.pipeline_name,
            "dry_run": True,
            "valid": len(plugin_errors) == 0,
            "task_count": len(config.tasks),
            "execution_order": execution_order,
            "execution_waves": execution_waves,
            "tasks": task_previews,
            "plugin_errors": plugin_errors,
        }
    except Exception as e:
        logger.error(f"Dry-run failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/run", response_model=PipelineResponse)
async def run_pipeline(request_body: PipelineRunRequest, request: Request):
    _require_permission(request, "run")
    try:
        config = load_config(request_body.config_path)
        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
        execution_order = dag_builder.get_execution_order()

        run_id = str(_uuid.uuid4())
        actor = _get_request_username(request)
        environment = _normalize_environment_profile(request_body.environment_profile)
        parameters = request_body.parameters or {}

        # Write to both run history (legacy) and persistent queue
        update_pipeline_status(
            config.pipeline_name,
            "queued",
            "Pipeline queued for execution",
            _run_detail_context(
                execution_order=execution_order,
                task_runs=_pending_task_runs(config, execution_order),
                parameters=parameters,
                environment_profile=environment["id"],
            ),
            run_id=run_id,
        )
        enqueue_run(run_id, config.pipeline_name, request_body.config_path, actor=actor)

        try:
            append_audit_event(
                "pipeline", "run_queued",
                actor=actor,
                resource=config.pipeline_name,
                details={
                    "run_id": run_id,
                    "environment_profile": environment["id"],
                    "runtime_parameters": parameters,
                },
            )
        except Exception as _ae:
            logger.warning("Audit log failed (run_queued): %s", _ae)

        get_worker_pool().submit(
            run_id,
            execute_pipeline_background,
            config,
            run_id,
            runtime_parameters=parameters,
            environment_profile=environment["id"],
        )

        return PipelineResponse(
            pipeline_name=config.pipeline_name,
            status="queued",
            message="Pipeline queued for execution",
            run_id=run_id,
            execution_order=execution_order,
        )
    except Exception as e:
        logger.error(f"Failed to start pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/run/sync", response_model=PipelineResponse)
async def run_pipeline_sync(request_body: PipelineRunRequest, request: Request = None):
    if request is not None:
        _require_permission(request, "run")
    try:
        config = load_config(request_body.config_path)
        if request_body.dry_run:
            # Redirect to the dry-run handler — just preview, don't execute
            dag_builder = DAGBuilder(config.tasks)
            dag_builder.build()
            execution_waves = dag_builder.get_execution_waves()
            execution_order = [t for wave in execution_waves for t in wave]
            return PipelineResponse(
                pipeline_name=config.pipeline_name,
                status="dry_run",
                message="Dry run completed — no tasks were executed",
                execution_order=execution_order,
            )
        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
        execution_waves = dag_builder.get_execution_waves()
        execution_order = [t for wave in execution_waves for t in wave]

        run_id = str(_uuid.uuid4())
        environment = _normalize_environment_profile(request_body.environment_profile)
        parameters = request_body.parameters or {}
        t0 = time.time()
        executor = PipelineExecutor()
        tasks_by_name = {task.name: task for task in config.tasks}
        success, task_results, errors, task_runs = _execute_parallel_with_task_runs(
            executor,
            tasks=tasks_by_name,
            execution_waves=execution_waves,
            config=_runtime_context(
                config=config,
                parameters=parameters,
                environment_profile=environment["id"],
            ),
            pipeline_name=config.pipeline_name,
            run_id=run_id,
        )
        duration = time.time() - t0

        # SLA check
        sla_violated = False
        if config.sla:
            sla_violated = check_sla_and_alert(config.pipeline_name, run_id, duration, config.sla)

        # Cost attribution
        try:
            record_run_cost(run_id, config.pipeline_name, getattr(config, "team", None),
                            len(config.tasks), duration)
        except Exception as _cost_exc:
            logger.warning("Cost recording skipped: %s", _cost_exc)

        status_value = "completed" if success else "failed"
        message = "Pipeline executed successfully" if success else "Pipeline failed"
        update_pipeline_status(
            config.pipeline_name,
            status_value,
            message,
            {
                "execution_order": execution_order,
                "success": success,
                "results": task_results,
                "errors": errors,
                "task_runs": task_runs,
                "runtime_parameters": parameters,
                "environment_profile": environment["id"],
                "environment": environment,
                "duration_seconds": round(duration, 2),
                "sla_violated": sla_violated,
            },
            run_id=run_id,
        )

        if success:
            get_trigger_manager().notify_pipeline_completed(config.pipeline_name, run_id)

        results = {task_name: success for task_name in execution_order} if success else None
        return PipelineResponse(
            pipeline_name=config.pipeline_name,
            status=status_value,
            message=message,
            run_id=run_id,
            execution_order=execution_order,
            results=results,
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Run lifecycle — cancel / status by run_id (editor+ / viewer+)
# ---------------------------------------------------------------------------

@app.post("/run/{run_id}/cancel")
async def cancel_run(run_id: str, request: Request):
    """Cancel a queued (not yet started) pipeline run."""
    _require_permission(request, "run")
    existing = get_run_by_id(run_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"No run found with id '{run_id}'")
    if not get_worker_pool().cancel(run_id):
        raise HTTPException(
            status_code=409,
            detail="Run cannot be cancelled — it is already running or has completed",
        )
    save_run_status(existing["pipeline_name"], run_id, "cancelled", "Cancelled by user")
    set_run_status_in_queue(run_id, "cancelled")
    return {"run_id": run_id, "status": "cancelled"}


@app.post("/run/{run_id}/repair", response_model=PipelineResponse)
async def repair_run(run_id: str, request_body: RepairRunRequest, request: Request):
    """Queue a repair run from the failed task or a selected task."""
    _require_permission(request, "run")
    existing = get_run_by_id(run_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"No run found with id '{run_id}'")

    config_path = _resolve_config_path_for_run(existing)
    config = load_config(config_path)
    from_task = request_body.from_task or _first_failed_task(existing)
    if not from_task:
        raise HTTPException(
            status_code=400,
            detail="No failed task was found for this run. Select a task to repair from.",
        )

    execution_waves = _repair_execution_waves(config, from_task)
    execution_order = [task_name for wave in execution_waves for task_name in wave]
    repair_run_id = str(_uuid.uuid4())
    actor = _get_request_username(request)

    update_pipeline_status(
        config.pipeline_name,
        "queued",
        f"Repair queued from task {from_task}",
        {
            "execution_order": execution_order,
            "task_runs": _pending_task_runs(config, execution_order),
            "repair": {
                "parent_run_id": run_id,
                "from_task": from_task,
            },
        },
        run_id=repair_run_id,
    )
    enqueue_run(repair_run_id, config.pipeline_name, config_path, actor=actor)

    try:
        append_audit_event(
            "pipeline",
            "run_repair_queued",
            actor=actor,
            resource=config.pipeline_name,
            details={
                "run_id": repair_run_id,
                "parent_run_id": run_id,
                "from_task": from_task,
            },
        )
    except Exception as _ae:
        logger.warning("Audit log failed (run_repair_queued): %s", _ae)

    get_worker_pool().submit(
        repair_run_id,
        execute_pipeline_background,
        config,
        repair_run_id,
        from_task,
        run_id,
    )

    return PipelineResponse(
        pipeline_name=config.pipeline_name,
        status="queued",
        message=f"Repair queued from task {from_task}",
        run_id=repair_run_id,
        execution_order=execution_order,
    )


@app.get("/run/{run_id}/status")
async def get_run_status_by_id(run_id: str, request: Request):
    """Return the latest status record for a specific run_id."""
    _require_permission(request, "read")
    record = get_run_by_id(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"No run found with id '{run_id}'")
    pool = get_worker_pool()
    record["is_running"] = pool.is_running(run_id)
    record["is_pending"] = pool.is_pending(run_id)
    return record


@app.get("/queue")
async def get_run_queue(
    status: Optional[str] = None,
    limit: int = 50,
    request: Request = None,
):
    """Return the persistent pipeline run queue.

    ?status=queued|running|completed|failed|cancelled filters by state.
    Useful for monitoring what is running or waiting.
    """
    if request is not None:
        _require_permission(request, "read")
    try:
        runs = get_queue_runs(status=status, limit=limit)
        pool = get_worker_pool()
        for r in runs:
            r["in_memory_running"] = pool.is_running(r["run_id"])
            r["in_memory_pending"] = pool.is_pending(r["run_id"])
        return {"runs": runs, "total": len(runs)}
    except Exception as e:
        logger.error("Failed to get queue: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Scheduling (editor+)
# ---------------------------------------------------------------------------

@app.post("/schedule", response_model=Dict[str, str])
async def schedule_pipeline(request_body: PipelineScheduleRequest, request: Request):
    _require_permission(request, "schedule")
    try:
        scheduler = get_scheduler()
        scheduler.start()
        if scheduler.schedule_pipeline(request_body.config_path, custom_schedule=request_body.schedule):
            return {"message": "Pipeline scheduled successfully"}
        raise HTTPException(status_code=400, detail="Failed to schedule pipeline")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to schedule pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/schedule/{pipeline_name}", response_model=Dict[str, str])
async def unschedule_pipeline(pipeline_name: str, request: Request):
    _require_permission(request, "schedule")
    try:
        scheduler = get_scheduler()
        if scheduler.unschedule_pipeline(pipeline_name):
            return {"message": f"Pipeline {pipeline_name} unscheduled successfully"}
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_name} not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unschedule pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scheduled", response_model=Dict[str, ScheduledPipelineInfo])
async def list_scheduled_pipelines():
    try:
        scheduler = get_scheduler()
        return scheduler.list_scheduled_pipelines()
    except Exception as e:
        logger.error(f"Failed to list scheduled pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Status & history (viewer+)
# ---------------------------------------------------------------------------

@app.get("/status")
async def get_pipeline_status(config_path: Optional[str] = None, pipeline_name: Optional[str] = None):
    try:
        if config_path:
            config = load_config(config_path)
            pipeline_name = config.pipeline_name
        if not pipeline_name:
            raise HTTPException(status_code=400, detail="config_path or pipeline_name is required")
        latest = get_latest_run(pipeline_name)
        if latest is None:
            raise HTTPException(status_code=404, detail=f"No runs found for pipeline '{pipeline_name}'")
        return latest
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to read pipeline status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/history/{pipeline_name}")
async def get_pipeline_history(pipeline_name: str, limit: int = 12):
    try:
        safe_limit = min(max(limit, 1), 50)
        runs = get_run_history(pipeline_name, limit=safe_limit)
        return {
            "pipeline_name": pipeline_name,
            "runs": runs,
            "total_runs": len(runs),
        }
    except Exception as e:
        logger.error(f"Failed to read pipeline history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# DAG (viewer+)
# ---------------------------------------------------------------------------

@app.get("/dag")
async def get_pipeline_dag(config_path: str):
    try:
        config = load_config(config_path)
        dag_builder = DAGBuilder(config.tasks)
        dag = dag_builder.build()
        return {
            "pipeline_name": config.pipeline_name,
            "nodes": list(dag.nodes()),
            "edges": [{"from": e[0], "to": e[1]} for e in dag.edges()],
            "execution_order": dag_builder.get_execution_order(),
        }
    except Exception as e:
        logger.error(f"Failed to get DAG: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------------
# Dashboard summary (viewer+)
# ---------------------------------------------------------------------------

@app.get("/dashboard-summary")
async def dashboard_summary():
    try:
        pipelines, failed_pipelines, _, _ = _discover_pipeline_files()
        scheduler = get_scheduler()
        scheduled = scheduler.list_scheduled_pipelines()

        pipeline_summaries: List[Dict[str, Any]] = []
        recent_runs: List[Dict[str, Any]] = []
        active_runs: List[Dict[str, Any]] = []

        for pipeline in pipelines:
            pipeline_name = pipeline["display_name"]
            latest_status = get_latest_run(pipeline_name)
            history = get_run_history(pipeline_name, limit=5)

            for run in history:
                recent_runs.append({
                    "run_id": run.get("run_id"),
                    "pipeline_name": pipeline_name,
                    "status": run.get("status"),
                    "message": run.get("message"),
                    "updated_at": run.get("updated_at"),
                    "details": run.get("details", {}),
                })

            schedule_info = scheduled.get(pipeline_name)
            health = latest_status.get("status", "idle") if latest_status else "idle"

            summary = {
                **pipeline,
                "last_status": latest_status.get("status") if latest_status else "never_run",
                "last_message": latest_status.get("message") if latest_status else "No runs yet",
                "last_updated_at": latest_status.get("updated_at") if latest_status else None,
                "last_run_id": latest_status.get("run_id") if latest_status else None,
                "run_count": len(history),
                "is_scheduled": schedule_info is not None,
                "next_run": schedule_info.get("next_run") if schedule_info else None,
                "schedule": schedule_info.get("schedule") if schedule_info else None,
                "health": health,
            }
            pipeline_summaries.append(summary)

            if health in {"started", "running"}:
                active_runs.append(summary)

        recent_runs.sort(key=lambda x: x.get("updated_at") or "", reverse=True)

        return {
            "stats": {
                "total_pipelines": len(pipelines),
                "failed_configs": len(failed_pipelines),
                "scheduled_pipelines": len(scheduled),
                "active_runs": len(active_runs),
                "successful_pipelines": sum(
                    1 for s in pipeline_summaries if s["last_status"] == "completed"
                ),
                "failed_pipelines": sum(
                    1 for s in pipeline_summaries if s["last_status"] == "failed"
                ),
            },
            "active_runs": active_runs,
            "recent_runs": recent_runs[:12],
            "pipeline_summaries": sorted(
                pipeline_summaries,
                key=lambda s: (
                    0 if s["last_status"] in {"running", "started"} else
                    1 if s["last_status"] == "failed" else
                    2 if s["last_status"] == "completed" else
                    3,
                    s["display_name"].lower(),
                ),
            ),
            "failed_pipelines": failed_pipelines,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        logger.error(f"Failed to build dashboard summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Admin — user management (admin only)
# ---------------------------------------------------------------------------

@app.get("/admin/users", response_model=List[UserResponse])
async def list_users_endpoint(request: Request):
    _require_permission(request, "*")
    return get_all_users()


@app.post("/admin/users", response_model=Dict[str, str], status_code=201)
async def create_user_endpoint(request_body: UserCreateRequest, request: Request):
    _require_permission(request, "*")
    if request_body.role not in ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role '{request_body.role}'. Must be one of: {', '.join(sorted(ROLES))}",
        )
    try:
        ok = create_user(
            request_body.username,
            request_body.password,
            request_body.role,
            request_body.team,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not ok:
        raise HTTPException(status_code=409, detail=f"User '{request_body.username}' already exists")
    try:
        append_audit_event("user_mgmt", "user_created", actor=_get_request_username(request), resource=request_body.username, details={"role": request_body.role})
    except Exception as _ae:
        logger.warning("Audit log failed (user_created): %s", _ae)
    return {"message": f"User '{request_body.username}' created with role '{request_body.role}'"}


@app.patch("/admin/users/{username}/role", response_model=Dict[str, str])
async def update_role_endpoint(username: str, request_body: UserRoleUpdateRequest, request: Request):
    _require_permission(request, "*")
    try:
        ok = update_user_role(username, request_body.role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not ok:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")
    try:
        append_audit_event("user_mgmt", "role_changed", actor=_get_request_username(request), resource=username, details={"new_role": request_body.role})
    except Exception as _ae:
        logger.warning("Audit log failed (role_changed): %s", _ae)
    return {"message": f"User '{username}' role updated to '{request_body.role}'"}


@app.delete("/admin/users/{username}", response_model=Dict[str, str])
async def delete_user_endpoint(username: str, request: Request):
    _require_permission(request, "*")
    try:
        ok = delete_user(username)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not ok:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")
    try:
        append_audit_event("user_mgmt", "user_deleted", actor=_get_request_username(request), resource=username)
    except Exception as _ae:
        logger.warning("Audit log failed (user_deleted): %s", _ae)
    return {"message": f"User '{username}' deleted"}


@app.get("/audit-log")
async def get_audit_log_endpoint(
    request: Request,
    limit: int = 100,
    actor: Optional[str] = None,
    resource: Optional[str] = None,
    event_type: Optional[str] = None,
):
    """Return recent audit log entries, newest first. Admin only."""
    _require_permission(request, "*")
    try:
        return {"events": get_audit_log(limit=limit, actor=actor, resource=resource, event_type=event_type)}
    except Exception as e:
        logger.error("Failed to fetch audit log: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/me")
async def get_current_user(request: Request):
    """Return info about the currently authenticated user."""
    user = _get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"username": user.get("username"), "role": user.get("role"), "team": user.get("team")}


# ---------------------------------------------------------------------------
# Error handler dispatch (P0.2)
# ---------------------------------------------------------------------------

def _dispatch_error_handlers(
    config: PipelineConfig,
    run_id: str,
    error_message: str,
    errors: Optional[Dict] = None,
) -> None:
    """Fire configured error_handlers on pipeline failure."""
    if not config.error_handlers:
        return

    from dataplatform.core.alerts import send_webhook, send_email

    for handler in config.error_handlers:
        h_type = handler.get("type", "")
        on = handler.get("on", "failure")
        # 'on' can be "failure", "always", or a list of event names
        triggers = [on] if isinstance(on, str) else on
        if "failure" not in triggers and "always" not in triggers:
            continue

        subject = f"[DataPlatform] Pipeline failed: {config.pipeline_name}"
        body = (
            f"Pipeline '{config.pipeline_name}' failed.\n\n"
            f"Run ID : {run_id}\n"
            f"Error  : {error_message}\n"
        )
        if errors:
            body += f"\nTask errors:\n" + "\n".join(
                f"  {k}: {v}" for k, v in errors.items()
            )

        payload = {
            "alert": "pipeline_failure",
            "pipeline": config.pipeline_name,
            "run_id": run_id,
            "error": error_message,
            "task_errors": errors or {},
        }

        try:
            if h_type == "webhook" and handler.get("webhook_url"):
                send_webhook(handler["webhook_url"], payload)
            elif h_type == "email" and handler.get("email"):
                send_email(handler["email"], subject, body)
            else:
                logger.warning(
                    "error_handler type '%s' is unknown or missing url/email — skipping", h_type
                )
        except Exception as exc:
            logger.error("error_handler dispatch failed (type=%s): %s", h_type, exc)


# ---------------------------------------------------------------------------
# Background execution helper
# ---------------------------------------------------------------------------

def execute_pipeline_background(
    config: PipelineConfig,
    run_id: str,
    repair_from_task: Optional[str] = None,
    parent_run_id: Optional[str] = None,
    runtime_parameters: Optional[Dict[str, Any]] = None,
    environment_profile: str = "local",
) -> None:
    """Run a pipeline synchronously inside a worker thread."""
    # ------------------------------------------------------------------
    # Per-run log file setup
    # ------------------------------------------------------------------
    _run_log_dir = Path("logs/runs")
    _run_log_dir.mkdir(parents=True, exist_ok=True)
    _run_log_path = _run_log_dir / f"{run_id}.log"

    _run_handler = logging.FileHandler(str(_run_log_path), encoding="utf-8")
    _run_handler.setLevel(logging.DEBUG)
    _run_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    )

    _watched_loggers = [
        logging.getLogger("dataplatform"),
        logging.getLogger("dataplatform.core"),
        logging.getLogger("dataplatform.plugins"),
    ]
    for _lg in _watched_loggers:
        _lg.addHandler(_run_handler)

    dag_builder = DAGBuilder(config.tasks)
    dag_builder.build()
    execution_waves = (
        _repair_execution_waves(config, repair_from_task)
        if repair_from_task else dag_builder.get_execution_waves()
    )
    execution_order = [t for wave in execution_waves for t in wave]
    task_runs: List[Dict[str, Any]] = _pending_task_runs(config, execution_order)
    repair_context = None
    if repair_from_task:
        repair_context = {
            "parent_run_id": parent_run_id,
            "from_task": repair_from_task,
        }
    run_context = _run_detail_context(
        execution_order=execution_order,
        task_runs=task_runs,
        parameters=runtime_parameters,
        environment_profile=environment_profile,
        repair=repair_context,
    )

    try:
        set_run_status_in_queue(run_id, "running")
        update_pipeline_status(
            config.pipeline_name,
            "running",
            "Repair run is running" if repair_from_task else "Pipeline is running",
            run_context,
            run_id=run_id,
        )

        t0 = time.time()
        executor = PipelineExecutor()
        tasks_by_name = {task.name: task for task in config.tasks}
        success, results, errors, task_runs = _execute_parallel_with_task_runs(
            executor,
            tasks=tasks_by_name,
            execution_waves=execution_waves,
            config=_runtime_context(
                config=config,
                parameters=runtime_parameters,
                environment_profile=environment_profile,
            ),
            pipeline_name=config.pipeline_name,
            run_id=run_id,
        )
        duration = time.time() - t0

        sla_violated = False
        if config.sla:
            sla_violated = check_sla_and_alert(config.pipeline_name, run_id, duration, config.sla)

        try:
            record_run_cost(run_id, config.pipeline_name, getattr(config, "team", None),
                            len(config.tasks), duration)
        except Exception as _cost_exc:
            logger.warning("Cost recording skipped: %s", _cost_exc)

        status = "completed" if success else "failed"
        set_run_status_in_queue(
            run_id, status,
            error="; ".join(f"{k}: {v}" for k, v in errors.items()) if errors and not success else None,
        )
        update_pipeline_status(
            config.pipeline_name,
            status,
            f"Repair run {status}" if repair_from_task else f"Pipeline {status}",
            {"execution_order": execution_order, "success": success, "results": results,
             "errors": errors, "task_runs": task_runs,
             "runtime_parameters": runtime_parameters or {},
             "environment_profile": _normalize_environment_profile(environment_profile)["id"],
             "environment": _normalize_environment_profile(environment_profile),
             "duration_seconds": round(duration, 2), "sla_violated": sla_violated,
             **({"repair": {"parent_run_id": parent_run_id, "from_task": repair_from_task}} if repair_from_task else {})},
            run_id=run_id,
        )
        if not success:
            error_msg = "; ".join(f"{k}: {v}" for k, v in errors.items()) if errors else "Pipeline failed"
            _dispatch_error_handlers(config, run_id, error_msg, errors)
        if success:
            get_trigger_manager().notify_pipeline_completed(config.pipeline_name, run_id)
        logger.info("Pipeline %s %s (run_id=%s)", config.pipeline_name, status, run_id)

    except Exception as e:
        set_run_status_in_queue(run_id, "failed", error=str(e))
        update_pipeline_status(
            config.pipeline_name,
            "failed",
            f"Repair run failed: {e}" if repair_from_task else f"Pipeline failed: {e}",
            {"execution_order": execution_order, "task_runs": task_runs,
             "runtime_parameters": runtime_parameters or {},
             "environment_profile": _normalize_environment_profile(environment_profile)["id"],
             "environment": _normalize_environment_profile(environment_profile),
             **({"repair": {"parent_run_id": parent_run_id, "from_task": repair_from_task}} if repair_from_task else {})},
            run_id=run_id,
        )
        _dispatch_error_handlers(config, run_id, str(e))
        logger.error("Pipeline %s failed (run_id=%s): %s", config.pipeline_name, run_id, e, exc_info=True)

    finally:
        # Remove and close the per-run log handler
        for _lg in _watched_loggers:
            _lg.removeHandler(_run_handler)
        _run_handler.flush()
        _run_handler.close()


# ---------------------------------------------------------------------------
# Observability — SSE log streaming
# ---------------------------------------------------------------------------

async def _generate_run_log_sse(run_id: str):
    """Async generator that yields SSE frames for a pipeline run's log file."""
    log_path = Path("logs/runs") / f"{run_id}.log"

    position = 0
    no_new_content_ticks = 0  # each tick = 0.5 s; 4 ticks = 2 s

    while True:
        # Read any new lines written since last check
        try:
            with open(str(log_path), "r", encoding="utf-8", errors="replace") as fh:
                fh.seek(position)
                chunk = fh.read()
                position = fh.tell()
        except OSError:
            break

        if chunk:
            no_new_content_ticks = 0
            for line in chunk.splitlines():
                line = line.strip()
                if line:
                    yield f"data: {line}\n\n"
        else:
            no_new_content_ticks += 1

        # Check termination: run finished AND 2 s of silence
        run_record = get_run_by_id(run_id)
        terminal_statuses = {"completed", "failed", "cancelled"}
        if run_record and run_record.get("status") in terminal_statuses:
            if no_new_content_ticks >= 4:
                break

        await asyncio.sleep(0.5)

    yield "data: [STREAM_END]\n\n"


@app.get("/run/{run_id}/logs/stream")
async def stream_run_logs(run_id: str, request: Request):
    """Stream log lines for a pipeline run as Server-Sent Events (SSE).

    The client should consume this as an EventSource or fetch with streaming.
    The stream closes automatically once the run reaches a terminal state and
    all buffered output has been flushed.
    """
    _require_permission(request, "read")

    log_path = Path("logs/runs") / f"{run_id}.log"
    if not log_path.exists():
        raise HTTPException(status_code=404, detail=f"No log file found for run '{run_id}'")

    return StreamingResponse(
        _generate_run_log_sse(run_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Dashboard SSE — real-time run-status push
# ---------------------------------------------------------------------------

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


async def _dashboard_event_generator(request: Request):
    """Push run-status changes to the browser without polling from the client.

    Protocol:
      event: snapshot        — sent once on connect; full queue state as JSON array
      event: queue_update    — sent when any run changes status; array of changed runs
      event: stats_update    — sent alongside queue_update; current summary counts
      event: heartbeat       — sent every 30 s to keep the connection alive
    """
    import json as _json
    loop = asyncio.get_event_loop()

    def _fetch_queue():
        return get_queue_runs(limit=100)

    def _make_stats(runs: list) -> dict:
        statuses = [r["status"] for r in runs]
        return {
            "active":    statuses.count("running") + statuses.count("queued"),
            "running":   statuses.count("running"),
            "queued":    statuses.count("queued"),
            "completed": statuses.count("completed"),
            "failed":    statuses.count("failed"),
        }

    # Initial snapshot
    runs = await loop.run_in_executor(None, _fetch_queue)
    last_states: dict[str, str] = {r["run_id"]: r["status"] for r in runs}
    yield f"event: snapshot\ndata: {_json.dumps(runs)}\n\n"

    tick = 0
    while True:
        if await request.is_disconnected():
            break
        await asyncio.sleep(2)
        tick += 1

        # Heartbeat every 30 s (15 × 2 s ticks)
        if tick % 15 == 0:
            yield "event: heartbeat\ndata: {}\n\n"

        runs = await loop.run_in_executor(None, _fetch_queue)
        current_states: dict[str, str] = {r["run_id"]: r["status"] for r in runs}

        changed = [r for r in runs if last_states.get(r["run_id"]) != r["status"]]
        if changed:
            last_states = current_states
            yield f"event: queue_update\ndata: {_json.dumps(changed)}\n\n"
            yield f"event: stats_update\ndata: {_json.dumps(_make_stats(runs))}\n\n"


@app.get("/events/stream")
async def dashboard_events_stream(request: Request):
    """Real-time run-status push stream for dashboard pages."""
    return StreamingResponse(
        _dashboard_event_generator(request),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


# ---------------------------------------------------------------------------
# Observability — lineage
# ---------------------------------------------------------------------------

@app.get("/lineage")
async def get_lineage_graph():
    """Return the full data lineage graph (all pipelines, all assets)."""
    try:
        return build_lineage_graph()
    except Exception as e:
        logger.error(f"Failed to build lineage graph: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/lineage/asset")
async def get_asset_lineage_endpoint(uri: str):
    """Return upstream producers and downstream consumers for a single asset URI."""
    try:
        return get_asset_lineage(uri)
    except Exception as e:
        logger.error(f"Failed to get asset lineage for {uri}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Observability — quality
# ---------------------------------------------------------------------------

@app.get("/quality/{pipeline_name}")
async def get_quality_results(pipeline_name: str, limit: int = 50):
    """Return recent quality check results for a pipeline."""
    try:
        results = get_pipeline_quality_history(pipeline_name, limit=limit)
        return {
            "pipeline_name": pipeline_name,
            "results": results,
            "total": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "failed": sum(1 for r in results if not r["passed"]),
        }
    except Exception as e:
        logger.error(f"Failed to get quality results for {pipeline_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Observability — SLA violations
# ---------------------------------------------------------------------------

@app.get("/sla/violations")
async def get_sla_violations_endpoint(pipeline_name: Optional[str] = None, limit: int = 20):
    """Return recent SLA violations, optionally filtered to a pipeline."""
    try:
        violations = get_sla_violations(pipeline_name=pipeline_name, limit=limit)
        return {"violations": violations, "total": len(violations)}
    except Exception as e:
        logger.error(f"Failed to get SLA violations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Observability — Prometheus metrics (public, no auth)
# ---------------------------------------------------------------------------

@app.get("/metrics")
async def prometheus_metrics():
    """Expose Prometheus-format metrics scraped from the metadata DB.

    This endpoint is intentionally public (no auth required) so that
    Prometheus / Grafana can scrape it without needing a session cookie.
    """
    try:
        text = generate_prometheus_text()
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(content=text, media_type=_METRICS_CONTENT_TYPE)
    except Exception as e:
        logger.error(f"Failed to generate metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/timeseries")
async def metrics_timeseries(range: str = "24h"):
    """Return bucketed time-series data for monitoring charts."""
    range_map = {"1h": 1, "6h": 6, "24h": 24, "7d": 168}
    hours = range_map.get(range, 24)
    try:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: get_run_timeseries(range_hours=hours))
        return data
    except Exception as e:
        logger.error(f"Failed to get timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Phase 3 — request / response models
# ---------------------------------------------------------------------------

class TriggerRegisterRequest(BaseModel):
    trigger_type: str                               # "file_sensor" | "pipeline_completion"
    pipeline_name: str                              # downstream pipeline to run
    config_path: str                                # YAML path of downstream pipeline
    watch_path: Optional[str] = None                # file_sensor only
    upstream_pipeline: Optional[str] = None         # pipeline_completion only
    poll_interval_seconds: int = 30


class TriggerResponse(BaseModel):
    trigger_id: str
    trigger_type: str
    pipeline_name: str
    config_path: str
    enabled: bool
    created_at: str
    last_fired_at: Optional[str] = None


class WebhookFireResponse(BaseModel):
    message: str
    pipeline_name: str
    run_id: str


class VersionMetadata(BaseModel):
    version_id: str
    pipeline_name: str
    version_hash: str
    saved_by: Optional[str] = None
    saved_at: str


class MetricComputeResponse(BaseModel):
    metric_name: str
    value: Optional[float] = None
    error: Optional[str] = None
    computed_at: str


# ---------------------------------------------------------------------------
# Phase 3 — event-driven triggers
# ---------------------------------------------------------------------------

@app.post("/triggers", response_model=TriggerResponse, status_code=201)
async def register_trigger(request_body: TriggerRegisterRequest, request: Request):
    """Register an event-driven trigger. Requires editor+ role."""
    _require_permission(request, "schedule")

    valid_types = {"file_sensor", "pipeline_completion"}
    if request_body.trigger_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid trigger_type. Must be one of: {sorted(valid_types)}",
        )
    if request_body.trigger_type == "file_sensor" and not request_body.watch_path:
        raise HTTPException(status_code=400, detail="file_sensor trigger requires watch_path")
    if request_body.trigger_type == "pipeline_completion" and not request_body.upstream_pipeline:
        raise HTTPException(
            status_code=400, detail="pipeline_completion trigger requires upstream_pipeline"
        )

    trigger_id = str(_uuid.uuid4())
    trigger_config: Dict[str, Any] = {
        "watch_path": request_body.watch_path,
        "upstream_pipeline": request_body.upstream_pipeline,
        "poll_interval_seconds": request_body.poll_interval_seconds,
    }

    ok = db_save_trigger(
        trigger_id,
        request_body.trigger_type,
        request_body.pipeline_name,
        request_body.config_path,
        trigger_config,
    )
    if not ok:
        raise HTTPException(status_code=409, detail="Trigger ID conflict — try again")

    tm = get_trigger_manager()
    from dataplatform.core.triggers import _make_pipeline_runner
    if request_body.trigger_type == "file_sensor":
        tm.register_file_sensor(
            trigger_id,
            request_body.watch_path,
            _make_pipeline_runner(request_body.config_path, trigger_id),
            request_body.poll_interval_seconds,
        )
    elif request_body.trigger_type == "pipeline_completion":
        tm.register_completion_trigger(
            trigger_id,
            request_body.upstream_pipeline,
            lambda pname, rid, _cp=request_body.config_path, _tid=trigger_id: _make_pipeline_runner(_cp, _tid)(),
        )

    now = datetime.utcnow().isoformat() + "Z"
    return TriggerResponse(
        trigger_id=trigger_id,
        trigger_type=request_body.trigger_type,
        pipeline_name=request_body.pipeline_name,
        config_path=request_body.config_path,
        enabled=True,
        created_at=now,
        last_fired_at=None,
    )


@app.get("/triggers", response_model=List[TriggerResponse])
async def list_triggers(request: Request):
    """List all registered triggers. Requires viewer+ role."""
    _require_permission(request, "read")
    rows = db_get_triggers()
    return [
        TriggerResponse(
            trigger_id=r["trigger_id"],
            trigger_type=r["trigger_type"],
            pipeline_name=r["pipeline_name"],
            config_path=r["config_path"],
            enabled=bool(r["enabled"]),
            created_at=r["created_at"],
            last_fired_at=r.get("last_fired_at"),
        )
        for r in rows
    ]


@app.delete("/triggers/{trigger_id}", response_model=Dict[str, str])
async def delete_trigger_endpoint(trigger_id: str, request: Request):
    """Remove a trigger. Requires editor+ role."""
    _require_permission(request, "schedule")
    get_trigger_manager().unregister(trigger_id)
    if not db_delete_trigger(trigger_id):
        raise HTTPException(status_code=404, detail=f"Trigger '{trigger_id}' not found")
    return {"message": f"Trigger '{trigger_id}' removed"}


@app.post("/triggers/webhook/{pipeline_name}", response_model=WebhookFireResponse)
async def fire_webhook_trigger(pipeline_name: str, config_path: str, request: Request):
    """Immediately fire a pipeline via webhook. Requires editor+ role."""
    _require_permission(request, "run")
    run_id = str(_uuid.uuid4())
    from dataplatform.core.triggers import _make_pipeline_runner
    runner = _make_pipeline_runner(config_path, f"webhook-{run_id}")
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, runner)
    return WebhookFireResponse(
        message="Pipeline queued via webhook trigger",
        pipeline_name=pipeline_name,
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Phase 3 — pipeline versioning
# ---------------------------------------------------------------------------

@app.get("/versions/{pipeline_name}", response_model=List[VersionMetadata])
async def list_pipeline_versions(pipeline_name: str, limit: int = 20, request: Request = None):
    """List version history for a pipeline (newest first)."""
    try:
        return list_versions(pipeline_name, limit=limit)
    except Exception as exc:
        logger.error("Failed to list versions for '%s': %s", pipeline_name, exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/versions/{pipeline_name}/{version_id}")
async def get_pipeline_version(pipeline_name: str, version_id: str):
    """Return the raw YAML content for a specific version."""
    content = get_version_content(pipeline_name, version_id)
    if content is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version '{version_id}' not found for pipeline '{pipeline_name}'",
        )
    return {"pipeline_name": pipeline_name, "version_id": version_id, "content": content}


@app.get("/versions/{pipeline_name}/{version_id_a}/diff/{version_id_b}")
async def diff_pipeline_versions_endpoint(pipeline_name: str, version_id_a: str, version_id_b: str):
    """Return a unified diff between two versions of a pipeline."""
    result = diff_versions(pipeline_name, version_id_a, version_id_b)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="One or both versions not found",
        )
    return {
        "pipeline_name": pipeline_name,
        "version_id_a": version_id_a,
        "version_id_b": version_id_b,
        "diff": result,
    }


@app.post("/versions/{pipeline_name}/{version_id}/restore")
async def restore_pipeline_version_endpoint(
    pipeline_name: str,
    version_id: str,
    request_body: VersionRestoreRequest,
    request: Request,
):
    """Restore a saved YAML version onto the active pipeline file."""
    user = _require_permission(request, "save")
    content = get_version_content(pipeline_name, version_id)
    if content is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version '{version_id}' not found for pipeline '{pipeline_name}'",
        )

    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        restored_config = load_config(str(tmp_path))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Saved version is invalid: {exc}")
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    if restored_config.pipeline_name != pipeline_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Saved version belongs to "
                f"'{restored_config.pipeline_name}', not '{pipeline_name}'"
            ),
        )

    target_path: Optional[Path] = None
    if request_body.config_path:
        candidate = Path(request_body.config_path)
        if not candidate.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Config file not found: {request_body.config_path}",
            )
        target_path = candidate
    else:
        pipelines, _, _, _ = _discover_pipeline_files()
        for pipeline in pipelines:
            if pipeline.get("display_name") == pipeline_name:
                target_path = Path(pipeline["file_path"])
                break

    if target_path is None:
        raise HTTPException(
            status_code=404,
            detail=f"Could not locate active config for pipeline '{pipeline_name}'",
        )
    if target_path.suffix.lower() not in {".yaml", ".yml"}:
        raise HTTPException(status_code=400, detail="Target config must be a YAML file")

    target_path.write_text(content, encoding="utf-8")
    validated_config = load_config(str(target_path))
    if validated_config.pipeline_name != pipeline_name:
        raise HTTPException(
            status_code=400,
            detail="Restored file did not validate as the requested pipeline",
        )

    try:
        save_version(pipeline_name, content, saved_by=user.get("username"))
        append_audit_event(
            "pipeline",
            "version_restore",
            actor=user.get("username"),
            resource=pipeline_name,
            details={"version_id": version_id, "config_path": str(target_path)},
        )
    except Exception as exc:
        logger.warning("Version restore audit/snapshot failed for '%s': %s", pipeline_name, exc)

    return {
        "pipeline_name": pipeline_name,
        "version_id": version_id,
        "config_path": str(target_path),
        "message": "Pipeline version restored successfully",
    }


# ---------------------------------------------------------------------------
# Phase 3 — semantic metrics layer
# ---------------------------------------------------------------------------

@app.get("/metrics/definitions")
async def list_metric_definitions_endpoint():
    """List all named metric definitions from the metrics/ directory.

    This endpoint is intentionally public so dashboards can enumerate metrics
    without authentication.
    """
    try:
        return {"metrics": list_metric_definitions()}
    except Exception as exc:
        logger.error("Failed to list metric definitions: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/metrics/{metric_name}/compute", response_model=MetricComputeResponse)
async def compute_metric_endpoint(metric_name: str, request: Request):
    """Compute a named metric on demand. Requires editor+ role."""
    _require_permission(request, "run")

    # Locate the metric definition file
    from dataplatform.core.semantic_metrics import _METRICS_DIR
    metric_file = _METRICS_DIR / f"{metric_name}.yaml"
    if not metric_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Metric definition '{metric_name}.yaml' not found in metrics/ directory",
        )
    try:
        defn = load_metric(str(metric_file))
        result = compute_metric(defn["metric_name"], defn["sql"])
        return MetricComputeResponse(**result)
    except Exception as exc:
        logger.error("Failed to compute metric '%s': %s", metric_name, exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/metrics/{metric_name}/history")
async def get_metric_history_endpoint(metric_name: str, limit: int = 50):
    """Return recent computed values for a named metric."""
    try:
        history = get_metric_history(metric_name, limit=limit)
        return {
            "metric_name": metric_name,
            "history": history,
            "total": len(history),
        }
    except Exception as exc:
        logger.error("Failed to get history for metric '%s': %s", metric_name, exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Phase 4 — page routes
# ---------------------------------------------------------------------------

@app.get("/catalog")
async def catalog_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "catalog.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Catalog page not found")


@app.get("/lineage-viz")
async def lineage_viz_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "lineage.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Lineage page not found")


@app.get("/costs")
async def costs_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "costs.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Costs page not found")


@app.get("/templates-ui")
async def templates_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "templates.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Templates page not found")


@app.get("/admin")
async def admin_page(request: Request):
    _require_permission(request, "*")
    page = Path(__file__).resolve().parent.parent / "static" / "admin.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Admin page not found")


@app.get("/alerts")
async def alerts_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "alerts.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Alerts page not found")


@app.get("/monitoring")
async def monitoring_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "monitoring.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Monitoring page not found")


@app.get("/job-builder")
async def job_builder_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "job_builder.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Job Builder page not found")


# ---------------------------------------------------------------------------
# Phase 4 — data catalog API
# ---------------------------------------------------------------------------

@app.get("/catalog/assets")
async def catalog_assets(q: Optional[str] = None, limit: int = 100):
    """Return all known data assets, optionally filtered by URI substring."""
    try:
        assets = search_assets(query=q, limit=limit)
        return {"assets": assets, "total": len(assets)}
    except Exception as exc:
        logger.error("Catalog assets failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/catalog/assets/detail")
async def catalog_asset_detail(uri: str):
    """Return full lineage detail for a single asset URI."""
    try:
        detail = get_asset_detail(uri)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Asset '{uri}' not found in catalog")
        return detail
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Asset detail failed for %s: %s", uri, exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/catalog/pipelines")
async def catalog_pipelines():
    """Return pipelines with their asset counts (derived from lineage records)."""
    try:
        return {"pipelines": get_pipeline_catalog()}
    except Exception as exc:
        logger.error("Pipeline catalog failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Phase 4 — cost attribution API
# ---------------------------------------------------------------------------

@app.get("/costs/summary")
async def costs_summary_endpoint(request: Request):
    """Return cost summary grouped by pipeline + team. Requires viewer+ role."""
    _require_permission(request, "read")
    try:
        return {
            "by_pipeline": get_cost_summary(),
            "by_team": get_team_cost_summary(),
        }
    except Exception as exc:
        logger.error("Cost summary failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/costs/{pipeline_name}")
async def pipeline_cost_history_endpoint(pipeline_name: str, limit: int = 20, request: Request = None):
    """Return per-run cost history for a pipeline."""
    try:
        history = get_pipeline_cost_history(pipeline_name, limit=limit)
        total = sum(r["estimated_cost_usd"] for r in history)
        return {
            "pipeline_name": pipeline_name,
            "history": history,
            "total_cost_usd": round(total, 6),
        }
    except Exception as exc:
        logger.error("Pipeline cost history failed for %s: %s", pipeline_name, exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Phase 4 — templates marketplace API
# ---------------------------------------------------------------------------

@app.get("/templates")
async def list_templates_endpoint():
    """List all available pipeline templates. Public endpoint."""
    try:
        return {"templates": list_templates()}
    except Exception as exc:
        logger.error("Template listing failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/templates/{template_id}")
async def get_template_endpoint(template_id: str):
    """Return the raw YAML content of a template."""
    content = get_template_content(template_id)
    if content is None:
        raise HTTPException(status_code=404, detail=f"Template '{template_id}' not found")
    return {"template_id": template_id, "content": content}


class UseTemplateRequest(BaseModel):
    new_pipeline_name: str


@app.post("/templates/{template_id}/use", response_model=PipelineSaveResponse)
async def use_template_endpoint(template_id: str, request_body: UseTemplateRequest, request: Request):
    """Instantiate a template as a new pipeline. Requires editor+ role."""
    _require_permission(request, "save")
    try:
        saved_path = use_template(template_id, request_body.new_pipeline_name)
        return PipelineSaveResponse(
            file_path=saved_path,
            pipeline_name=request_body.new_pipeline_name,
            message=f"Template '{template_id}' instantiated as '{request_body.new_pipeline_name}'",
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.error("Template use failed for '%s': %s", template_id, exc)
        raise HTTPException(status_code=400, detail=str(exc))


# ---------------------------------------------------------------------------
# Git integration — page
# ---------------------------------------------------------------------------

@app.get("/git-integration")
async def git_integration_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "git_integration.html"
    if page.exists(): return _page(page)
    raise HTTPException(status_code=404, detail="Git Integration page not found")


# ---------------------------------------------------------------------------
# Git integration — API
# ---------------------------------------------------------------------------

class GitRemoteCreate(BaseModel):
    name: str
    remote_url: str
    auth_type: str = "token"
    token: Optional[str] = None
    branch: str = "main"
    pipelines_path: str = "pipelines"


class GitPushRequest(BaseModel):
    pipeline_name: str
    commit_message: Optional[str] = None


@app.post("/git/remotes", status_code=201)
async def create_git_remote(body: GitRemoteCreate, request: Request):
    """Register a new Git remote. Requires editor+ role."""
    user = _require_permission(request, "save")
    try:
        remote_id = register_remote(
            name=body.name,
            remote_url=body.remote_url,
            auth_type=body.auth_type,
            token=body.token,
            branch=body.branch,
            pipelines_path=body.pipelines_path,
            created_by=user.get("username"),
        )
        return {"remote_id": remote_id, "name": body.name, "message": f"Remote '{body.name}' registered"}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.error("Create git remote failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/git/remotes")
async def list_git_remotes_endpoint(request: Request):
    """List all registered Git remotes (tokens masked). Requires viewer+ role."""
    _require_permission(request, "read")
    try:
        return {"remotes": list_remotes()}
    except Exception as exc:
        logger.error("List git remotes failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/git/remotes/{remote_id}")
async def delete_git_remote_endpoint(remote_id: str, request: Request):
    """Delete a Git remote and its local clone. Requires admin role."""
    _require_permission(request, "*")
    if not delete_remote(remote_id):
        raise HTTPException(status_code=404, detail="Remote not found")
    return {"message": "Remote deleted"}


@app.post("/git/remotes/{remote_id}/test")
async def test_git_remote(remote_id: str, request: Request):
    """Test connectivity to a Git remote."""
    _require_permission(request, "read")
    result = git_test_connection(remote_id)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error", "Connection failed"))
    return result


@app.post("/git/remotes/{remote_id}/push")
async def push_pipeline_to_git(remote_id: str, body: GitPushRequest, request: Request):
    """Push a pipeline YAML to the configured Git remote. Requires editor+ role."""
    user = _require_permission(request, "save")
    pipelines_dir = Path("pipelines")
    yaml_file = pipelines_dir / f"{body.pipeline_name}.yaml"
    if not yaml_file.exists():
        raise HTTPException(status_code=404, detail=f"Pipeline '{body.pipeline_name}' not found")
    yaml_content = yaml_file.read_text(encoding="utf-8")
    result = git_push_pipeline(
        remote_id=remote_id,
        pipeline_name=body.pipeline_name,
        yaml_content=yaml_content,
        commit_message=body.commit_message,
        pushed_by=user.get("username"),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Push failed"))
    return result


@app.post("/git/remotes/{remote_id}/pull")
async def pull_pipelines_from_git(remote_id: str, request: Request):
    """Pull all pipeline YAMLs from the Git remote into local pipelines/. Requires editor+ role."""
    user = _require_permission(request, "save")
    result = git_pull_pipelines(remote_id=remote_id, pulled_by=user.get("username"))
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Pull failed"))
    return result


@app.get("/git/remotes/{remote_id}/status")
async def git_remote_status(remote_id: str, request: Request):
    """Compare local pipelines/ with the remote repo. Requires viewer+ role."""
    _require_permission(request, "read")
    result = git_get_status(remote_id)
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error", "Status check failed"))
    return result


@app.get("/git/remotes/{remote_id}/log")
async def git_push_log_endpoint(remote_id: str, limit: int = 30, request: Request = None):
    """Return the push history for a remote."""
    if request:
        _require_permission(request, "read")
    return {"log": git_get_push_log(remote_id, limit=limit)}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
