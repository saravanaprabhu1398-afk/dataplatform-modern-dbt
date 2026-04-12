from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import uvicorn
import logging
import os
import base64
import hashlib
import hmac
import json
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
    get_all_pipeline_names,
    get_sla_violations,
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
from dataplatform.core.database import (
    save_trigger as db_save_trigger,
    get_triggers as db_get_triggers,
    get_trigger as db_get_trigger,
    delete_trigger as db_delete_trigger,
    update_trigger_last_fired,
)
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


# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------

class PipelineRunRequest(BaseModel):
    config_path: str


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
    if login_file.exists():
        return FileResponse(str(login_file), media_type="text/html")
    raise HTTPException(status_code=404, detail=f"Login page not found at {login_file}")


@app.post("/login")
async def login(request: LoginRequest):
    user = verify_user(request.username, request.password)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid username or password")
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

@app.get("/")
async def root():
    landing_page = Path(__file__).resolve().parent.parent / "static" / "landing.html"
    if landing_page.exists():
        return FileResponse(str(landing_page), media_type="text/html")
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
        return FileResponse(str(static_index), media_type="text/html")
    raise HTTPException(status_code=404, detail=f"Dashboard not found at {static_index}")


@app.get("/generator")
async def generator_page():
    generator_index = Path(__file__).resolve().parent.parent / "static" / "generator.html"
    if generator_index.exists():
        return FileResponse(str(generator_index), media_type="text/html")
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

@app.post("/run", response_model=PipelineResponse)
async def run_pipeline(request_body: PipelineRunRequest, background_tasks: BackgroundTasks, request: Request):
    _require_permission(request, "run")
    try:
        config = load_config(request_body.config_path)
        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
        execution_order = dag_builder.get_execution_order()

        run_id = str(_uuid.uuid4())
        update_pipeline_status(
            config.pipeline_name,
            "started",
            "Pipeline execution started",
            {"execution_order": execution_order},
            run_id=run_id,
        )
        background_tasks.add_task(execute_pipeline_background, config, execution_order, run_id)

        return PipelineResponse(
            pipeline_name=config.pipeline_name,
            status="started",
            message="Pipeline execution started",
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
        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
        execution_waves = dag_builder.get_execution_waves()
        execution_order = [t for wave in execution_waves for t in wave]

        run_id = str(_uuid.uuid4())
        t0 = time.time()
        executor = PipelineExecutor()
        success, task_results, errors = executor.execute_pipeline_parallel(
            tasks={(task.id or task.name): task for task in config.tasks},
            execution_waves=execution_waves,
            config={"file_path": config.file_path},
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
            {"execution_order": execution_order, "success": success, "duration_seconds": round(duration, 2), "sla_violated": sla_violated},
            run_id=run_id,
        )

        if success:
            get_trigger_manager().notify_pipeline_completed(config.pipeline_name, run_id)

        results = {task_name: success for task_name in execution_order} if success else None
        return PipelineResponse(
            pipeline_name=config.pipeline_name,
            status=status_value,
            message=message,
            execution_order=execution_order,
            results=results,
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
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
async def get_pipeline_history(pipeline_name: str):
    try:
        runs = get_run_history(pipeline_name, limit=5)
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
    return {"message": f"User '{username}' deleted"}


@app.get("/me")
async def get_current_user(request: Request):
    """Return info about the currently authenticated user."""
    user = _get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"username": user.get("username"), "role": user.get("role"), "team": user.get("team")}


# ---------------------------------------------------------------------------
# Background execution helper
# ---------------------------------------------------------------------------

async def execute_pipeline_background(
    config: PipelineConfig, execution_order: List[str], run_id: str
):
    try:
        update_pipeline_status(
            config.pipeline_name,
            "running",
            "Pipeline is running in background",
            {"execution_order": execution_order},
            run_id=run_id,
        )

        dag_builder = DAGBuilder(config.tasks)
        dag_builder.build()
        execution_waves = dag_builder.get_execution_waves()

        t0 = time.time()
        executor = PipelineExecutor()
        success, results, errors = executor.execute_pipeline_parallel(
            tasks={(task.id or task.name): task for task in config.tasks},
            execution_waves=execution_waves,
            config={"file_path": config.file_path},
            pipeline_name=config.pipeline_name,
            run_id=run_id,
        )
        duration = time.time() - t0

        sla_violated = False
        if config.sla:
            sla_violated = check_sla_and_alert(config.pipeline_name, run_id, duration, config.sla)

        # Cost attribution
        try:
            record_run_cost(run_id, config.pipeline_name, getattr(config, "team", None),
                            len(config.tasks), duration)
        except Exception as _cost_exc:
            logger.warning("Cost recording skipped: %s", _cost_exc)

        status = "completed" if success else "failed"
        update_pipeline_status(
            config.pipeline_name,
            status,
            f"Background pipeline {status}",
            {"execution_order": execution_order, "success": success, "results": results,
             "errors": errors, "duration_seconds": round(duration, 2), "sla_violated": sla_violated},
            run_id=run_id,
        )
        if success:
            get_trigger_manager().notify_pipeline_completed(config.pipeline_name, run_id)
        logger.info(f"Background pipeline {config.pipeline_name} {status}")

    except Exception as e:
        update_pipeline_status(
            config.pipeline_name,
            "failed",
            f"Background pipeline failed: {e}",
            {"execution_order": execution_order},
            run_id=run_id,
        )
        logger.error(f"Background pipeline execution failed: {e}", exc_info=True)


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
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Catalog page not found")


@app.get("/lineage-viz")
async def lineage_viz_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "lineage.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Lineage page not found")


@app.get("/costs")
async def costs_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "costs.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Costs page not found")


@app.get("/templates-ui")
async def templates_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "templates.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Templates page not found")


@app.get("/admin")
async def admin_page(request: Request):
    _require_permission(request, "*")
    page = Path(__file__).resolve().parent.parent / "static" / "admin.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Admin page not found")


@app.get("/alerts")
async def alerts_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "alerts.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Alerts page not found")


@app.get("/monitoring")
async def monitoring_page(request: Request):
    page = Path(__file__).resolve().parent.parent / "static" / "monitoring.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="Monitoring page not found")


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
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
