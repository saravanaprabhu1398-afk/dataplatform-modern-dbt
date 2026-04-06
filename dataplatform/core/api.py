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
from dataplatform.core.executor import PipelineExecutor
from dataplatform.core.scheduler import get_scheduler
from dataplatform.core.logging_config import setup_logging
from dataplatform.core.pipeline_generator import (
    generate_pipeline_yaml_from_text,
    save_generated_pipeline,
)

# Set up logging from environment
logger = setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    json_format=os.getenv("LOG_JSON", "false").lower() in ("1", "true", "yes"),
    log_file=os.getenv("LOG_FILE", "logs/pipeline.log")
)

app = FastAPI(title="Data Platform API", version="0.1.0")

# Start scheduler on app startup
@app.on_event("startup")
async def startup_event():
    scheduler = get_scheduler()
    scheduler.start()
    logger.info("Pipeline scheduler started")

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

pipeline_statuses: Dict[str, List[Dict[str, Any]]] = {}


pipeline_runs: Dict[str, List[Dict[str, Any]]] = {}

SESSION_COOKIE_NAME = "dpflow_session"
SESSION_DURATION_SECONDS = 60 * 60 * 12
AUTH_USERNAME = os.getenv("DATAPLATFORM_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("DATAPLATFORM_PASSWORD", "admin")
SESSION_SECRET = os.getenv("DATAPLATFORM_SESSION_SECRET", "dpflow-dev-secret-change-me")
PUBLIC_PATH_PREFIXES = ("/login", "/static", "/health")
PUBLIC_PATHS = {"/login", "/health"}

if AUTH_USERNAME == "admin" and AUTH_PASSWORD == "admin":
    logger.warning("Using default DATAPLATFORM_USERNAME/DATAPLATFORM_PASSWORD credentials. Set environment variables before production use.")
if SESSION_SECRET == "dpflow-dev-secret-change-me":
    logger.warning("Using default DATAPLATFORM_SESSION_SECRET. Set a strong secret before production use.")


def _sign_value(value: str) -> str:
    return hmac.new(SESSION_SECRET.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def _create_session_cookie(username: str) -> str:
    payload = {
        "username": username,
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


def _is_authenticated(request: Request) -> bool:
    return _read_session_cookie(request.cookies.get(SESSION_COOKIE_NAME)) is not None


def _set_session_cookie(response: Response, username: str) -> None:
    response.set_cookie(
        SESSION_COOKIE_NAME,
        _create_session_cookie(username),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=SESSION_DURATION_SECONDS,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


def _is_browser_request(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept


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


def update_pipeline_status(pipeline_name: str, status: str, message: str, details: Optional[Dict[str, Any]] = None):
    """Update pipeline status and maintain history of runs."""
    if pipeline_name not in pipeline_runs:
        pipeline_runs[pipeline_name] = []

    current_time = datetime.utcnow()
    run_id = current_time.timestamp()

    # Create new status entry
    status_entry = {
        "run_id": run_id,
        "status": status,
        "message": message,
        "details": details or {},
        "updated_at": current_time.isoformat() + "Z"
    }

    # Find the current run (latest run_id) or create new run
    if pipeline_runs[pipeline_name]:
        current_run = pipeline_runs[pipeline_name][-1]
        if isinstance(current_run, list):
            # Old format - convert
            pipeline_runs[pipeline_name] = [{"run_id": run_id, "statuses": [current_run]}]
            current_run = pipeline_runs[pipeline_name][-1]
        else:
            current_run = pipeline_runs[pipeline_name][-1]

        # If this is a new run (started status), create new run entry
        if status == "started":
            pipeline_runs[pipeline_name].append({
                "run_id": run_id,
                "statuses": [status_entry]
            })
        else:
            # Add to current run
            if "statuses" not in current_run:
                current_run["statuses"] = [current_run.copy()]
            current_run["statuses"].append(status_entry)
    else:
        # First run
        pipeline_runs[pipeline_name].append({
            "run_id": run_id,
            "statuses": [status_entry]
        })

    # Keep only last 5 runs
    if len(pipeline_runs[pipeline_name]) > 5:
        pipeline_runs[pipeline_name] = pipeline_runs[pipeline_name][-5:]

    return status_entry
    return pipeline_statuses[pipeline_name]


class PipelineRunRequest(BaseModel):
    config_path: str


class PipelineScheduleRequest(BaseModel):
    config_path: str
    schedule: Optional[Dict[str, str]] = None  # Custom schedule: minute, hour, day, month, day_of_week


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


def _discover_pipeline_files() -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Path, Optional[Path]]:
    """Discover pipeline YAML files and return loaded/failed metadata."""
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
                "description": getattr(config, 'description', 'No description'),
                "file_path": str(yaml_file),
                "task_count": len(config.tasks),
                "status": "loaded",
                "error": None
            })
        except Exception as e:
            failed_pipelines.append({
                "name": yaml_file.name,
                "file_path": str(yaml_file),
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            })

    return pipelines, failed_pipelines, workspace_root, pipelines_dir if pipelines_dir.exists() else None


@app.get("/login")
async def login_page(request: Request):
    """Serve the login page."""
    if _is_authenticated(request):
        return RedirectResponse(url="/", status_code=303)

    api_file = Path(__file__).resolve()
    login_file = api_file.parent.parent / "static" / "login.html"
    if login_file.exists():
        return FileResponse(str(login_file), media_type="text/html")
    raise HTTPException(status_code=404, detail=f"Login page not found at {login_file}")


@app.post("/login")
async def login(request: LoginRequest):
    """Create an authenticated session."""
    if request.username != AUTH_USERNAME or request.password != AUTH_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    response = JSONResponse({"message": "Login successful"})
    _set_session_cookie(response, request.username)
    return response


@app.post("/logout")
async def logout():
    """Clear the current session."""
    response = JSONResponse({"message": "Logged out"})
    _clear_session_cookie(response)
    return response


@app.get("/")
async def root():
    """Serve the landing page."""
    api_file = Path(__file__).resolve()
    landing_page = api_file.parent.parent / "static" / "landing.html"
    logger.info(f"Looking for landing page at: {landing_page}")
    logger.info(f"File exists: {landing_page.exists()}")

    if landing_page.exists():
        logger.info(f"Serving landing page from: {landing_page}")
        return FileResponse(str(landing_page), media_type="text/html")
    else:
        logger.warning(f"Landing page not found at: {landing_page}")
        return {
            "message": "Data Platform API",
            "version": "0.1.0",
            "landing": "/",
            "dashboard": "/dashboard",
            "generator": "/generator",
            "static_path": str(landing_page)
        }


@app.get("/dashboard")
async def dashboard():
    """Serve the dashboard page."""
    api_file = Path(__file__).resolve()
    static_index = api_file.parent.parent / "static" / "index.html"
    
    if static_index.exists():
        return FileResponse(str(static_index), media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail=f"Dashboard not found at {static_index}")


@app.get("/generator")
async def generator_page():
    """Serve the pipeline generator page."""
    api_file = Path(__file__).resolve()
    generator_index = api_file.parent.parent / "static" / "generator.html"

    if generator_index.exists():
        return FileResponse(str(generator_index), media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail=f"Generator page not found at {generator_index}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "0.1.0"
    }


@app.get("/info")
async def get_info():
    """Get API and workspace information."""
    workspace_root = Path(__file__).parent.parent.parent
    return {
        "api_version": "0.1.0",
        "workspace_root": str(workspace_root),
        "workspace_exists": workspace_root.exists(),
        "static_dir": str(Path(__file__).parent.parent / "static"),
        "static_exists": (Path(__file__).parent.parent / "static").exists(),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }


@app.get("/pipeline-config")
async def get_pipeline_config(config_path: str):
    """Return raw and parsed pipeline configuration for preview."""
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


@app.post("/generate-pipeline", response_model=PipelineGenerationResponse)
async def generate_pipeline(request: PipelineGenerationRequest):
    """Generate pipeline YAML from free-form text."""
    try:
        return PipelineGenerationResponse(**generate_pipeline_yaml_from_text(request.input_text))
    except Exception as e:
        logger.error(f"Failed to generate pipeline YAML: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/save-pipeline", response_model=PipelineSaveResponse)
async def save_pipeline(request: PipelineSaveRequest):
    """Save generated pipeline YAML into the pipelines directory."""
    try:
        file_path = save_generated_pipeline(request.yaml_content, request.filename)
        config = load_config(file_path)
        return PipelineSaveResponse(
            file_path=file_path,
            pipeline_name=config.pipeline_name,
            message="Pipeline saved successfully"
        )
    except Exception as e:
        logger.error(f"Failed to save generated pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/run", response_model=PipelineResponse)
async def run_pipeline(request: PipelineRunRequest, background_tasks: BackgroundTasks):
    """Run a pipeline from config file."""
    try:
        config = load_config(request.config_path)

        # Build DAG
        dag_builder = DAGBuilder(config.tasks)
        dag = dag_builder.build()
        execution_order = dag_builder.get_execution_order()

        # Execute pipeline in background
        update_pipeline_status(
            config.pipeline_name,
            "started",
            "Pipeline execution started",
            {"execution_order": execution_order}
        )
        background_tasks.add_task(execute_pipeline_background, config, execution_order)

        return PipelineResponse(
            pipeline_name=config.pipeline_name,
            status="started",
            message="Pipeline execution started",
            execution_order=execution_order
        )

    except Exception as e:
        logger.error(f"Failed to start pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/pipelines")
async def list_pipelines():
    """List available pipeline configuration files from pipelines folder and root."""
    try:
        pipelines, failed_pipelines, workspace_root, pipelines_dir = _discover_pipeline_files()

        logger.info(f"Returning {len(pipelines)} pipelines, {len(failed_pipelines)} failed")
        return {
            "pipelines": pipelines,
            "failed_pipelines": failed_pipelines,
            "workspace_root": str(workspace_root),
            "pipelines_folder": str(pipelines_dir) if pipelines_dir.exists() else None
        }
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list pipelines: {str(e)}")


@app.get("/status")
async def get_pipeline_status(config_path: Optional[str] = None, pipeline_name: Optional[str] = None):
    """Get the latest pipeline status."""
    try:
        if config_path:
            config = load_config(config_path)
            pipeline_name = config.pipeline_name

        if not pipeline_name:
            raise HTTPException(status_code=400, detail="config_path or pipeline_name is required")

        runs = pipeline_runs.get(pipeline_name, [])
        if not runs:
            raise HTTPException(status_code=404, detail=f"No runs found for pipeline {pipeline_name}")

        # Get the latest run and its latest status
        latest_run = runs[-1]
        if "statuses" in latest_run and latest_run["statuses"]:
            return latest_run["statuses"][-1]
        else:
            # Fallback for old format
            return latest_run

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to read pipeline status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/history/{pipeline_name}")
async def get_pipeline_history(pipeline_name: str):
    """Get the historic runs for a pipeline (last 5 runs)."""
    try:
        runs = pipeline_runs.get(pipeline_name, [])

        # Convert runs to the expected format for frontend
        formatted_runs = []
        for run in runs:
            if "statuses" in run:
                # New format - get the final status of each run
                final_status = run["statuses"][-1]
                formatted_runs.append(final_status)
            else:
                # Old format fallback
                formatted_runs.append(run)

        return {
            "pipeline_name": pipeline_name,
            "runs": formatted_runs,
            "total_runs": len(formatted_runs)
        }
    except Exception as e:
        logger.error(f"Failed to read pipeline history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboard-summary")
async def dashboard_summary():
    """Return Airflow-style overview data for the dashboard."""
    try:
        pipelines, failed_pipelines, _, _ = _discover_pipeline_files()
        scheduler = get_scheduler()
        scheduled = scheduler.list_scheduled_pipelines()

        pipeline_summaries: List[Dict[str, Any]] = []
        recent_runs: List[Dict[str, Any]] = []
        active_runs: List[Dict[str, Any]] = []

        for pipeline in pipelines:
            pipeline_name = pipeline["display_name"]
            runs = pipeline_runs.get(pipeline_name, [])
            latest_status = None
            latest_run_count = len(runs)

            if runs:
                latest_run = runs[-1]
                if latest_run.get("statuses"):
                    latest_status = latest_run["statuses"][-1]

                for run in runs:
                    if run.get("statuses"):
                        final_status = run["statuses"][-1]
                        recent_runs.append({
                            "pipeline_name": pipeline_name,
                            "status": final_status.get("status"),
                            "message": final_status.get("message"),
                            "updated_at": final_status.get("updated_at"),
                            "details": final_status.get("details", {}),
                        })

            schedule_info = scheduled.get(pipeline_name)
            health = "idle"
            if latest_status:
                health = latest_status.get("status", "idle")

            summary = {
                **pipeline,
                "last_status": latest_status.get("status") if latest_status else "never_run",
                "last_message": latest_status.get("message") if latest_status else "No runs yet",
                "last_updated_at": latest_status.get("updated_at") if latest_status else None,
                "run_count": latest_run_count,
                "is_scheduled": schedule_info is not None,
                "next_run": schedule_info.get("next_run") if schedule_info else None,
                "schedule": schedule_info.get("schedule") if schedule_info else None,
                "health": health,
            }
            pipeline_summaries.append(summary)

            if latest_status and latest_status.get("status") in {"started", "running"}:
                active_runs.append(summary)

        recent_runs.sort(key=lambda item: item.get("updated_at") or "", reverse=True)

        return {
            "stats": {
                "total_pipelines": len(pipelines),
                "failed_configs": len(failed_pipelines),
                "scheduled_pipelines": len(scheduled),
                "active_runs": len(active_runs),
                "successful_pipelines": sum(1 for item in pipeline_summaries if item["last_status"] == "completed"),
                "failed_pipelines": sum(1 for item in pipeline_summaries if item["last_status"] == "failed"),
            },
            "active_runs": active_runs,
            "recent_runs": recent_runs[:12],
            "pipeline_summaries": sorted(
                pipeline_summaries,
                key=lambda item: (
                    0 if item["last_status"] in {"running", "started"} else
                    1 if item["last_status"] == "failed" else
                    2 if item["last_status"] == "completed" else
                    3
                ,
                    item["display_name"].lower(),
                )
            ),
            "failed_pipelines": failed_pipelines,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        logger.error(f"Failed to build dashboard summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run/sync", response_model=PipelineResponse)
async def run_pipeline_sync(request: PipelineRunRequest):
    """Run a pipeline synchronously."""
    try:
        config = load_config(request.config_path)

        # Build DAG
        dag_builder = DAGBuilder(config.tasks)
        dag = dag_builder.build()
        execution_order = dag_builder.get_execution_order()

        # Execute pipeline
        executor = PipelineExecutor()
        success, results, errors = executor.execute_pipeline(
            tasks={task.name: task for task in config.tasks},
            execution_order=execution_order,
            config={"file_path": config.file_path}
        )

        status_value = "completed" if success else "failed"
        message = "Pipeline executed successfully" if success else "Pipeline failed"
        update_pipeline_status(
            config.pipeline_name,
            status_value,
            message,
            {"execution_order": execution_order, "success": success}
        )

        results = {task_name: success for task_name in execution_order} if success else None

        return PipelineResponse(
            pipeline_name=config.pipeline_name,
            status=status_value,
            message=message,
            execution_order=execution_order,
            results=results
        )

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/schedule", response_model=Dict[str, str])
async def schedule_pipeline(request: PipelineScheduleRequest):
    """Schedule a pipeline for automatic execution with optional custom schedule."""
    try:
        scheduler = get_scheduler()
        scheduler.start()

        if scheduler.schedule_pipeline(request.config_path, custom_schedule=request.schedule):
            return {"message": "Pipeline scheduled successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to schedule pipeline")

    except Exception as e:
        logger.error(f"Failed to schedule pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/schedule/{pipeline_name}", response_model=Dict[str, str])
async def unschedule_pipeline(pipeline_name: str):
    """Remove a pipeline from the schedule."""
    try:
        scheduler = get_scheduler()
        if scheduler.unschedule_pipeline(pipeline_name):
            return {"message": f"Pipeline {pipeline_name} unscheduled successfully"}
        else:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_name} not found")

    except Exception as e:
        logger.error(f"Failed to unschedule pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scheduled", response_model=Dict[str, ScheduledPipelineInfo])
async def list_scheduled_pipelines():
    """List all scheduled pipelines."""
    try:
        scheduler = get_scheduler()
        return scheduler.list_scheduled_pipelines()

    except Exception as e:
        logger.error(f"Failed to list scheduled pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dag")
async def get_pipeline_dag(config_path: str):
    """Get the DAG structure for a pipeline."""
    try:
        config = load_config(config_path)

        dag_builder = DAGBuilder(config.tasks)
        dag = dag_builder.build()

        # Convert networkx graph to dict representation
        nodes = list(dag.nodes())
        edges = list(dag.edges())

        return {
            "pipeline_name": config.pipeline_name,
            "nodes": nodes,
            "edges": [{"from": edge[0], "to": edge[1]} for edge in edges],
            "execution_order": dag_builder.get_execution_order()
        }

    except Exception as e:
        logger.error(f"Failed to get DAG: {e}")
        raise HTTPException(status_code=400, detail=str(e))


async def execute_pipeline_background(config: PipelineConfig, execution_order: List[str]):
    """Execute pipeline in background."""
    try:
        update_pipeline_status(
            config.pipeline_name,
            "running",
            "Pipeline is running in background",
            {"execution_order": execution_order}
        )

        executor = PipelineExecutor()
        success, results, errors = executor.execute_pipeline(
            tasks={task.name: task for task in config.tasks},
            execution_order=execution_order,
            config={"file_path": config.file_path}
        )

        status = "completed" if success else "failed"
        update_pipeline_status(
            config.pipeline_name,
            status,
            f"Background pipeline {status}",
            {"execution_order": execution_order, "success": success, "results": results, "errors": errors}
        )
        logger.info(f"Background pipeline {config.pipeline_name} {status}")

    except Exception as e:
        update_pipeline_status(
            config.pipeline_name,
            "failed",
            f"Background pipeline failed: {e}",
            {"execution_order": execution_order}
        )
        logger.error(f"Background pipeline execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
