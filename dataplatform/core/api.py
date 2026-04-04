from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import uvicorn
import logging
import os
from dataplatform.core.config import load_config, PipelineConfig
from dataplatform.core.dag import DAGBuilder
from dataplatform.core.executor import PipelineExecutor
from dataplatform.core.scheduler import get_scheduler
from dataplatform.core.logging_config import setup_logging

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


@app.get("/")
async def root():
    """Serve the main dashboard or API root."""
    # Use resolve() to get absolute path
    api_file = Path(__file__).resolve()
    static_index = api_file.parent.parent / "static" / "index.html"
    logger.info(f"Looking for dashboard at: {static_index}")
    logger.info(f"File exists: {static_index.exists()}")
    
    if static_index.exists():
        logger.info(f"Serving dashboard from: {static_index}")
        return FileResponse(str(static_index), media_type="text/html")
    else:
        logger.warning(f"Dashboard file not found at: {static_index}")
        return {"message": "Data Platform API", "version": "0.1.0", "dashboard": "/dashboard", "static_path": str(static_index)}


@app.get("/dashboard")
async def dashboard():
    """Serve the dashboard page."""
    api_file = Path(__file__).resolve()
    static_index = api_file.parent.parent / "static" / "index.html"
    
    if static_index.exists():
        return FileResponse(str(static_index), media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail=f"Dashboard not found at {static_index}")


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
        pipelines = []
        failed_pipelines = []
        
        # Get the workspace root directory (go up from dataplatform/core to root)
        workspace_root = Path(__file__).parent.parent.parent
        logger.info(f"Scanning for pipelines in: {workspace_root}")
        
        # Look for YAML files in pipelines folder (preferred) and root directory
        pipelines_dir = workspace_root / "pipelines"
        search_dirs = []
        
        if pipelines_dir.exists():
            search_dirs.append(pipelines_dir)
            logger.info(f"Found pipelines folder: {pipelines_dir}")
        else:
            logger.warning(f"Pipelines folder not found at: {pipelines_dir}")
        
        # Also search root for backward compatibility
        search_dirs.append(workspace_root)
        
        # Collect all YAML files from search directories
        yaml_files_set = set()  # Use set to avoid duplicates
        for search_dir in search_dirs:
            yaml_files = list(search_dir.glob("*.yaml"))
            for f in yaml_files:
                # Skip if from root but also exists in pipelines folder
                if f.parent == workspace_root and (pipelines_dir / f.name).exists():
                    continue
                yaml_files_set.add(f)
        
        yaml_files = list(yaml_files_set)
        logger.info(f"Found {len(yaml_files)} YAML files: {[f.name for f in yaml_files]}")
        
        # Load and validate pipelines
        for yaml_file in sorted(yaml_files):
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
                logger.info(f"Loaded pipeline: {yaml_file.name}")
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Failed to load config {yaml_file}: {error_msg}")
                # Add to failed pipelines for display
                failed_pipelines.append({
                    "name": yaml_file.name,
                    "file_path": str(yaml_file),
                    "status": "error",
                    "error": error_msg,
                    "error_type": type(e).__name__
                })

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
        success = executor.execute_pipeline(
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