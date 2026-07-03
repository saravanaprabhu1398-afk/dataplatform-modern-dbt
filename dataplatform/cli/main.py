import typer
import os
import shutil
import time
from pathlib import Path
from dataplatform.core.logging_config import setup_logging, log_pipeline_start, log_pipeline_success, log_pipeline_failure

# Set up enhanced logging
logger = setup_logging()

app = typer.Typer()


@app.command()
def init(project_name: str):
    """Initialize a new data platform project."""
    project_dir = Path(project_name)
    if project_dir.exists():
        typer.echo(f"Project directory {project_name} already exists!")
        raise typer.Exit(1)

    # Create project structure
    project_dir.mkdir()
    (project_dir / "data").mkdir()
    (project_dir / "logs").mkdir()
    (project_dir / "models").mkdir()  # for dbt

    # Copy template
    template_dir = Path(__file__).parent.parent / "templates"
    shutil.copy(template_dir / "pipeline.yaml", project_dir / "pipeline.yaml")

    typer.echo(f"Project {project_name} initialized successfully!")


@app.command()
def run(config_path: str):
    """Run a pipeline from config file."""
    from dataplatform.core.config import load_config
    from dataplatform.core.dag import DAGBuilder
    from dataplatform.core.executor import PipelineExecutor

    try:
        config = load_config(config_path)
        typer.echo(f"Loaded pipeline: {config.pipeline_name}")

        # Log pipeline start
        log_pipeline_start(config.pipeline_name, len(config.tasks))
        start_time = time.time()

        dag_builder = DAGBuilder(config.tasks)
        dag = dag_builder.build()
        execution_order = dag_builder.get_execution_order()

        typer.echo(f"Execution order: {execution_order}")

        executor = PipelineExecutor()
        success, _results, _errors = executor.execute_pipeline(
            tasks={task.name: task for task in config.tasks},
            execution_order=execution_order,
            config={"file_path": config.file_path}
        )

        duration = time.time() - start_time

        if success:
            log_pipeline_success(config.pipeline_name, duration)
            typer.echo("Pipeline executed successfully!")
        else:
            log_pipeline_failure(config.pipeline_name, "unknown", duration)
            typer.echo("Pipeline failed!", err=True)
            raise typer.Exit(1)

    except Exception as e:
        logger.error(f"Pipeline execution error: {e}", exc_info=True)
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def schedule(config_path: str):
    """Schedule a pipeline for automatic execution."""
    from dataplatform.core.scheduler import get_scheduler

    scheduler = get_scheduler()
    scheduler.start()  # Ensure scheduler is running

    if scheduler.schedule_pipeline(config_path):
        typer.echo(f"Pipeline scheduled successfully from: {config_path}")
    else:
        typer.echo("Failed to schedule pipeline", err=True)
        raise typer.Exit(1)


@app.command()
def unschedule(pipeline_name: str):
    """Remove a pipeline from the schedule."""
    from dataplatform.core.scheduler import get_scheduler

    scheduler = get_scheduler()
    if scheduler.unschedule_pipeline(pipeline_name):
        typer.echo(f"Pipeline {pipeline_name} unscheduled successfully")
    else:
        typer.echo(f"Failed to unschedule pipeline {pipeline_name}", err=True)
        raise typer.Exit(1)


@app.command()
def serve(host: str = "0.0.0.0", port: int = 8000):
    """Start the FastAPI server."""
    import uvicorn
    from dataplatform.core.api import app

    typer.echo(f"Starting API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)


@app.command()
def install(plugin_name: str):
    """Install a plugin from the marketplace."""
    from dataplatform.core.marketplace import get_registry

    registry = get_registry()
    if registry.install_plugin(plugin_name):
        typer.echo(f"Plugin {plugin_name} installed successfully")
    else:
        typer.echo(f"Failed to install plugin {plugin_name}", err=True)
        raise typer.Exit(1)


@app.command()
def list_plugins():
    """List available plugins in the marketplace."""
    from dataplatform.core.marketplace import get_registry

    registry = get_registry()
    plugins = registry.list_plugins()

    typer.echo("Available plugins:")
    for name, info in plugins.items():
        status = "✓" if registry.is_installed(name) else " "
        typer.echo(f"  {status} {name}: {info['description']} ({info['type']})")


if __name__ == "__main__":
    app()