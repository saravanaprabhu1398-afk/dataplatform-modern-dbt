from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import List, Literal, Optional, Dict, Any, Union
from pathlib import Path


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

class TaskLineage(BaseModel):
    """Declares what data assets a task reads from and writes to."""
    model_config = ConfigDict(extra="forbid")

    reads_from: Optional[List[str]] = None   # e.g. ["postgres://mydb/public.orders"]
    writes_to: Optional[List[str]] = None    # e.g. ["s3://bucket/orders.parquet"]


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

class QualityCheck(BaseModel):
    """A single SQL-based assertion run after a task completes."""
    model_config = ConfigDict(extra="forbid")

    name: str                               # human-readable label
    sql: str                                # DuckDB SQL returning a scalar value
    expect: Optional[Any] = None            # exact expected value
    expect_min: Optional[float] = None      # actual >= expect_min
    expect_max: Optional[float] = None      # actual <= expect_max


class TaskQuality(BaseModel):
    """Quality gate configuration attached to a task."""
    model_config = ConfigDict(extra="forbid")

    checks: Optional[List[QualityCheck]] = None


# ---------------------------------------------------------------------------
# SLA and alerting
# ---------------------------------------------------------------------------

class AlertConfig(BaseModel):
    """Where to send an alert when an SLA is violated."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["email", "webhook"]
    email: Optional[str] = None          # required when type == "email"
    webhook_url: Optional[str] = None    # required when type == "webhook"


class SLAConfig(BaseModel):
    """Pipeline-level SLA definition."""
    model_config = ConfigDict(extra="forbid")

    max_duration_minutes: float          # fail the SLA if pipeline exceeds this
    alert: Optional[AlertConfig] = None  # where to send the alert


# ---------------------------------------------------------------------------
# Event-driven triggers
# ---------------------------------------------------------------------------

class TriggerDefinition(BaseModel):
    """Event-driven trigger declared on a pipeline config."""
    model_config = ConfigDict(extra="forbid")

    type: Literal["file_sensor", "pipeline_completion"]
    watch_path: Optional[str] = None            # file_sensor: filesystem path to watch
    upstream_pipeline: Optional[str] = None     # pipeline_completion: upstream name
    poll_interval_seconds: int = 30             # file_sensor polling cadence

    @model_validator(mode="after")
    def validate_trigger_fields(self) -> "TriggerDefinition":
        if self.type == "file_sensor" and not self.watch_path:
            raise ValueError("file_sensor trigger requires watch_path")
        if self.type == "pipeline_completion" and not self.upstream_pipeline:
            raise ValueError("pipeline_completion trigger requires upstream_pipeline")
        return self


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

class Task(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    id: Optional[str] = None          # stable machine-friendly identifier; used in depends_on references
    type: Literal["executor", "transformer"]
    plugin: str
    operation: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    retries: int = Field(default=0, ge=0)
    timeout: Optional[int] = None     # per-task timeout in seconds (informational for now)
    depends_on: Optional[List[str]] = None

    # Phase 2 — observability
    lineage: Optional[TaskLineage] = None
    quality: Optional[TaskQuality] = None

    @field_validator("name", "plugin")
    @classmethod
    def validate_required_strings(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned

    @field_validator("type", mode="before")
    @classmethod
    def normalize_task_type(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("plugin", mode="before")
    @classmethod
    def normalize_plugin(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("depends_on")
    @classmethod
    def normalize_dependencies(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return value

        cleaned_dependencies: List[str] = []
        for dependency in value:
            dependency_name = dependency.strip()
            if not dependency_name:
                raise ValueError("dependencies must not contain empty task names")
            cleaned_dependencies.append(dependency_name)
        return cleaned_dependencies


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_name: str
    description: Optional[str] = None
    file_path: Optional[str] = None
    team: Optional[str] = None           # owning team — used for RBAC and discovery
    tasks: List[Task] = Field(min_length=1)
    schedule: Optional[Dict[str, str]] = None

    # Phase 2 — observability
    sla: Optional[SLAConfig] = None

    # Phase 3 — event-driven triggers
    triggers: Optional[List[TriggerDefinition]] = None

    # Phase 4 — catalog / marketplace metadata
    tags: Optional[List[str]] = None

    # Error handling hooks (stored for future use; not enforced at validation time)
    error_handlers: Optional[List[Dict[str, Any]]] = None

    @field_validator("pipeline_name")
    @classmethod
    def validate_pipeline_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("pipeline_name must not be empty")
        return cleaned

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, value: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        if value is None:
            return value

        allowed_keys = {"minute", "hour", "day", "month", "day_of_week"}
        unknown_keys = sorted(set(value.keys()) - allowed_keys)
        if unknown_keys:
            raise ValueError(f"schedule contains unsupported keys: {', '.join(unknown_keys)}")

        normalized_schedule: Dict[str, str] = {}
        for key, schedule_value in value.items():
            if schedule_value is None:
                raise ValueError(f"schedule field '{key}' must not be null")
            cleaned = str(schedule_value).strip()
            if not cleaned:
                raise ValueError(f"schedule field '{key}' must not be empty")
            normalized_schedule[key] = cleaned
        return normalized_schedule

    @model_validator(mode="after")
    def validate_task_graph(self) -> "PipelineConfig":
        task_names = [task.name for task in self.tasks]
        duplicates = sorted({name for name in task_names if task_names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate task names are not allowed: {', '.join(duplicates)}")

        # known identifiers: both name and id (when present) are valid dependency targets
        known_ids = {task.id for task in self.tasks if task.id}
        known_names = set(task_names) | known_ids
        for task in self.tasks:
            canonical = task.id or task.name
            if task.depends_on:
                unknown_dependencies = [dependency for dependency in task.depends_on if dependency not in known_names]
                if unknown_dependencies:
                    raise ValueError(
                        f"task '{task.name}' depends on unknown tasks: {', '.join(unknown_dependencies)}"
                    )
                if canonical in task.depends_on:
                    raise ValueError(f"task '{task.name}' cannot depend on itself")
        return self


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> PipelineConfig:
    """Load and validate pipeline configuration from YAML file."""
    import yaml

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file {config_path} not found")

    with open(config_file, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError(f"Config file {config_path} is empty")
    if not isinstance(data, dict):
        raise ValueError(f"Config file {config_path} must contain a YAML object at the top level")

    return PipelineConfig(**data)
