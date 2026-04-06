from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import List, Optional, Dict, Any, Literal
from pathlib import Path


class Task(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["executor", "transformer"]
    plugin: str
    operation: Optional[str] = None  # Explicit operation type (not derived from task name)
    config: Optional[Dict[str, Any]] = None  # Plugin-specific configuration
    retries: int = Field(default=0, ge=0)
    depends_on: Optional[List[str]] = None

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


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_name: str
    description: Optional[str] = None
    file_path: Optional[str] = None
    tasks: List[Task] = Field(min_length=1)
    schedule: Optional[Dict[str, str]] = None

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

        known_names = set(task_names)
        for task in self.tasks:
            if task.depends_on:
                unknown_dependencies = [dependency for dependency in task.depends_on if dependency not in known_names]
                if unknown_dependencies:
                    raise ValueError(
                        f"task '{task.name}' depends on unknown tasks: {', '.join(unknown_dependencies)}"
                    )
                if task.name in task.depends_on:
                    raise ValueError(f"task '{task.name}' cannot depend on itself")
        return self


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
