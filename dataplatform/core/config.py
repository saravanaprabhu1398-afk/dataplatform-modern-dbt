from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from pathlib import Path


class Task(BaseModel):
    name: str
    type: str  # executor or transformer
    plugin: str
    operation: Optional[str] = None  # Explicit operation type (not derived from task name)
    config: Optional[Dict[str, Any]] = None  # Plugin-specific configuration
    retries: int = 0
    depends_on: Optional[List[str]] = None


class PipelineConfig(BaseModel):
    pipeline_name: str
    description: Optional[str] = None
    file_path: Optional[str] = None
    tasks: List[Task]
    schedule: Optional[Dict[str, str]] = None


def load_config(config_path: str) -> PipelineConfig:
    """Load and validate pipeline configuration from YAML file."""
    import yaml

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file {config_path} not found")

    with open(config_file, 'r') as f:
        data = yaml.safe_load(f)

    return PipelineConfig(**data)