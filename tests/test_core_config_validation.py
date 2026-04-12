import tempfile
from pathlib import Path

import pytest

from dataplatform.core.config import load_config


def write_yaml(content: str) -> str:
    temp_dir = tempfile.mkdtemp()
    file_path = Path(temp_dir) / "pipeline.yaml"
    file_path.write_text(content, encoding="utf-8")
    return str(file_path)


def test_load_config_rejects_duplicate_task_names():
    path = write_yaml(
        """
pipeline_name: duplicate_tasks
tasks:
  - name: extract
    type: executor
    plugin: python
  - name: extract
    type: executor
    plugin: shell
"""
    )

    with pytest.raises(ValueError, match="duplicate task names"):
        load_config(path)


def test_load_config_rejects_unknown_dependencies():
    path = write_yaml(
        """
pipeline_name: bad_dependencies
tasks:
  - name: extract
    type: executor
    plugin: python
    depends_on: [missing_task]
"""
    )

    with pytest.raises(ValueError, match="depends on unknown tasks"):
        load_config(path)


def test_load_config_rejects_unknown_schedule_keys():
    path = write_yaml(
        """
pipeline_name: bad_schedule
schedule:
  timezone: UTC
tasks:
  - name: extract
    type: executor
    plugin: python
"""
    )

    with pytest.raises(ValueError, match="unsupported keys"):
        load_config(path)
