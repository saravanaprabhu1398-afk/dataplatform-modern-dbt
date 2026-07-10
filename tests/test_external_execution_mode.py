from pathlib import Path

import pytest

from dataplatform.core import api
from dataplatform.core.api import PipelineRunRequest
from dataplatform.core.config import PipelineConfig, Task


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module

    db_file = str(tmp_path / "external_mode_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module._engine = None
    db_module.init_db()
    yield
    db_module._initialized = False
    db_module._engine = None


class DummyDagBuilder:
    def __init__(self, tasks):
        self.tasks = tasks

    def build(self):
        return {}

    def get_execution_order(self):
        return [task.name for task in self.tasks]

    def get_execution_waves(self):
        return [[task.name for task in self.tasks]]


class FailingPool:
    def submit(self, *args, **kwargs):
        raise AssertionError("external execution mode must not submit to in-memory worker")


@pytest.mark.asyncio
async def test_run_pipeline_external_mode_enqueues_without_in_memory_submit(monkeypatch):
    from dataplatform.core.database import get_queue_runs

    monkeypatch.setenv("DATAPLATFORM_EXECUTION_MODE", "external")
    monkeypatch.setattr(api, "_require_permission", lambda request, action: {"username": "admin", "role": "admin"})
    monkeypatch.setattr(api, "_get_request_username", lambda request: "admin")
    monkeypatch.setattr(api, "get_worker_pool", lambda: FailingPool())
    monkeypatch.setattr(api, "DAGBuilder", DummyDagBuilder)

    config = PipelineConfig(
        pipeline_name="external_mode_pipeline",
        tasks=[Task(name="extract", type="executor", plugin="python", config={})],
        file_path="pipelines/external_mode_pipeline.yaml",
    )
    monkeypatch.setattr(api, "load_config", lambda path: config)

    response = await api.run_pipeline(PipelineRunRequest(config_path=config.file_path), request=None)

    assert response.status == "queued"
    queued = get_queue_runs(status="queued")
    assert len(queued) == 1
    assert queued[0]["run_id"] == response.run_id
    assert queued[0]["pipeline_name"] == "external_mode_pipeline"
