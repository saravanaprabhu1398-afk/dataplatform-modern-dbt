import pytest
from pathlib import Path
from dataplatform.core import api
from dataplatform.core.api import PipelineRunRequest
from dataplatform.core.config import PipelineConfig, Task


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "sync_run_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module.init_db()
    yield
    db_module._initialized = False


class DummyDagBuilder:
    def __init__(self, tasks):
        self.tasks = tasks

    def build(self):
        return {}

    def get_execution_order(self):
        return [task.name for task in self.tasks]

    def get_execution_waves(self):
        return [[task.name for task in self.tasks]]


class FailingExecutor:
    def execute_pipeline(self, tasks, execution_order, config):
        return False, {name: False for name in execution_order}, {"extract_orders": "boom"}

    def execute_pipeline_parallel(self, tasks, execution_waves, config, max_workers=4,
                                    pipeline_name="", run_id=""):
        all_tasks = [t for wave in execution_waves for t in wave]
        return False, {name: False for name in all_tasks}, {"extract_orders": "boom"}


@pytest.mark.asyncio
async def test_run_pipeline_sync_reports_failed_status(monkeypatch):
    config = PipelineConfig(
        pipeline_name="sync_failure_pipeline",
        tasks=[
            Task(name="extract_orders", type="executor", plugin="python", config={}),
        ],
        file_path="pipelines/sync_failure_pipeline.yaml",
    )

    monkeypatch.setattr(api, "load_config", lambda path: config)
    monkeypatch.setattr(api, "DAGBuilder", DummyDagBuilder)
    monkeypatch.setattr(api, "PipelineExecutor", lambda: FailingExecutor())

    response = await api.run_pipeline_sync(PipelineRunRequest(config_path=config.file_path))

    assert response.status == "failed"
    assert response.message == "Pipeline failed"
    assert response.results is None
