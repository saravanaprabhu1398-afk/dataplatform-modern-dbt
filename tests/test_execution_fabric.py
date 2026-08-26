from dataplatform.core.config import PipelineConfig, Task
from dataplatform.core.execution_fabric import (
    build_execution_fabric,
    infer_task_execution_layer,
    normalize_environment_profile,
)


def test_normalize_environment_profile_supports_deploy_anywhere_profiles():
    profile = normalize_environment_profile("on-prem")
    assert profile["id"] == "on_prem"
    assert profile["deployment_target"] == "on_prem"


def test_infers_layers_from_task_plugin_and_operation():
    assert infer_task_execution_layer(Task(name="extract_api", type="executor", plugin="api")) == "ingest"
    assert infer_task_execution_layer(Task(name="validate_orders", type="executor", plugin="duckdb")) == "quality"
    assert infer_task_execution_layer(Task(name="run_models", type="transformer", plugin="dbt")) == "transform"
    assert infer_task_execution_layer(Task(name="load_curated", type="executor", plugin="snowflake")) == "serve"
    assert infer_task_execution_layer(Task(name="notify", type="executor", plugin="email")) == "operate"


def test_build_execution_fabric_groups_tasks_by_layer_and_target():
    config = PipelineConfig(
        pipeline_name="fabric_demo",
        execution={
            "profile": "prod",
            "deployment_target": "kubernetes",
            "max_parallel_tasks": 8,
        },
        tasks=[
            Task(name="extract", type="executor", plugin="api"),
            Task(name="check", type="executor", plugin="duckdb", execution_layer="quality"),
            Task(name="model", type="transformer", plugin="dbt", depends_on=["check"]),
            Task(name="publish", type="executor", plugin="snowflake", depends_on=["model"]),
        ],
    )

    fabric = build_execution_fabric(config, "prod", [["extract", "check"], ["model"], ["publish"]])
    active_layers = {layer["id"]: layer["task_count"] for layer in fabric["layers"] if layer["enabled"]}

    assert fabric["profile"]["id"] == "prod"
    assert fabric["deployment_target"]["id"] == "kubernetes"
    assert active_layers == {
        "ingest": 1,
        "quality": 1,
        "transform": 1,
        "serve": 1,
    }
    assert fabric["execution_waves"] == [["extract", "check"], ["model"], ["publish"]]
