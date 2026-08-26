"""Execution fabric metadata for multi-layer, deploy-anywhere runs.

This module is intentionally declarative. The current executor still runs
locally, but API/UI/runtime contexts can now describe where a pipeline is
intended to run and how its tasks map across platform layers.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional


EXECUTION_LAYERS: List[Dict[str, Any]] = [
    {
        "id": "ingest",
        "name": "Ingest",
        "order": 10,
        "description": "Land files, events, API extracts, and database reads.",
        "default_plugins": ["api", "file", "postgres", "mysql", "kafka"],
    },
    {
        "id": "quality",
        "name": "Quality",
        "order": 20,
        "description": "Validate shape, freshness, completeness, and business rules.",
        "default_plugins": ["duckdb"],
    },
    {
        "id": "transform",
        "name": "Transform",
        "order": 30,
        "description": "Prepare trusted datasets with SQL, dbt, Python, or Spark.",
        "default_plugins": ["dbt", "duckdb", "python", "spark"],
    },
    {
        "id": "serve",
        "name": "Serve",
        "order": 40,
        "description": "Publish curated outputs to warehouses, marts, APIs, or files.",
        "default_plugins": ["bigquery", "snowflake", "file", "api"],
    },
    {
        "id": "operate",
        "name": "Operate",
        "order": 50,
        "description": "Notify, audit, recover, and automate operational tasks.",
        "default_plugins": ["email", "shell"],
    },
]

DEPLOYMENT_TARGETS: List[Dict[str, Any]] = [
    {
        "id": "local",
        "name": "Local",
        "description": "Single-process API and worker for laptops or small servers.",
        "runtime": "local-worker",
        "deployable": True,
    },
    {
        "id": "docker",
        "name": "Docker",
        "description": "Containerized API and worker with mounted pipelines and logs.",
        "runtime": "container-worker",
        "deployable": True,
    },
    {
        "id": "kubernetes",
        "name": "Kubernetes",
        "description": "Cluster runtime for scaled workers, jobs, and managed secrets.",
        "runtime": "kubernetes-worker",
        "deployable": True,
    },
    {
        "id": "cloud",
        "name": "Cloud",
        "description": "Managed cloud runtime backed by cloud databases and object storage.",
        "runtime": "cloud-worker",
        "deployable": True,
    },
    {
        "id": "on_prem",
        "name": "On-Prem",
        "description": "Private network or air-gapped deployment with local control.",
        "runtime": "private-worker",
        "deployable": True,
    },
]

ENVIRONMENT_PROFILES: Dict[str, Dict[str, Any]] = {
    "local": {
        "id": "local",
        "name": "Local",
        "description": "Run on the local worker with local files and default plugins.",
        "compute": "local-worker",
        "badge": "Default",
        "deployment_target": "local",
        "isolation": "process",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
    "dev": {
        "id": "dev",
        "name": "Development",
        "description": "Shared development runtime for isolated test inputs and dev credentials.",
        "compute": "dev-worker",
        "badge": "Dev",
        "deployment_target": "docker",
        "isolation": "container",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
    "prod": {
        "id": "prod",
        "name": "Production",
        "description": "Production runtime profile for governed, repeatable operational runs.",
        "compute": "prod-worker",
        "badge": "Prod",
        "deployment_target": "kubernetes",
        "isolation": "namespace",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
    "docker": {
        "id": "docker",
        "name": "Docker",
        "description": "Portable container runtime for single-host deployment.",
        "compute": "container-worker",
        "badge": "Docker",
        "deployment_target": "docker",
        "isolation": "container",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
    "kubernetes": {
        "id": "kubernetes",
        "name": "Kubernetes",
        "description": "Cluster runtime for teams that need horizontal scale.",
        "compute": "kubernetes-worker",
        "badge": "K8s",
        "deployment_target": "kubernetes",
        "isolation": "namespace",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
    "cloud": {
        "id": "cloud",
        "name": "Cloud",
        "description": "Cloud-hosted runtime for managed infrastructure patterns.",
        "compute": "cloud-worker",
        "badge": "Cloud",
        "deployment_target": "cloud",
        "isolation": "account",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
    "on_prem": {
        "id": "on_prem",
        "name": "On-Prem",
        "description": "Private network runtime for regulated or air-gapped deployments.",
        "compute": "private-worker",
        "badge": "Private",
        "deployment_target": "on_prem",
        "isolation": "network",
        "layers": ["ingest", "quality", "transform", "serve", "operate"],
    },
}


LAYER_IDS = tuple(layer["id"] for layer in EXECUTION_LAYERS)
TARGET_IDS = tuple(target["id"] for target in DEPLOYMENT_TARGETS)


def list_execution_layers() -> List[Dict[str, Any]]:
    return deepcopy(EXECUTION_LAYERS)


def list_deployment_targets() -> List[Dict[str, Any]]:
    return deepcopy(DEPLOYMENT_TARGETS)


def list_environment_profiles() -> List[Dict[str, Any]]:
    return [deepcopy(profile) for profile in ENVIRONMENT_PROFILES.values()]


def normalize_environment_profile(profile_id: Optional[str]) -> Dict[str, Any]:
    key = (profile_id or "local").strip().lower().replace("-", "_")
    return deepcopy(ENVIRONMENT_PROFILES.get(key, ENVIRONMENT_PROFILES["local"]))


def get_deployment_target(target_id: Optional[str]) -> Dict[str, Any]:
    key = (target_id or "local").strip().lower().replace("-", "_")
    targets = {target["id"]: target for target in DEPLOYMENT_TARGETS}
    return deepcopy(targets.get(key, targets["local"]))


def infer_task_execution_layer(task: Any) -> str:
    explicit = getattr(task, "execution_layer", None)
    if explicit:
        return str(explicit).strip().lower().replace("-", "_")

    plugin = str(getattr(task, "plugin", "") or "").lower()
    task_type = str(getattr(task, "type", "") or "").lower()
    operation = str(getattr(task, "operation", "") or "").lower()
    name = str(getattr(task, "name", "") or "").lower()
    config = getattr(task, "config", None) or {}
    config_operation = str(config.get("operation", "") or "").lower() if isinstance(config, dict) else ""
    op = operation or config_operation

    has_checks = isinstance(config, dict) and bool(config.get("checks"))

    if getattr(task, "quality", None) or "quality" in name or "validate" in name or op == "validate":
        return "quality"
    if plugin in {"api", "postgres", "mysql", "kafka"}:
        if op in {"post", "put", "patch", "publish", "load"} or any(word in name for word in ("load", "publish", "serve")):
            return "serve"
        return "ingest"
    if plugin == "file":
        if op in {"write", "create", "append", "copy", "move", "merge"} or "load" in name:
            return "serve"
        return "ingest"
    if plugin == "duckdb" and (op == "validate" or has_checks):
        return "quality"
    if plugin in {"dbt", "duckdb", "python", "spark"} or task_type == "transformer":
        return "transform"
    if plugin in {"snowflake", "bigquery"} or op in {"load", "export"}:
        return "serve"
    if plugin in {"email", "shell"} or op in {"send", "execute"}:
        return "operate"
    return "transform"


def build_task_layer_map(tasks: Iterable[Any]) -> Dict[str, str]:
    return {getattr(task, "name", ""): infer_task_execution_layer(task) for task in tasks}


def build_execution_fabric(
    config: Any,
    profile_id: Optional[str] = "local",
    execution_waves: Optional[List[List[str]]] = None,
) -> Dict[str, Any]:
    profile = normalize_environment_profile(profile_id)
    execution_config = getattr(config, "execution", None)
    requested_target = getattr(execution_config, "deployment_target", None) if execution_config else None
    target = get_deployment_target(requested_target or profile.get("deployment_target"))
    default_layer = getattr(execution_config, "default_layer", None) if execution_config else None

    layer_lookup = {layer["id"]: deepcopy(layer) for layer in EXECUTION_LAYERS}
    tasks_by_layer: Dict[str, List[Dict[str, Any]]] = {layer_id: [] for layer_id in layer_lookup}

    for task in getattr(config, "tasks", []) or []:
        layer_id = getattr(task, "execution_layer", None) or default_layer or infer_task_execution_layer(task)
        layer_id = str(layer_id).strip().lower().replace("-", "_")
        if layer_id not in tasks_by_layer:
            layer_id = "transform"
        tasks_by_layer[layer_id].append({
            "name": getattr(task, "name", ""),
            "id": getattr(task, "id", None),
            "plugin": getattr(task, "plugin", ""),
            "type": getattr(task, "type", ""),
            "operation": getattr(task, "operation", None),
        })

    layers: List[Dict[str, Any]] = []
    for layer in EXECUTION_LAYERS:
        layer_id = layer["id"]
        layer_tasks = tasks_by_layer[layer_id]
        layers.append({
            **deepcopy(layer),
            "task_count": len(layer_tasks),
            "tasks": layer_tasks,
            "enabled": bool(layer_tasks),
        })

    warnings: List[str] = []
    if requested_target and requested_target != profile.get("deployment_target"):
        warnings.append(
            "Pipeline deployment_target overrides the selected runtime profile target."
        )

    return {
        "profile": profile,
        "deployment_target": target,
        "layers": layers,
        "task_count": sum(len(tasks) for tasks in tasks_by_layer.values()),
        "active_layer_count": sum(1 for tasks in tasks_by_layer.values() if tasks),
        "execution_waves": execution_waves or [],
        "warnings": warnings,
    }
