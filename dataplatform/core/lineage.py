"""Data lineage recording and graph construction.

Each pipeline task can declare what assets it reads from and writes to via
the ``lineage`` block in the pipeline YAML:

    tasks:
      - name: load_orders
        plugin: postgres
        lineage:
          reads_from: ["postgres://mydb/public.orders"]
          writes_to:  ["duckdb://data/orders.parquet"]

After a task runs successfully, :func:`record_task_lineage` persists those
declarations.  :func:`build_lineage_graph` then assembles the full graph for
the ``/lineage`` API endpoint and the UI.
"""
import logging
from typing import Any, Dict, List, Optional

from dataplatform.core.config import TaskLineage
from dataplatform.core.database import (
    get_full_lineage_graph,
    get_lineage_for_asset,
    save_lineage_record,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------

def record_task_lineage(
    run_id: str,
    pipeline_name: str,
    task_name: str,
    lineage: TaskLineage,
) -> None:
    """Persist all reads_from / writes_to edges declared by a task."""
    if lineage.reads_from:
        for asset_uri in lineage.reads_from:
            try:
                save_lineage_record(run_id, pipeline_name, task_name, "reads_from", asset_uri)
            except Exception as exc:
                logger.warning("Failed to save lineage record (%s reads_from %s): %s", task_name, asset_uri, exc)

    if lineage.writes_to:
        for asset_uri in lineage.writes_to:
            try:
                save_lineage_record(run_id, pipeline_name, task_name, "writes_to", asset_uri)
            except Exception as exc:
                logger.warning("Failed to save lineage record (%s writes_to %s): %s", task_name, asset_uri, exc)


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_lineage_graph() -> Dict[str, Any]:
    """Return a graph dict with ``nodes`` and ``edges`` for D3.js / UI rendering.

    Node types:
      - ``asset``  — a data store URI (postgres://…, s3://…, etc.)
      - ``task``   — a pipeline task node

    Edge directions:
      - reads_from: asset → task
      - writes_to:  task  → asset
    """
    edges_raw = get_full_lineage_graph()

    node_ids: Dict[str, Dict[str, Any]] = {}   # id → node dict
    edges: List[Dict[str, Any]] = []

    def _ensure_node(node_id: str, node_type: str, extra: Optional[Dict] = None) -> None:
        if node_id not in node_ids:
            node_ids[node_id] = {"id": node_id, "type": node_type, **(extra or {})}

    for row in edges_raw:
        pipeline = row["pipeline_name"]
        task = row["task_name"]
        direction = row["direction"]
        asset = row["asset_uri"]

        task_id = f"{pipeline}/{task}"
        _ensure_node(task_id, "task", {"pipeline": pipeline, "task": task})
        _ensure_node(asset, "asset")

        if direction == "reads_from":
            edges.append({"from": asset, "to": task_id, "direction": "reads_from"})
        else:  # writes_to
            edges.append({"from": task_id, "to": asset, "direction": "writes_to"})

    return {
        "nodes": list(node_ids.values()),
        "edges": edges,
    }


def get_asset_lineage(asset_uri: str) -> Dict[str, Any]:
    """Return upstream tasks (that write to this asset) and downstream tasks
    (that read from this asset) for a specific asset URI."""
    records = get_lineage_for_asset(asset_uri)

    upstream: List[Dict[str, Any]] = []    # tasks that write_to this asset
    downstream: List[Dict[str, Any]] = []  # tasks that read_from this asset

    for rec in records:
        entry = {
            "pipeline_name": rec["pipeline_name"],
            "task_name": rec["task_name"],
            "run_id": rec["run_id"],
            "last_seen": rec["recorded_at"],
        }
        if rec["direction"] == "writes_to":
            upstream.append(entry)
        else:
            downstream.append(entry)

    return {
        "asset_uri": asset_uri,
        "upstream": upstream,    # who produces this asset
        "downstream": downstream,  # who consumes this asset
    }
