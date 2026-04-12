"""Data catalog: searchable index of all known data assets and their lineage.

Assets are derived from lineage_records — any URI declared in a task's
reads_from or writes_to becomes a catalog entry with usage statistics.

Public API:
    search_assets(query, limit) -> List[Dict]
    get_asset_detail(asset_uri) -> Optional[Dict]
    get_pipeline_catalog() -> List[Dict]
"""
import logging
from typing import Any, Dict, List, Optional

from dataplatform.core.database import (
    init_db,
    get_catalog_assets,
    get_catalog_asset_detail,
)

logger = logging.getLogger(__name__)


def search_assets(query: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Return known data assets with usage counts.

    Each result includes:
    - asset_uri: the full URI (e.g., ``postgres://mydb/orders``)
    - pipeline_count: distinct pipelines that reference this asset
    - read_count: times appeared as reads_from
    - write_count: times appeared as writes_to
    - last_seen_at: most recent lineage record timestamp
    - asset_type: inferred from URI scheme (postgres, s3, duckdb, kafka, …)
    """
    init_db()
    rows = get_catalog_assets(query=query, limit=limit)
    for row in rows:
        row["asset_type"] = _infer_type(row["asset_uri"])
    return rows


def get_asset_detail(asset_uri: str) -> Optional[Dict[str, Any]]:
    """Return full detail for a single asset URI, or None if unknown."""
    init_db()
    pipelines = get_catalog_asset_detail(asset_uri)
    if not pipelines:
        return None
    return {
        "asset_uri": asset_uri,
        "asset_type": _infer_type(asset_uri),
        "pipeline_count": len({r["pipeline_name"] for r in pipelines}),
        "pipelines": pipelines,
    }


def get_pipeline_catalog() -> List[Dict[str, Any]]:
    """Return all pipelines that have lineage records, with asset counts."""
    init_db()
    from dataplatform.core.database import _get_conn
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT pipeline_name,
                   COUNT(DISTINCT asset_uri) AS asset_count,
                   SUM(CASE WHEN direction='reads_from' THEN 1 ELSE 0 END) AS inputs,
                   SUM(CASE WHEN direction='writes_to'  THEN 1 ELSE 0 END) AS outputs,
                   MAX(recorded_at) AS last_run_at
            FROM lineage_records
            GROUP BY pipeline_name
            ORDER BY last_run_at DESC
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SCHEME_LABELS = {
    "postgres": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "mysql": "MySQL",
    "s3": "S3",
    "gcs": "GCS",
    "duckdb": "DuckDB",
    "kafka": "Kafka",
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
    "file": "File",
    "http": "HTTP",
    "https": "HTTP",
    "spark": "Spark",
}


def _infer_type(uri: str) -> str:
    """Return a human-readable data source type from a URI scheme."""
    if "://" in uri:
        scheme = uri.split("://")[0].lower()
        return _SCHEME_LABELS.get(scheme, scheme.capitalize())
    return "Unknown"
