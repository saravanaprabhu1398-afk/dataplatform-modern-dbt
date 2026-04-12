"""Pipeline version history: save, list, retrieve, and diff YAML configs.

Every time a pipeline YAML is saved via the API, a version snapshot is
stored in the metadata DB.  Identical content (same SHA-256) is deduplicated
per pipeline — only changed content creates a new version entry.

Public API:
    save_version(pipeline_name, content, saved_by) -> Optional[str]
    list_versions(pipeline_name, limit) -> List[Dict]
    get_version_content(pipeline_name, version_id) -> Optional[str]
    diff_versions(pipeline_name, version_id_a, version_id_b) -> Optional[str]
"""
import difflib
import hashlib
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from dataplatform.core.database import (
    init_db,
    save_pipeline_version as _db_save,
    get_pipeline_versions as _db_list,
    get_pipeline_version_content as _db_get_content,
)

logger = logging.getLogger(__name__)


def save_version(
    pipeline_name: str,
    content: str,
    saved_by: Optional[str] = None,
) -> Optional[str]:
    """Persist a new pipeline version if the content has changed.

    Returns the new version_id if saved, or None if the content is
    identical to an existing version for this pipeline.
    """
    init_db()
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    version_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"

    saved = _db_save(version_id, pipeline_name, content_hash, content, saved_by, now)
    if saved:
        logger.info("Saved version %s for pipeline '%s'", version_id[:8], pipeline_name)
        return version_id
    logger.debug("No version saved for '%s' — content unchanged", pipeline_name)
    return None


def list_versions(pipeline_name: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Return version metadata (newest first) for a pipeline, without content."""
    init_db()
    return _db_list(pipeline_name, limit)


def get_version_content(pipeline_name: str, version_id: str) -> Optional[str]:
    """Return the raw YAML content for a specific version, or None if not found."""
    init_db()
    return _db_get_content(pipeline_name, version_id)


def diff_versions(
    pipeline_name: str,
    version_id_a: str,
    version_id_b: str,
) -> Optional[str]:
    """Return a unified diff between two versions.

    Returns None if either version does not exist.
    Returns an empty string if the versions are identical.
    """
    a = get_version_content(pipeline_name, version_id_a)
    b = get_version_content(pipeline_name, version_id_b)

    if a is None or b is None:
        return None

    return "".join(
        difflib.unified_diff(
            a.splitlines(keepends=True),
            b.splitlines(keepends=True),
            fromfile=f"{pipeline_name}@{version_id_a[:8]}",
            tofile=f"{pipeline_name}@{version_id_b[:8]}",
        )
    )
