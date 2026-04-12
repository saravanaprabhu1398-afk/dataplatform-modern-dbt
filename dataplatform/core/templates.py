"""Pipeline templates marketplace: browse and instantiate predefined pipeline patterns.

Template YAML files live in the project-root ``templates/`` directory.
Each file is a standard PipelineConfig YAML that may also include extra
fields (``tags``, ``description``) for marketplace metadata.

Public API:
    list_templates() -> List[Dict]
    get_template_content(template_id) -> Optional[str]
    use_template(template_id, new_pipeline_name) -> str   — returns saved path
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"
_PIPELINES_DIR = Path(__file__).resolve().parent.parent.parent / "pipelines"


def list_templates() -> List[Dict[str, Any]]:
    """Return metadata for all templates in the templates/ directory."""
    if not _TEMPLATES_DIR.exists():
        return []

    import yaml
    results: List[Dict[str, Any]] = []
    for path in sorted(_TEMPLATES_DIR.glob("*.yaml")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                continue
            results.append({
                "template_id": path.stem,
                "pipeline_name": data.get("pipeline_name", path.stem),
                "description": data.get("description", ""),
                "tags": data.get("tags", []),
                "task_count": len(data.get("tasks", [])),
                "plugins": sorted({t.get("plugin", "") for t in data.get("tasks", []) if t.get("plugin")}),
            })
        except Exception as exc:
            logger.warning("Failed to load template %s: %s", path.name, exc)
    return results


def get_template_content(template_id: str) -> Optional[str]:
    """Return the raw YAML content of a template, or None if not found."""
    path = _TEMPLATES_DIR / f"{template_id}.yaml"
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def use_template(template_id: str, new_pipeline_name: str) -> str:
    """Copy a template into pipelines/ with a new pipeline_name.

    The YAML is re-serialised so the pipeline_name field is updated.
    Returns the absolute path of the saved pipeline file.
    """
    import yaml

    content = get_template_content(template_id)
    if content is None:
        raise FileNotFoundError(f"Template '{template_id}' not found in templates/ directory")

    data = yaml.safe_load(content)
    data["pipeline_name"] = new_pipeline_name
    # Remove marketplace-only fields that PipelineConfig does not accept
    data.pop("tags", None)

    _PIPELINES_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _PIPELINES_DIR / f"{new_pipeline_name}.yaml"
    output_path.write_text(
        yaml.dump(data, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    logger.info("Template '%s' instantiated as '%s' at %s", template_id, new_pipeline_name, output_path)
    return str(output_path)
