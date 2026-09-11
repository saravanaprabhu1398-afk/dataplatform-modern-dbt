"""Getting a run's parameters into the task that needs them.

Runs already recorded ``runtime_parameters``, but nothing consumed them: they
were metadata shown on a run page while every task executed the same literal
config it always had. A parameter a task cannot see is a parameter that does
not exist, and without one a backfill can only re-run today repeatedly.

Tasks reference values with ``{{ name }}`` in their config:

    tasks:
      - name: load_orders
        plugin: duckdb
        config:
          sql: "SELECT * FROM orders WHERE created_at >= '{{ logical_start }}'
                                       AND created_at <  '{{ logical_end }}'"

Two decisions worth stating:

**Unknown placeholders raise.** A missing value silently rendering as empty
text turns ``WHERE created_at >= ''`` into a query that runs, returns the wrong
rows, and reports success. Failing the task is the only honest option.

**It is substitution, not templating.** There is no expression evaluation, no
loops, no attribute access -- a name maps to a string. Pipeline YAML can
already run Python through the python plugin, so this is not a security
boundary; it is a simplicity one. A template language here would be a second
language to learn, debug and escape.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Mapping, Optional, Set

logger = logging.getLogger(__name__)

#: ``{{ name }}`` with optional surrounding whitespace. Dots are allowed so
#: user parameters can be namespaced as ``params.customer``.
PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_.]*)\s*\}\}")

#: Keys the platform itself provides. Anything else has to come from the run.
RESERVED = ("logical_start", "logical_end", "ds", "ds_end", "run_id", "pipeline_name")

#: Where the runner hands a task its parameters. Removed from the config
#: before the plugin sees it -- it is platform context, not plugin input.
RUN_PARAMETERS_KEY = "_run_parameters"


class UnknownParameter(KeyError):
    """A task referenced a parameter the run does not provide."""

    def __init__(self, name: str, available: Set[str]) -> None:
        self.name = name
        self.available = available
        super().__init__(
            "unknown parameter {0!r}; the run provides {1}".format(
                name, ", ".join(sorted(available)) or "no parameters"
            )
        )


def available_parameters(
    interval: Optional[Any] = None,
    runtime_parameters: Optional[Mapping[str, Any]] = None,
    run_id: str = "",
    pipeline_name: str = "",
) -> Dict[str, str]:
    """Everything a task may reference, flattened to strings.

    User parameters are exposed twice -- bare and under ``params.`` -- so a
    pipeline can disambiguate its own ``ds`` from the platform's if it has one.
    """
    values: Dict[str, str] = {}
    if interval is not None:
        values.update(interval.as_parameters())
    if run_id:
        values["run_id"] = run_id
    if pipeline_name:
        values["pipeline_name"] = pipeline_name

    for key, value in (runtime_parameters or {}).items():
        text = "" if value is None else str(value)
        values.setdefault(str(key), text)
        values["params.{0}".format(key)] = text

    return values


def render(value: Any, parameters: Mapping[str, str]) -> Any:
    """Substitute placeholders in *value*, recursing into lists and dicts.

    Non-string leaves are returned untouched, so numbers and booleans keep
    their type. Raises :class:`UnknownParameter` rather than guessing.
    """
    if isinstance(value, str):
        return _render_string(value, parameters)
    if isinstance(value, list):
        return [render(item, parameters) for item in value]
    if isinstance(value, tuple):
        return tuple(render(item, parameters) for item in value)
    if isinstance(value, dict):
        return {key: render(item, parameters) for key, item in value.items()}
    return value


def _render_string(text: str, parameters: Mapping[str, str]) -> str:
    def replace(match: "re.Match[str]") -> str:
        name = match.group(1)
        if name not in parameters:
            raise UnknownParameter(name, set(parameters))
        return parameters[name]

    return PLACEHOLDER.sub(replace, text)


def placeholders_in(value: Any) -> List[str]:
    """Every parameter name referenced by a config, for validation."""
    found: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, str):
            found.extend(PLACEHOLDER.findall(node))
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for item in node.values():
                walk(item)

    walk(value)
    return found


def render_task_config(
    config: Dict[str, Any],
    parameters: Mapping[str, str],
) -> Dict[str, Any]:
    """Render a task's config, leaving it untouched when it references nothing.

    Skipping the walk for configs without placeholders keeps the common path
    free and, more usefully, means a pipeline that never opted in cannot be
    broken by a substitution bug.
    """
    if not config or not placeholders_in(config):
        return config
    return render(config, parameters)
