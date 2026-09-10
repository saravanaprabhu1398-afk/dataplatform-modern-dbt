"""Checks that fail a pull request before a column disappears from production.

A lineage graph nobody queries is a diagram.  These are the two questions worth
asking automatically, on every change:

**Did this change remove a column something reads?**
    The graph before the change knows who reads what.  If the new SQL stops
    producing a column, every downstream column that reads it is listed, and
    the check fails.  Removing a column nothing reads is a note, not a failure.

**Does the declared lineage still match the SQL?**
    A task's ``reads_from`` block is a promise made by hand, in a different
    file from the query.  Nothing checked it until now, so it drifts.  Declared
    lineage stops being decoration the day something compares it to the parsed
    truth.

Both return findings rather than printing, so the CLI, a test and CI can share
them.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dataplatform.core.column_lineage import (
    KIND_AMBIGUOUS,
    KIND_UNRESOLVED_STAR,
    ROWS_COLUMN,
    ColumnLineage,
    extract_column_lineage,
)
from dataplatform.core.lineage_impact import column_impact
from dataplatform.core.sql_lineage import extract_lineage

logger = logging.getLogger(__name__)

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"

CODE_COLUMN_REMOVED = "column_removed"
CODE_COLUMN_ORPHANED = "column_orphaned"
CODE_DECLARED_DRIFT = "declared_drift"
CODE_UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class Finding:
    """One thing worth telling a reviewer about."""

    severity: str
    code: str
    asset: str
    message: str
    details: List[str] = field(default_factory=list)

    def render(self) -> str:
        marker = "ERROR  " if self.severity == SEVERITY_ERROR else "warning"
        lines = ["{0} {1}: {2}".format(marker, self.asset, self.message)]
        lines.extend("           {0}".format(detail) for detail in self.details)
        return "\n".join(lines)


@dataclass
class CheckReport:
    """Findings from one or more checks."""

    findings: List[Finding] = field(default_factory=list)

    @property
    def errors(self) -> List[Finding]:
        return [f for f in self.findings if f.severity == SEVERITY_ERROR]

    @property
    def warnings(self) -> List[Finding]:
        return [f for f in self.findings if f.severity == SEVERITY_WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def extend(self, findings: Iterable[Finding]) -> "CheckReport":
        self.findings.extend(findings)
        return self

    def render(self) -> str:
        if not self.findings:
            return "lineage checks: no findings"
        body = "\n".join(finding.render() for finding in self.findings)
        return "{0}\n\n{1} error(s), {2} warning(s)".format(
            body, len(self.errors), len(self.warnings)
        )


# ---------------------------------------------------------------------------
# Parsing a set of models
# ---------------------------------------------------------------------------

def parse_models(
    paths: Sequence[Path],
    schema: Optional[Dict[str, Sequence[str]]] = None,
) -> Dict[str, ColumnLineage]:
    """Parse each SQL file, keyed by the asset it writes."""
    models: Dict[str, ColumnLineage] = {}
    for path in paths:
        try:
            sql = Path(path).read_text()
        except OSError as exc:
            logger.warning("could not read %s: %s", path, exc)
            continue

        lineage = extract_column_lineage(sql, schema=schema)
        if lineage.target and lineage.target != "(query)":
            models[lineage.target] = lineage
    return models


def edges_of(models: Dict[str, ColumnLineage]) -> List[Dict[str, Any]]:
    """Flatten parsed models into the edge rows the impact walk consumes."""
    return [
        {
            "target_asset": edge.target.asset,
            "target_column": edge.target.column,
            "source_asset": edge.source.asset,
            "source_column": edge.source.column,
            "kind": edge.kind,
            "expression": edge.expression,
        }
        for lineage in models.values()
        for edge in lineage.edges
    ]


def produced_columns(edges: Iterable[Dict[str, Any]], asset: str) -> List[str]:
    """Columns an asset produced, according to a graph."""
    columns = []
    for edge in edges:
        if edge["target_asset"] != asset:
            continue
        column = edge["target_column"]
        if column != ROWS_COLUMN and column not in columns:
            columns.append(column)
    return columns


# ---------------------------------------------------------------------------
# Check: a column disappeared
# ---------------------------------------------------------------------------

def check_removed_columns(
    before_edges: Sequence[Dict[str, Any]],
    after_models: Dict[str, ColumnLineage],
) -> List[Finding]:
    """Columns the new SQL stops producing, and who was reading them.

    The blast radius is computed on the *before* graph, because that is where
    the readers are: after the change they are broken, not absent.
    """
    findings: List[Finding] = []

    for asset, lineage in sorted(after_models.items()):
        before = produced_columns(before_edges, asset)
        if not before:
            continue  # nothing recorded for this asset yet -- nothing to lose

        after = set(lineage.target_columns)
        for column in before:
            if column in after:
                continue

            impact = column_impact(asset, column, before_edges)
            if impact.hits:
                findings.append(
                    Finding(
                        severity=SEVERITY_ERROR,
                        code=CODE_COLUMN_REMOVED,
                        asset="{0}.{1}".format(asset, column),
                        message="no longer produced, and {0} column(s) read it".format(
                            len(impact.hits)
                        ),
                        details=[
                            "{0:<38} {1}".format(str(hit.column), hit.kind)
                            for hit in impact.hits
                        ]
                        + (
                            ["{0} of these rest on an unresolved star or an "
                             "ambiguous column".format(len(impact.uncertain))]
                            if impact.uncertain else []
                        ),
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=SEVERITY_WARNING,
                        code=CODE_COLUMN_ORPHANED,
                        asset="{0}.{1}".format(asset, column),
                        message="no longer produced; nothing recorded reads it",
                    )
                )

    return findings


def check_unresolved(after_models: Dict[str, ColumnLineage]) -> List[Finding]:
    """Report what the parser could not resolve, so coverage stays honest."""
    findings = []
    for asset, lineage in sorted(after_models.items()):
        assumed = [
            edge for edge in lineage.edges
            if edge.kind in (KIND_UNRESOLVED_STAR, KIND_AMBIGUOUS)
        ]
        # A model whose only "note" is a constant column is fully understood.
        # Warning about it would train people to ignore the warnings.
        if not assumed and not lineage.unresolved:
            continue

        resolved, total = lineage.coverage()
        findings.append(
            Finding(
                severity=SEVERITY_WARNING,
                code=CODE_UNRESOLVED,
                asset=asset,
                message="{0} of {1} output columns fully resolved".format(resolved, total),
                details=list(lineage.unresolved),
            )
        )
    return findings


# ---------------------------------------------------------------------------
# Check: declared lineage drifted from the SQL
# ---------------------------------------------------------------------------

def runtime_bindings(task: Dict[str, Any]) -> Dict[str, str]:
    """Names a plugin materialises at run time, mapped to the real asset.

    The DuckDB executor runs ``CREATE TABLE data AS SELECT * FROM
    read_csv_auto(<file_path>)`` before the task's SQL, so a query reading
    ``data`` is really reading that file.  Without this, the drift check
    reports a task whose declared lineage is *correct* -- which is worse than
    no check, because a checker that cries wolf gets switched off.

    Bindings are per plugin and deliberately narrow: an alias with no
    configured source is left alone rather than guessed at.
    """
    config = task.get("config") or {}
    plugin = (task.get("plugin") or "").lower()
    bindings: Dict[str, str] = {}

    if plugin == "duckdb":
        file_path = config.get("file_path")
        if file_path:
            bindings["data"] = "file://{0}".format(file_path)

    return bindings


def check_declared_drift(pipeline_paths: Sequence[Path]) -> List[Finding]:
    """Compare each task's declared reads_from against what its SQL reads."""
    import yaml

    findings: List[Finding] = []

    for path in pipeline_paths:
        try:
            document = yaml.safe_load(Path(path).read_text()) or {}
        except Exception as exc:
            logger.warning("could not read pipeline %s: %s", path, exc)
            continue

        for task in document.get("tasks") or []:
            sql = (task.get("config") or {}).get("sql")
            declared = ((task.get("lineage") or {}).get("reads_from")) or []
            if not sql or not declared:
                continue

            task_name = task.get("name") or task.get("id") or "?"
            bindings = runtime_bindings(task)
            derived = {
                bindings.get(asset.name, asset.uri)
                for asset in extract_lineage(sql).reads
            }
            declared_set = set(declared)

            missing = sorted(derived - declared_set)
            extra = sorted(declared_set - derived)
            if not missing and not extra:
                continue

            details = []
            details.extend("read by the SQL but not declared: {0}".format(u) for u in missing)
            details.extend("declared but not read by the SQL: {0}".format(u) for u in extra)
            findings.append(
                Finding(
                    severity=SEVERITY_ERROR,
                    code=CODE_DECLARED_DRIFT,
                    asset="{0}::{1}".format(Path(path).name, task_name),
                    message="declared lineage disagrees with the SQL",
                    details=details,
                )
            )

    return findings
