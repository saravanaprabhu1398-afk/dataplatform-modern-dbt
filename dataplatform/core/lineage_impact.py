"""Blast radius: what breaks if this column changes.

The graph is not the product. The product is the answer to a question someone
asks under time pressure -- "can I drop this column?" -- and the answer has to
include the parts nobody remembers, which is why join keys and unresolved
stars are carried through rather than filtered out.

    >>> impact = column_impact("orders", "amount")
    >>> [str(hit.column) for hit in impact.hits]
    ['daily_revenue.gross', 'finance_export.revenue']

The edges are rows in a table. Transitive closure over a few thousand of them
is a breadth-first walk, not a reason to add a graph database.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dataplatform.core.column_lineage import (
    KIND_AMBIGUOUS,
    KIND_JOIN_KEY,
    KIND_UNRESOLVED_STAR,
    STAR_COLUMN,
    ColumnRef,
)

logger = logging.getLogger(__name__)

#: How deep the walk goes before it assumes the graph is cyclic.
MAX_HOPS = 25


@dataclass(frozen=True)
class ImpactHit:
    """One downstream column, and how it depends on the column in question."""

    column: ColumnRef
    kind: str
    hops: int
    via: str
    expression: str = ""

    @property
    def is_certain(self) -> bool:
        """False when this hit rests on an assumption rather than a resolution."""
        return self.kind not in (KIND_AMBIGUOUS, KIND_UNRESOLVED_STAR)


@dataclass
class ImpactReport:
    """Everything downstream of one column."""

    root: ColumnRef
    hits: List[ImpactHit] = field(default_factory=list)
    truncated: bool = False

    @property
    def assets(self) -> List[str]:
        seen = []
        for hit in self.hits:
            if hit.column.asset not in seen:
                seen.append(hit.column.asset)
        return seen

    @property
    def uncertain(self) -> List[ImpactHit]:
        return [hit for hit in self.hits if not hit.is_certain]

    @property
    def breaks_rows(self) -> List[ImpactHit]:
        """Hits where the column decides which rows exist, not which values."""
        return [hit for hit in self.hits if hit.kind == KIND_JOIN_KEY]

    def summary(self) -> str:
        if not self.hits:
            return "{0} has no recorded downstream columns".format(self.root)

        lines = [
            "{0} is read by {1} column(s) across {2} asset(s):".format(
                self.root, len(self.hits), len(self.assets)
            )
        ]
        for hit in self.hits:
            marker = " " if hit.is_certain else "?"
            lines.append(
                "  {0} {1:<38} {2:<16} {3}".format(
                    marker, str(hit.column), hit.kind, hit.expression[:44]
                )
            )
        if self.uncertain:
            lines.append(
                "  {0} of these rest on an unresolved star or an ambiguous column".format(
                    len(self.uncertain)
                )
            )
        if self.breaks_rows:
            lines.append(
                "  {0} join key(s): changing this changes which rows exist".format(
                    len(self.breaks_rows)
                )
            )
        if self.truncated:
            lines.append("  (walk stopped at {0} hops)".format(MAX_HOPS))
        return "\n".join(lines)


def _index(edges: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """Group edges by the source column they read."""
    index: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for edge in edges:
        key = (str(edge["source_asset"]), str(edge["source_column"]))
        index.setdefault(key, []).append(edge)
    return index


def column_impact(
    asset: str,
    column: str,
    edges: Optional[Iterable[Dict[str, Any]]] = None,
) -> ImpactReport:
    """Every column downstream of ``asset.column``, transitively.

    ``edges`` defaults to everything recorded in the lineage store; pass a list
    to analyse a graph that has not been persisted (a pull request, say).
    """
    if edges is None:
        from dataplatform.core.database import get_column_edges

        edges = get_column_edges()

    index = _index(edges)
    report = ImpactReport(root=ColumnRef(asset, column))

    seen: Set[Tuple[str, str]] = {(asset, column)}
    frontier: List[Tuple[Tuple[str, str], int, str]] = [((asset, column), 0, str(report.root))]

    while frontier:
        (current_asset, current_column), hops, path = frontier.pop(0)
        if hops >= MAX_HOPS:
            report.truncated = True
            continue

        # A star edge on the source means "every column of this asset", so a
        # named column is covered by it too.
        candidates = list(index.get((current_asset, current_column), []))
        candidates.extend(index.get((current_asset, STAR_COLUMN), []))

        for edge in candidates:
            target = (str(edge["target_asset"]), str(edge["target_column"]))
            if target in seen:
                continue
            seen.add(target)

            report.hits.append(
                ImpactHit(
                    column=ColumnRef(*target),
                    kind=str(edge["kind"]),
                    hops=hops + 1,
                    via=path,
                    expression=str(edge.get("expression") or ""),
                )
            )
            frontier.append((target, hops + 1, "{0} -> {1}.{2}".format(path, *target)))

    report.hits.sort(key=lambda hit: (hit.hops, hit.column.asset, hit.column.column))
    return report


def record_column_lineage(
    lineage: Any,
    run_id: str = "",
    pipeline_name: str = "",
    task_name: str = "",
) -> int:
    """Persist a :class:`~dataplatform.core.column_lineage.ColumnLineage`."""
    from dataplatform.core.database import replace_column_lineage

    if not lineage.target or not lineage.edges:
        return 0

    rows = [
        {
            "target_column": edge.target.column,
            "source_asset": edge.source.asset,
            "source_column": edge.source.column,
            "kind": edge.kind,
            "expression": edge.expression,
        }
        for edge in lineage.edges
    ]
    return replace_column_lineage(
        lineage.target, rows, run_id=run_id,
        pipeline_name=pipeline_name, task_name=task_name,
    )
