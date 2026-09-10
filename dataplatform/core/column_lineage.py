"""Column-level lineage: which input column produced which output column.

Table-level lineage answers "something downstream might care", which is the
answer that makes people ignore lineage tools.  Dropping a column from a
hundred-column table breaks the three dashboards reading *that column*, not
the forty reading the table.

    >>> lineage = extract_column_lineage(
    ...     "CREATE TABLE daily AS SELECT SUM(amount) AS gross FROM orders")
    >>> lineage.edges
    [daily.gross <- orders.amount (aggregate)]

Four things this records that a naive implementation misses:

``join_key``
    A column that never appears in the output but decides which rows do.
    Dropping one breaks everything downstream, so it is an edge like any
    other -- targeted at the sentinel column ``(rows)``.

``aggregate`` versus ``derived`` versus ``direct``
    How a column was produced changes what a schema change does to it.

``unresolved_star``
    ``SELECT *`` cannot be resolved without knowing the source's columns.
    Pass a ``schema`` and it expands; without one, the edge is recorded as an
    explicit unknown rather than dropped. A lineage tool that quietly
    under-reports is worse than none, because people trust it right up until
    the outage.

``ambiguous``
    An unqualified column with several candidate sources resolves to all of
    them, flagged. For impact analysis a visible over-approximation beats a
    confident guess.

sqlglot ships its own ``lineage`` helper. This one exists because it
classifies edges, and because it reports what it could not resolve instead of
returning a graph that looks complete.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import Scope, build_scope

from dataplatform.core.sql_lineage import (
    DEFAULT_DIALECT,
    AssetRef,
    _asset_from_table,
    _identifier_text,
    _write_target,
)

logger = logging.getLogger(__name__)

KIND_DIRECT = "direct"
KIND_DERIVED = "derived"
KIND_AGGREGATE = "aggregate"
KIND_JOIN_KEY = "join_key"
KIND_UNRESOLVED_STAR = "unresolved_star"
KIND_AMBIGUOUS = "ambiguous"
#: A column with no column inputs at all: COUNT(*), a literal, NOW().
#: Recorded explicitly, because "depends on nothing" is knowledge, and a
#: column missing from the graph cannot be seen to disappear.
KIND_CONSTANT = "constant"

#: Target column for edges that decide which *rows* exist rather than which
#: values a column takes.
ROWS_COLUMN = "(rows)"

#: Source column for a star that could not be expanded.
STAR_COLUMN = "*"

MAX_EXPRESSION = 120


@dataclass(frozen=True)
class ColumnRef:
    """One column of one asset."""

    asset: str
    column: str

    def __str__(self) -> str:
        return "{0}.{1}".format(self.asset, self.column)


@dataclass(frozen=True)
class ColumnEdge:
    """A produced column and the input it came from."""

    target: ColumnRef
    source: ColumnRef
    kind: str = KIND_DIRECT
    expression: str = ""

    def __repr__(self) -> str:  # pragma: no cover - debugging affordance
        return "{0} <- {1} ({2})".format(self.target, self.source, self.kind)


@dataclass
class ColumnLineage:
    """Every column edge a statement produces, plus what it could not resolve."""

    target: Optional[str] = None
    #: The file this was parsed from, when it came from one. Carried so a CI
    #: finding can point at the line a reviewer needs to look at.
    source_path: Optional[str] = None
    edges: List[ColumnEdge] = field(default_factory=list)
    #: Every column the statement produces, including ones with no column
    #: inputs. Kept separately from the edges: a column with no source -- a
    #: COUNT(*) or a literal -- still exists, and leaving it out of the
    #: denominator would flatter the coverage number.
    output_columns: List[str] = field(default_factory=list)
    #: Genuine gaps in knowledge: an unexpandable star, an ambiguous column.
    unresolved: List[str] = field(default_factory=list)
    #: Things worth saying that are not gaps -- a column known to depend on no
    #: column at all. Kept apart so a report can distinguish "we do not know"
    #: from "there is nothing to know".
    notes: List[str] = field(default_factory=list)
    unsupported: List[str] = field(default_factory=list)

    @property
    def parsed(self) -> bool:
        return not self.unsupported

    @property
    def target_columns(self) -> List[str]:
        """Output columns, whether or not any edge was resolved for them."""
        seen = list(self.output_columns)
        for edge in self.edges:
            if edge.target.column != ROWS_COLUMN and edge.target.column not in seen:
                seen.append(edge.target.column)
        return seen

    def coverage(self) -> Tuple[int, int]:
        """(fully resolved output columns, total output columns).

        A column counts as unresolved when it rests on an assumption -- an
        unexpandable star, or an ambiguous reference. A column known to depend
        on no column at all is *resolved*: "nothing" is an answer.
        """
        assumed = {
            edge.target.column
            for edge in self.edges
            if edge.kind in (KIND_UNRESOLVED_STAR, KIND_AMBIGUOUS)
        }
        with_edges = {
            edge.target.column
            for edge in self.edges
            if edge.target.column != ROWS_COLUMN
        }
        columns = self.target_columns
        unresolved = [
            column for column in columns
            if column in assumed or column not in with_edges
        ]
        return len(columns) - len(unresolved), len(columns)

    def sources_for(self, column: str) -> List[ColumnRef]:
        return [edge.source for edge in self.edges if edge.target.column == column]


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------

class _Resolver:
    """Resolves a column reference down to the physical columns behind it."""

    def __init__(self, schema: Optional[Dict[str, Sequence[str]]] = None) -> None:
        self.schema = {
            asset.lower(): [str(column) for column in columns]
            for asset, columns in (schema or {}).items()
        }
        self.notes: List[str] = []

    # -- helpers ----------------------------------------------------------

    def columns_of(self, asset: str) -> Optional[List[str]]:
        return self.schema.get(asset.lower())

    def _source_asset(self, node: exp.Table) -> Optional[str]:
        asset = _asset_from_table(node)
        return asset.name if asset is not None else None

    # -- the recursive walk ------------------------------------------------

    def resolve(
        self,
        scope: Scope,
        table_alias: str,
        column: str,
        depth: int = 0,
    ) -> List[ColumnRef]:
        """Physical columns behind ``table_alias.column`` inside *scope*."""
        if depth > 12:  # cyclic or pathological nesting
            self.notes.append("resolution depth exceeded for {0}".format(column))
            return []

        if table_alias:
            source = scope.sources.get(table_alias)
            if source is None:
                self.notes.append("unknown source alias {0!r}".format(table_alias))
                return []
            return self._resolve_in_source(source, column, depth)

        candidates = list(scope.sources.items())
        if len(candidates) == 1:
            return self._resolve_in_source(candidates[0][1], column, depth)

        # Unqualified column, several sources. Prefer the catalog if it can
        # settle it; otherwise report every candidate rather than pick one.
        narrowed = [
            source for _, source in candidates
            if self._source_has_column(source, column)
        ]
        if len(narrowed) == 1:
            return self._resolve_in_source(narrowed[0], column, depth)

        self.notes.append(
            "ambiguous column {0!r} across {1} sources".format(column, len(candidates))
        )
        resolved: List[ColumnRef] = []
        for _, source in candidates:
            resolved.extend(self._resolve_in_source(source, column, depth))
        return resolved

    def _source_has_column(self, source, column: str) -> bool:
        if isinstance(source, exp.Table):
            asset = self._source_asset(source)
            columns = self.columns_of(asset) if asset else None
            return bool(columns) and column in columns
        if isinstance(source, Scope):
            return any(
                projection.alias_or_name == column
                for projection in source.expression.expressions
            )
        return False

    def _resolve_in_source(self, source, column: str, depth: int) -> List[ColumnRef]:
        if isinstance(source, exp.Table):
            asset = self._source_asset(source)
            return [ColumnRef(asset, column)] if asset else []

        if isinstance(source, Scope):
            for projection in source.expression.expressions:
                if projection.alias_or_name != column:
                    continue
                return self.columns_in(source, projection, depth + 1)

            # Not projected by name: a star inside the inner scope may supply it.
            if any(isinstance(p, exp.Star) for p in source.expression.expressions):
                return self._through_star(source, column, depth)

            self.notes.append("column {0!r} not projected by its source".format(column))
        return []

    def _through_star(self, source: Scope, column: str, depth: int) -> List[ColumnRef]:
        """A star in an inner scope passes a named column straight through."""
        resolved: List[ColumnRef] = []
        for alias, inner in source.sources.items():
            resolved.extend(self._resolve_in_source(inner, column, depth + 1))
        return resolved

    def columns_in(
        self, scope: Scope, projection: exp.Expression, depth: int = 0
    ) -> List[ColumnRef]:
        """Physical columns feeding one projection expression."""
        resolved: List[ColumnRef] = []
        for node in projection.find_all(exp.Column):
            for ref in self.resolve(scope, node.table, node.name, depth):
                if ref not in resolved:
                    resolved.append(ref)
        return resolved


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def _classify(projection: exp.Expression) -> str:
    body = projection.this if isinstance(projection, exp.Alias) else projection
    if list(projection.find_all(exp.AggFunc)):
        return KIND_AGGREGATE
    if isinstance(body, exp.Column):
        return KIND_DIRECT
    return KIND_DERIVED


def _snippet(expression: exp.Expression) -> str:
    text = " ".join(expression.sql(dialect=DEFAULT_DIALECT).split())
    return text[:MAX_EXPRESSION]


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_column_lineage(
    sql: str,
    dialect: str = DEFAULT_DIALECT,
    schema: Optional[Dict[str, Sequence[str]]] = None,
    target: Optional[str] = None,
) -> ColumnLineage:
    """Column edges produced by *sql*. Never raises.

    ``schema`` maps asset name to column names and is used to expand ``SELECT *``
    and to settle ambiguous unqualified columns. Without it, both are recorded
    as explicit unknowns.
    """
    lineage = ColumnLineage(target=target)
    if not sql or not sql.strip():
        return lineage

    try:
        statements = sqlglot.parse(sql, read=dialect)
    except ParseError as exc:
        lineage.unsupported.append(str(exc).splitlines()[0])
        return lineage
    except Exception as exc:  # lineage must never break a task
        lineage.unsupported.append(repr(exc))
        return lineage

    for statement in statements:
        if statement is None:
            continue
        try:
            _analyze_statement(statement, lineage, schema, target)
        except Exception as exc:
            logger.warning("column lineage failed for a statement: %s", exc)
            lineage.unsupported.append(repr(exc))

    return lineage


def _analyze_statement(
    statement: exp.Expression,
    lineage: ColumnLineage,
    schema: Optional[Dict[str, Sequence[str]]],
    target_override: Optional[str],
) -> None:
    target_node = _write_target(statement)
    target_name = target_override
    if target_name is None and target_node is not None:
        asset = _asset_from_table(target_node)
        target_name = asset.name if asset else None
    if target_name is None:
        target_name = lineage.target or "(query)"
    lineage.target = target_name

    select = statement.find(exp.Select)
    if select is None:
        return

    resolver = _Resolver(schema)
    for branch in _select_branches(statement):
        scope = build_scope(branch)
        if scope is None:
            continue
        _edges_for_scope(scope, branch, target_name, resolver, lineage)

    for note in resolver.notes:
        if note not in lineage.unresolved:
            lineage.unresolved.append(note)


def _select_branches(statement: exp.Expression) -> List[exp.Select]:
    """Every SELECT that contributes output columns.

    A UNION contributes from both sides; output names come from each branch's
    own aliases, which is what the engine does when the branches disagree.
    """
    union = statement.find(exp.Union)
    if union is not None:
        return [node for node in (union.left, union.right) if isinstance(node, exp.Select)]
    select = statement.find(exp.Select)
    return [select] if select is not None else []


def _edges_for_scope(
    scope: Scope,
    select: exp.Select,
    target_name: str,
    resolver: _Resolver,
    lineage: ColumnLineage,
) -> None:
    for projection in select.expressions:
        if isinstance(projection, exp.Star):
            _expand_star(scope, target_name, resolver, lineage)
            continue

        column_name = projection.alias_or_name
        if not column_name:
            column_name = _snippet(projection)

        if column_name not in lineage.output_columns:
            lineage.output_columns.append(column_name)

        kind = _classify(projection)
        expression = _snippet(projection)
        before = len(resolver.notes)
        sources = resolver.columns_in(scope, projection)
        if not sources:
            # COUNT(*), a literal, a function of nothing. Give it an edge with
            # an empty source: the column exists, depends on no column, and
            # must still be visible when someone deletes it.
            note = "{0!r} has no column inputs ({1})".format(column_name, expression)
            if note not in lineage.notes:
                lineage.notes.append(note)
            _add_edge(
                lineage,
                ColumnEdge(
                    target=ColumnRef(target_name, column_name),
                    source=ColumnRef("", ""),
                    kind=KIND_CONSTANT,
                    expression=expression,
                ),
            )
        ambiguous = len(resolver.notes) > before and any(
            "ambiguous" in note for note in resolver.notes[before:]
        )

        for source in sources:
            _add_edge(
                lineage,
                ColumnEdge(
                    target=ColumnRef(target_name, column_name),
                    source=source,
                    kind=KIND_AMBIGUOUS if ambiguous else kind,
                    expression=expression,
                ),
            )

    _join_key_edges(scope, select, target_name, resolver, lineage)


def _expand_star(
    scope: Scope, target_name: str, resolver: _Resolver, lineage: ColumnLineage
) -> None:
    """Expand ``SELECT *`` from the catalog, or record it as an unknown."""
    for alias, source in scope.sources.items():
        if isinstance(source, exp.Table):
            asset = resolver._source_asset(source)
            if not asset:
                continue
            columns = resolver.columns_of(asset)
            if columns:
                for column in columns:
                    if column not in lineage.output_columns:
                        lineage.output_columns.append(column)
                    _add_edge(
                        lineage,
                        ColumnEdge(
                            target=ColumnRef(target_name, column),
                            source=ColumnRef(asset, column),
                            kind=KIND_DIRECT,
                            expression="*",
                        ),
                    )
                continue

            lineage.unresolved.append(
                "SELECT * over {0}, whose columns are not in the catalog".format(asset)
            )
            if STAR_COLUMN not in lineage.output_columns:
                lineage.output_columns.append(STAR_COLUMN)
            _add_edge(
                lineage,
                ColumnEdge(
                    target=ColumnRef(target_name, STAR_COLUMN),
                    source=ColumnRef(asset, STAR_COLUMN),
                    kind=KIND_UNRESOLVED_STAR,
                    expression="*",
                ),
            )
        elif isinstance(source, Scope):
            for projection in source.expression.expressions:
                name = projection.alias_or_name
                if not name:
                    continue
                if name not in lineage.output_columns:
                    lineage.output_columns.append(name)
                for ref in resolver.resolve(scope, alias, name):
                    _add_edge(
                        lineage,
                        ColumnEdge(
                            target=ColumnRef(target_name, name),
                            source=ref,
                            kind=_classify(projection),
                            expression="*",
                        ),
                    )


def _join_key_edges(
    scope: Scope,
    select: exp.Select,
    target_name: str,
    resolver: _Resolver,
    lineage: ColumnLineage,
) -> None:
    """Columns that decide which rows survive, not which values appear."""
    for join in select.args.get("joins") or []:
        condition = join.args.get("on")
        if condition is None:
            continue
        for ref in resolver.columns_in(scope, condition):
            _add_edge(
                lineage,
                ColumnEdge(
                    target=ColumnRef(target_name, ROWS_COLUMN),
                    source=ref,
                    kind=KIND_JOIN_KEY,
                    expression=_snippet(condition),
                ),
            )


def _add_edge(lineage: ColumnLineage, edge: ColumnEdge) -> None:
    if edge not in lineage.edges:
        lineage.edges.append(edge)
