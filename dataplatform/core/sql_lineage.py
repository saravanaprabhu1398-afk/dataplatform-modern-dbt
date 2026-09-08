"""Table-level lineage, derived from the SQL rather than guessed at.

The previous extractor matched three regexes over the query text.  That is
fast to write and wrong in ways nobody notices: a CTE name is reported as a
table, the word ``from`` inside a string literal invents one, a ``MERGE`` is
invisible, and ``SELECT * FROM 'data/raw.json'`` produces no lineage at all.

Lineage is a compiler problem.  This module parses the statement, resolves
scope, and reports what the query actually reads and writes:

    >>> extract_lineage("WITH recent AS (SELECT * FROM orders) SELECT id FROM recent")
    SqlLineage(reads=[orders], writes=[], ...)

Design decisions worth knowing:

* **One dialect.**  DuckDB, matching the plugin this serves.  Dialect coverage
  is infinite and proves nothing after the first one.
* **Never raises.**  Lineage is recorded best-effort after a task succeeds; a
  parser that throws would turn an observability feature into an outage.
  Unparseable input is reported in ``unsupported`` and the rest is returned.
* **Identity is normalised once.**  ``public.orders``, ``orders`` and
  ``"Orders"`` are the same asset.  Quoted identifiers keep their case,
  because in a case-sensitive catalog they genuinely differ.
* **A file is not a table.**  ``FROM 'data/raw.json'`` and
  ``read_csv('data/raw.csv')`` are file assets, and saying so is the point:
  the regex silently dropped both.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

logger = logging.getLogger(__name__)

DEFAULT_DIALECT = "duckdb"

#: Schemas that mean "no schema in particular" and are dropped from identity.
DEFAULT_SCHEMAS = {"main", "public"}

#: Extensions that make a bare string a file rather than a table name.
FILE_SUFFIXES = (".csv", ".json", ".jsonl", ".parquet", ".ndjson", ".tsv", ".txt", ".xlsx")

KIND_TABLE = "table"
KIND_FILE = "file"


@dataclass(frozen=True)
class AssetRef:
    """One asset a statement touches."""

    name: str
    kind: str = KIND_TABLE

    @property
    def uri(self) -> str:
        """Stable URI, matching the convention already in the lineage store."""
        if self.kind == KIND_FILE:
            return "file://{0}".format(self.name)
        return "duckdb://local/{0}".format(self.name)

    def __str__(self) -> str:  # pragma: no cover - debugging affordance
        return self.name


@dataclass
class SqlLineage:
    """What a statement reads and writes."""

    reads: List[AssetRef] = field(default_factory=list)
    writes: List[AssetRef] = field(default_factory=list)
    unsupported: List[str] = field(default_factory=list)

    @property
    def parsed(self) -> bool:
        return not self.unsupported

    def as_uris(self) -> Dict[str, List[str]]:
        """The shape the lineage recorder already consumes."""
        return {
            "reads_from": [asset.uri for asset in self.reads],
            "writes_to": [asset.uri for asset in self.writes],
        }

    def names(self) -> Dict[str, List[str]]:
        """Bare names, for tests and human-readable output."""
        return {
            "reads_from": [asset.name for asset in self.reads],
            "writes_to": [asset.name for asset in self.writes],
        }


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def _looks_like_path(name: str) -> bool:
    lowered = name.lower()
    return "/" in name or lowered.endswith(FILE_SUFFIXES)


def _identifier_text(node: Optional[exp.Expression]) -> str:
    """Identifier text, lowercased unless it was quoted."""
    if node is None:
        return ""
    if isinstance(node, exp.Identifier):
        return node.name if node.quoted else node.name.lower()
    return str(node).lower()


def _asset_from_table(node: exp.Table) -> Optional[AssetRef]:
    """Normalise a table node into an asset, or None when it is not one."""
    name = _identifier_text(node.this) if isinstance(node.this, exp.Identifier) else ""
    if not name:
        return None

    if _looks_like_path(name):
        return AssetRef(name=name, kind=KIND_FILE)

    schema = _identifier_text(node.args.get("db"))
    catalog = _identifier_text(node.args.get("catalog"))
    parts = [part for part in (catalog, schema) if part and part not in DEFAULT_SCHEMAS]
    parts.append(name)
    return AssetRef(name=".".join(parts), kind=KIND_TABLE)


def _files_in_table_function(node: exp.Table) -> List[AssetRef]:
    """File assets named by a table function such as ``read_csv('x.csv')``."""
    assets = []
    for literal in node.find_all(exp.Literal):
        if literal.is_string and _looks_like_path(literal.this):
            assets.append(AssetRef(name=literal.this, kind=KIND_FILE))
    return assets


# ---------------------------------------------------------------------------
# Statement analysis
# ---------------------------------------------------------------------------

def _write_target(statement: exp.Expression) -> Optional[exp.Table]:
    """The table a statement writes to, if it writes to one."""
    if not isinstance(
        statement, (exp.Insert, exp.Create, exp.Update, exp.Delete, exp.Merge)
    ):
        return None

    target = statement.this
    if isinstance(target, exp.Schema):      # CREATE TABLE t (cols)
        target = target.this
    if isinstance(target, exp.Alias):       # MERGE INTO t AS d
        target = target.this
    return target if isinstance(target, exp.Table) else None


def _cte_names(statement: exp.Expression) -> Set[str]:
    """Names defined by CTEs in this statement -- never real assets."""
    return {
        cte.alias_or_name.lower()
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name
    }


def _is_cte_reference(node: exp.Table, cte_names: Set[str]) -> bool:
    """True when this table node is really a reference to a CTE.

    A schema-qualified name is never a CTE reference, so a CTE that shadows a
    table name does not hide ``public.recent``.
    """
    if node.args.get("db") or node.args.get("catalog"):
        return False
    return _identifier_text(node.this).lower() in cte_names


def _analyze(statement: exp.Expression, lineage: SqlLineage) -> None:
    """Add one statement's reads and writes to *lineage*."""
    cte_names = _cte_names(statement)
    target = _write_target(statement)

    if target is not None:
        asset = _asset_from_table(target)
        if asset is not None:
            _add(lineage.writes, asset)

    for node in statement.find_all(exp.Table):
        if node is target:
            continue
        if _is_cte_reference(node, cte_names):
            continue

        asset = _asset_from_table(node)
        if asset is not None:
            _add(lineage.reads, asset)
            continue

        # A table node with no name is a table function: read_csv('x.csv').
        for file_asset in _files_in_table_function(node):
            _add(lineage.reads, file_asset)


def _add(collection: List[AssetRef], asset: AssetRef) -> None:
    """Append, preserving order and ignoring repeats."""
    if asset not in collection:
        collection.append(asset)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def extract_lineage(sql: str, dialect: str = DEFAULT_DIALECT) -> SqlLineage:
    """Return what *sql* reads and writes. Never raises."""
    lineage = SqlLineage()
    if not sql or not sql.strip():
        return lineage

    try:
        statements = sqlglot.parse(sql, read=dialect)
    except ParseError as exc:
        logger.debug("could not parse SQL for lineage: %s", exc)
        lineage.unsupported.append(str(exc).splitlines()[0])
        return lineage
    except Exception as exc:  # defensive: lineage must never break a task
        logger.warning("unexpected error parsing SQL for lineage: %s", exc)
        lineage.unsupported.append(repr(exc))
        return lineage

    for statement in statements:
        if statement is None:
            continue
        try:
            _analyze(statement, lineage)
        except Exception as exc:  # one bad statement must not lose the others
            logger.warning("lineage analysis failed for a statement: %s", exc)
            lineage.unsupported.append(repr(exc))

    # A table that is both written and read -- INSERT INTO t SELECT FROM t --
    # legitimately appears in both lists.  Only the target *node* is excluded
    # from reads, not every mention of its name.
    return lineage


def extract_lineage_uris(sql: str, dialect: str = DEFAULT_DIALECT) -> Dict[str, List[str]]:
    """Drop-in replacement for the previous regex extractor's return shape."""
    return extract_lineage(sql, dialect=dialect).as_uris()
