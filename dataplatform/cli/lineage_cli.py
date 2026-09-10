"""``dataplatform lineage`` -- ask the graph a question, or gate a change on it.

    dataplatform lineage scan   --models demo/fixtures/models --write
    dataplatform lineage impact --column orders.amount
    dataplatform lineage check  --models demo/fixtures/models --git-ref main
    dataplatform lineage drift  --pipelines pipelines

``impact`` answers "can I drop this?" and exits non-zero when the answer is no.
``check`` and ``drift`` are the CI surface: they fail a pull request with the
list of columns it would break, and with any declared lineage that no longer
matches the SQL.
"""
from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import typer

lineage_app = typer.Typer(help="Column-level lineage: coverage, impact and CI checks.")

DEFAULT_MODELS = "demo/fixtures/models"
DEFAULT_PIPELINES = "pipelines"


def _model_paths(directory: str) -> List[Path]:
    paths = sorted(Path(directory).glob("*.sql"))
    if not paths:
        raise typer.BadParameter("no .sql files found in {0}".format(directory))
    return paths


def _load_schema(path: Optional[str]) -> Optional[Dict[str, Sequence[str]]]:
    """A catalog is what turns SELECT * from an unknown into an answer."""
    if not path:
        return None
    return json.loads(Path(path).read_text())


def _models_at_ref(paths: Sequence[Path], ref: str, schema) -> Dict:
    """Parse the same models as they were at a git ref.

    Used as the "before" graph so the check works on a fresh clone in CI,
    without depending on a populated lineage store.
    """
    from dataplatform.core.lineage_checks import parse_models

    with tempfile.TemporaryDirectory(prefix="lineage-ref-") as workdir:
        restored = []
        for path in paths:
            result = subprocess.run(
                ["git", "show", "{0}:{1}".format(ref, path.as_posix())],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                continue  # the file did not exist at that ref: a new model
            target = Path(workdir) / path.name
            target.write_text(result.stdout)
            restored.append(target)
        return parse_models(restored, schema=schema)


@lineage_app.command("scan")
def scan(
    models: str = typer.Option(DEFAULT_MODELS, help="Directory of .sql models."),
    schema: Optional[str] = typer.Option(None, help="JSON catalog: {asset: [columns]}."),
    write: bool = typer.Option(False, "--write", help="Persist edges to the lineage store."),
):
    """Parse models and report column coverage, with a denominator."""
    from dataplatform.core.database import init_db
    from dataplatform.core.lineage_checks import parse_models
    from dataplatform.core.lineage_impact import record_column_lineage

    parsed = parse_models(_model_paths(models), schema=_load_schema(schema))
    resolved_total = column_total = edge_total = 0

    typer.echo("{0:<26} {1:>8} {2:>10} {3:>8}".format("asset", "columns", "resolved", "edges"))
    typer.echo("-" * 56)
    for asset, lineage in sorted(parsed.items()):
        resolved, total = lineage.coverage()
        resolved_total += resolved
        column_total += total
        edge_total += len(lineage.edges)
        typer.echo("{0:<26} {1:>8} {2:>10} {3:>8}".format(
            asset, total, resolved, len(lineage.edges)))

    typer.echo("\n{0} of {1} output columns fully resolved across {2} assets, {3} edges".format(
        resolved_total, column_total, len(parsed), edge_total))

    for asset, lineage in sorted(parsed.items()):
        for note in lineage.unresolved:
            typer.echo("  unresolved  {0}: {1}".format(asset, note))
    for asset, lineage in sorted(parsed.items()):
        for note in lineage.notes:
            typer.echo("  note        {0}: {1}".format(asset, note))

    if write:
        init_db()
        written = sum(record_column_lineage(lineage) for lineage in parsed.values())
        typer.echo("\nrecorded {0} edge(s) to the lineage store".format(written))


@lineage_app.command("impact")
def impact(
    column: str = typer.Option(..., help="Column to trace, as asset.column."),
    models: Optional[str] = typer.Option(None, help="Parse this directory instead of the store."),
    schema: Optional[str] = typer.Option(None, help="JSON catalog: {asset: [columns]}."),
    as_json: bool = typer.Option(False, "--json", help="Emit the report as JSON."),
):
    """What breaks if this column changes. Exits 1 when anything reads it."""
    from dataplatform.core.lineage_checks import edges_of, parse_models
    from dataplatform.core.lineage_impact import column_impact

    asset, _, name = column.rpartition(".")
    if not asset or not name:
        raise typer.BadParameter("expected asset.column, got {0!r}".format(column))

    edges = None
    if models:
        edges = edges_of(parse_models(_model_paths(models), schema=_load_schema(schema)))

    report = column_impact(asset, name, edges)

    if as_json:
        typer.echo(json.dumps(
            {
                "column": column,
                "hits": [
                    {"column": str(hit.column), "kind": hit.kind, "hops": hit.hops,
                     "certain": hit.is_certain, "expression": hit.expression}
                    for hit in report.hits
                ],
                "assets": report.assets,
                "uncertain": len(report.uncertain),
                "join_keys": len(report.breaks_rows),
            },
            indent=2,
        ))
    else:
        typer.echo(report.summary())

    if report.hits:
        raise typer.Exit(1)


@lineage_app.command("check")
def check(
    models: str = typer.Option(DEFAULT_MODELS, help="Directory of .sql models."),
    git_ref: Optional[str] = typer.Option(
        None, "--git-ref", help="Compare against the models at this ref instead of the store."
    ),
    schema: Optional[str] = typer.Option(None, help="JSON catalog: {asset: [columns]}."),
    strict: bool = typer.Option(False, "--strict", help="Fail on warnings too."),
    annotate: bool = typer.Option(
        False,
        "--annotate",
        help="Also emit GitHub Actions annotations, so the finding appears on the "
        "pull request instead of only in the job log.",
    ),
):
    """Fail when a change removes a column something downstream reads."""
    from dataplatform.core.database import get_column_edges, init_db
    from dataplatform.core.lineage_checks import (
        CheckReport,
        check_removed_columns,
        check_unresolved,
        edges_of,
        parse_models,
    )

    paths = _model_paths(models)
    catalog = _load_schema(schema)
    after = parse_models(paths, schema=catalog)

    if git_ref:
        before_edges = edges_of(_models_at_ref(paths, git_ref, catalog))
        source = "models at {0}".format(git_ref)
    else:
        init_db()
        before_edges = get_column_edges()
        source = "the lineage store"

    report = CheckReport()
    report.extend(check_removed_columns(before_edges, after))
    report.extend(check_unresolved(after))

    typer.echo("comparing {0} model(s) against {1}\n".format(len(after), source))
    typer.echo(report.render())

    if annotate:
        for finding in report.findings:
            typer.echo(finding.as_github_annotation())

    if report.errors or (strict and report.warnings):
        raise typer.Exit(1)


@lineage_app.command("drift")
def drift(
    pipelines: str = typer.Option(DEFAULT_PIPELINES, help="Directory of pipeline YAML."),
):
    """Fail when declared reads_from disagrees with what the SQL reads."""
    from dataplatform.core.lineage_checks import CheckReport, check_declared_drift

    paths = sorted(Path(pipelines).glob("*.yaml")) + sorted(Path(pipelines).glob("*.yml"))
    if not paths:
        raise typer.BadParameter("no pipeline YAML found in {0}".format(pipelines))

    report = CheckReport().extend(check_declared_drift(paths))
    typer.echo("checked {0} pipeline file(s)\n".format(len(paths)))
    typer.echo(report.render())

    if report.errors:
        raise typer.Exit(1)
