"""Tests for the lineage CI checks.

Two properties matter more than coverage here. A check that misses a real
breakage is useless; a check that reports a breakage that is not real gets
switched off, which is worse. Both directions are tested.
"""
import json
from pathlib import Path

import pytest

from dataplatform.core.column_lineage import KIND_DIRECT, extract_column_lineage
from dataplatform.core.lineage_checks import (
    CODE_COLUMN_ORPHANED,
    CODE_COLUMN_REMOVED,
    CODE_DECLARED_DRIFT,
    CODE_UNRESOLVED,
    SEVERITY_ERROR,
    SEVERITY_WARNING,
    CheckReport,
    Finding,
    check_declared_drift,
    check_removed_columns,
    check_unresolved,
    edges_of,
    parse_models,
    produced_columns,
    runtime_bindings,
)

CATALOG = {
    "orders": ["id", "customer_id", "amount", "created_at"],
    "customers": ["id", "region"],
}


def write(directory, name, sql):
    path = Path(directory) / name
    path.write_text(sql)
    return path


@pytest.fixture()
def models(tmp_path):
    write(tmp_path, "daily.sql",
          "CREATE TABLE daily AS SELECT day, SUM(amount) AS gross, COUNT(*) AS n "
          "FROM orders GROUP BY day")
    write(tmp_path, "export.sql",
          "CREATE TABLE export AS SELECT day, gross AS revenue FROM daily")
    return tmp_path


class TestParsing:
    def test_models_are_keyed_by_the_asset_they_write(self, models):
        parsed = parse_models(sorted(models.glob("*.sql")))
        assert set(parsed) == {"daily", "export"}

    def test_a_bare_select_is_not_a_model(self, tmp_path):
        write(tmp_path, "q.sql", "SELECT amount FROM orders")
        assert parse_models(sorted(tmp_path.glob("*.sql"))) == {}

    def test_unreadable_files_are_skipped(self, tmp_path):
        assert parse_models([tmp_path / "missing.sql"]) == {}

    def test_edges_flatten_for_the_impact_walk(self, models):
        edges = edges_of(parse_models(sorted(models.glob("*.sql"))))
        assert {"target_asset", "target_column", "source_asset", "source_column",
                "kind", "expression"} <= set(edges[0])

    def test_produced_columns_ignores_the_rows_sentinel(self):
        edges = [
            {"target_asset": "t", "target_column": "a", "source_asset": "s",
             "source_column": "x", "kind": KIND_DIRECT},
            {"target_asset": "t", "target_column": "(rows)", "source_asset": "s",
             "source_column": "k", "kind": "join_key"},
        ]
        assert produced_columns(edges, "t") == ["a"]


class TestRemovedColumns:
    def _before(self, models):
        return edges_of(parse_models(sorted(models.glob("*.sql"))))

    def test_removing_a_read_column_is_an_error(self, models, tmp_path):
        before = self._before(models)
        write(models, "daily.sql",
              "CREATE TABLE daily AS SELECT day, COUNT(*) AS n FROM orders GROUP BY day")
        after = parse_models([models / "daily.sql"])

        findings = check_removed_columns(before, after)

        assert [f.code for f in findings] == [CODE_COLUMN_REMOVED]
        assert findings[0].severity == SEVERITY_ERROR
        assert "daily.gross" == findings[0].asset
        assert any("export.revenue" in detail for detail in findings[0].details)

    def test_removing_an_unread_column_is_only_a_warning(self, models):
        before = self._before(models)
        write(models, "daily.sql",
              "CREATE TABLE daily AS SELECT day, SUM(amount) AS gross FROM orders GROUP BY day")
        after = parse_models([models / "daily.sql"])

        findings = check_removed_columns(before, after)

        assert [f.code for f in findings] == [CODE_COLUMN_ORPHANED]
        assert findings[0].severity == SEVERITY_WARNING

    def test_an_unchanged_model_produces_no_findings(self, models):
        before = self._before(models)
        after = parse_models(sorted(models.glob("*.sql")))
        assert check_removed_columns(before, after) == []

    def test_adding_a_column_is_not_a_finding(self, models):
        before = self._before(models)
        write(models, "daily.sql",
              "CREATE TABLE daily AS SELECT day, SUM(amount) AS gross, COUNT(*) AS n, "
              "MAX(amount) AS peak FROM orders GROUP BY day")
        after = parse_models([models / "daily.sql"])

        assert check_removed_columns(before, after) == []

    def test_a_brand_new_model_cannot_remove_anything(self, tmp_path):
        write(tmp_path, "new.sql", "CREATE TABLE brand_new AS SELECT amount FROM orders")
        after = parse_models([tmp_path / "new.sql"])

        assert check_removed_columns([], after) == []

    def test_the_blast_radius_is_computed_on_the_before_graph(self, models):
        # After the change the reader is broken, not absent -- so it must still
        # be found, which means walking the graph as it was.
        before = self._before(models)
        write(models, "daily.sql",
              "CREATE TABLE daily AS SELECT day FROM orders GROUP BY day")
        after = parse_models(sorted(models.glob("*.sql")))

        removed = [f for f in check_removed_columns(before, after)
                   if f.code == CODE_COLUMN_REMOVED]
        assert any("export.revenue" in d for f in removed for d in f.details)


class TestUnresolved:
    def test_unresolvable_star_is_reported_with_a_denominator(self, tmp_path):
        write(tmp_path, "copy.sql", "CREATE TABLE copy AS SELECT * FROM mystery")
        findings = check_unresolved(parse_models([tmp_path / "copy.sql"]))

        assert findings[0].code == CODE_UNRESOLVED
        assert findings[0].severity == SEVERITY_WARNING
        assert "0 of 1" in findings[0].message

    def test_a_fully_resolved_model_is_silent(self, tmp_path):
        write(tmp_path, "ok.sql", "CREATE TABLE ok AS SELECT amount FROM orders")
        assert check_unresolved(parse_models([tmp_path / "ok.sql"])) == []


class TestDeclaredDrift:
    def _pipeline(self, tmp_path, task):
        import yaml

        path = tmp_path / "p.yaml"
        path.write_text(yaml.safe_dump({"pipeline_name": "p", "tasks": [task]}))
        return [path]

    def test_missing_declaration_is_an_error(self, tmp_path):
        paths = self._pipeline(tmp_path, {
            "name": "t", "plugin": "duckdb",
            "config": {"sql": "SELECT * FROM orders JOIN customers ON true"},
            "lineage": {"reads_from": ["duckdb://local/orders"]},
        })
        findings = check_declared_drift(paths)

        assert findings[0].code == CODE_DECLARED_DRIFT
        assert any("customers" in detail and "not declared" in detail
                   for detail in findings[0].details)

    def test_stale_declaration_is_an_error(self, tmp_path):
        paths = self._pipeline(tmp_path, {
            "name": "t", "plugin": "duckdb",
            "config": {"sql": "SELECT * FROM orders"},
            "lineage": {"reads_from": ["duckdb://local/orders", "duckdb://local/legacy"]},
        })
        findings = check_declared_drift(paths)

        assert any("legacy" in detail and "not read" in detail
                   for detail in findings[0].details)

    def test_agreement_produces_nothing(self, tmp_path):
        paths = self._pipeline(tmp_path, {
            "name": "t", "plugin": "duckdb",
            "config": {"sql": "SELECT * FROM orders"},
            "lineage": {"reads_from": ["duckdb://local/orders"]},
        })
        assert check_declared_drift(paths) == []

    def test_tasks_without_a_declaration_are_skipped(self, tmp_path):
        paths = self._pipeline(tmp_path, {
            "name": "t", "plugin": "duckdb", "config": {"sql": "SELECT * FROM orders"},
        })
        assert check_declared_drift(paths) == []


class TestRuntimeBindings:
    """The DuckDB executor materialises `data` from file_path before the SQL runs."""

    def test_duckdb_data_alias_resolves_to_the_file(self):
        task = {"plugin": "duckdb", "config": {"file_path": "data/sales/raw.csv"}}
        assert runtime_bindings(task) == {"data": "file://data/sales/raw.csv"}

    def test_no_binding_without_a_file_path(self):
        assert runtime_bindings({"plugin": "duckdb", "config": {}}) == {}

    def test_other_plugins_bind_nothing(self):
        assert runtime_bindings({"plugin": "postgres", "config": {"file_path": "x.csv"}}) == {}

    def test_a_correctly_declared_file_task_is_not_flagged(self, tmp_path):
        # The false positive this exists to prevent: the SQL says `data`, the
        # declaration says the file, and both are right.
        import yaml

        path = tmp_path / "p.yaml"
        path.write_text(yaml.safe_dump({"pipeline_name": "p", "tasks": [{
            "name": "top", "plugin": "duckdb",
            "config": {"file_path": "data/sales/raw.csv",
                       "sql": "SELECT product FROM data GROUP BY product"},
            "lineage": {"reads_from": ["file://data/sales/raw.csv"]},
        }]}))

        assert check_declared_drift([path]) == []


class TestReport:
    def test_errors_decide_the_verdict(self):
        report = CheckReport().extend([
            Finding(SEVERITY_WARNING, CODE_UNRESOLVED, "a", "just a note"),
        ])
        assert report.ok

        report.extend([Finding(SEVERITY_ERROR, CODE_COLUMN_REMOVED, "b", "broken")])
        assert not report.ok

    def test_empty_report_says_so(self):
        assert "no findings" in CheckReport().render()

    def test_render_counts_both_severities(self):
        report = CheckReport().extend([
            Finding(SEVERITY_ERROR, CODE_COLUMN_REMOVED, "a", "x"),
            Finding(SEVERITY_WARNING, CODE_UNRESOLVED, "b", "y"),
        ])
        assert "1 error(s), 1 warning(s)" in report.render()


class TestGitHubAnnotations:
    """A failing check that says only "exit code 1" has told the reader nothing."""

    def test_an_error_renders_as_an_error_annotation(self, models):
        before = edges_of(parse_models(sorted(models.glob("*.sql"))))
        write(models, "daily.sql",
              "CREATE TABLE daily AS SELECT day, COUNT(*) AS n FROM orders GROUP BY day")
        after = parse_models([models / "daily.sql"])

        line = check_removed_columns(before, after)[0].as_github_annotation()

        assert line.startswith("::error ")
        assert "daily.gross" in line
        assert "export.revenue" in line

    def test_the_annotation_points_at_the_file(self, models):
        before = edges_of(parse_models(sorted(models.glob("*.sql"))))
        write(models, "daily.sql", "CREATE TABLE daily AS SELECT day FROM orders")
        after = parse_models([models / "daily.sql"])

        line = check_removed_columns(before, after)[0].as_github_annotation()

        assert "file={0}".format(models / "daily.sql") in line

    def test_a_warning_renders_as_a_warning(self, tmp_path):
        write(tmp_path, "copy.sql", "CREATE TABLE copy AS SELECT * FROM mystery")
        line = check_unresolved(parse_models([tmp_path / "copy.sql"]))[0].as_github_annotation()

        assert line.startswith("::warning ")

    def test_a_finding_without_a_file_still_renders(self):
        line = Finding(SEVERITY_ERROR, CODE_COLUMN_REMOVED, "a.b", "gone").as_github_annotation()

        assert line == "::error ::a.b: gone"

    def test_annotations_are_single_line(self, models):
        # A workflow command spanning lines is silently ignored by the runner.
        before = edges_of(parse_models(sorted(models.glob("*.sql"))))
        write(models, "daily.sql", "CREATE TABLE daily AS SELECT day FROM orders")
        after = parse_models([models / "daily.sql"])

        for finding in check_removed_columns(before, after):
            assert "\n" not in finding.as_github_annotation()
