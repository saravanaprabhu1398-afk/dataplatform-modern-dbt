"""Tests for blast radius: the transitive walk over column edges."""
from pathlib import Path

import pytest

import dataplatform.core.database as db
from dataplatform.core.column_lineage import (
    KIND_AGGREGATE,
    KIND_AMBIGUOUS,
    KIND_DIRECT,
    KIND_JOIN_KEY,
    KIND_UNRESOLVED_STAR,
    STAR_COLUMN,
    extract_column_lineage,
)
from dataplatform.core.lineage_impact import column_impact, record_column_lineage


def edge(target_asset, target_column, source_asset, source_column, kind=KIND_DIRECT):
    return {
        "target_asset": target_asset, "target_column": target_column,
        "source_asset": source_asset, "source_column": source_column,
        "kind": kind, "expression": "",
    }


CHAIN = [
    edge("stg", "amount", "orders", "amount"),
    edge("daily", "gross", "stg", "amount", KIND_AGGREGATE),
    edge("export", "revenue", "daily", "gross"),
    edge("unrelated", "x", "customers", "name"),
]


class TestWalk:
    def test_follows_the_chain_transitively(self):
        report = column_impact("orders", "amount", CHAIN)
        assert [str(hit.column) for hit in report.hits] == [
            "stg.amount", "daily.gross", "export.revenue"
        ]

    def test_hop_counts_grow_with_distance(self):
        hops = {str(hit.column): hit.hops for hit in column_impact("orders", "amount", CHAIN).hits}
        assert hops == {"stg.amount": 1, "daily.gross": 2, "export.revenue": 3}

    def test_unrelated_columns_are_not_reached(self):
        assert "unrelated.x" not in [
            str(hit.column) for hit in column_impact("orders", "amount", CHAIN).hits
        ]

    def test_unknown_column_has_no_impact(self):
        report = column_impact("orders", "nonexistent", CHAIN)
        assert report.hits == []
        assert "no recorded downstream" in report.summary()

    def test_assets_are_listed_once_each(self):
        assert column_impact("orders", "amount", CHAIN).assets == ["stg", "daily", "export"]

    def test_a_cycle_terminates(self):
        cyclic = [edge("a", "x", "b", "x"), edge("b", "x", "a", "x")]
        report = column_impact("a", "x", cyclic)
        assert [str(hit.column) for hit in report.hits] == ["b.x"]


class TestUncertainty:
    def test_a_star_edge_propagates_any_column_of_its_source(self):
        edges = [edge("copy", STAR_COLUMN, "orders", STAR_COLUMN, KIND_UNRESOLVED_STAR)]
        report = column_impact("orders", "amount", edges)

        assert [str(hit.column) for hit in report.hits] == ["copy.*"]
        assert report.uncertain, "a star hit must be flagged as uncertain"

    def test_ambiguous_edges_are_flagged(self):
        edges = [edge("t", "amount", "orders", "amount", KIND_AMBIGUOUS)]
        report = column_impact("orders", "amount", edges)

        assert not report.hits[0].is_certain
        assert len(report.uncertain) == 1

    def test_certain_edges_are_not_flagged(self):
        report = column_impact("orders", "amount", CHAIN)
        assert report.uncertain == []

    def test_join_keys_are_called_out_separately(self):
        edges = [edge("daily", "(rows)", "orders", "id", KIND_JOIN_KEY)]
        report = column_impact("orders", "id", edges)

        assert len(report.breaks_rows) == 1
        assert "which rows exist" in report.summary()


@pytest.fixture()
def store(tmp_path, monkeypatch):
    db_file = tmp_path / "platform.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_file))
    db._initialized = False
    db._DB_PATH = Path(str(db_file))
    db._engine = None
    db.init_db()
    yield db
    db._initialized = False
    db._engine = None


class TestStorage:
    def test_round_trip_through_the_store(self, store):
        lineage = extract_column_lineage(
            "CREATE TABLE daily AS SELECT SUM(amount) AS gross FROM orders"
        )
        written = record_column_lineage(lineage, pipeline_name="p", task_name="t")

        edges = store.get_column_edges(target_asset="daily")
        assert written == 1
        assert edges[0]["source_asset"] == "orders"
        assert edges[0]["kind"] == KIND_AGGREGATE

    def test_rerunning_replaces_rather_than_accumulates(self, store):
        lineage = extract_column_lineage(
            "CREATE TABLE daily AS SELECT SUM(amount) AS gross FROM orders")
        record_column_lineage(lineage)
        record_column_lineage(lineage)

        assert len(store.get_column_edges(target_asset="daily")) == 1

    def test_a_column_that_stops_being_produced_disappears(self, store):
        record_column_lineage(extract_column_lineage(
            "CREATE TABLE daily AS SELECT SUM(amount) AS gross, MAX(amount) AS peak FROM orders"))
        record_column_lineage(extract_column_lineage(
            "CREATE TABLE daily AS SELECT SUM(amount) AS gross FROM orders"))

        columns = {row["target_column"] for row in store.get_column_edges(target_asset="daily")}
        assert columns == {"gross"}

    def test_impact_reads_the_store_by_default(self, store):
        record_column_lineage(extract_column_lineage(
            "CREATE TABLE daily AS SELECT SUM(amount) AS gross FROM orders"))
        record_column_lineage(extract_column_lineage(
            "CREATE TABLE export AS SELECT gross AS revenue FROM daily"))

        report = column_impact("orders", "amount")
        assert [str(hit.column) for hit in report.hits] == ["daily.gross", "export.revenue"]

    def test_nothing_recorded_for_an_empty_lineage(self, store):
        assert record_column_lineage(extract_column_lineage("not sql ((")) == 0
