"""Tests for data lineage recording and graph construction."""
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "lineage_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    yield
    db_module._initialized = False


from dataplatform.core.config import TaskLineage
from dataplatform.core.database import init_db, get_full_lineage_graph, get_lineage_for_asset
from dataplatform.core.lineage import record_task_lineage, build_lineage_graph, get_asset_lineage


class TestRecordTaskLineage:
    def setup_method(self):
        init_db()

    def test_records_reads_from(self):
        lineage = TaskLineage(reads_from=["postgres://mydb/orders"])
        record_task_lineage("run-1", "orders_pipe", "load", lineage)
        records = get_lineage_for_asset("postgres://mydb/orders")
        assert len(records) == 1
        assert records[0]["direction"] == "reads_from"
        assert records[0]["task_name"] == "load"

    def test_records_writes_to(self):
        lineage = TaskLineage(writes_to=["s3://bucket/orders.parquet"])
        record_task_lineage("run-1", "orders_pipe", "load", lineage)
        records = get_lineage_for_asset("s3://bucket/orders.parquet")
        assert records[0]["direction"] == "writes_to"

    def test_records_both_directions(self):
        lineage = TaskLineage(
            reads_from=["postgres://mydb/orders"],
            writes_to=["s3://bucket/orders.parquet"],
        )
        record_task_lineage("run-1", "pipe", "transform", lineage)
        reads = get_lineage_for_asset("postgres://mydb/orders")
        writes = get_lineage_for_asset("s3://bucket/orders.parquet")
        assert len(reads) == 1
        assert len(writes) == 1

    def test_multiple_assets_per_direction(self):
        lineage = TaskLineage(reads_from=["db://a", "db://b", "db://c"])
        record_task_lineage("run-1", "pipe", "merge", lineage)
        for asset in ["db://a", "db://b", "db://c"]:
            assert len(get_lineage_for_asset(asset)) == 1

    def test_empty_lineage_records_nothing(self):
        lineage = TaskLineage()
        record_task_lineage("run-1", "pipe", "task", lineage)
        assert get_full_lineage_graph() == []

    def test_invalid_db_does_not_raise(self, monkeypatch):
        """Lineage errors are swallowed with a warning — never crash the pipeline."""
        from unittest.mock import patch
        lineage = TaskLineage(reads_from=["db://x"])
        with patch("dataplatform.core.lineage.save_lineage_record", side_effect=Exception("DB error")):
            record_task_lineage("run-1", "pipe", "task", lineage)  # should not raise


class TestBuildLineageGraph:
    def setup_method(self):
        init_db()

    def test_empty_graph(self):
        graph = build_lineage_graph()
        assert graph["nodes"] == []
        assert graph["edges"] == []

    def test_graph_has_asset_and_task_nodes(self):
        lineage = TaskLineage(
            reads_from=["postgres://mydb/orders"],
            writes_to=["duckdb://data/orders.csv"],
        )
        record_task_lineage("r1", "orders_pipe", "load", lineage)
        graph = build_lineage_graph()

        types = {n["id"]: n["type"] for n in graph["nodes"]}
        assert types["postgres://mydb/orders"] == "asset"
        assert types["duckdb://data/orders.csv"] == "asset"
        assert types["orders_pipe/load"] == "task"

    def test_reads_from_edge_direction(self):
        lineage = TaskLineage(reads_from=["src://a"])
        record_task_lineage("r1", "pipe", "task", lineage)
        graph = build_lineage_graph()
        edge = graph["edges"][0]
        assert edge["from"] == "src://a"
        assert edge["to"] == "pipe/task"
        assert edge["direction"] == "reads_from"

    def test_writes_to_edge_direction(self):
        lineage = TaskLineage(writes_to=["dest://b"])
        record_task_lineage("r1", "pipe", "task", lineage)
        graph = build_lineage_graph()
        edge = graph["edges"][0]
        assert edge["from"] == "pipe/task"
        assert edge["to"] == "dest://b"
        assert edge["direction"] == "writes_to"


class TestGetAssetLineage:
    def setup_method(self):
        init_db()

    def test_upstream_producers(self):
        lineage = TaskLineage(writes_to=["s3://out/data.csv"])
        record_task_lineage("r1", "pipe", "export", lineage)
        result = get_asset_lineage("s3://out/data.csv")
        assert result["asset_uri"] == "s3://out/data.csv"
        assert len(result["upstream"]) == 1
        assert result["upstream"][0]["task_name"] == "export"

    def test_downstream_consumers(self):
        lineage = TaskLineage(reads_from=["kafka://topic/orders"])
        record_task_lineage("r1", "pipe", "ingest", lineage)
        result = get_asset_lineage("kafka://topic/orders")
        assert len(result["downstream"]) == 1
        assert result["downstream"][0]["task_name"] == "ingest"

    def test_unknown_asset_returns_empty_lists(self):
        result = get_asset_lineage("unknown://asset")
        assert result["upstream"] == []
        assert result["downstream"] == []
