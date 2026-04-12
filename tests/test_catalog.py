"""Tests for the data catalog (catalog.py)."""
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    import dataplatform.core.database as db_module
    db_file = str(tmp_path / "catalog_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    db_module.init_db()
    yield
    db_module._initialized = False


from dataplatform.core.database import save_lineage_record
from dataplatform.core.catalog import search_assets, get_asset_detail, get_pipeline_catalog, _infer_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def seed_lineage(*records):
    """Insert lineage records: (run_id, pipeline, task, direction, asset_uri)."""
    for r in records:
        save_lineage_record(*r)


# ---------------------------------------------------------------------------
# _infer_type
# ---------------------------------------------------------------------------

class TestInferType:
    def test_postgres_scheme(self):
        assert _infer_type("postgres://host/db/table") == "PostgreSQL"

    def test_postgresql_scheme(self):
        assert _infer_type("postgresql://host/db") == "PostgreSQL"

    def test_s3_scheme(self):
        assert _infer_type("s3://bucket/key") == "S3"

    def test_duckdb_scheme(self):
        assert _infer_type("duckdb://data/db.db") == "DuckDB"

    def test_kafka_scheme(self):
        assert _infer_type("kafka://broker/topic") == "Kafka"

    def test_unknown_scheme(self):
        t = _infer_type("myproto://server/path")
        assert t == "Myproto"

    def test_no_scheme(self):
        assert _infer_type("just-a-path") == "Unknown"


# ---------------------------------------------------------------------------
# search_assets
# ---------------------------------------------------------------------------

class TestSearchAssets:
    def test_empty_when_no_lineage(self):
        assert search_assets() == []

    def test_returns_known_assets(self):
        seed_lineage(("r1", "pipe_a", "extract", "reads_from", "postgres://db/orders"))
        assets = search_assets()
        assert len(assets) == 1
        assert assets[0]["asset_uri"] == "postgres://db/orders"

    def test_counts_reads_and_writes(self):
        seed_lineage(
            ("r1", "pipe_a", "extract", "reads_from", "postgres://db/orders"),
            ("r2", "pipe_a", "load", "writes_to", "postgres://db/orders"),
        )
        assets = search_assets()
        row = next(a for a in assets if a["asset_uri"] == "postgres://db/orders")
        assert row["read_count"] == 1
        assert row["write_count"] == 1

    def test_pipeline_count_distinct(self):
        seed_lineage(
            ("r1", "pipe_a", "t1", "reads_from", "s3://bucket/data.csv"),
            ("r2", "pipe_b", "t2", "reads_from", "s3://bucket/data.csv"),
            ("r3", "pipe_a", "t3", "reads_from", "s3://bucket/data.csv"),  # same pipe_a
        )
        assets = search_assets()
        row = next(a for a in assets if a["asset_uri"] == "s3://bucket/data.csv")
        assert row["pipeline_count"] == 2  # pipe_a and pipe_b

    def test_query_filter(self):
        seed_lineage(
            ("r1", "p", "t", "reads_from", "postgres://db/orders"),
            ("r2", "p", "t", "reads_from", "s3://bucket/sales"),
        )
        result = search_assets(query="postgres")
        assert len(result) == 1
        assert "postgres" in result[0]["asset_uri"]

    def test_query_filter_no_match(self):
        seed_lineage(("r1", "p", "t", "reads_from", "postgres://db/orders"))
        assert search_assets(query="kafka") == []

    def test_asset_type_inferred(self):
        seed_lineage(("r1", "p", "t", "reads_from", "duckdb://data/db"))
        result = search_assets()
        assert result[0]["asset_type"] == "DuckDB"

    def test_limit_respected(self):
        for i in range(10):
            seed_lineage((f"r{i}", "p", "t", "reads_from", f"s3://bucket/file{i}.csv"))
        result = search_assets(limit=3)
        assert len(result) <= 3


# ---------------------------------------------------------------------------
# get_asset_detail
# ---------------------------------------------------------------------------

class TestGetAssetDetail:
    def test_returns_none_for_unknown(self):
        assert get_asset_detail("kafka://topic/x") is None

    def test_returns_detail_for_known(self):
        seed_lineage(("r1", "orders_pipe", "load", "writes_to", "s3://output/orders.csv"))
        detail = get_asset_detail("s3://output/orders.csv")
        assert detail is not None
        assert detail["asset_uri"] == "s3://output/orders.csv"
        assert detail["pipeline_count"] == 1

    def test_pipelines_list(self):
        seed_lineage(
            ("r1", "pipe_a", "task1", "reads_from", "db://shared"),
            ("r2", "pipe_b", "task2", "writes_to", "db://shared"),
        )
        detail = get_asset_detail("db://shared")
        names = {p["pipeline_name"] for p in detail["pipelines"]}
        assert "pipe_a" in names
        assert "pipe_b" in names

    def test_asset_type_in_detail(self):
        seed_lineage(("r1", "p", "t", "reads_from", "snowflake://account/db"))
        detail = get_asset_detail("snowflake://account/db")
        assert detail["asset_type"] == "Snowflake"


# ---------------------------------------------------------------------------
# get_pipeline_catalog
# ---------------------------------------------------------------------------

class TestGetPipelineCatalog:
    def test_empty_when_no_lineage(self):
        assert get_pipeline_catalog() == []

    def test_returns_pipeline_with_asset_counts(self):
        seed_lineage(
            ("r1", "my_pipe", "extract", "reads_from", "postgres://db/t"),
            ("r1", "my_pipe", "load", "writes_to", "s3://bucket/out"),
        )
        catalog = get_pipeline_catalog()
        row = next(r for r in catalog if r["pipeline_name"] == "my_pipe")
        assert row["asset_count"] == 2
        assert row["inputs"] == 1
        assert row["outputs"] == 1
