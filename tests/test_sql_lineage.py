"""Tests for parser-derived SQL lineage.

The corpus in ``demo/fixtures/sql_lineage_cases.json`` is the contract: every
case carries the expected reads and writes, written by hand, and a note saying
what it is there to catch. The same file scores the superseded regex extractor
in ``demo/scripts/lineage_parser_scorecard.py``, so the two never drift apart.
"""
import json
from pathlib import Path

import pytest

from dataplatform.core.sql_lineage import (
    AssetRef,
    KIND_FILE,
    KIND_TABLE,
    extract_lineage,
    extract_lineage_uris,
)

CASES_PATH = Path(__file__).resolve().parents[1] / "demo" / "fixtures" / "sql_lineage_cases.json"
CASES = json.loads(CASES_PATH.read_text())["cases"]


def ids(case):
    return case["id"]


class TestCorpus:
    @pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
    def test_expected_lineage(self, case):
        names = extract_lineage(case["sql"]).names()

        assert sorted(names["reads_from"]) == sorted(case["reads"]), case["why"]
        assert sorted(names["writes_to"]) == sorted(case["writes"]), case["why"]

    def test_corpus_is_not_trivially_small(self):
        # The corpus is the argument. If it shrinks, the argument shrinks.
        assert len(CASES) >= 25
        assert len({case["id"] for case in CASES}) == len(CASES)


class TestNeverRaises:
    """Lineage is recorded after a task succeeds. It must not become an outage."""

    @pytest.mark.parametrize(
        "sql",
        ["", "   ", "this is not sql at all ((", "SELECT FROM WHERE",
         "DROP TABLE;;;", "\x00\x01", "SELECT * FROM " + "a" * 5000],
    )
    def test_bad_input_is_reported_not_raised(self, sql):
        lineage = extract_lineage(sql)
        assert isinstance(lineage.reads, list)
        assert isinstance(lineage.writes, list)

    def test_unparseable_sql_is_flagged(self):
        lineage = extract_lineage("this is not sql at all ((")
        assert not lineage.parsed
        assert lineage.unsupported
        assert lineage.reads == [] and lineage.writes == []

    def test_valid_sql_is_marked_parsed(self):
        assert extract_lineage("SELECT * FROM orders").parsed


class TestIdentity:
    def test_default_schemas_are_dropped(self):
        for sql in ("SELECT * FROM public.orders", "SELECT * FROM main.orders"):
            assert extract_lineage(sql).names()["reads_from"] == ["orders"]

    def test_non_default_schema_is_kept(self):
        assert extract_lineage("SELECT * FROM finance.orders").names()["reads_from"] == [
            "finance.orders"
        ]

    def test_unquoted_case_folds_but_quoted_does_not(self):
        assert extract_lineage("SELECT * FROM ORDERS").names()["reads_from"] == ["orders"]
        assert extract_lineage('SELECT * FROM "ORDERS"').names()["reads_from"] == ["ORDERS"]

    def test_files_and_tables_are_different_kinds(self):
        files = extract_lineage("SELECT * FROM 'data/raw.json'").reads
        tables = extract_lineage("SELECT * FROM orders").reads

        assert files[0].kind == KIND_FILE
        assert tables[0].kind == KIND_TABLE

    def test_uris_distinguish_kind(self):
        assert AssetRef("orders").uri == "duckdb://local/orders"
        assert AssetRef("data/raw.json", KIND_FILE).uri == "file://data/raw.json"

    def test_uri_shape_matches_the_recorder(self):
        uris = extract_lineage_uris("INSERT INTO fact SELECT * FROM stg")
        assert uris == {
            "reads_from": ["duckdb://local/stg"],
            "writes_to": ["duckdb://local/fact"],
        }


class TestOrderingAndDuplicates:
    def test_repeated_sources_appear_once(self):
        sql = "SELECT * FROM orders a JOIN orders b ON a.id = b.pid JOIN orders c ON c.id = a.id"
        assert extract_lineage(sql).names()["reads_from"] == ["orders"]

    def test_read_order_follows_the_query(self):
        sql = "SELECT * FROM zebra JOIN apple ON apple.id = zebra.id"
        assert extract_lineage(sql).names()["reads_from"] == ["zebra", "apple"]
