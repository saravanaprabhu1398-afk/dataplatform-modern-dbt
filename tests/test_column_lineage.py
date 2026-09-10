"""Tests for column-level lineage.

The interesting assertions are not "an edge exists" but what *kind* of edge it
is, and what happens when the query cannot be resolved. A lineage tool that
quietly under-reports is worse than none, so the unresolved cases get as much
attention as the resolved ones.
"""
import pytest

from dataplatform.core.column_lineage import (
    KIND_AGGREGATE,
    KIND_AMBIGUOUS,
    KIND_DERIVED,
    KIND_DIRECT,
    KIND_JOIN_KEY,
    KIND_UNRESOLVED_STAR,
    ROWS_COLUMN,
    ColumnRef,
    extract_column_lineage,
)

SCHEMA = {
    "orders": ["id", "customer_id", "amount", "created_at"],
    "customers": ["id", "region", "name"],
    "discounts": ["order_id", "value"],
}


def edges(sql, **kwargs):
    return extract_column_lineage(sql, **kwargs).edges


def pairs(sql, **kwargs):
    return {
        (str(edge.target), str(edge.source), edge.kind)
        for edge in edges(sql, **kwargs)
    }


class TestKinds:
    def test_direct_column(self):
        assert ("t.region", "customers.region", KIND_DIRECT) in pairs(
            "CREATE TABLE t AS SELECT region FROM customers"
        )

    def test_aggregate(self):
        assert ("t.gross", "orders.amount", KIND_AGGREGATE) in pairs(
            "CREATE TABLE t AS SELECT SUM(amount) AS gross FROM orders"
        )

    def test_derived_expression(self):
        assert ("t.day", "orders.created_at", KIND_DERIVED) in pairs(
            "CREATE TABLE t AS SELECT DATE_TRUNC('day', created_at) AS day FROM orders"
        )

    def test_case_expression_has_every_input(self):
        result = pairs(
            "CREATE TABLE t AS SELECT CASE WHEN amount > 0 THEN status ELSE 'none' END "
            "AS bucket FROM orders"
        )
        assert ("t.bucket", "orders.amount", KIND_DERIVED) in result
        assert ("t.bucket", "orders.status", KIND_DERIVED) in result

    def test_window_function_depends_on_partition_and_order(self):
        result = pairs(
            "CREATE TABLE t AS SELECT ROW_NUMBER() OVER "
            "(PARTITION BY customer_id ORDER BY created_at) AS rn FROM orders"
        )
        assert ("t.rn", "orders.customer_id", KIND_DERIVED) in result
        assert ("t.rn", "orders.created_at", KIND_DERIVED) in result


class TestJoinKeys:
    def test_join_columns_are_recorded_against_rows(self):
        result = pairs(
            "CREATE TABLE t AS SELECT c.region FROM orders o "
            "JOIN customers c ON c.id = o.customer_id"
        )
        assert ("t.{0}".format(ROWS_COLUMN), "customers.id", KIND_JOIN_KEY) in result
        assert ("t.{0}".format(ROWS_COLUMN), "orders.customer_id", KIND_JOIN_KEY) in result

    def test_join_key_is_not_confused_with_an_output_column(self):
        lineage = extract_column_lineage(
            "CREATE TABLE t AS SELECT c.region FROM orders o "
            "JOIN customers c ON c.id = o.customer_id"
        )
        assert lineage.target_columns == ["region"]

    def test_cross_join_without_condition_adds_no_keys(self):
        result = pairs("CREATE TABLE t AS SELECT o.id FROM orders o CROSS JOIN customers c")
        assert not any(kind == KIND_JOIN_KEY for _, _, kind in result)


class TestThroughScopes:
    def test_resolves_through_a_cte(self):
        sql = """
        CREATE TABLE t AS
        WITH recent AS (SELECT id, amount FROM orders)
        SELECT SUM(r.amount) AS gross FROM recent r
        """
        assert ("t.gross", "orders.amount", KIND_AGGREGATE) in pairs(sql)

    def test_resolves_through_nested_ctes(self):
        sql = """
        CREATE TABLE t AS
        WITH a AS (SELECT id, amount FROM orders),
             b AS (SELECT id, amount * 2 AS doubled FROM a)
        SELECT doubled FROM b
        """
        assert ("t.doubled", "orders.amount", KIND_DIRECT) in pairs(sql)

    def test_resolves_through_a_subquery(self):
        sql = "CREATE TABLE t AS SELECT x.amount FROM (SELECT amount FROM orders) x"
        assert ("t.amount", "orders.amount", KIND_DIRECT) in pairs(sql)

    def test_union_contributes_from_both_branches(self):
        sql = """
        CREATE TABLE t AS
        SELECT id FROM orders
        UNION ALL
        SELECT id FROM archived_orders
        """
        result = pairs(sql)
        assert ("t.id", "orders.id", KIND_DIRECT) in result
        assert ("t.id", "archived_orders.id", KIND_DIRECT) in result

    def test_self_join_aliases_resolve_to_one_asset(self):
        sql = ("CREATE TABLE t AS SELECT a.amount FROM orders a "
               "JOIN orders b ON b.parent_id = a.id")
        assert ("t.amount", "orders.amount", KIND_DIRECT) in pairs(sql)


class TestTargets:
    def test_insert_select_targets_the_inserted_table(self):
        assert extract_column_lineage(
            "INSERT INTO fact SELECT amount FROM orders"
        ).target == "fact"

    def test_create_view_targets_the_view(self):
        assert extract_column_lineage(
            "CREATE VIEW v AS SELECT amount FROM orders"
        ).target == "v"

    def test_bare_select_has_a_placeholder_target(self):
        lineage = extract_column_lineage("SELECT amount FROM orders")
        assert lineage.target == "(query)"

    def test_explicit_target_overrides(self):
        lineage = extract_column_lineage("SELECT amount FROM orders", target="my_model")
        assert lineage.target == "my_model"
        assert lineage.edges[0].target == ColumnRef("my_model", "amount")


class TestStarExpansion:
    def test_star_expands_when_the_catalog_knows_the_columns(self):
        result = pairs("CREATE TABLE t AS SELECT * FROM customers", schema=SCHEMA)

        assert ("t.region", "customers.region", KIND_DIRECT) in result
        assert ("t.name", "customers.name", KIND_DIRECT) in result
        assert not any(kind == KIND_UNRESOLVED_STAR for _, _, kind in result)

    def test_star_is_an_explicit_unknown_without_a_catalog(self):
        lineage = extract_column_lineage("CREATE TABLE t AS SELECT * FROM mystery")

        assert [edge.kind for edge in lineage.edges] == [KIND_UNRESOLVED_STAR]
        assert lineage.unresolved
        assert "mystery" in lineage.unresolved[0]

    def test_unresolved_star_is_never_silent(self):
        # The failure mode this guards: emitting nothing and looking complete.
        lineage = extract_column_lineage("CREATE TABLE t AS SELECT * FROM mystery")
        assert lineage.edges, "an unexpandable star must still produce an edge"

    def test_star_through_a_cte_resolves_from_its_projections(self):
        sql = """
        CREATE TABLE t AS
        WITH recent AS (SELECT id, SUM(amount) AS gross FROM orders GROUP BY id)
        SELECT * FROM recent
        """
        result = pairs(sql)
        assert ("t.id", "orders.id", KIND_DIRECT) in result
        assert ("t.gross", "orders.amount", KIND_AGGREGATE) in result

    def test_coverage_counts_unresolved_columns(self):
        known = extract_column_lineage(
            "CREATE TABLE t AS SELECT amount FROM orders")
        unknown = extract_column_lineage(
            "CREATE TABLE t AS SELECT * FROM mystery")

        assert known.coverage() == (1, 1)
        assert unknown.coverage() == (0, 1)


class TestAmbiguity:
    def test_unqualified_column_with_two_sources_reports_both(self):
        sql = ("CREATE TABLE t AS SELECT amount FROM orders o "
               "JOIN customers c ON c.id = o.customer_id")
        result = pairs(sql)

        assert ("t.amount", "orders.amount", KIND_AMBIGUOUS) in result
        assert ("t.amount", "customers.amount", KIND_AMBIGUOUS) in result

    def test_the_catalog_settles_ambiguity(self):
        sql = ("CREATE TABLE t AS SELECT amount FROM orders o "
               "JOIN customers c ON c.id = o.customer_id")
        result = pairs(sql, schema=SCHEMA)

        assert ("t.amount", "orders.amount", KIND_DIRECT) in result
        assert not any(kind == KIND_AMBIGUOUS for _, _, kind in result)

    def test_ambiguity_is_noted(self):
        lineage = extract_column_lineage(
            "CREATE TABLE t AS SELECT amount FROM orders o "
            "JOIN customers c ON c.id = o.customer_id"
        )
        assert any("ambiguous" in note for note in lineage.unresolved)

    def test_single_source_needs_no_qualification(self):
        assert ("t.amount", "orders.amount", KIND_DIRECT) in pairs(
            "CREATE TABLE t AS SELECT amount FROM orders"
        )


class TestNeverRaises:
    @pytest.mark.parametrize(
        "sql",
        ["", "   ", "not sql at all ((", "SELECT FROM WHERE", "\x00",
         "CREATE TABLE t AS SELECT", "WITH a AS (SELECT 1) SELECT"],
    )
    def test_bad_input_is_reported_not_raised(self, sql):
        lineage = extract_column_lineage(sql)
        assert isinstance(lineage.edges, list)

    def test_unparseable_is_flagged(self):
        lineage = extract_column_lineage("not sql at all ((")
        assert not lineage.parsed
        assert lineage.edges == []

    def test_constant_projection_has_no_sources(self):
        lineage = extract_column_lineage("CREATE TABLE t AS SELECT 1 AS one FROM orders")
        assert lineage.edges == []


class TestQuerying:
    def test_sources_for_a_column(self):
        sql = ("CREATE TABLE t AS SELECT SUM(o.amount) - SUM(d.value) AS net "
               "FROM orders o JOIN discounts d ON d.order_id = o.id")
        sources = {str(ref) for ref in
                   extract_column_lineage(sql).sources_for("net")}

        assert sources == {"orders.amount", "discounts.value"}


class TestColumnsWithoutSources:
    """A column with no column inputs still exists, and still counts."""

    def test_count_star_is_an_output_column(self):
        lineage = extract_column_lineage(
            "CREATE TABLE t AS SELECT day, COUNT(*) AS orders FROM stg GROUP BY 1"
        )
        assert "orders" in lineage.target_columns

    def test_count_star_counts_against_coverage(self):
        lineage = extract_column_lineage(
            "CREATE TABLE t AS SELECT day, COUNT(*) AS orders FROM stg GROUP BY 1"
        )
        assert lineage.coverage() == (1, 2)

    def test_the_reason_is_recorded(self):
        lineage = extract_column_lineage(
            "CREATE TABLE t AS SELECT COUNT(*) AS orders FROM stg")
        assert any("no column inputs" in note for note in lineage.unresolved)

    def test_a_literal_column_is_counted_too(self):
        lineage = extract_column_lineage(
            "CREATE TABLE t AS SELECT 1 AS one, amount FROM orders")
        assert lineage.target_columns == ["one", "amount"]
        assert lineage.coverage() == (1, 2)
