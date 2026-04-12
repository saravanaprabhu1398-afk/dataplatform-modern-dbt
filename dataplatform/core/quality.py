"""Data quality check runner.

Quality checks are declared in the pipeline YAML as DuckDB SQL queries that
return a scalar.  After a task runs successfully, these checks are executed
and their results are persisted to the ``quality_results`` DB table.

Example YAML:

    tasks:
      - name: load_orders
        plugin: duckdb
        quality:
          checks:
            - name: no_nulls
              sql: "SELECT COUNT(*) FROM 'data/orders.csv' WHERE id IS NULL"
              expect: 0
            - name: min_row_count
              sql: "SELECT COUNT(*) FROM 'data/orders.csv'"
              expect_min: 10

DuckDB can query CSV, Parquet, JSON, and remote files out of the box, so the
SQL checks work across many storage formats without extra configuration.
"""
import logging
from typing import Any, Dict, List, Optional

from dataplatform.core.config import QualityCheck
from dataplatform.core.database import get_quality_results, save_quality_result

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Check evaluation
# ---------------------------------------------------------------------------

def _evaluate(check: QualityCheck, actual: Any) -> bool:
    """Return True if *actual* satisfies the check's expectations."""
    if check.expect is not None:
        # Try numeric comparison first, fall back to equality
        try:
            return float(actual) == float(check.expect)
        except (TypeError, ValueError):
            return actual == check.expect

    passed = True
    if check.expect_min is not None:
        try:
            passed = passed and float(actual) >= float(check.expect_min)
        except (TypeError, ValueError):
            passed = False

    if check.expect_max is not None:
        try:
            passed = passed and float(actual) <= float(check.expect_max)
        except (TypeError, ValueError):
            passed = False

    return passed


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_quality_checks(
    checks: List[QualityCheck],
    run_id: str,
    pipeline_name: str,
    task_name: str,
) -> List[Dict[str, Any]]:
    """Execute *checks* using DuckDB and persist results.

    Returns a list of result dicts.  Each dict has:
      - name: check name
      - passed: bool
      - actual: the scalar returned by the SQL
      - expected: the expectation from config
      - error: error message if the SQL raised an exception
    """
    import duckdb

    results: List[Dict[str, Any]] = []
    conn = duckdb.connect(":memory:")

    try:
        for check in checks:
            result_dict = _run_single_check(conn, check)
            results.append(result_dict)

            _expected_str = _format_expectation(check)
            save_quality_result(
                run_id=run_id,
                pipeline_name=pipeline_name,
                task_name=task_name,
                check_name=check.name,
                passed=result_dict["passed"],
                actual_value=str(result_dict["actual"]) if result_dict["actual"] is not None else None,
                expected_value=_expected_str,
                error=result_dict.get("error"),
            )

            level = logging.INFO if result_dict["passed"] else logging.WARNING
            logger.log(
                level,
                "Quality check '%s.%s.%s': %s (actual=%s, expected=%s)",
                pipeline_name, task_name, check.name,
                "PASSED" if result_dict["passed"] else "FAILED",
                result_dict["actual"],
                _expected_str,
            )
    finally:
        conn.close()

    return results


def _run_single_check(conn: Any, check: QualityCheck) -> Dict[str, Any]:
    """Execute one check SQL and return a result dict."""
    try:
        row = conn.execute(check.sql).fetchone()
        actual = row[0] if row else None
        passed = _evaluate(check, actual)
        return {
            "name": check.name,
            "passed": passed,
            "actual": actual,
            "expected": _format_expectation(check),
            "error": None,
        }
    except Exception as exc:
        return {
            "name": check.name,
            "passed": False,
            "actual": None,
            "expected": _format_expectation(check),
            "error": str(exc),
        }


def _format_expectation(check: QualityCheck) -> str:
    if check.expect is not None:
        return f"== {check.expect}"
    parts = []
    if check.expect_min is not None:
        parts.append(f">= {check.expect_min}")
    if check.expect_max is not None:
        parts.append(f"<= {check.expect_max}")
    return " and ".join(parts) if parts else "any"


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_pipeline_quality_history(pipeline_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return the most recent quality check results for *pipeline_name*."""
    return get_quality_results(pipeline_name, limit)
