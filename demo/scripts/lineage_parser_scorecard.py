"""Score the old regex extractor against the parser, case by case.

Both implementations are run over the same hand-checked corpus
(`demo/fixtures/sql_lineage_cases.json`), so the improvement is a measurement
rather than a claim. The regex version is kept callable precisely so this
comparison stays runnable instead of becoming an assertion about deleted code.

    python demo/scripts/lineage_parser_scorecard.py
    python demo/scripts/lineage_parser_scorecard.py --verbose
    python demo/scripts/lineage_parser_scorecard.py --pipelines
"""
import json
import sys
from pathlib import Path

CASES = Path(__file__).resolve().parents[1] / "fixtures" / "sql_lineage_cases.json"


def load_cases():
    return json.loads(CASES.read_text())["cases"]


def regex_result(sql):
    from dataplatform.plugins.executors.duckdb_plugin import _extract_sql_lineage

    try:
        out = _extract_sql_lineage(sql)
    except Exception as exc:  # the old one can throw on odd input
        return {"reads": ["<error: {0}>".format(exc)], "writes": []}
    strip = lambda uris: sorted(u.replace("duckdb://local/", "") for u in uris)
    return {"reads": strip(out["reads_from"]), "writes": strip(out["writes_to"])}


def parser_result(sql):
    from dataplatform.core.sql_lineage import extract_lineage

    names = extract_lineage(sql).names()
    return {"reads": sorted(names["reads_from"]), "writes": sorted(names["writes_to"])}


def matches(result, case):
    return (
        result["reads"] == sorted(case["reads"])
        and result["writes"] == sorted(case["writes"])
    )


def scan_pipelines():
    """Report tasks in pipelines/ whose recorded lineage the parser changes."""
    import glob

    import yaml

    from dataplatform.core.sql_lineage import extract_lineage
    from dataplatform.plugins.executors.duckdb_plugin import _extract_sql_lineage

    print("\nlineage that would change in this repo's own pipelines:")
    changed = 0
    for path in sorted(glob.glob("pipelines/*.yaml")):
        try:
            document = yaml.safe_load(Path(path).read_text()) or {}
        except Exception as exc:
            print("  {0}: unreadable ({1})".format(path, exc))
            continue

        for task in document.get("tasks") or []:
            sql = (task.get("config") or {}).get("sql")
            if not sql:
                continue
            old = _extract_sql_lineage(sql)
            new = extract_lineage(sql).as_uris()
            if old == new:
                continue
            changed += 1
            print("\n  {0}  task={1}".format(path, task.get("name") or task.get("id")))
            print("    sql     {0}".format(" ".join(sql.split())[:84]))
            print("    regex   reads={0} writes={1}".format(old["reads_from"], old["writes_to"]))
            print("    parsed  reads={0} writes={1}".format(new["reads_from"], new["writes_to"]))

    print("\n  {0} task(s) currently recording lineage the SQL does not support".format(changed))
    return changed


def main(argv):
    verbose = "--verbose" in argv
    cases = load_cases()

    regex_ok = parser_ok = 0
    failures = []

    for case in cases:
        old = regex_result(case["sql"])
        new = parser_result(case["sql"])
        old_pass, new_pass = matches(old, case), matches(new, case)
        regex_ok += old_pass
        parser_ok += new_pass
        if not new_pass:
            failures.append((case, new))
        if verbose:
            print("{0:<34} regex {1}  parser {2}".format(
                case["id"], "ok  " if old_pass else "WRONG", "ok" if new_pass else "WRONG"))
            if not old_pass:
                print("    expected reads={0} writes={1}".format(
                    sorted(case["reads"]), sorted(case["writes"])))
                print("    regex    reads={0} writes={1}".format(old["reads"], old["writes"]))

    total = len(cases)
    print("\n{0:<28} {1:>8} {2:>10}".format("", "correct", "of total"))
    print("{0:<28} {1:>8} {2:>10}".format("regex extractor", regex_ok, total))
    print("{0:<28} {1:>8} {2:>10}".format("sqlglot parser", parser_ok, total))
    print("\n{0} cases the regex gets wrong, silently.".format(total - regex_ok))

    for case, got in failures:
        print("\nPARSER FAILURE: {0} ({1})".format(case["id"], case["why"]))
        print("  sql      {0}".format(case["sql"].replace("\n", " ")))
        print("  expected reads={0} writes={1}".format(
            sorted(case["reads"]), sorted(case["writes"])))
        print("  got      reads={0} writes={1}".format(got["reads"], got["writes"]))

    if "--pipelines" in argv:
        scan_pipelines()

    return 0 if parser_ok == total else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
