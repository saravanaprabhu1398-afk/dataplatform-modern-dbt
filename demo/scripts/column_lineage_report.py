"""Column lineage coverage, stated with a denominator.

Runs the parser over a directory of SQL models and reports how many output
columns it could fully resolve -- and, more usefully, which ones it could not
and why. A coverage number without a denominator, or without the list of
unresolved columns, is marketing.

    python demo/scripts/column_lineage_report.py
    python demo/scripts/column_lineage_report.py --impact orders.amount
"""
import sys
from pathlib import Path

MODELS = Path(__file__).resolve().parents[1] / "fixtures" / "models"

# What the catalog knows. Everything absent from here is a genuine unknown,
# which is the point: unresolved columns should be visible, not assumed.
# Shared with `dataplatform lineage --schema`, so the two cannot disagree.
import json as _json

SCHEMA = _json.loads((MODELS.parent / "catalog.json").read_text())


def main(argv):
    from dataplatform.core.column_lineage import (
        KIND_AMBIGUOUS,
        KIND_JOIN_KEY,
        KIND_UNRESOLVED_STAR,
        extract_column_lineage,
    )
    from dataplatform.core.lineage_impact import column_impact

    files = sorted(MODELS.glob("*.sql"))
    if not files:
        print("no models found in {0}".format(MODELS))
        return 1

    all_edges = []
    resolved_total = column_total = 0
    unresolved_notes = []
    informational = []

    print("{0:<24} {1:>8} {2:>10} {3:>10} {4:>10}".format(
        "model", "columns", "resolved", "join keys", "edges"))
    print("-" * 66)

    for path in files:
        lineage = extract_column_lineage(path.read_text(), schema=SCHEMA)
        resolved, total = lineage.coverage()
        resolved_total += resolved
        column_total += total

        join_keys = sum(1 for edge in lineage.edges if edge.kind == KIND_JOIN_KEY)
        print("{0:<24} {1:>8} {2:>10} {3:>10} {4:>10}".format(
            path.stem, total, resolved, join_keys, len(lineage.edges)))

        for note in lineage.unresolved:
            unresolved_notes.append("{0}: {1}".format(path.stem, note))
        for note in lineage.notes:
            informational.append("{0}: {1}".format(path.stem, note))

        all_edges.extend(
            {
                "target_asset": edge.target.asset, "target_column": edge.target.column,
                "source_asset": edge.source.asset, "source_column": edge.source.column,
                "kind": edge.kind, "expression": edge.expression,
            }
            for edge in lineage.edges
        )

    print("\n{0} of {1} output columns fully resolved across {2} models".format(
        resolved_total, column_total, len(files)))
    assumed = [e for e in all_edges if e["kind"] in (KIND_UNRESOLVED_STAR, KIND_AMBIGUOUS)]
    print("{0} column(s) not fully resolved, {1} of them resting on an assumption:".format(
        column_total - resolved_total, len(assumed)))
    for note in unresolved_notes:
        print("  - {0}".format(note))
    if informational:
        print("\nknown to depend on no column (not a gap):")
        for note in informational:
            print("  - {0}".format(note))

    target = None
    for index, arg in enumerate(argv):
        if arg == "--impact" and index + 1 < len(argv):
            target = argv[index + 1]
    if target:
        asset, _, column = target.rpartition(".")
        print("")
        print(column_impact(asset, column, all_edges).summary())

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
