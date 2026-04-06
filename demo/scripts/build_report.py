import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--json-output", required=True)
    parser.add_argument("--markdown-output", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    json_output = Path(args.json_output)
    markdown_output = Path(args.markdown_output)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    totals_by_region = defaultdict(float)
    high_priority_orders = 0

    with input_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
            amount = float(row["amount"])
            totals_by_region[row["region"]] += amount
            if row["priority"] == "high":
                high_priority_orders += 1

    total_revenue = sum(float(row["amount"]) for row in rows)
    summary = {
        "row_count": len(rows),
        "total_revenue": round(total_revenue, 2),
        "high_priority_orders": high_priority_orders,
        "regions": {region: round(total, 2) for region, total in sorted(totals_by_region.items())},
    }

    json_output.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    markdown_lines = [
        "# Local Demo Sales Report",
        "",
        f"- Orders processed: **{summary['row_count']}**",
        f"- Total revenue: **${summary['total_revenue']:.2f}**",
        f"- High priority orders: **{summary['high_priority_orders']}**",
        "",
        "## Revenue by Region",
        "",
    ]
    for region, amount in summary["regions"].items():
        markdown_lines.append(f"- {region}: ${amount:.2f}")

    markdown_output.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    print(f"Wrote summary report to {json_output} and {markdown_output}")


if __name__ == "__main__":
    main()
