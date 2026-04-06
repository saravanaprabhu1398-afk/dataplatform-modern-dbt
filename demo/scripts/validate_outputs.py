import argparse
import csv
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--curated-input", required=True)
    parser.add_argument("--summary-input", required=True)
    parser.add_argument("--markdown-input", required=True)
    args = parser.parse_args()

    curated_path = Path(args.curated_input)
    summary_path = Path(args.summary_input)
    markdown_path = Path(args.markdown_input)

    missing = [str(path) for path in [curated_path, summary_path, markdown_path] if not path.exists()]
    if missing:
        print(f"Missing expected outputs: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    with curated_path.open("r", newline="", encoding="utf-8") as handle:
        row_count = sum(1 for _ in csv.DictReader(handle))

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")

    if row_count != summary.get("row_count"):
        print("Summary row_count does not match curated CSV", file=sys.stderr)
        sys.exit(1)

    if "Local Demo Sales Report" not in markdown:
        print("Markdown report header missing", file=sys.stderr)
        sys.exit(1)

    print("Validation succeeded for curated data and generated reports")


if __name__ == "__main__":
    main()
