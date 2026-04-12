import argparse
import csv
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    curated_rows = []
    with input_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            amount = float(row["amount"])
            curated_rows.append(
                {
                    "order_id": row["order_id"],
                    "customer": row["customer"],
                    "region": row["region"].title(),
                    "amount": f"{amount:.2f}",
                    "status": row["status"],
                    "priority": "high" if amount >= 1500 else "standard",
                }
            )

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(curated_rows[0].keys()))
        writer.writeheader()
        writer.writerows(curated_rows)

    print(f"Curated {len(curated_rows)} rows into {output_path}")


if __name__ == "__main__":
    main()
