import argparse
import csv
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = [
        {"order_id": "1001", "customer": "Acme Retail", "region": "north", "amount": "1250.50", "status": "paid"},
        {"order_id": "1002", "customer": "Blue Ocean", "region": "south", "amount": "980.00", "status": "paid"},
        {"order_id": "1003", "customer": "Acme Retail", "region": "north", "amount": "410.25", "status": "pending"},
        {"order_id": "1004", "customer": "Summit Foods", "region": "west", "amount": "2200.00", "status": "paid"},
        {"order_id": "1005", "customer": "Delta Stores", "region": "east", "amount": "1575.75", "status": "paid"},
        {"order_id": "1006", "customer": "Blue Ocean", "region": "south", "amount": "640.00", "status": "paid"},
    ]

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} demo orders at {output_path}")


if __name__ == "__main__":
    main()
