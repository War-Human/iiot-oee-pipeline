"""
verify_gold.py

Checks gold against silver two ways:
1. Correctness: re-runs the real aggregate_partition() logic from
   build_gold.py against each silver file, and compares the result to
   what's actually sitting in the matching gold file. If they differ,
   something in gold-etl's file-handling/partitioning broke, even if
   the aggregation math itself (covered by test_gold_logic.py) is fine.
2. The honesty regression guard: quality_pct and oee_pct must be null
   on every single gold row, always -- this must never silently start
   getting filled in with a fake number.

Run this from your project's TOP folder:

    python tests/verify_gold.py

Add --check 8 for automated pass/fail:

    python tests/verify_gold.py --check 8
"""

import argparse
import glob
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "gold-etl"))
import build_gold as gold  # noqa: E402

SILVER_GLOB = str(PROJECT_ROOT / "data" / "silver" / "date=*" / "hour=*" / "silver.jsonl")
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"


def load_gold_rows(gold_path: Path) -> list:
    if not gold_path.exists():
        return None
    rows = []
    with open(gold_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", type=int, choices=[8], default=None)
    args = parser.parse_args()

    silver_files = sorted(Path(p) for p in glob.glob(SILVER_GLOB))
    mismatches = []
    missing_gold_files = []
    quality_leaks = []
    total_rows_checked = 0

    for silver_file in silver_files:
        date_part, hour_part = gold.partition_for(silver_file)
        gold_path = GOLD_ROOT / f"date={date_part}" / f"hour={hour_part}" / "kpi_summary.jsonl"

        actual_rows = load_gold_rows(gold_path)
        if actual_rows is None:
            missing_gold_files.append(str(gold_path))
            continue

        expected_rows = gold.aggregate_partition(silver_file)
        expected_by_machine = {r["machine_id"]: r for r in expected_rows}
        actual_by_machine = {r["machine_id"]: r for r in actual_rows}

        for machine_id, expected in expected_by_machine.items():
            total_rows_checked += 1
            actual = actual_by_machine.get(machine_id)
            if actual is None:
                mismatches.append(f"{gold_path}: missing row for {machine_id}")
                continue
            if actual != expected:
                mismatches.append(f"{gold_path} [{machine_id}]: expected {expected} but found {actual}")

        for row in actual_rows:
            if row.get("quality_pct") is not None or row.get("oee_pct") is not None:
                quality_leaks.append(f"{gold_path} [{row.get('machine_id')}]: quality_pct or oee_pct is NOT null")

    print("========================================")
    print(f"Silver partitions found:    {len(silver_files)}")
    print(f"Gold rows checked:          {total_rows_checked}")
    print(f"Missing gold files:         {len(missing_gold_files)} (should be 0 once gold-etl has run)")
    print(f"Mismatched rows:            {len(mismatches)} (should be 0)")
    print(f"quality_pct/oee_pct leaks:  {len(quality_leaks)} (should be 0 -- honesty regression guard)")
    print("========================================")

    if mismatches:
        print("Mismatch details:")
        for m in mismatches[:10]:
            print(f"  {m}")
    if quality_leaks:
        print("Leak details:")
        for leak in quality_leaks[:10]:
            print(f"  {leak}")

    overall = (
        total_rows_checked > 0
        and not missing_gold_files
        and not mismatches
        and not quality_leaks
    )
    print(f"\nOVERALL: {'PASS' if overall else 'FAIL'}")

    if args.check == 8:
        sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()