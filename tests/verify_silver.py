"""
verify_silver.py

Checks that the silver layer reconciles correctly against bronze:
every valid bronze record should produce exactly one silver record,
every invalid (parse_error) bronze record should produce exactly one
silver reject, there should be no duplicates, and no batch_complete
record should ever be flagged for missing temperature/rpm (the
regression this whole rebuild was partly about).

Run this from your project's TOP folder:

    python tests/verify_silver.py

Add --check N for automated pass/fail (exit code 0/1):

    python tests/verify_silver.py --check 6
"""

import argparse
import glob
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_GLOB = str(PROJECT_ROOT / "data" / "raw" / "date=*" / "hour=*" / "bronze.jsonl")
SILVER_GLOB = str(PROJECT_ROOT / "data" / "silver" / "date=*" / "hour=*" / "silver.jsonl")
SILVER_REJECTS_GLOB = str(PROJECT_ROOT / "data" / "silver" / "date=*" / "hour=*" / "rejects.jsonl")


def count_bronze():
    valid, invalid = 0, 0
    for f in glob.glob(BRONZE_GLOB):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("note"):
                    invalid += 1
                else:
                    valid += 1
    return valid, invalid


def load_silver():
    records = []
    for f in sorted(glob.glob(SILVER_GLOB)):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def count_silver_rejects():
    count = 0
    for f in glob.glob(SILVER_REJECTS_GLOB):
        with open(f, encoding="utf-8") as fh:
            count += sum(1 for line in fh if line.strip())
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", type=int, choices=[6, 7], default=None)
    args = parser.parse_args()

    bronze_valid, bronze_invalid = count_bronze()
    silver_records = load_silver()
    silver_rejects = count_silver_rejects()

    event_ids = [r.get("event_id") for r in silver_records]
    duplicates = len(event_ids) - len(set(event_ids))

    batch_complete_bugs = [
        r for r in silver_records
        if r.get("payload", {}).get("status") == "batch_complete"
        and ("temperature_missing" in r.get("dq_flags", []) or "rpm_missing" in r.get("dq_flags", []))
    ]

    print("========================================")
    print(f"Bronze valid records:      {bronze_valid}")
    print(f"Bronze invalid records:    {bronze_invalid} (parse errors)")
    print(f"Silver records:            {len(silver_records)}")
    print(f"Silver reject records:     {silver_rejects}")
    print("----------------------------------------")
    print(f"Silver duplicates:         {duplicates} (should be 0)")
    print(f"batch_complete false-flags: {len(batch_complete_bugs)} (should be 0 -- regression guard)")
    print("========================================")

    reconciled = (len(silver_records) == bronze_valid) and (silver_rejects == bronze_invalid)
    no_dupes = duplicates == 0
    no_batch_bug = len(batch_complete_bugs) == 0

    print(f"Reconciliation (silver == bronze valid, rejects == bronze invalid): "
          f"{'PASS' if reconciled else 'FAIL'}")
    print(f"No duplicates: {'PASS' if no_dupes else 'FAIL'}")
    print(f"No batch_complete false-flags: {'PASS' if no_batch_bug else 'FAIL'}")

    overall = reconciled and no_dupes and no_batch_bug
    print(f"\nOVERALL: {'PASS' if overall else 'FAIL'}")

    if args.check in (6, 7):
        sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()