"""
verify_bronze.py

Run this from your project's TOP folder (the one with docker-compose.yml in it):

    py tests/verify_bronze.py

It reads every file under data/raw/ and prints simple PASS/FAIL results.
You do not need to understand the code to use it -- just run it and read
the printed lines.

For automated scripts (like run_test.ps1), add --check N to also exit
with code 0 (pass) or 1 (fail) for that specific test, e.g.:
    
    py tests/verify_bronze.py --check 1

To capture a "snapshot" of the current state for before/after comparison
(see compare_snapshots.py), add --save-snapshot <path>, e.g.:

    py tests/verify_bronze.py --save-snapshot tests/snapshots/before.json"""


import argparse
import glob
import json
import sys
from pathlib import Path

# This script lives in a "tests" folder one level under the project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_GLOB = str(PROJECT_ROOT / "data" / "raw" / "date=*" / "hour=*" / "bronze.jsonl")

# Change these if you used different test event_ids in TEST 3 of the guide.
DOWNTIME_TEST_IDS = ["mytest-1", "mytest-2", "mytest-3"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", type=int, choices=[1, 2, 3], default=None,
        help="After printing the report, exit 0 (pass) or 1 (fail) for this test number.",)
    parser.add_argument(
        "--save-snapshot", type=str, default=None,
        help="Save a lightweight snapshot of the current state to this file (JSON),"
             " for later comparison with compare_snapshots.py.",
    )
    args = parser.parse_args()

    files = sorted(glob.glob(RAW_GLOB))
    print("========================================")
    print(f"Looking for data in: {PROJECT_ROOT / 'data' / 'raw'}")
    print(f"Files found: {len(files)}")

    if not files:
        print("RESULT: FAIL")
        print("No bronze.jsonl files found at all. Make sure:")
        print("  1) You ran 'docker compose up -d --build' first")
        print("  2) You waited at least 10-20 seconds for data to appear")
        print("  3) You are running this command from the right folder")
        if args.save_snapshot:
            Path(args.save_snapshot).parent.mkdir(parents=True, exist_ok=True)
            Path(args.save_snapshot).write_text(json.dumps({"records": []}, indent=2))
        sys.exit(1)

    total = 0
    broken_lines = 0
    event_ids = {}
    malformed = 0
    machines = {}
    all_records = [] # lightweight extract, kept in file order (== time order)

    for f in files:
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    broken_lines += 1
                    continue
                total += 1
                event_id = record.get("event_id")
                event_ids[event_id] = event_ids.get(event_id, 0) + 1
                if record.get("note"):
                    malformed += 1
                machine_id = record.get("payload", {}).get("machine_id", "UNKNOWN")
                machines[machine_id] = machines.get(machine_id, 0) + 1
                all_records.append({
                    "event_id": event_id,
                    "ingest_ts": record.get("ingest_ts"),
                    "machine_id": machine_id,
                    "status": record.get("payload", {}).get("status"),
                    "note": record.get("note"),
                })
    duplicates = {k: v for k, v in event_ids.items() if v > 1}
    test1_pass = total > 0 and broken_lines == 0 and len(duplicates) == 0
    test3_found = {tid: (tid in event_ids) for tid in DOWNTIME_TEST_IDS}

    print("----------------------------------------")
    print(f"Total messages recorded:     {total}")
    print(f"Completely broken lines:     {broken_lines}  (should be 0)")
    print(f"Unique event_ids:            {len(event_ids)}")
    print(f"Duplicate event_ids:         {len(duplicates)}  (should be 0)")
    print(f"Malformed/parse_error notes: {malformed}  (0 unless you ran TEST 2)")
    print(f"Machines seen and counts:    {machines}")
    print("----------------------------------------")

    print()
    print("TEST 1 (baseline correctness):",
          "PASS" if test1_pass else "FAIL")

    print()
    print("TEST 3 markers (published while bronze-writer was stopped):")
    for test_id in DOWNTIME_TEST_IDS:
        if test3_found[test_id]:
            print(f"  '{test_id}': FOUND -> the message survived (bug may be fixed)")
        else:
            print(f"  '{test_id}': NOT FOUND -> the message was lost (known bug, expected today)")

    print("========================================")
    # Line a script can grab with a simple text search, without parsing JSON.
    print(f"TOTAL_COUNT={total}")

    if args.save_snapshot:
        snapshot_path = Path(args.save_snapshot)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(json.dumps({"records": all_records}, indent=2))
        print(f"Saved snapshot to: {snapshot_path}")

    if args.check == 1:
        sys.exit(0 if test1_pass else 1)
    elif args.check == 2:
        sys.exit(0 if malformed >= 1 else 1)
    elif args.check == 3:
        sys.exit(0 if all(test3_found.values()) else 1)


if __name__ == "__main__":
    main()