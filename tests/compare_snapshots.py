"""
compare_snapshots.py

Compares two snapshot files (made with verify_bronze.py --save-snapshot)
and prints exactly what changed between them: totals, the last entry in
each, and every new entry that appeared in between.

Usage:
    python tests/compare_snapshots.py tests/snapshots/before.json tests/snapshots/after.json
"""

import json
import sys
from pathlib import Path


def load(path: str) -> list:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return data.get("records", [])


def describe(record: dict) -> str:
    if record is None:
        return "(none)"
    return (
        f"event_id={record.get('event_id')} | "
        f"time={record.get('ingest_ts')} | "
        f"machine={record.get('machine_id')} | "
        f"status={record.get('status')}"
        + (f" | note={record.get('note')}" if record.get("note") else "")
    )


def main():
    if len(sys.argv) != 3:
        print("Usage: python compare_snapshots.py <before.json> <after.json>")
        sys.exit(1)

    before = load(sys.argv[1])
    after = load(sys.argv[2])

    before_ids = {r["event_id"] for r in before}
    after_ids = {r["event_id"] for r in after}
    new_ids = after_ids - before_ids

    new_records = [r for r in after if r["event_id"] in new_ids]
    # Keep them in the order they were written (== chronological order).
    new_records.sort(key=lambda r: after.index(r))

    print("========================================")
    print("BEFORE")
    print(f"  Total entries: {len(before)}")
    print(f"  Last entry:    {describe(before[-1] if before else None)}")
    print()
    print("AFTER")
    print(f"  Total entries: {len(after)}")
    print(f"  Last entry:    {describe(after[-1] if after else None)}")
    print("----------------------------------------")
    print(f"NEW ENTRIES DURING THIS WINDOW: {len(new_records)}")
    print("----------------------------------------")

    if not new_records:
        print("  (nothing new arrived between the two snapshots)")
    else:
        # Show every new entry if there aren't too many; otherwise show the
        # first and last few so it stays readable.
        if len(new_records) <= 30:
            for r in new_records:
                print(f"  {describe(r)}")
        else:
            for r in new_records[:15]:
                print(f"  {describe(r)}")
            print(f"  ... ({len(new_records) - 30} more not shown) ...")
            for r in new_records[-15:]:
                print(f"  {describe(r)}")

    print("========================================")


if __name__ == "__main__":
    main()
    