"""
build_gold.py

Aggregates the silver layer into business-ready KPI rows: one row per
(date, hour, machine_id), matching bronze/silver's own partitioning.

Honest scope -- read this before trusting the numbers:
  - Availability% and downtime are computed from real status transitions.
  - Performance% compares real cycle_time_min (from batch_complete events)
    against each machine type's best-case cycle time.
  - Quality% and Wastage% are NOT computed. There is currently no data
    source for units rejected or material wasted anywhere in bronze or
    silver -- rather than silently assume 100% quality (which would make
    a fake number look real), every gold row explicitly carries
    quality_pct: null and a note explaining why.
  - Manpower utilization is NOT computed for the same reason -- no labor
    data exists yet.
  - A full, real OEE score (Availability x Performance x Quality) is
    therefore NOT computed here on purpose -- multiplying in a missing
    Quality term would produce a number that looks precise but isn't.

How Availability is actually computed:
  For each machine, telemetry readings (running/degrading/down) are
  sorted by their own timestamp. The time between each pair of
  consecutive readings is attributed to whichever status was active at
  the start of that interval. The very last reading in a partition has
  no following reading yet, so its trailing interval is not counted --
  a small, known undercount at the edge of each hour, not an error.

Usage:
    python build_gold.py --input-root silver --output-root gold
    python build_gold.py --input-root silver --output-root gold --interval 60
"""

import argparse
import glob
import json
import re
import signal
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

# Best-case ("ideal") cycle time per machine type, in minutes -- the
# lower bound of each type's cycle_time_range_min in the simulator.
# This is the standard OEE convention: Performance% compares actual
# cycle time against the fastest realistic cycle, not an average.
IDEAL_CYCLE_TIME_MIN = {
    "colloid_mill": 20.0,
    "kettle": 60.0,
    "mixer": 15.0,
}

TELEMETRY_STATUSES = {"running", "degrading", "down"}
PARTITION_RE = re.compile(r"date=([^/\\]+)[/\\]hour=([^/\\]+)")


def find_silver_files(input_root: Path) -> list:
    pattern = str(input_root / "date=*" / "hour=*" / "silver.jsonl")
    return sorted(Path(p) for p in glob.glob(pattern))


def partition_for(silver_file: Path) -> tuple:
    match = PARTITION_RE.search(str(silver_file))
    if match:
        return match.group(1), match.group(2)
    return "unknown", "unknown"


def load_watermarks(state_path: Path) -> dict:
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {}


def save_watermarks(state_path: Path, watermarks: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(watermarks, indent=2), encoding="utf-8")


def parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str)


def aggregate_machine(records: list, machine_type: str) -> dict:
    """records: all silver records for one machine in one partition,
    already sorted by payload timestamp."""
    telemetry = [r for r in records if r["payload"].get("status") in TELEMETRY_STATUSES]
    batches = [r for r in records if r["payload"].get("status") == "batch_complete"]

    running_min = 0.0
    down_min = 0.0
    for i in range(len(telemetry) - 1):
        t_now = parse_ts(telemetry[i]["payload"]["timestamp"])
        t_next = parse_ts(telemetry[i + 1]["payload"]["timestamp"])
        delta_min = (t_next - t_now).total_seconds() / 60.0
        if delta_min < 0:
            continue  # out-of-order timestamp, skip rather than corrupt the total
        status = telemetry[i]["payload"].get("status")
        if status == "down":
            down_min += delta_min
        else:  # running or degrading both count as "up" for availability
            running_min += delta_min

    total_min = running_min + down_min
    availability_pct = round(running_min / total_min * 100, 1) if total_min > 0 else None

    cycle_times = [
        b["payload"].get("cycle_time_min") for b in batches
        if isinstance(b["payload"].get("cycle_time_min"), (int, float))
    ]
    batches_completed = len(batches)
    avg_cycle_time_min = round(statistics.mean(cycle_times), 2) if cycle_times else None
    ideal_cycle_time_min = IDEAL_CYCLE_TIME_MIN.get(machine_type)

    performance_pct = None
    if avg_cycle_time_min and ideal_cycle_time_min:
        performance_pct = round(min(100.0, ideal_cycle_time_min / avg_cycle_time_min * 100), 1)

    return {
        "machine_type": machine_type,
        "running_min": round(running_min, 2),
        "down_min": round(down_min, 2),
        "availability_pct": availability_pct,
        "batches_completed": batches_completed,
        "avg_cycle_time_min": avg_cycle_time_min,
        "ideal_cycle_time_min": ideal_cycle_time_min,
        "performance_pct": performance_pct,
        "quality_pct": None,
        "quality_pct_note": "not computable yet -- no reject/scrap data source exists in bronze/silver",
        "oee_pct": None,
        "oee_pct_note": "not computed -- would require quality_pct, which is not yet available",
    }


def aggregate_partition(silver_file: Path) -> list:
    records = []
    with open(silver_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    by_machine = {}
    for r in records:
        machine_id = r["payload"].get("machine_id", "UNKNOWN")
        by_machine.setdefault(machine_id, []).append(r)

    date_part, hour_part = partition_for(silver_file)
    rows = []
    for machine_id, machine_records in by_machine.items():
        machine_records.sort(key=lambda r: r["payload"].get("timestamp", ""))
        machine_type = machine_records[0]["payload"].get("machine_type")
        row = aggregate_machine(machine_records, machine_type)
        row.update({"date": date_part, "hour": hour_part, "machine_id": machine_id})
        rows.append(row)
    return rows


def run_once(input_root: Path, output_root: Path, state_path: Path) -> None:
    watermarks = load_watermarks(state_path)
    silver_files = find_silver_files(input_root)
    updated = 0

    for silver_file in silver_files:
        key = str(silver_file.relative_to(input_root))
        current_size = silver_file.stat().st_size
        if watermarks.get(key) == current_size:
            continue  # unchanged since last run, nothing to redo

        rows = aggregate_partition(silver_file)
        date_part, hour_part = partition_for(silver_file)
        gold_path = output_root / f"date={date_part}" / f"hour={hour_part}" / "kpi_summary.jsonl"
        gold_path.parent.mkdir(parents=True, exist_ok=True)
        with open(gold_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        watermarks[key] = current_size
        updated += 1

    save_watermarks(state_path, watermarks)
    if updated:
        print(f"[info] recomputed {updated} partition(s)", flush=True)


_stop_requested = False


def _request_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", required=True, help="Silver root folder")
    parser.add_argument("--output-root", required=True, help="Gold root folder to write into")
    parser.add_argument("--state-file", default=None)
    parser.add_argument("--interval", type=float, default=None)
    args = parser.parse_args()

    input_root = Path(args.input_root)
    output_root = Path(args.output_root)
    state_path = Path(args.state_file) if args.state_file else output_root / "_state" / "watermarks.json"

    if not args.interval:
        run_once(input_root, output_root, state_path)
        return

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    print(f"[info] running continuously every {args.interval}s. Ctrl+C or docker stop to exit.", flush=True)
    while not _stop_requested:
        run_once(input_root, output_root, state_path)
        for _ in range(int(args.interval)):
            if _stop_requested:
                break
            time.sleep(1)
    print("[info] stopped.", flush=True)


if __name__ == "__main__":
    main()