"""
clean_to_silver.py (v2 -- rebuilt for the MQTT bronze format)

Reads the partitioned bronze data bronze-writer produces
(data/raw/date=YYYY-MM-DD/hour=HH/bronze.jsonl) and produces a matching,
cleaned silver layer at data/silver/date=YYYY-MM-DD/hour=HH/silver.jsonl.

What changed from the old (pre-MQTT) version:
  - Reads bronze's envelope format ({ingest_ts, event_id, mqtt_topic, note,
    payload}) instead of the simulator's raw output directly.
  - Only reads NEW bytes since the last run (tracked in a watermark file),
    instead of re-reading and re-cleaning the entire history every cycle.
  - Dedupes on the real event_id bronze already assigns, not the old
    fragile (machine_id, timestamp) key.
  - "batch_complete" records are handled as their own kind -- they never
    had temperature/rpm to begin with, so they no longer get incorrectly
    flagged as "temperature_missing".
  - Mirrors bronze's own date=/hour= partitioning in the silver output.

Known, deliberate scope limits (documented, not hidden):
  - Forward-fill "last good reading per machine" only persists for the
    life of one running container -- a restart starts that state fresh.
    This is fine in practice: it only affects the first bad reading for
    a machine right after startup, which stays correctly flagged as
    unresolved (null) rather than guessed at.
  - Dedup is only applied within newly-read bytes each cycle, not
    against silver's full history. Safe as long as bronze itself never
    re-delivers an event_id it already wrote once -- true today, since
    bronze-writer's persistent MQTT session guarantees at-least-once,
    exactly-once-in-practice delivery per event_id at this scale.

Usage:
    python clean_to_silver.py --input-root raw --output-root silver
    python clean_to_silver.py --input-root raw --output-root silver --interval 20
"""

import argparse
import glob
import json
import re
import signal
import sys
import time
from pathlib import Path

PLAUSIBLE_BOUNDS = {
    "colloid_mill": {"temp_c": (0.0, 90.0), "rpm": (0.0, 6000.0)},
    "kettle": {"temp_c": (0.0, 220.0), "rpm": (0.0, 100.0)},
    "mixer": {"temp_c": (0.0, 60.0), "rpm": (0.0, 250.0)},
}

TELEMETRY_STATUSES = {"running", "degrading", "down"}
PARTITION_RE = re.compile(r"date=([^/\\]+)[/\\]hour=([^/\\]+)")

# machine_id -> {"temperature_c": x, "rpm": y}, forward-fill state for
# the life of this process.
_last_good: dict = {}


def find_bronze_files(input_root: Path) -> list:
    pattern = str(input_root / "date=*" / "hour=*" / "bronze.jsonl")
    return sorted(Path(p) for p in glob.glob(pattern))


def load_watermarks(state_path: Path) -> dict:
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {}


def save_watermarks(state_path: Path, watermarks: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(watermarks, indent=2), encoding="utf-8")


def partition_for(bronze_file: Path) -> tuple:
    match = PARTITION_RE.search(str(bronze_file))
    if match:
        return match.group(1), match.group(2)
    return "unknown", "unknown"


def clean_telemetry(payload: dict) -> tuple:
    """Returns (temperature_c_clean, rpm_clean, dq_flags) for a
    running/degrading/down record."""
    machine_id = payload.get("machine_id", "UNKNOWN")
    machine_type = payload.get("machine_type")
    bounds = PLAUSIBLE_BOUNDS.get(machine_type)
    dq_flags = []
    prior = _last_good.setdefault(machine_id, {"temperature_c": None, "rpm": None})

    if bounds is None:
        dq_flags.append("unknown_machine_type")
        return payload.get("temperature_c"), payload.get("rpm"), dq_flags

    temp = payload.get("temperature_c")
    temp_lo, temp_hi = bounds["temp_c"]
    if temp is None:
        dq_flags.append("temperature_missing")
        temp_clean = prior["temperature_c"]
        if temp_clean is not None:
            dq_flags.append("temperature_forward_filled")
    elif not (temp_lo <= temp <= temp_hi):
        dq_flags.append("temperature_out_of_range")
        temp_clean = prior["temperature_c"]
        if temp_clean is not None:
            dq_flags.append("temperature_forward_filled")
    else:
        temp_clean = temp
        prior["temperature_c"] = temp

    rpm = payload.get("rpm")
    rpm_lo, rpm_hi = bounds["rpm"]
    if rpm is None:
        dq_flags.append("rpm_missing")
        rpm_clean = prior["rpm"]
        if rpm_clean is not None:
            dq_flags.append("rpm_forward_filled")
    elif not (rpm_lo <= rpm <= rpm_hi):
        dq_flags.append("rpm_out_of_range")
        rpm_clean = prior["rpm"]
        if rpm_clean is not None:
            dq_flags.append("rpm_forward_filled")
    else:
        rpm_clean = rpm
        prior["rpm"] = rpm

    return temp_clean, rpm_clean, dq_flags


def clean_batch_complete(payload: dict) -> list:
    """batch_complete records have cycle_time_min, never temperature/rpm --
    flag only if that field itself is actually missing or nonsensical."""
    dq_flags = []
    cycle_time = payload.get("cycle_time_min")
    if cycle_time is None:
        dq_flags.append("cycle_time_missing")
    elif not isinstance(cycle_time, (int, float)) or cycle_time <= 0:
        dq_flags.append("cycle_time_invalid")
    return dq_flags


def process_envelope(envelope: dict) -> tuple:
    """Returns (silver_record_or_None, reject_record_or_None)."""
    if envelope.get("note"):
        return None, {
            "event_id": envelope.get("event_id"),
            "ingest_ts": envelope.get("ingest_ts"),
            "reason": f"bronze_parse_error: {envelope['note']}",
        }

    payload = envelope.get("payload", {})
    status = payload.get("status")
    silver_record = dict(envelope)  # keep everything bronze had

    if status in TELEMETRY_STATUSES:
        temp_clean, rpm_clean, dq_flags = clean_telemetry(payload)
        silver_record["temperature_c_clean"] = temp_clean
        silver_record["rpm_clean"] = rpm_clean
        silver_record["dq_flags"] = dq_flags
    elif status == "batch_complete":
        silver_record["dq_flags"] = clean_batch_complete(payload)
    else:
        silver_record["dq_flags"] = ["unknown_status"]

    return silver_record, None


def append_jsonl(path: Path, records: list) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def run_once(input_root: Path, output_root: Path, state_path: Path) -> None:
    watermarks = load_watermarks(state_path)
    bronze_files = find_bronze_files(input_root)

    total_new = 0
    total_flagged = 0
    total_rejected = 0

    for bronze_file in bronze_files:
        key = str(bronze_file.relative_to(input_root))
        offset = watermarks.get(key, 0)

        file_size = bronze_file.stat().st_size
        if file_size <= offset:
            continue  # nothing new in this file since last run

        with open(bronze_file, "r", encoding="utf-8") as f:
            f.seek(offset)
            new_text = f.read()
            new_offset = f.tell()

        date_part, hour_part = partition_for(bronze_file)
        silver_path = output_root / f"date={date_part}" / f"hour={hour_part}" / "silver.jsonl"
        rejects_path = output_root / f"date={date_part}" / f"hour={hour_part}" / "rejects.jsonl"

        silver_batch = []
        rejects_batch = []
        seen_ids = set()

        for line in new_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError:
                rejects_batch.append({"reason": "unparseable_envelope", "raw": line})
                total_rejected += 1
                continue

            event_id = envelope.get("event_id")
            if event_id in seen_ids:
                continue  # duplicate within this batch, drop silently
            seen_ids.add(event_id)

            silver_record, reject_record = process_envelope(envelope)
            if silver_record is not None:
                silver_batch.append(silver_record)
                if silver_record.get("dq_flags"):
                    total_flagged += 1
                total_new += 1
            if reject_record is not None:
                rejects_batch.append(reject_record)
                total_rejected += 1

        append_jsonl(silver_path, silver_batch)
        append_jsonl(rejects_path, rejects_batch)
        watermarks[key] = new_offset

    save_watermarks(state_path, watermarks)
    if total_new or total_rejected:
        print(
            f"[info] processed {total_new} new records ({total_flagged} flagged), "
            f"{total_rejected} rejected",
            flush=True,
        )


_stop_requested = False


def _request_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", required=True, help="Bronze root folder (contains date=*/hour=*/)")
    parser.add_argument("--output-root", required=True, help="Silver root folder to write into")
    parser.add_argument(
        "--state-file", default=None,
        help="Where to store the watermark file (default: <output-root>/_state/watermarks.json)",
    )
    parser.add_argument(
        "--interval", type=float, default=None,
        help="If set, run continuously every N seconds. Omit for a single one-shot run.",
    )
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