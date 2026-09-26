"""
test_silver_logic.py

Fast, deterministic tests for clean_to_silver.py's cleaning logic --
no Docker, no running pipeline, no waiting. Run any time you change
clean_to_silver.py to catch a regression in seconds.

Usage:
    python tests/test_silver_logic.py

Exits 0 if everything passes, 1 if anything fails (with details printed).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "silver-etl"))
import clean_to_silver as silver  # noqa: E402  # type: ignore[import-not-found]

failures = []


def check(name: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(name)


def reset_state():
    silver._last_good.clear()


# ----------------------------------------------------------------------
# Regression guard: batch_complete must NEVER be flagged for missing
# temperature/rpm -- this is the exact bug Kimi's review found.
# ----------------------------------------------------------------------
reset_state()
envelope = {
    "event_id": "e1", "note": None,
    "payload": {"status": "batch_complete", "machine_id": "CM-01", "cycle_time_min": 28.4},
}
record, reject = silver.process_envelope(envelope)
check(
    "batch_complete with valid cycle_time gets no dq_flags",
    record is not None and record["dq_flags"] == [],
    f"got dq_flags={record.get('dq_flags') if record else None}",
)
check(
    "batch_complete never gets temperature_missing",
    record is not None and "temperature_missing" not in record["dq_flags"],
)

# ----------------------------------------------------------------------
# batch_complete with a genuinely missing cycle_time SHOULD be flagged
# ----------------------------------------------------------------------
reset_state()
envelope = {"event_id": "e2", "note": None, "payload": {"status": "batch_complete", "machine_id": "CM-01"}}
record, _ = silver.process_envelope(envelope)
check(
    "batch_complete with missing cycle_time IS flagged",
    record is not None and "cycle_time_missing" in record["dq_flags"],
)

# ----------------------------------------------------------------------
# A clean running reading passes through untouched
# ----------------------------------------------------------------------
reset_state()
envelope = {
    "event_id": "e3", "note": None,
    "payload": {"status": "running", "machine_id": "CM-01", "machine_type": "colloid_mill",
                "temperature_c": 45.0, "rpm": 4000.0},
}
record, _ = silver.process_envelope(envelope)
check(
    "clean running reading has no dq_flags",
    record is not None and record["dq_flags"] == [],
)
check("clean reading keeps its real temperature", record["temperature_c_clean"] == 45.0)

# ----------------------------------------------------------------------
# An out-of-range spike gets flagged; with no prior value, stays null
# ----------------------------------------------------------------------
reset_state()
envelope = {
    "event_id": "e4", "note": None,
    "payload": {"status": "running", "machine_id": "CM-02", "machine_type": "colloid_mill",
                "temperature_c": 999.0, "rpm": 4000.0},
}
record, _ = silver.process_envelope(envelope)
check(
    "out-of-range temperature is flagged",
    "temperature_out_of_range" in record["dq_flags"],
)
check(
    "out-of-range temperature with no prior good value stays null",
    record["temperature_c_clean"] is None,
)

# ----------------------------------------------------------------------
# Forward-fill: a later spike on the SAME machine should use the
# earlier good reading
# ----------------------------------------------------------------------
reset_state()
good = {
    "event_id": "e5", "note": None,
    "payload": {"status": "running", "machine_id": "CM-03", "machine_type": "colloid_mill",
                "temperature_c": 47.0, "rpm": 4050.0},
}
silver.process_envelope(good)
bad = {
    "event_id": "e6", "note": None,
    "payload": {"status": "running", "machine_id": "CM-03", "machine_type": "colloid_mill",
                "temperature_c": -40.0, "rpm": 4060.0},
}
record, _ = silver.process_envelope(bad)
check(
    "forward-fill uses the prior good reading, not null",
    record["temperature_c_clean"] == 47.0,
    f"got {record['temperature_c_clean']}",
)
check("forward-fill sets the forward_filled flag", "temperature_forward_filled" in record["dq_flags"])

# ----------------------------------------------------------------------
# A bronze-level parse error routes to rejects, never to silver
# ----------------------------------------------------------------------
reset_state()
envelope = {"event_id": "e7", "note": "parse_error: bad json", "payload": {"raw_payload": "garbage"}}
record, reject = silver.process_envelope(envelope)
check("bronze parse error produces no silver record", record is None)
check("bronze parse error produces a reject record", reject is not None and reject["event_id"] == "e7")

# ----------------------------------------------------------------------
print()
if failures:
    print(f"FAILED: {len(failures)} check(s) did not pass: {failures}")
    sys.exit(1)
else:
    print("All checks passed.")
    sys.exit(0)
    