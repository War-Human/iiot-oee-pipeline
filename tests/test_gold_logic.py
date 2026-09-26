"""
test_gold_logic.py

Fast, deterministic tests for build_gold.py's aggregation math -- no
Docker, no running pipeline. Run any time you change build_gold.py.

Usage:
    python tests/test_gold_logic.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gold-etl"))
import build_gold as gold  # noqa: E402

failures = []


def check(name: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(name)


def rec(ts, machine_id, machine_type, status, cycle_time_min=None):
    payload = {"timestamp": ts, "machine_id": machine_id, "machine_type": machine_type, "status": status}
    if cycle_time_min is not None:
        payload["cycle_time_min"] = cycle_time_min
    return {"payload": payload}


# ----------------------------------------------------------------------
# Hand-verified case: 2 min running, 2 min down, 1 batch at 25 min
# (ideal for colloid_mill is 20 min) -> availability 50%, performance 80%
# ----------------------------------------------------------------------
records = [
    rec("2026-09-23T10:00:00+00:00", "CM-01", "colloid_mill", "running"),
    rec("2026-09-23T10:01:00+00:00", "CM-01", "colloid_mill", "running"),
    rec("2026-09-23T10:02:00+00:00", "CM-01", "colloid_mill", "down"),
    rec("2026-09-23T10:03:00+00:00", "CM-01", "colloid_mill", "down"),
    rec("2026-09-23T10:04:00+00:00", "CM-01", "colloid_mill", "running"),
    rec("2026-09-23T10:05:00+00:00", "CM-01", "colloid_mill", "batch_complete", cycle_time_min=25.0),
]
result = gold.aggregate_machine(records, "colloid_mill")
check("availability is exactly 50%", result["availability_pct"] == 50.0, f"got {result['availability_pct']}")
check("running_min is 2.0", result["running_min"] == 2.0, f"got {result['running_min']}")
check("down_min is 2.0", result["down_min"] == 2.0, f"got {result['down_min']}")
check("performance is exactly 80%", result["performance_pct"] == 80.0, f"got {result['performance_pct']}")
check("batches_completed is 1", result["batches_completed"] == 1)

# ----------------------------------------------------------------------
# quality_pct and oee_pct must ALWAYS be null -- this is the honesty
# guard: no data source exists for these yet, and nothing should ever
# silently fill them in.
# ----------------------------------------------------------------------
check("quality_pct is always null", result["quality_pct"] is None)
check("oee_pct is always null", result["oee_pct"] is None)
check("quality_pct_note explains why", "no reject/scrap" in result["quality_pct_note"])

# ----------------------------------------------------------------------
# "degrading" counts as up-time for availability, same as "running"
# ----------------------------------------------------------------------
records2 = [
    rec("2026-09-23T11:00:00+00:00", "KT-01", "kettle", "degrading"),
    rec("2026-09-23T11:01:00+00:00", "KT-01", "kettle", "degrading"),
    rec("2026-09-23T11:02:00+00:00", "KT-01", "kettle", "down"),
]
result2 = gold.aggregate_machine(records2, "kettle")
check("degrading counts as up-time, not down-time", result2["down_min"] == 0.0, f"down_min={result2['down_min']}")
check(
    "both measured intervals (2 min) attributed to running, none to down",
    result2["running_min"] == 2.0,
    f"got {result2['running_min']} (down's own trailing interval is correctly unmeasured, since it's the last record)",
)

# ----------------------------------------------------------------------
# No batch_complete events at all -> performance/avg_cycle_time stay null,
# not zero (zero would misleadingly suggest an instant cycle)
# ----------------------------------------------------------------------
records3 = [
    rec("2026-09-23T12:00:00+00:00", "MX-01", "mixer", "running"),
    rec("2026-09-23T12:01:00+00:00", "MX-01", "mixer", "running"),
]
result3 = gold.aggregate_machine(records3, "mixer")
check("no batches -> avg_cycle_time_min is null, not 0", result3["avg_cycle_time_min"] is None)
check("no batches -> performance_pct is null, not 0", result3["performance_pct"] is None)

# ----------------------------------------------------------------------
# Only one telemetry reading -> no interval to measure -> availability null
# ----------------------------------------------------------------------
records4 = [rec("2026-09-23T13:00:00+00:00", "CM-02", "colloid_mill", "running")]
result4 = gold.aggregate_machine(records4, "colloid_mill")
check("a single reading gives null availability, not a fake 100%", result4["availability_pct"] is None)

# ----------------------------------------------------------------------
print()
if failures:
    print(f"FAILED: {len(failures)} check(s) did not pass: {failures}")
    sys.exit(1)
else:
    print("All checks passed.")
    sys.exit(0)