"""
machine_telemetry_simulator.py

Generates plausible real-time telemetry for colloid mills, kettles, and mixers
in a bitumen/membrane-style manufacturing plant. Intended to feed the raw
(bronze) layer of a data lake pipeline.

Each machine runs its own async loop, emitting a heartbeat reading at a
configurable interval and cycling through batches. Most failures are preceded
by a "degrading" phase (temperature/RPM drifting away from normal over
several minutes) before the machine goes "down" — this precursor window is
what a predictive-maintenance model would learn to recognize. A small
fraction of failures are sudden instead, with no precursor, mirroring the
fact that not every real-world failure is predictable. The simulator also
occasionally emits a bad/anomalous reading so downstream cleaning logic
(dedupe, range checks, null handling) has something real to catch.

Each reading includes a "status" of "running", "degrading", "down", or
"batch_complete". While degrading, a "minutes_to_failure" field counts down —
this is the label a supervised model would be trained to predict.

Usage examples
---------------
Print to stdout, 3 default machines, one reading every 5s:
    python machine_telemetry_simulator.py

Write JSON Lines to a local file (simulating a raw landing zone):
    python machine_telemetry_simulator.py --output file --file-path raw/telemetry.jsonl

POST each reading to an HTTP endpoint (e.g. API Gateway / IoT rule proxy):
    python machine_telemetry_simulator.py --output http --endpoint-url https://your-endpoint/ingest

Run for a fixed duration with reproducible randomness:
    python machine_telemetry_simulator.py --duration 300 --seed 42

Requires: Python 3.9+. `requests` only needed for --output http.
"""

import argparse
import asyncio
import json
import random
import signal
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ----------------------------------------------------------------------------
# Machine type profiles — tune these to match your target plant
# ----------------------------------------------------------------------------

MACHINE_PROFILES = {
    "colloid_mill": {
        "temp_range_c": (40.0, 55.0),
        "rpm_range": (3500.0, 4800.0),
        "cycle_time_range_min": (20.0, 35.0),
        "degradation_prob_per_tick": 0.0015,
        "degradation_minutes_range": (25.0, 45.0),
        "degradation_temp_drift_c": 12.0,
        "degradation_rpm_drift_pct": 0.12,
        "sudden_down_prob_per_tick": 0.001,
        "downtime_minutes_range": (2.0, 10.0),
        "ambient_temp_c": 28.0,
    },
    "kettle": {
        "temp_range_c": (140.0, 180.0),
        "rpm_range": (20.0, 60.0),
        "cycle_time_range_min": (60.0, 120.0),
        "degradation_prob_per_tick": 0.001,
        "degradation_minutes_range": (30.0, 60.0),
        "degradation_temp_drift_c": 20.0,
        "degradation_rpm_drift_pct": 0.15,
        "sudden_down_prob_per_tick": 0.0008,
        "downtime_minutes_range": (5.0, 20.0),
        "ambient_temp_c": 28.0,
    },
    "mixer": {
        "temp_range_c": (25.0, 40.0),
        "rpm_range": (80.0, 150.0),
        "cycle_time_range_min": (15.0, 30.0),
        "degradation_prob_per_tick": 0.0018,
        "degradation_minutes_range": (15.0, 30.0),
        "degradation_temp_drift_c": 10.0,
        "degradation_rpm_drift_pct": 0.18,
        "sudden_down_prob_per_tick": 0.0012,
        "downtime_minutes_range": (2.0, 8.0),
        "ambient_temp_c": 28.0,
    },
}

DEFAULT_MACHINES = [
    ("CM-01", "colloid_mill"),
    ("CM-02", "colloid_mill"),
    ("KT-01", "kettle"),
    ("MX-01", "mixer"),
]


# ----------------------------------------------------------------------------
# Output sinks
# ----------------------------------------------------------------------------

class Sink:
    async def write(self, record: dict) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class StdoutSink(Sink):
    async def write(self, record: dict) -> None:
        print(json.dumps(record), flush=True)


class FileSink(Sink):
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    async def write(self, record: dict) -> None:
        self._fh.write(json.dumps(record) + "\n")
        self._fh.flush()

    async def close(self) -> None:
        self._fh.close()


class HttpSink(Sink):
    def __init__(self, endpoint_url: str):
        try:
            import requests  # noqa: F401  (imported lazily so it's optional)
        except ImportError as exc:
            raise SystemExit(
                "The 'requests' package is required for --output http. "
                "Install it with: pip install requests"
            ) from exc
        self.endpoint_url = endpoint_url

    async def write(self, record: dict) -> None:
        import requests

        def _post():
            try:
                requests.post(self.endpoint_url, json=record, timeout=5)
            except requests.RequestException as exc:
                print(f"[warn] failed to POST reading: {exc}", file=sys.stderr)

        await asyncio.to_thread(_post)



class MqttSink(Sink):
    """Publishes each record to an MQTT broker (e.g. Mosquitto).

    The simulator's job ends the moment the broker accepts the message;
    whoever subscribes (bronze-writer today, a dashboard tomorrow) is
    none of the simulator's business. QoS 1 = 'at least once' delivery,
    so the broker may redeliver -- that's why every record carries an
    event_id for idempotent downstream dedup.
    """
    def __init__(self, host: str, port: int, topic_prefix: str, qos: int):
        try:
            import paho.mqtt.client as mqtt  # noqa: F401
        except ImportError as exc:
            raise SystemExit(
                "The 'paho-mqtt' package is required for --output mqtt. "
                "Install it with: pip install paho-mqtt"
            ) from exc
        import paho.mqtt.client as mqtt

        self.topic_prefix = topic_prefix.rstrip("/")
        self.qos = qos
        self.client = mqtt.Client()
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()

    async def write(self, record: dict) -> None:
        topic = f"{self.topic_prefix}/{record['machine_id']}/telemetry"
        payload = json.dumps(record)
        info = self.client.publish(topic, payload, qos=self.qos)
        # surface broker-rejected messages instead of silently dropping them
        # (MQTT_ERR_SUCCESS == 0)
        if info.rc != 0:
            print(f"[warn] publish failed rc={info.rc} topic={topic}", file=sys.stderr)

    async def close(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


def build_sink(args: argparse.Namespace) -> Sink:
    if args.output == "stdout":
        return StdoutSink()
    if args.output == "file":
        if not args.file_path:
            raise SystemExit("--file-path is required when --output file")
        return FileSink(args.file_path)
    if args.output == "http":
        if not args.endpoint_url:
            raise SystemExit("--endpoint-url is required when --output http")
        return HttpSink(args.endpoint_url)
    if args.output == "mqtt":
        return MqttSink(args.mqtt_host, args.mqtt_port, args.mqtt_topic_prefix, args.mqtt_qos)
    raise SystemExit(f"Unknown output sink: {args.output}")


# ----------------------------------------------------------------------------
# Machine simulation
# ----------------------------------------------------------------------------

@dataclass
class BatchState:
    batch_id: str
    planned_cycle_min: float
    elapsed_min: float = 0.0


@dataclass
class MachineState:
    machine_id: str
    machine_type: str
    profile: dict
    phase: str = "running"  # "running" | "degrading" | "down"
    down_until_ticks: int = 0
    degrade_ticks_remaining: int = 0
    degrade_total_ticks: int = 0
    batch: BatchState = field(default=None)

    def new_batch(self) -> BatchState:
        lo, hi = self.profile["cycle_time_range_min"]
        return BatchState(
            batch_id=str(uuid.uuid4())[:8],
            planned_cycle_min=round(random.uniform(lo, hi), 1),
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _maybe_anomaly(value: float, anomaly_rate: float, kind: str) -> float:
    """With small probability, corrupt a reading to exercise cleaning logic."""
    if random.random() >= anomaly_rate:
        return value
    if kind == "spike":
        return round(value * random.choice([2.5, 3.0, -1.0]), 2)
    if kind == "null":
        return None
    return value


async def run_machine(
    state: MachineState,
    interval_s: float,
    anomaly_rate: float,
    sink: Sink,
    stop_event: asyncio.Event,
) -> None:
    profile = state.profile
    state.batch = state.new_batch()
    tick = 0

    # small per-machine jitter so machines don't all emit in lockstep
    await asyncio.sleep(random.uniform(0, interval_s))

    while not stop_event.is_set():
        tick += 1
        tick_minutes = interval_s / 60.0

        # ---- DOWN: machine is stopped, waiting out its downtime ----
        if state.phase == "down":
            state.down_until_ticks -= 1
            temp = round(
                profile["ambient_temp_c"] + random.uniform(-1.0, 1.0), 2
            )
            record = {
                "timestamp": _now_iso(),
                "event_id": str(uuid.uuid4()),
                "machine_id": state.machine_id,
                "machine_type": state.machine_type,
                "status": "down",
                "batch_id": state.batch.batch_id,
                "temperature_c": temp,
                "rpm": 0.0,
                "minutes_to_failure": None,
                "elapsed_batch_min": round(state.batch.elapsed_min, 2),
            }
            await sink.write(record)
            if state.down_until_ticks <= 0:
                state.phase = "running"
            await asyncio.sleep(interval_s)
            continue

        # ---- DEGRADING: precursor window before a wear-based failure ----
        if state.phase == "degrading":
            state.degrade_ticks_remaining -= 1
            progress = 1.0 - (
                state.degrade_ticks_remaining / state.degrade_total_ticks
            )
            progress = min(max(progress, 0.0), 1.0)

            temp_lo, temp_hi = profile["temp_range_c"]
            rpm_lo, rpm_hi = profile["rpm_range"]
            base_temp = random.uniform(temp_lo, temp_hi)
            base_rpm = random.uniform(rpm_lo, rpm_hi)

            temperature = round(
                base_temp
                + progress * profile["degradation_temp_drift_c"]
                + random.uniform(-1.0, 1.0),
                2,
            )
            rpm = round(
                base_rpm * (1.0 - progress * profile["degradation_rpm_drift_pct"])
                + random.uniform(-5.0, 5.0),
                1,
            )
            minutes_to_failure = round(
                state.degrade_ticks_remaining * tick_minutes, 2
            )

            state.batch.elapsed_min += tick_minutes
            record = {
                "timestamp": _now_iso(),
                "event_id": str(uuid.uuid4()),
                "machine_id": state.machine_id,
                "machine_type": state.machine_type,
                "status": "degrading",
                "batch_id": state.batch.batch_id,
                "temperature_c": temperature,
                "rpm": rpm,
                "minutes_to_failure": minutes_to_failure,
                "elapsed_batch_min": round(state.batch.elapsed_min, 2),
            }
            await sink.write(record)

            if state.degrade_ticks_remaining <= 0:
                down_lo, down_hi = profile["downtime_minutes_range"]
                down_minutes = random.uniform(down_lo, down_hi)
                state.down_until_ticks = max(1, int(down_minutes * 60 / interval_s))
                state.phase = "down"

            await asyncio.sleep(interval_s)
            continue

        # ---- RUNNING: normal operation, may enter degrading or fail suddenly ----
        if random.random() < profile["degradation_prob_per_tick"]:
            deg_lo, deg_hi = profile["degradation_minutes_range"]
            deg_minutes = random.uniform(deg_lo, deg_hi)
            state.degrade_total_ticks = max(1, int(deg_minutes * 60 / interval_s))
            state.degrade_ticks_remaining = state.degrade_total_ticks
            state.phase = "degrading"
            continue

        if random.random() < profile["sudden_down_prob_per_tick"]:
            down_lo, down_hi = profile["downtime_minutes_range"]
            down_minutes = random.uniform(down_lo, down_hi)
            state.down_until_ticks = max(1, int(down_minutes * 60 / interval_s))
            state.phase = "down"
            continue

        temp_lo, temp_hi = profile["temp_range_c"]
        rpm_lo, rpm_hi = profile["rpm_range"]
        temperature = round(random.uniform(temp_lo, temp_hi), 2)
        rpm = round(random.uniform(rpm_lo, rpm_hi), 1)

        temperature = _maybe_anomaly(temperature, anomaly_rate, "spike")
        rpm = _maybe_anomaly(rpm, anomaly_rate, "null")

        state.batch.elapsed_min += tick_minutes

        record = {
            "timestamp": _now_iso(),
            "event_id": str(uuid.uuid4()),
            "machine_id": state.machine_id,
            "machine_type": state.machine_type,
            "status": "running",
            "batch_id": state.batch.batch_id,
            "temperature_c": temperature,
            "rpm": rpm,
            "minutes_to_failure": None,
            "elapsed_batch_min": round(state.batch.elapsed_min, 2),
        }
        await sink.write(record)

        if state.batch.elapsed_min >= state.batch.planned_cycle_min:
            completion = {
                "timestamp": _now_iso(),
                "event_id": str(uuid.uuid4()),
                "machine_id": state.machine_id,
                "machine_type": state.machine_type,
                "status": "batch_complete",
                "batch_id": state.batch.batch_id,
                "cycle_time_min": round(state.batch.elapsed_min, 2),
            }
            await sink.write(completion)
            state.batch = state.new_batch()

        await asyncio.sleep(interval_s)


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

def parse_machines(spec: Optional[str]) -> list:
    """Parse '--machines CM-01:colloid_mill,KT-01:kettle' into tuples."""
    if not spec:
        return DEFAULT_MACHINES
    machines = []
    for item in spec.split(","):
        machine_id, _, machine_type = item.partition(":")
        machine_id, machine_type = machine_id.strip(), machine_type.strip()
        if machine_type not in MACHINE_PROFILES:
            raise SystemExit(
                f"Unknown machine type '{machine_type}'. "
                f"Choose from: {', '.join(MACHINE_PROFILES)}"
            )
        machines.append((machine_id, machine_type))
    return machines


async def main_async(args: argparse.Namespace) -> None:
    if args.seed is not None:
        random.seed(args.seed)

    machines = parse_machines(args.machines)
    sink = build_sink(args)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Not supported on some platforms (e.g. Windows) — Ctrl+C still
            # works there via the KeyboardInterrupt fallback in main().
            pass

    tasks = [
        asyncio.create_task(
            run_machine(
                MachineState(mid, mtype, MACHINE_PROFILES[mtype]),
                args.interval,
                args.anomaly_rate,
                sink,
                stop_event,
            )
        )
        for mid, mtype in machines
    ]

    if args.duration:
        async def _timed_stop():
            await asyncio.sleep(args.duration)
            stop_event.set()

        tasks.append(asyncio.create_task(_timed_stop()))

    try:
        await asyncio.gather(*tasks)
    finally:
        await sink.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval", type=float, default=5.0,
        help="Seconds between readings per machine (default: 5)",
    )
    parser.add_argument(
        "--machines", type=str, default=None,
        help="Comma list like 'CM-01:colloid_mill,KT-01:kettle'. "
             "Defaults to 2 colloid mills, 1 kettle, 1 mixer.",
    )
    parser.add_argument(
        "--output", choices=["stdout", "file", "http", "mqtt"], default="stdout",
        help="Where readings are sent (default: stdout)",
    )
    parser.add_argument("--file-path", type=str, default=None,
                         help="Required when --output file")
    parser.add_argument("--endpoint-url", type=str, default=None,
                         help="Required when --output http")
    parser.add_argument("--mqtt-host", type=str, default="mosquitto",
                        help="MQTT broker hostname (default: 'mosquitto' -- the "
                             "compose service name, resolvable on the Docker network)")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--mqtt-topic-prefix", type=str, default="factory",
                        help="Topic prefix; records go to <prefix>/<machine_id>/telemetry")
    parser.add_argument("--mqtt-qos", type=int, default=1, choices=[0, 1, 2],
                        help="MQTT QoS (default: 1 = at-least-once)")
    parser.add_argument(
        "--anomaly-rate", type=float, default=0.02,
        help="Probability per reading of an injected bad value (default: 0.02)",
    )
    parser.add_argument(
        "--duration", type=float, default=None,
        help="Stop after N seconds. Omit to run until Ctrl+C.",
    )
    parser.add_argument("--seed", type=int, default=None,
                         help="Random seed for reproducible runs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)


if __name__ == "__main__":
    main()