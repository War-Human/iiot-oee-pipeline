"""
bronze_writer.py

Subscribes to the MQTT broker (Mosquitto) and appends every incoming
telemetry message to the BRONZE layer: raw, unmodified payloads, filed in
date/hour-partitioned JSONL files.

Bronze rules this writer follows:
  * Payload bytes are stored AS RECEIVED (no parsing, no repair, no
    dropping). If a line is not valid JSON, the raw text is still kept,
    with a parse_error note -- bronze never throws data away.
  * Two pieces of LANDING METADATA are appended alongside the payload
    (this is not 'changing' the data, it is provenance):
        - ingest_ts   : when this writer received the message
        - event_id    : carried through from the simulator if present;
                        generated here only if a sender omitted one
  * Files are partitioned  <root>/date=YYYY-MM-DD/hour=HH/bronze.jsonl
    so any hour can be replayed or reprocessed independently.
  * Writes are append-only. Files are never rewritten, only ever added to.

Broker QoS 1 means messages can arrive twice (and a redelivery can race
its original). That's fine at this layer: downstream silver dedups on
event_id. Bronze is the honest record of what arrived and when.
"""

import argparse
import json
import signal
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    raise SystemExit("This service requires paho-mqtt: pip install paho-mqtt")


stop_requested = False


def _on_signal(signum, frame):
    global stop_requested
    stop_requested = True


class BronzeWriter:
    def __init__(self, root: Path, topic: str, qos: int):
        self.root = root
        self.topic = topic
        self.qos = qos
        self._fh = None          # currently open file handle
        self._fh_partition = None  # (date, hour) the handle belongs to
        self.received = 0
        self.malformed = 0

    # ---- file handling -------------------------------------------------
    def _file_for(self, now: datetime):
        partition = (now.strftime("%Y-%m-%d"), now.strftime("%H"))
        if self._fh is None or partition != self._fh_partition:
            if self._fh is not None:
                self._fh.close()
            path = self.root / f"date={partition[0]}" / f"hour={partition[1]}" / "bronze.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(path, "a", encoding="utf-8")
            self._fh_partition = partition
        return self._fh

    # ---- MQTT callbacks -------------------------------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            print(f"[error] connect failed rc={rc}", file=sys.stderr)
            return
        session_present = flags.get("session present", 0)
        if session_present:
            print("[info] resumed existing session -- any messages queued "
                  "while offline will now be delivered", flush=True)
        else:
            print("[info] starting a fresh session with the broker", flush=True)
        client.subscribe(self.topic, qos=self.qos)
        print(f"[info] subscribed to '{self.topic}' (qos={self.qos})", flush=True)

    def on_message(self, client, userdata, msg):
        now = datetime.now(timezone.utc)
        try:
            record = json.loads(msg.payload.decode("utf-8"))
            if not isinstance(record, dict):
                raise ValueError("payload is not a JSON object")
            event_id = record.get("event_id") or str(uuid.uuid4())
            note = None
        except (ValueError, UnicodeDecodeError) as exc:
            # keep the raw bytes anyway -- bronze does not discard
            record = {"raw_payload": msg.payload.decode("utf-8", errors="replace")}
            event_id = str(uuid.uuid4())
            note = f"parse_error: {exc}"
            self.malformed += 1

        envelope = {
            "ingest_ts": now.isoformat(timespec="milliseconds"),
            "event_id": event_id,
            "mqtt_topic": msg.topic,
            "note": note,
            "payload": record,
        }
        fh = self._file_for(now)
        fh.write(json.dumps(envelope) + "\n")
        fh.flush()  # survive crashes: every message hits disk immediately
        self.received += 1
        if self.received % 100 == 0:
            print(f"[info] {self.received} messages filed "
                  f"({self.malformed} malformed)", flush=True)

    # ---- lifecycle -------------------------------------------------------
    def run(self, host: str, port: int):
        # A fixed client_id + clean_session=False tells the broker "keep a
        # mailbox for exactly this client, even while it's disconnected."
        # Without both of these, every reconnect looks like a brand-new,
        # unknown client to the broker, and anything published while this
        # service was down is silently dropped rather than queued.
        client = mqtt.Client(client_id="bronze-writer", clean_session=False)
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.connect(host, port, keepalive=60)
        client.loop_start()
        print(f"[info] connecting to mqtt://{host}:{port} ...", flush=True)
        try:
            while not stop_requested:
                # paho runs in its own thread; sleep keeps main thread alive
                # and lets signals land promptly
                import time
                time.sleep(0.5)
        finally:
            client.loop_stop()
            client.disconnect()
            if self._fh is not None:
                self._fh.close()
            print(f"[info] stopped. total filed: {self.received} "
                  f"({self.malformed} malformed)", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mqtt-host", default="mosquitto",
                        help="Broker hostname (default: 'mosquitto')")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--topic", default="factory/+/telemetry",
                        help="Subscription (default: all machines). '+' = one level wildcard")
    parser.add_argument("--qos", type=int, default=1, choices=[0, 1, 2])
    parser.add_argument("--root", default="/data/raw",
                        help="Bronze root directory (default: /data/raw)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    BronzeWriter(Path(args.root), args.topic, args.qos).run(args.mqtt_host, args.mqtt_port)


if __name__ == "__main__":
    main()
