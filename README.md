# Industrial IoT OEE Pipeline — Step 1: MQTT Pipeline

Industrial IoT data pipeline for a bitumen/membrane manufacturing plant.
Machine telemetry flows through a real pub/sub path instead of shared
folders.

## Architecture

    machine-floor (simulator)  --MQTT publish-->  mosquitto (broker)
                                                        |
                                                        | subscribe
                                                        v
                                              bronze-writer
                                              -> data/raw/date=.../hour=.../bronze.jsonl

- **machine-floor**: simulates 4 machines (colloid mills, kettle, mixer).
  Each reading carries an `event_id`. Publishes to
  `factory/<machine_id>/telemetry`, QoS 1. Does not know who consumes it.
- **mosquitto**: the post office. eclipse-mosquitto, local-dev config
  (anonymous, no persistence). Production: auth + TLS on 8883.
- **bronze-writer**: appends every message AS RECEIVED to partitioned
  bronze files, adding only landing metadata (`ingest_ts`, and an
  `event_id` if the sender omitted one). Malformed payloads are kept
  and flagged, never dropped.

## Run

    docker compose up --build

Watch messages flow:

    docker logs -f bronze-writer        # filing progress
    ls data/raw/date=*/hour=*/          # partitions appearing
    head -1 data/raw/date=*/hour=*/bronze.jsonl

Watch a specific machine live (optional, needs mosquitto-clients):

    docker exec mosquitto mosquitto_sub -t 'factory/CM-01/telemetry' -C 5

Stop: Ctrl+C, or `docker compose down`.

## Crash tests (the "real system" test)

1. `docker stop machine-floor` -> bronze-writer keeps running, no errors.
2. `docker start machine-floor` -> flow resumes, no duplicates lost.
3. `docker stop bronze-writer` for a minute -> broker buffers (QoS 1);
   on restart, recent messages are redelivered. Bronze may contain
   duplicates of redelivered messages -- BY DESIGN. Silver (Step 2)
   dedups on `event_id`.

## Bronze record shape (landing metadata + untouched payload)

    {"ingest_ts": "...", "event_id": "...", "mqtt_topic": "...",
     "note": null | "parse_error: ...", "payload": { ...as sent... }}

## What Step 2 adds

- Choose storage: Floci (full AWS emulator) or MinIO (S3-compatible store)
- bronze-writer writes objects instead of local files
- silver ETL rewired to read the partitioned archive, typed timestamps,
  dedup on event_id, MERGE/idempotent writes

## Copyright

Copyright © 2026 Rahul Yadav. All rights reserved.

This repository is published for portfolio and educational reference purposes. The source code may not be copied, modified, redistributed, or used commercially without the author's explicit permission.
