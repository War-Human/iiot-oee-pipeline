# How to Test Your Arista Project — Step by Step

**Faster option:** once you're comfortable with what each test checks
(read below at least once), you can run any single test in full
isolation with one command instead of typing everything by hand:

    .\tests\run_test.ps1 -Test 1
    .\tests\run_test.ps1 -Test 8
    .\tests\run_test.ps1 -Test All

This resets everything to a clean state before each test, so results
never get mixed up with leftovers from a previous run. The manual
steps below still matter for understanding *why* each check exists.

There are also two fast unit tests that need no Docker at all — see
the "UNIT TESTS" section near the bottom. Run those first; they take
under a second combined.

---

Follow this from top to bottom. Every command is meant to be copy-pasted
exactly as written. Whenever it says "PowerShell", it means the terminal
inside VS Code (View menu -> Terminal, or Ctrl+` ).

Before you start:
- Docker Desktop must be open and fully started (its whale icon in your
  system tray should be steady, not animating/loading).
- VS Code must have your project folder open (the one with
  docker-compose.yml directly inside it).
- Your `tests` folder should contain: run_test.ps1, verify_bronze.py,
  verify_silver.py, verify_gold.py, compare_snapshots.py,
  test_silver_logic.py, test_gold_logic.py
- On your machine, the Python command is `py`, not `python` — every
  place below that says `python ...`, type `py ...` instead.
- Each new terminal window needs
  `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` run
  once before `.ps1` scripts will work.

---

## STEP 0 — Start everything fresh

    docker compose down -v
    docker compose up -d --build

Wait about 15 seconds, then check everything actually started:

    docker compose ps

**What you should see:** FIVE rows (mosquitto, machine-floor,
bronze-writer, silver-etl, gold-etl), each with a STATUS that says
something like "Up X seconds". If any row says "Exited" or
"Restarting" over and over, something is wrong -- stop here and check
`docker compose logs <service>` before continuing.

---

## TEST 1 — Does data flow correctly, with nothing broken?

1. Wait about 2 minutes, doing nothing.
2. Run:

       docker compose logs bronze-writer

   **What you should see:** lines like
   `[info] 100 messages filed (0 malformed)` — that number in
   parentheses should stay at 0. No "Traceback" or "[error]" anywhere.
3. Run the automatic checker:

       py tests\verify_bronze.py

   **PASS looks like:** `TEST 1 (baseline correctness): PASS`

---

## TEST 2 — Does the system survive a broken/garbage message instead of crashing?

1. Send one message that is intentionally garbage, not real data:

       docker exec mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -m "not valid json {{{"

2. Wait 5 seconds, then check nothing crashed:

       docker compose logs bronze-writer --tail 10

   **PASS:** normal-looking log lines, no crash. `py tests\verify_bronze.py`
   should show `Malformed/parse_error notes: 1`.

---

## TEST 3 — If bronze-writer goes down, does it lose data?

1. Stop the writer on purpose:

       docker compose stop bronze-writer

2. While it's stopped, send 3 clearly-labeled test messages. Use this
   exact piped format — it avoids a Windows quoting bug that can
   silently corrupt the JSON:

       '{"machine_id":"TEST","event_id":"mytest-1"}' | docker exec -i mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -q 1 -l
       '{"machine_id":"TEST","event_id":"mytest-2"}' | docker exec -i mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -q 1 -l
       '{"machine_id":"TEST","event_id":"mytest-3"}' | docker exec -i mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -q 1 -l

   Each command should run silently with no error.
3. Start the writer again:

       docker compose start bronze-writer

4. Wait 5 seconds, then run the checker again:

       py tests\verify_bronze.py

   **PASS:** all three markers say "FOUND -> the message survived".

---

## TEST 4 — If the message broker crashes, does everything recover by itself?

1. Stop the broker:

       docker compose stop mosquitto

2. Wait 10 seconds, then start it again:

       docker compose start mosquitto

3. Wait 30 seconds, doing nothing.
4. Run the checker, note the "Total messages recorded" number.
5. Wait another 30 seconds, run it again.

   **PASS:** the total went UP between the two runs — proves everything
   reconnected on its own. (`docker compose logs machine-floor` may
   still show stale "publish failed" warnings even after it's actually
   recovered — that's a known cosmetic logging delay, not a real
   failure. Trust the growing message count instead.)

---

## TEST 5 — Does it stay stable if you just leave it running?

1. Leave everything running, untouched, for 15 minutes.
2. `docker compose ps` — all FIVE containers should still say "Up",
   with uptime close to 15 minutes (not repeatedly resetting, which
   would mean something is crash-looping).
3. `py tests\verify_bronze.py` — total message count should be much
   higher than before, proving it kept working the whole time.

---

## TEST 6 — Does the silver layer reconcile exactly with bronze?

1. Let the pipeline run normally for about 30 seconds.
2. Freeze new data so silver can catch up:

       docker compose stop machine-floor

3. Wait 25 seconds (silver-etl's check cycle).
4. Run:

       py tests\verify_silver.py

   **PASS:** "Reconciliation" says PASS (silver record count exactly
   matches bronze's valid record count), no duplicates, and
   "batch_complete false-flags: 0".

---

## TEST 7 — Does silver-etl catch up correctly after being restarted?

1. Let the pipeline run for about 20 seconds so silver-etl processes
   its first batch.
2. Stop it:

       docker compose stop silver-etl

3. Let bronze keep accumulating for 15 seconds, then freeze new data:

       docker compose stop machine-floor

4. Restart silver-etl:

       docker compose start silver-etl

5. Wait 25 seconds, then run:

       py tests\verify_silver.py

   **PASS:** reconciliation still PASSes — silver caught up on
   everything bronze collected while it was down, nothing lost,
   nothing duplicated.

---

## TEST 8 — Does the gold layer reconcile exactly with silver?

1. Let the pipeline run normally for about 30 seconds.
2. Freeze new data so silver can settle:

       docker compose stop machine-floor

3. Wait 25 seconds (silver-etl's check cycle).
4. Restart gold-etl to force it to recompute immediately instead of
   waiting up to 60 seconds for its next natural cycle:

       docker compose restart gold-etl

5. Wait 5 seconds, then run:

       py tests\verify_gold.py

   **PASS:** every gold row exactly matches an independent
   recomputation from silver (not just "looks reasonable" — a real,
   byte-for-byte comparison), and `quality_pct`/`oee_pct` are null on
   every row (the honesty guard — these must never get silently
   filled in with a fake number until a real data source exists for
   them).

---

## UNIT TESTS — Fast logic checks (no Docker needed)

Run these any time you change `clean_to_silver.py` or `build_gold.py`,
or just to sanity check the math in under a second:

    py tests\test_silver_logic.py
    py tests\test_gold_logic.py

**PASS looks like:** every line says `[PASS]`, ending in
`All checks passed.` These include permanent regression guards —
`batch_complete` must never get flagged as "temperature_missing"
again, and `quality_pct`/`oee_pct` must never come back non-null
without a real fix that actually earns it.

---

## Cleaning up when you're done testing

    docker compose down

This stops everything. Your data stays in `data/raw`, `data/silver`,
and `data/gold` either way, so nothing is lost by stopping. Use
`docker compose down -v` instead if you also want to reset the
broker's saved session data.

---

## Test results log

| Date       | Unit-S | Unit-G | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | Notes |
|------------|--------|--------|---|---|---|---|---|---|---|---|-------|
| 2026-09-22 | PASS   | —      | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — | First full run after the bronze-writer session-persistence fix and the silver-etl rebuild. All 4 containers running at the time (gold-etl didn't exist yet). |
|            |        |        |   |   |   |   |   |   |   |   |       |

("Unit-S" = test_silver_logic.py, "Unit-G" = test_gold_logic.py)