<#
run_test.ps1

Runs ONE test at a time, from a completely clean state, so the result
you see is not mixed up with leftovers from a previous test.

USAGE (run from anywhere, in PowerShell, inside your project):
    .\tests\run_test.ps1 -Test 1
    .\tests\run_test.ps1 -Test 2
    .\tests\run_test.ps1 -Test 3
    .\tests\run_test.ps1 -Test 4
    .\tests\run_test.ps1 -Test 5
    .\tests\run_test.ps1 -Test 6      (silver layer reconciles with bronze)
    .\tests\run_test.ps1 -Test 7      (silver-etl resumes correctly after being restarted)
    .\tests\run_test.ps1 -Test 8      (gold layer reconciles exactly with silver)
    .\tests\run_test.ps1 -Test All      (runs all 8, one after another)

Each run:
  1. Wipes old data and containers so you start from zero
  2. Rebuilds and starts everything fresh
  3. Does exactly the one thing that test is checking
  4. Prints a single clear PASS or FAIL
  5. Shuts everything down again, ready for the next test
#>

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("1", "2", "3", "4", "5", "6", "7", "8", "All")]
    [string]$Test
)

# This script lives in a "tests" folder one level under the project root.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

function Write-Pass($msg) { Write-Host $msg -ForegroundColor Green }
function Write-Fail($msg) { Write-Host $msg -ForegroundColor Red }
function Write-Step($msg) { Write-Host $msg -ForegroundColor Cyan }

function Reset-Environment {
    Write-Step "Resetting: removing old containers, network, and broker data..."
    docker compose down -v 2>&1 | Out-Null

    if (Test-Path "data\raw") {
        Remove-Item -Recurse -Force "data\raw\*" -ErrorAction SilentlyContinue
    } else {
        New-Item -ItemType Directory -Path "data\raw" -Force | Out-Null
    }

    if (Test-Path "data\silver") {
        Remove-Item -Recurse -Force "data\silver\*" -ErrorAction SilentlyContinue
    } else {
        New-Item -ItemType Directory -Path "data\silver" -Force | Out-Null
    }

    if (Test-Path "data\gold") {
        Remove-Item -Recurse -Force "data\gold\*" -ErrorAction SilentlyContinue
    } else {
        New-Item -ItemType Directory -Path "data\gold" -Force | Out-Null
    }

    Write-Step "Building and starting fresh containers..."
    # NOTE: do not pipe this command's output on Windows -- doing so
    # (e.g. "| Out-Null") can trigger a known Docker Desktop bug
    # ("failed to get console: The handle is invalid.") that silently
    # kills the build. Letting it print normally avoids that.
    $env:BUILDKIT_PROGRESS = "plain"
    docker compose up -d --build
    if ($LASTEXITCODE -ne 0) {
        Write-Fail "`nFAILED to build/start containers (exit code $LASTEXITCODE). Stopping here -- the test result below would be meaningless."
        exit 1
    }

    Write-Step "Confirming all 5 containers are actually running..."
    Start-Sleep -Seconds 2
    $running = @(docker compose ps --status running --format "{{.Name}}")
    $runningCount = $running.Count
    if ($runningCount -lt 5) {
        Write-Fail "`nOnly $runningCount of 5 containers are running. Something failed silently."
        Write-Host "Run 'docker compose ps' and 'docker compose logs' by hand to see what happened." -ForegroundColor Red
        exit 1
    }
    Write-Pass "All 5 containers confirmed running."
}

function Get-TotalCount {
    $output = py tests\verify_bronze.py
    $match = $output | Select-String "TOTAL_COUNT=(\d+)"
    if ($match) { return [int]$match.Matches[0].Groups[1].Value }
    return 0
}

function Run-Test1 {
    Write-Host "`n===== TEST 1: Baseline correctness =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 30 seconds for normal data to flow..."
    Start-Sleep -Seconds 30
    py tests\verify_bronze.py --check 1
    if ($LASTEXITCODE -eq 0) { Write-Pass "`nTEST 1 RESULT: PASS" } else { Write-Fail "`nTEST 1 RESULT: FAIL" }
    docker compose down | Out-Null
}

function Run-Test2 {
    Write-Host "`n===== TEST 2: Survives a garbage message =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 10 seconds for the pipeline to connect..."
    Start-Sleep -Seconds 10
    Write-Step "Sending one intentionally broken message..."
    docker exec mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -m "not valid json {{{"
    Start-Sleep -Seconds 5
    py tests\verify_bronze.py --check 2
    if ($LASTEXITCODE -eq 0) { Write-Pass "`nTEST 2 RESULT: PASS" } else { Write-Fail "`nTEST 2 RESULT: FAIL" }
    docker compose down | Out-Null
}

function Run-Test3 {
    Write-Host "`n===== TEST 3: No data loss when bronze-writer goes down =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 15 seconds so bronze-writer registers its session first..."
    Start-Sleep -Seconds 15

    Write-Step "Taking a 'before' snapshot..."
    py tests\verify_bronze.py --save-snapshot tests\snapshots\test3_before.json | Out-Null

    Write-Step "Stopping bronze-writer..."
    docker compose stop bronze-writer | Out-Null
    Start-Sleep -Seconds 2
    Write-Step "Publishing 3 test messages while it's down..."
    '{"machine_id":"TEST","event_id":"mytest-1"}' | docker exec -i mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -q 1 -l
    '{"machine_id":"TEST","event_id":"mytest-2"}' | docker exec -i mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -q 1 -l
    '{"machine_id":"TEST","event_id":"mytest-3"}' | docker exec -i mosquitto mosquitto_pub -t "factory/CM-01/telemetry" -q 1 -l
    Write-Step "Restarting bronze-writer..."
    docker compose start bronze-writer | Out-Null
    Start-Sleep -Seconds 5

    Write-Step "Taking an 'after' snapshot..."
    py tests\verify_bronze.py --save-snapshot tests\snapshots\test3_after.json | Out-Null

    Write-Host "`n--- Comparison: what actually changed ---" -ForegroundColor Cyan
    py tests\compare_snapshots.py tests\snapshots\test3_before.json tests\snapshots\test3_after.json

    py tests\verify_bronze.py --check 3 | Out-Null
    if ($LASTEXITCODE -eq 0) { Write-Pass "`nTEST 3 RESULT: PASS" } else { Write-Fail "`nTEST 3 RESULT: FAIL" }
    docker compose down | Out-Null
}

function Run-Test4 {
    Write-Host "`n===== TEST 4: Auto-recovery after broker crash =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 15 seconds for normal data to start flowing..."
    Start-Sleep -Seconds 15

    Write-Step "Taking a 'before' snapshot..."
    py tests\verify_bronze.py --save-snapshot tests\snapshots\test4_before.json | Out-Null

    Write-Step "Stopping the broker..."
    docker compose stop mosquitto | Out-Null
    Start-Sleep -Seconds 10
    Write-Step "Starting the broker again..."
    docker compose start mosquitto | Out-Null
    Write-Step "Waiting 30 seconds to see if things recover on their own..."
    Start-Sleep -Seconds 30

    Write-Step "Taking an 'after' snapshot..."
    py tests\verify_bronze.py --save-snapshot tests\snapshots\test4_after.json | Out-Null

    Write-Host "`n--- Comparison: what actually changed ---" -ForegroundColor Cyan
    py tests\compare_snapshots.py tests\snapshots\test4_before.json tests\snapshots\test4_after.json

    $before = (Get-Content tests\snapshots\test4_before.json | ConvertFrom-Json).records.Count
    $after = (Get-Content tests\snapshots\test4_after.json | ConvertFrom-Json).records.Count
    if ($after -gt $before) { Write-Pass "`nTEST 4 RESULT: PASS (count went up on its own)" }
    else { Write-Fail "`nTEST 4 RESULT: FAIL (count did not increase)" }
    docker compose down | Out-Null
}

function Run-Test5 {
    Write-Host "`n===== TEST 5: Stays stable over time =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 15 seconds for normal data to start flowing..."
    Start-Sleep -Seconds 15
    $before = Get-TotalCount
    Write-Step "Message count now: $before"
    Write-Step "Waiting 3 minutes, untouched, to check stability..."
    Start-Sleep -Seconds 180
    $after = Get-TotalCount
    Write-Step "Message count after 3 minutes: $after"
    Write-Host "`nContainer status (all five should say 'Up', not 'Restarting'):" -ForegroundColor Cyan
    docker compose ps
    if ($after -gt $before) { Write-Pass "`nTEST 5 RESULT: PASS (kept working the whole time)" }
    else { Write-Fail "`nTEST 5 RESULT: FAIL (count did not grow -- something stalled)" }
    docker compose down | Out-Null
}

function Run-Test6 {
    Write-Host "`n===== TEST 6: Silver layer reconciles with bronze =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 30 seconds for data to flow through bronze and silver..."
    Start-Sleep -Seconds 30
    Write-Step "Freezing new data (stopping machine-floor) so silver can catch up..."
    docker compose stop machine-floor | Out-Null
    Write-Step "Waiting 25 seconds for silver-etl's next cycle to fully catch up..."
    Start-Sleep -Seconds 25
    py tests\verify_silver.py --check 6
    if ($LASTEXITCODE -eq 0) { Write-Pass "`nTEST 6 RESULT: PASS" } else { Write-Fail "`nTEST 6 RESULT: FAIL" }
    docker compose down | Out-Null
}

function Run-Test7 {
    Write-Host "`n===== TEST 7: silver-etl resumes correctly after a restart =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 20 seconds so silver-etl processes its first batch..."
    Start-Sleep -Seconds 20
    Write-Step "Stopping silver-etl..."
    docker compose stop silver-etl | Out-Null
    Write-Step "Letting bronze keep accumulating for 15 seconds while silver-etl is down..."
    Start-Sleep -Seconds 15
    Write-Step "Stopping machine-floor so the data stream has a clean endpoint to catch up to..."
    docker compose stop machine-floor | Out-Null
    Write-Step "Restarting silver-etl..."
    docker compose start silver-etl | Out-Null
    Write-Step "Waiting 25 seconds for it to catch up on everything it missed..."
    Start-Sleep -Seconds 25
    py tests\verify_silver.py --check 7
    if ($LASTEXITCODE -eq 0) { Write-Pass "`nTEST 7 RESULT: PASS" } else { Write-Fail "`nTEST 7 RESULT: FAIL" }
    docker compose down | Out-Null
}

function Run-Test8 {
    Write-Host "`n===== TEST 8: Gold layer reconciles with silver =====" -ForegroundColor Yellow
    Reset-Environment
    Write-Step "Waiting 30 seconds for data to flow through bronze, silver, and gold..."
    Start-Sleep -Seconds 30
    Write-Step "Freezing new data (stopping machine-floor) so silver can settle..."
    docker compose stop machine-floor | Out-Null
    Write-Step "Waiting 25 seconds for silver-etl's next cycle to fully catch up..."
    Start-Sleep -Seconds 25
    Write-Step "Restarting gold-etl to force an immediate recompute (instead of waiting up to 60s)..."
    docker compose restart gold-etl | Out-Null
    Write-Step "Waiting 5 seconds for it to write its output..."
    Start-Sleep -Seconds 5
    py tests\verify_gold.py --check 8
    if ($LASTEXITCODE -eq 0) { Write-Pass "`nTEST 8 RESULT: PASS" } else { Write-Fail "`nTEST 8 RESULT: FAIL" }
    docker compose down | Out-Null
}

switch ($Test) {
    "1"   { Run-Test1 }
    "2"   { Run-Test2 }
    "3"   { Run-Test3 }
    "4"   { Run-Test4 }
    "5"   { Run-Test5 }
    "6"   { Run-Test6 }
    "7"   { Run-Test7 }
    "8"   { Run-Test8 }
    "All" {
        Run-Test1
        Run-Test2
        Run-Test3
        Run-Test4
        Run-Test5
        Run-Test6
        Run-Test7
        Run-Test8
        Write-Host "`nAll 8 tests finished. Scroll up to see each individual result." -ForegroundColor Yellow
    }
}