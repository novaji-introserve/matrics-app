#!/bin/bash
# ============================================================
# SeaTunnel ETL Runner
# Runs all 5 jobs in dependency order on every invocation.
# Designed to be triggered by cron.
#
# Logging:
#   etl_runner.log  — JSON Lines (one JSON object per line).
#                     This is the single Elasticsearch / Filebeat
#                     source.  Every event is self-contained.
#   etl_verbose.log — Raw SeaTunnel output for debugging.
#                     Not indexed by Elasticsearch.
# ============================================================

SEATUNNEL_HOME="${SEATUNNEL_HOME:-/opt/apache-seatunnel-2.3.5}"
JOBS_DIR="${JOBS_DIR:-$(dirname "$0")/jobs}"
LOG_DIR="${LOG_DIR:-$(dirname "$0")/logs}"
LOCK_FILE="/tmp/seatunnel_etl.lock"
LOG_FILE="$LOG_DIR/etl_runner.log"      # JSON Lines — Elasticsearch reads this
VERBOSE_LOG="$LOG_DIR/etl_verbose.log"  # Raw SeaTunnel output — for debugging

mkdir -p "$LOG_DIR"

# Unique ID for every run — ties all events in one cycle together
RUN_ID="$(hostname -s)_$(date -u '+%Y%m%d_%H%M%S')_$$"
HOST="$(hostname -s)"

# ── JSON Lines emitter ────────────────────────────────────────
# Usage: jlog LEVEL EVENT [key value ...]
# Numeric values are written without quotes; strings are quoted.
jlog() {
  local level="$1"
  local event="$2"
  shift 2

  local extras=""
  while [ $# -ge 2 ]; do
    local k="$1" v="$2"
    shift 2
    # Escape any double-quotes inside the value
    v="${v//\\/\\\\}"
    v="${v//\"/\\\"}"
    if [[ "$v" =~ ^-?[0-9]+$ ]]; then
      extras="${extras},\"${k}\":${v}"
    else
      extras="${extras},\"${k}\":\"${v}\""
    fi
  done

  printf '{"timestamp":"%s","level":"%s","event":"%s","run_id":"%s","host":"%s"%s}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "$level" "$event" "$RUN_ID" "$HOST" \
    "$extras" \
    >> "$LOG_FILE"
}

# ── Parse SeaTunnel summary block ────────────────────────────
# Extracts: Total Read Count / Total Write Count / Total Failed Count
parse_seatunnel_stats() {
  local output="$1"
  local r w f
  r=$(printf '%s' "$output" | grep 'Total Read Count'   | grep -o '[0-9][0-9]*' | tail -1)
  w=$(printf '%s' "$output" | grep 'Total Write Count'  | grep -o '[0-9][0-9]*' | tail -1)
  f=$(printf '%s' "$output" | grep 'Total Failed Count' | grep -o '[0-9][0-9]*' | tail -1)
  printf '%d %d %d' "${r:-0}" "${w:-0}" "${f:-0}"
}

# ── Job runner ────────────────────────────────────────────────
run_job() {
  local job_num="$1"
  local job_file="$2"
  local job_name="$3"

  jlog INFO job_start job_num "$job_num" job_name "$job_name" job_file "$job_file"
  local t_start
  t_start=$(date +%s)

  # Capture all SeaTunnel output (stdout + stderr) for parsing
  local raw_output
  raw_output=$("$SEATUNNEL_HOME/bin/seatunnel.sh" \
    --config "$JOBS_DIR/$job_file" \
    --master local 2>&1)
  local exit_code=$?
  local elapsed=$(( $(date +%s) - t_start ))

  # Append raw output to verbose log with a header so it's navigable
  {
    printf '\n### Job %d — %s — %s ###\n' "$job_num" "$job_name" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf '%s\n' "$raw_output"
  } >> "$VERBOSE_LOG"

  # Extract row counts from SeaTunnel's summary block
  local rows_read rows_written rows_failed
  read -r rows_read rows_written rows_failed <<< "$(parse_seatunnel_stats "$raw_output")"

  if [ "$exit_code" -eq 0 ]; then
    jlog INFO job_success \
      job_num    "$job_num"      job_name      "$job_name" \
      duration_s "$elapsed"      rows_read     "$rows_read" \
      rows_written "$rows_written" rows_failed "$rows_failed"
  else
    jlog ERROR job_failed \
      job_num    "$job_num"      job_name      "$job_name" \
      duration_s "$elapsed"      exit_code     "$exit_code" \
      rows_read  "$rows_read"    rows_written  "$rows_written" \
      rows_failed "$rows_failed"
  fi

  return "$exit_code"
}

# ── Lock — prevent overlapping runs ──────────────────────────
if [ -f "$LOCK_FILE" ]; then
  LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null)
  if kill -0 "$LOCK_PID" 2>/dev/null; then
    jlog WARN cycle_skipped message "Previous ETL run (PID $LOCK_PID) still active"
    exit 0
  else
    jlog WARN lock_stale message "Stale lock (PID $LOCK_PID) removed"
    rm -f "$LOCK_FILE"
  fi
fi
echo $$ > "$LOCK_FILE"

# ── Load env vars ─────────────────────────────────────────────
ENV_FILE="${ENV_FILE:-$(dirname "$0")/../../.env}"
if [ -f "$ENV_FILE" ]; then
  set -o allexport
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +o allexport
fi

# ── Rotate verbose log if > 200 MB ───────────────────────────
if [ -f "$VERBOSE_LOG" ]; then
  size=$(stat -c%s "$VERBOSE_LOG" 2>/dev/null || echo 0)
  if [ "$size" -gt 209715200 ]; then
    mv "$VERBOSE_LOG" "${VERBOSE_LOG}.1"
  fi
fi

# ── ETL cycle ─────────────────────────────────────────────────
CYCLE_START=$(date +%s)
jlog INFO cycle_start

FAILED=0

# Job 01 — reference tables (no upstream dependencies)
run_job 1 "01_reference_tables.conf" "Reference Tables" || FAILED=1

# Job 02 — customers (needs reference tables)
if [ $FAILED -eq 0 ]; then
  run_job 2 "02_customers.conf" "Customers" || FAILED=1
else
  jlog WARN job_skipped job_num 2 job_name "Customers" reason "Job 1 failed"
fi

# Job 03 — accounts (needs customers)
if [ $FAILED -eq 0 ]; then
  run_job 3 "03_accounts.conf" "Accounts" || FAILED=1
else
  jlog WARN job_skipped job_num 3 job_name "Accounts" reason "Upstream failure"
fi

# Job 04 — transactions (needs accounts)
if [ $FAILED -eq 0 ]; then
  run_job 4 "04_transactions.conf" "Transactions" || FAILED=1
else
  jlog WARN job_skipped job_num 4 job_name "Transactions" reason "Upstream failure"
fi

# Job 05 — digital products & watchlists (independent — always runs)
run_job 5 "05_digital_products.conf" "Digital Products & Watchlists"
JOB5_RC=$?

# ── Cycle summary ─────────────────────────────────────────────
rm -f "$LOCK_FILE"

CYCLE_ELAPSED=$(( $(date +%s) - CYCLE_START ))

if [ $FAILED -eq 0 ] && [ $JOB5_RC -eq 0 ]; then
  jlog INFO cycle_end status "success" duration_s "$CYCLE_ELAPSED"
else
  jlog ERROR cycle_end status "failed"  duration_s "$CYCLE_ELAPSED"
fi
