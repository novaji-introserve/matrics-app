#!/bin/bash
# Compliance Daemon — runs screening, escalation and AML rebuild on separate intervals.
# Follows the same pattern as ETL_script/run_forever.sh.

SCREEN_INTERVAL=${SCREEN_INTERVAL:-300}          # default 5 min
ESCALATION_INTERVAL=${ESCALATION_INTERVAL:-3600} # default 1 hour

LOG_FILE="/var/log/icomply/ServerLog.log"

log_job() {
  echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" | tee -a "$LOG_FILE"
}

log_job "========================================"
log_job "Compliance Daemon starting"
log_job "  Screen interval     : ${SCREEN_INTERVAL}s"
log_job "  Escalation interval : ${ESCALATION_INTERVAL}s"
log_job "  AML rebuild         : Sundays 02:00"
log_job "========================================"

cd /app

# Background: escalation engine — every ESCALATION_INTERVAL seconds
run_escalation_loop() {
  while true; do
    sleep "${ESCALATION_INTERVAL}"
    log_job "--- Escalation Engine ---"
    if python3 run_escalation.py >> "$LOG_FILE" 2>&1; then
      log_job "✓ Escalation complete"
    else
      log_job "✗ Escalation failed (non-critical)"
    fi
  done
}

# Background: AML profile rebuild — Sundays at 02:xx, checked hourly
run_rebuild_loop() {
  while true; do
    sleep 3600
    DAY=$(date +%u)   # 1=Mon … 7=Sun
    HOUR=$(date +%H)
    if [ "$DAY" = "7" ] && [ "$HOUR" = "02" ]; then
      log_job "--- AML Profile Rebuild (weekly) ---"
      if python3 aml_profile_rebuild.py >> "$LOG_FILE" 2>&1; then
        log_job "✓ AML rebuild complete"
      else
        log_job "✗ AML rebuild failed (non-critical)"
      fi
      sleep 3600  # skip remainder of this hour to avoid re-run
    fi
  done
}

run_escalation_loop &
ESCALATION_PID=$!
log_job "Escalation loop started (PID: $ESCALATION_PID)"

run_rebuild_loop &
REBUILD_PID=$!
log_job "Rebuild loop started (PID: $REBUILD_PID)"

log_job "Starting transaction screening loop (every ${SCREEN_INTERVAL}s)..."

# Main loop: transaction screening — keeps the container alive
while true; do
  log_job "--- Transaction Screening ---"
  if python3 screen_daemon.py >> "$LOG_FILE" 2>&1; then
    log_job "✓ Screening complete"
  else
    log_job "✗ Screening failed (non-critical)"
  fi
  sleep "${SCREEN_INTERVAL}"
done
