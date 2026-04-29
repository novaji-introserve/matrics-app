#!/bin/bash
# Manual one-shot trigger — runs all compliance jobs once in sequence.
# Use this to test or force a full run on demand.
# run_forever.sh handles the scheduled path in production.

set -euo pipefail
cd /app

LOG_FILE="/var/log/icomply/ServerLog.log"

log_job() {
  echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" | tee -a "$LOG_FILE"
}

log_job "========================================"
log_job "Compliance Jobs — manual full run"
log_job "========================================"

log_job "[1/3] Transaction Screening"
if python3 screen_daemon.py >> "$LOG_FILE" 2>&1; then
  log_job "✓ Screening complete"
else
  log_job "✗ Screening failed"
  exit 1
fi

log_job "[2/3] Escalation Engine"
if python3 run_escalation.py >> "$LOG_FILE" 2>&1; then
  log_job "✓ Escalation complete"
else
  log_job "✗ Escalation failed (non-critical)"
fi

log_job "[3/3] AML Profile Rebuild"
if python3 aml_profile_rebuild.py >> "$LOG_FILE" 2>&1; then
  log_job "✓ AML rebuild complete"
else
  log_job "✗ AML rebuild failed (non-critical)"
fi

log_job "========================================"
log_job "Manual run complete"
log_job "========================================"
