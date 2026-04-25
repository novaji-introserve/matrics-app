#!/bin/bash

# Log script start
echo "$(date): Starting Compute Average Risk Score For Account Job" >> /data/odoo/ETL_script/cron_execution.log

# Set paths for better readability
PYTHON_PATH="/data/odoo/.venv/bin/python"
SCRIPT_PATH="/data/odoo/ETL_script/update_script/compute_account_score.py"
CONFIG_PATH="/data/odoo/ETL_script/update_script/settings.conf"
LOG_FILE="/data/odoo/ETL_script/cron_execution.log"

# Run with lock using full path to python in virtual environment
flock -w 600 /tmp/aggregate.lock -c "$PYTHON_PATH $SCRIPT_PATH --config $CONFIG_PATH" >> "$LOG_FILE" 2>&1

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "$(date): Compute Average Risk Score For Account Job completed successfully" >> "$LOG_FILE"
else
    echo "$(date): Compute Average Risk Score For Account Job completed successfully" >> "$LOG_FILE"
fi