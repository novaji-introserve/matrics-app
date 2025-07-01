#!/bin/bash

# Log script start
echo "$(date): Starting Compute Risk Rating/Score job" >> /data/odoo/ETL_script/cron_execution.log

# Run with lock
# flock -w 500 /tmp/compute_risk.lock -c "cd /data/odoo/ETL_script/update_script && /data/odoo/.venv/bin/python /data/odoo/ETL_script/update_script/compute_risk_score.py --config /data/odoo/ETL_script/update_script/settings.conf" >> /data/odoo/ETL_script/cron_execution.log 2>&1
flock -w 500 /tmp/compute_risk.lock -c "cd /data/odoo/ETL_script/update_script && /data/odoo/.venv/bin/python /data/odoo/ETL_script/update_script/compute_risk_score.py --config /data/odoo/ETL_script/update_script/settings.conf" >> /data/odoo/ETL_script/cron_execution.log 2>&1


# Log completion
echo "$(date): Compute Risk Rating/Score Job completed" >> /data/odoo/ETL_script/cron_execution.log