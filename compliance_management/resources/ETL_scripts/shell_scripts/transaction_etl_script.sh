#!/bin/bash

# Log script start
echo "$(date): Starting Transaction ETL job" >> /data/odoo/ETL_script/cron_execution.log

# Run with lock
flock -w 1000 /tmp/etl_trans.lock -c "cd /data/odoo/ETL_script/etl_scripts && /data/odoo/.venv/bin/python /data/odoo/ETL_script/etl_scripts/transaction_etl_script.py" >> /data/odoo/ETL_script/cron_execution.log 2>&1

# Log completion
echo "$(date): Transaction ETL Job completed" >> /data/odoo/ETL_script/cron_execution.log

