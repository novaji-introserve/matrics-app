#!/bin/bash

# Log script start
echo "$(date): Starting Get Customer job" >> /data/odoo/ETL_script/cron_execution.log

# Run with lock
flock -w 500 /tmp/etl_scripts.lock -c "cd /data/odoo/ETL_script/etl_scripts && /data/odoo/.venv/bin/python /data/odoo/ETL_script/etl_scripts/customer_etl_script.py" >> /data/odoo/ETL_script/cron_execution.log 2>&1

# Log completion
echo "$(date): Get Customer Job completed" >> /data/odoo/ETL_script/cron_execution.log

