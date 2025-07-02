#!/bin/bash

# Log script start
echo "$(date): Starting Update Customer Profile(PEP, origin)" >> /data/odoo/ETL_script/cron_execution.log

# Run with lock using full path to python in virtual environment
flock -w 60 /tmp/update_cust_profile.lock -c "/data/odoo/.venv/bin/python /data/odoo/ETL_script/update_script/update-delete-script.py --config /data/odoo/ETL_script/update_script/settings.conf" >> /data/odoo/ETL_script/cron_execution.log 2>&1

# Log completion
echo "$(date): Update Customer Profile(PEP, origin) Job completed" >> /data/odoo/ETL_script/cron_execution.log
