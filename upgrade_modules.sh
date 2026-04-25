#!/usr/bin/env bash


# ./upgrade_modules.sh compliance_management case_management
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-odoo-16-odoo16-1}"
DB_NAME="${DB_NAME:-icomply_dev}"
DB_HOST="${DB_HOST:-pgbouncer}"
DB_PORT="${DB_PORT:-5433}"
DB_USER="${DB_USER:-odoo}"
DB_PASSWORD="${DB_PASSWORD:-odoo16@2022}"

if [ "$#" -eq 0 ]; then
  MODULES="compliance_management"
else
  MODULES="$(IFS=,; echo "$*")"
fi

docker exec -it "$CONTAINER_NAME" bash -lc \
  "/usr/bin/odoo -d $DB_NAME -u $MODULES --db_host=$DB_HOST --db_port=$DB_PORT --db_user=$DB_USER --db_password=$DB_PASSWORD --stop-after-init"
