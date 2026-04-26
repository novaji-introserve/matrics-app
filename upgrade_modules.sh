#!/usr/bin/env bash


# ./upgrade_modules.sh compliance_management case_management
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -o allexport
  source "$SCRIPT_DIR/.env"
  set +o allexport
fi

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

docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec odoo16 \
  /usr/bin/odoo -d "$DB_NAME" -u "$MODULES" \
  --db_host="$DB_HOST" --db_port="$DB_PORT" \
  --db_user="$DB_USER" --db_password="$DB_PASSWORD" \
  --stop-after-init
