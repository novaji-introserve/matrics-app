#!/usr/bin/env bash
# Initialise a fresh Odoo database on a new deployment.
#
# Usage:
#   ./init_db.sh                        # init base only
#   ./init_db.sh compliance_management  # init base + listed modules
#   DB_NAME=mydb ./init_db.sh           # override database name

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -o allexport
  source "$SCRIPT_DIR/.env"
  set +o allexport
fi

CONTAINER_NAME="${CONTAINER_NAME:-matrics-odoo16-1}"
DB_CONTAINER="${DB_CONTAINER:-matrics-db-1}"
DB_NAME="${DB_NAME:-icomply_dev}"
DB_HOST="${DB_HOST:-pgbouncer}"
DB_PORT="${DB_PORT:-5433}"
DB_USER="${DB_USER:-odoo}"
DB_PASSWORD="${DB_PASSWORD:-odoo16@2022}"
ODOO_CONF="${ODOO_CONF:-/etc/odoo/odoo.conf}"

if [ "$#" -eq 0 ]; then
  MODULES="base"
else
  MODULES="base,$(IFS=,; echo "$*")"
fi

echo "==> Creating database '$DB_NAME' on PostgreSQL (direct, bypassing pgbouncer)..."
docker exec "$DB_CONTAINER" psql -U "$DB_USER" -tc \
  "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" \
  | grep -q 1 && echo "    Database already exists, skipping creation." || \
  docker exec "$DB_CONTAINER" psql -U "$DB_USER" -c "CREATE DATABASE \"$DB_NAME\";"

echo "==> Initialising Odoo with modules: $MODULES ..."
docker exec -it "$CONTAINER_NAME" bash -lc \
  "/usr/bin/odoo -c $ODOO_CONF -d $DB_NAME -i $MODULES \
   --db_host=$DB_HOST --db_port=$DB_PORT \
   --db_user=$DB_USER --db_password=$DB_PASSWORD \
   --without-demo=all --stop-after-init"

echo "==> Done. Start Odoo normally with: docker compose up -d odoo16"
