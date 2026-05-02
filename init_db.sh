#!/usr/bin/env bash
# Initialise a fresh Odoo database on a new deployment.
#
# Usage:
#   ./init_db.sh                        # init base only
#   ./init_db.sh compliance_management  # init base + listed modules
#   DB_NAME=mydb ./init_db.sh           # override database name

set -euo pipefail

load_dotenv() {
  local env_file="$1"
  local line key value

  while IFS= read -r line || [ -n "$line" ]; do
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" != *=* ]] && continue

    key="${line%%=*}"
    value="${line#*=}"

    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"

    if [[ "$value" =~ ^\".*\"$ || "$value" =~ ^\'.*\'$ ]]; then
      value="${value:1:${#value}-2}"
    fi

    export "$key=$value"
  done < "$env_file"
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  load_dotenv "$SCRIPT_DIR/.env"
fi

COMPOSE_FILE="${COMPOSE_FILE:-$SCRIPT_DIR/docker-compose.yml}"
ODOO_SERVICE="${ODOO_SERVICE:-odoo16}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-db}"
DB_NAME="${DB_NAME:-icomply_dev}"
POSTGRES_SERVICE_HOST="${POSTGRES_SERVICE_HOST:-db}"
POSTGRES_SERVICE_PORT="${POSTGRES_SERVICE_PORT:-5432}"
DB_USER="${DB_USER:-odoo}"
DB_PASSWORD="${DB_PASSWORD:-odoo16@2022}"
ODOO_CONF="${ODOO_CONF:-/etc/odoo/odoo.conf}"

if [ "$#" -eq 0 ]; then
  MODULES="base,web,fpg_redis_session,queue_job,ro_cache_redis,access_app"
else
  MODULES="base,web,fpg_redis_session,queue_job,ro_cache_redis,access_app,$(IFS=,; echo "$*")"
fi

echo "==> Creating database '$DB_NAME' on PostgreSQL (direct, bypassing pgbouncer)..."
docker compose -f "$COMPOSE_FILE" exec -T -e PGPASSWORD="$DB_PASSWORD" "$POSTGRES_SERVICE" \
  psql -U "$DB_USER" -d postgres -tc \
  "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" \
  | grep -q 1 && echo "    Database already exists, skipping creation." || \
  docker compose -f "$COMPOSE_FILE" exec -T -e PGPASSWORD="$DB_PASSWORD" "$POSTGRES_SERVICE" \
    psql -U "$DB_USER" -d postgres -c "CREATE DATABASE \"$DB_NAME\";"

echo "==> Initialising Odoo with modules: $MODULES ..."
docker compose -f "$COMPOSE_FILE" exec -T "$ODOO_SERVICE" bash -lc \
  "/usr/bin/odoo -c $ODOO_CONF -d $DB_NAME -i $MODULES \
   --db_host=$POSTGRES_SERVICE_HOST --db_port=$POSTGRES_SERVICE_PORT \
   --db_user=$DB_USER --db_password=$DB_PASSWORD \
   --without-demo=all --stop-after-init"

echo "==> Done. Start Odoo normally with: docker compose up -d odoo16"
