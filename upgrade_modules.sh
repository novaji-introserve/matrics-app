#!/usr/bin/env bash

# Usage:
#   ./upgrade_modules.sh                         # update compliance_management
#   ./upgrade_modules.sh alert_management        # install/update one module
#   ./upgrade_modules.sh mod1 mod2 mod3          # install/update several at once
#   ./upgrade_modules.sh --full                  # full sequential deploy (all modules in order)

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

DB_NAME="${DB_NAME:-icomply_dev}"
DB_USER="${DB_USER:-odoo}"
DB_PASSWORD="${DB_PASSWORD:-odoo16@2022}"
ODOO_SERVICE="${ODOO_SERVICE:-odoo16}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-db}"

# Odoo upgrade must connect directly to PostgreSQL, NOT through pgbouncer.
# The upgrade process calls _create_empty_database() which connects to the
# 'postgres' system database — pgbouncer only proxies app databases, so
# connections through it fail at upgrade time.
POSTGRES_SERVICE_HOST="${POSTGRES_SERVICE_HOST:-db}"
POSTGRES_SERVICE_PORT="${POSTGRES_SERVICE_PORT:-5432}"

ODOO_CONF="/etc/odoo/odoo.conf"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Canonical deployment order — respects module dependency graph
FULL_DEPLOY_SEQUENCE=(
  access_apps 
  compliance_management
  alert_management
  case_management
  regulatory_reports
  nfiu_reporting
)

# Return the install state of a module from the database.
# Runs psql inside the db container (direct PostgreSQL, no pgbouncer needed).
get_module_state() {
  local module="$1"
  local state

  if ! state="$(
    docker compose -f "$COMPOSE_FILE" exec -T -e PGPASSWORD="$DB_PASSWORD" "$POSTGRES_SERVICE" \
      psql -U "$DB_USER" -d "$DB_NAME" -tAc \
      "SELECT state FROM ir_module_module WHERE name='$module' LIMIT 1;" \
      | tr -d '[:space:]'
  )"; then
    echo "Failed to query module state for '$module' from PostgreSQL." >&2
    return 1
  fi

  printf '%s' "$state"
}

run_module() {
  local module="$1"
  local state
  state=$(get_module_state "$module")

  local flag
  if [ "$state" = "installed" ]; then
    flag="-u"
    echo ">>> Updating: $module"
  else
    flag="-i"
    echo ">>> Installing: $module"
  fi

  docker compose -f "$COMPOSE_FILE" exec -T "$ODOO_SERVICE" \
    /usr/bin/odoo \
    -c "$ODOO_CONF" \
    -d "$DB_NAME" \
    "$flag" "$module" \
    --db_host="$POSTGRES_SERVICE_HOST" \
    --db_port="$POSTGRES_SERVICE_PORT" \
    --db_user="$DB_USER" \
    --db_password="$DB_PASSWORD" \
    --stop-after-init
}

if [ "${1:-}" = "--full" ]; then
  echo "=== Full deployment sequence ==="
  for module in "${FULL_DEPLOY_SEQUENCE[@]}"; do
    run_module "$module"
    echo "--- $module done ---"
  done
  echo "=== Full deployment complete ==="
  exit 0
fi

# Single / ad-hoc invocation
if [ "$#" -eq 0 ]; then
  MODULES="compliance_management"
else
  MODULES="$(IFS=,; echo "$*")"
fi

INSTALL_MODULES=""
UPDATE_MODULES=""

for module in ${MODULES//,/ }; do
  state=$(get_module_state "$module")
  if [ "$state" = "installed" ]; then
    UPDATE_MODULES="${UPDATE_MODULES:+$UPDATE_MODULES,}$module"
  else
    INSTALL_MODULES="${INSTALL_MODULES:+$INSTALL_MODULES,}$module"
  fi
done

echo "Installing: ${INSTALL_MODULES:-none}"
echo "Updating:   ${UPDATE_MODULES:-none}"

ODOO_FLAGS=()
[ -n "$INSTALL_MODULES" ] && ODOO_FLAGS+=(-i "$INSTALL_MODULES")
[ -n "$UPDATE_MODULES"  ] && ODOO_FLAGS+=(-u "$UPDATE_MODULES")

docker compose -f "$COMPOSE_FILE" exec -T "$ODOO_SERVICE" \
  /usr/bin/odoo \
  -c "$ODOO_CONF" \
  -d "$DB_NAME" \
  "${ODOO_FLAGS[@]}" \
  --db_host="$POSTGRES_SERVICE_HOST" \
  --db_port="$POSTGRES_SERVICE_PORT" \
  --db_user="$DB_USER" \
  --db_password="$DB_PASSWORD" \
  --stop-after-init
