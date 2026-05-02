#!/bin/sh

set -eu

: "${POSTGRES_SERVICE_HOST:=db}"
: "${POSTGRES_SERVICE_PORT:=5432}"
: "${PGBOUNCER_DB_USER:=odoo}"
: "${PGBOUNCER_DB_PASSWORD:=odoo16@2022}"
: "${PGBOUNCER_ADMIN_USERS:=${PGBOUNCER_DB_USER}}"

envsubst '${POSTGRES_SERVICE_HOST} ${POSTGRES_SERVICE_PORT} ${PGBOUNCER_DB_USER} ${PGBOUNCER_DB_PASSWORD} ${PGBOUNCER_ADMIN_USERS}' \
  < /etc/pgbouncer/pgbouncer.ini.template \
  > /etc/pgbouncer/pgbouncer.ini

printf '"%s" "md5%s"\n' \
  "$PGBOUNCER_DB_USER" \
  "$(printf '%s' "${PGBOUNCER_DB_PASSWORD}${PGBOUNCER_DB_USER}" | md5sum | awk '{print $1}')" \
  > /etc/pgbouncer/userlist.txt

exec "$@"
