#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

cd "$(dirname "$0")"

echo "Restarting all services..."
docker compose -f "$COMPOSE_FILE" restart

echo "Service status:"
docker compose -f "$COMPOSE_FILE" ps
