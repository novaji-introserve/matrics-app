#!/bin/bash
set -e

bash "${SEATUNNEL_HOME}/bin/seatunnel-cluster.sh" start

# Block and stream logs so the container stays alive
exec tail -f "${SEATUNNEL_HOME}/logs/seatunnel-engine-server.log"
