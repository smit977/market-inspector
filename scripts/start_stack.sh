#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/stack_common.sh"
"${COMPOSE[@]}" config --quiet
"${COMPOSE[@]}" up --detach --wait --wait-timeout 240 "${LIVE_SERVICES[@]}"
"$STACK_ROOT/scripts/status_stack.sh"
printf '\nKafka UI: http://127.0.0.1:8080\nGrafana:  http://127.0.0.1:3000\nMCP:      http://127.0.0.1:8001/mcp\nLive stack started. Stateful feature windows warm up after restart.\n'
