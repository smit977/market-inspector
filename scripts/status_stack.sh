#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/stack_common.sh"
"${COMPOSE[@]}" ps --all
ids="$("${COMPOSE[@]}" ps --all --quiet)"
if [[ -z "$ids" ]]; then echo 'No project containers found.'; exit 1; fi
export LIVE_SERVICE_LIST="$(printf '%s\n' "${LIVE_SERVICES[@]}")"
docker inspect --format '{{json .}}' $ids | python3 -c '
import json, os, sys
services = os.environ["LIVE_SERVICE_LIST"].splitlines()
containers = {}
for line in sys.stdin:
    c = json.loads(line)
    name = c.get("Config", {}).get("Labels", {}).get("com.docker.compose.service")
    state = c.get("State", {})
    health = state.get("Health", {}).get("Status", "no-healthcheck")
    containers[name] = (state.get("Status", "missing"), health, c.get("RestartCount", 0))
failed = False
for name in services:
    state, health, restarts = containers.get(name, ("missing", "missing", "?"))
    print(f"{name:26} {state} {health} restarts={restarts}")
    if state != "running" or health in ("unhealthy", "starting"):
        failed = True
sys.exit(int(failed))
'
