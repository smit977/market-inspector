#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/stack_common.sh"
"${COMPOSE[@]}" --profile '*' stop --timeout 60
printf '\nStack stopped. Persistent volumes, .env, and replay archives are preserved.\nIt is now safe to close Terminal and quit Docker Desktop.\n'
