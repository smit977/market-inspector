#!/usr/bin/env bash
# Shared local operations configuration. No credentials are sourced or printed.
set -euo pipefail
STACK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$STACK_ROOT"
[[ -f .env ]] || { echo 'Missing local .env; restore it before operating the stack.' >&2; exit 1; }
export COMPOSE_PROFILES= PROCESSING_MODE=live
COMPOSE=(docker compose --project-name market-move-integrity --file "$STACK_ROOT/docker-compose.yml" --env-file "$STACK_ROOT/.env")
LIVE_SERVICES=(kafka kafka-ui clickhouse clickhouse-writer grafana mcp-server
 binance-trades coinbase-trades kraken-trades okx-trades bybit-trades
 binance-orderbook trade-normalizer orderbook-reconstructor feature-engine
 cross-venue-features derivatives-ingestion derivatives-features move-detector integrity-engine)
