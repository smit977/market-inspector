#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created local .env from .env.example"
fi

command -v openssl >/dev/null 2>&1 || {
  echo "OpenSSL is required to generate local development passwords." >&2
  exit 1
}

generate_secret() {
  openssl rand -hex 24
}

set_if_blank() {
  local key="$1"
  local current
  current="$(awk -F= -v k="$key" '$1==k {sub(/^[^=]*=/,""); print; exit}' .env)"
  if [[ -n "$current" ]]; then
    return 0
  fi
  local value
  value="$(generate_secret)"
  awk -v k="$key" -v v="$value" '
    BEGIN { found=0 }
    index($0, k "=")==1 { print k "=" v; found=1; next }
    { print }
    END { if (!found) print k "=" v }
  ' .env > .env.tmp
  mv .env.tmp .env
  echo "Generated local value for $key"
}

set_if_blank GRAFANA_ADMIN_PASSWORD
set_if_blank CLICKHOUSE_GRAFANA_PASSWORD
set_if_blank CLICKHOUSE_MCP_PASSWORD
chmod 600 .env
echo "Local environment is ready. Secrets remain in gitignored .env."
