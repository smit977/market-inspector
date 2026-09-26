# Market Move Integrity MCP server

The MCP service exposes read-only analytical tools backed by the curated
ClickHouse database. It does not consume Kafka, connect to exchange accounts,
place orders, or produce trading recommendations. Kafka remains the transport
and ClickHouse remains the historical query layer.

## Start and connect

Set non-empty `CLICKHOUSE_MCP_PASSWORD` in the ignored `.env` file. Copy
`.env.example` to `.env` if needed, then start MCP and its ClickHouse reader
provisioner:

```sh
docker compose up -d --build mcp-server
docker compose ps mcp-server clickhouse-mcp-user clickhouse
```

The local Streamable HTTP endpoint is:

```text
http://127.0.0.1:8001/mcp
```

The container listens on `MCP_HOST` (default `0.0.0.0`) and `MCP_PORT` (default
`8001`); Compose publishes it only on `127.0.0.1`. `MCP_TRANSPORT=stdio` runs
the same registered tools over stdio for a local MCP host. HTTP is the Docker
default. Health is checked by verifying the local listener; each data tool
independently reports a structured ClickHouse-unavailable error if storage is
down.

## Tools

- `get_market_summary` — spot, cross-venue, derivatives, latest stored integrity
  event/anomaly, and data freshness.
- `get_latest_market_metrics` — pair/venue/window spot metrics.
- `get_active_market_moves` — recent anomaly-triggered moves, with optional
  severity floor.
- `get_integrity_score` and `get_integrity_components` — stored integrity
  result, component availability and evidence.
- `compare_exchanges` and `get_cross_venue_confirmation` — venue observations,
  confirmation, dispersion, deviation, volume share, and present lead/lag data.
- `get_orderbook_analysis` — best prices, spread, depth, imbalance, change
  fields, and related neutral liquidity evidence.
- `get_derivatives_analysis` and `get_liquidation_activity` — venue-level
  derivative and liquidation evidence. Missing financial fields stay null.
- `get_anomalies` — bounded recent anomaly events and evidence.
- `get_historical_similar_events` — deterministic weighted feature similarity.
  It is not outcome prediction and does not predict future prices.
- `get_data_quality` and `get_pipeline_health` — persisted table freshness and
  only health signals available from ClickHouse. Kafka lag, source WebSocket
  state, and DLQ counts are reported unavailable because they are not persisted
  in the analytical database.
- `query_clickhouse` — restricted table/column selector; it does not accept SQL.
- `generate_incident_report` — report-ready stored event evidence and a bounded
  timeline without causal claims.

Example tool arguments:

```json
{"canonical_pair":"BTC-USD","lookback_minutes":15}
```

```json
{"table":"market_metrics","columns":["timestamp","exchange","vwap","spread_bps"],"canonical_pair":"BTC-USD","lookback_minutes":15,"limit":100}
```

Every response includes `generated_at`, `data_as_of`, `source_tables`,
`quality_flags`, and `query_duration_ms`. The `evidence` object separates
`observed_fact`, `calculated_metric`, `statistical_inference`, and
`ai_interpretation`; the MCP layer leaves AI interpretation null. A missing
integrity event is returned as an explicit no-event state, never as a fabricated
score.

## Security and query bounds

`clickhouse-mcp-user` provisions `mcp_reader` with SELECT only on the five
curated tables in `market_integrity`. It has no write, schema, or system-table
grants. Set and rotate `CLICKHOUSE_MCP_PASSWORD` via `.env`; it is not embedded
in responses or source-controlled files.

All tool SQL is authored by the server, parameterized, filtered by a bounded
lookback or specific event ID, ordered by time, and capped. Regular lookbacks
are limited to one day; latest integrity/event lookup is capped at 365 days,
event-correlated evidence uses a 30-minute window, `get_data_quality` accepts
at most 60 minutes, the pipeline health probe checks dense feeds over five
minutes, and the constrained table query allows at most 30 days. Anomaly and
integrity tables are sparse event histories; an interval without a new event is
not treated as a failed stream feed.
`query_clickhouse` accepts only an allowlisted table and its allowlisted columns,
defaults to at most 500 rows, and caps at 5,000. Each query has a configurable
five-second execution timeout (`MCP_QUERY_TIMEOUT_SECONDS`, maximum 30 seconds).
The ClickHouse reader also enforces a 5,000-row server-side ceiling. Hot metric
and integrity lookups avoid memory-intensive `FINAL` reads: bounded queries
deduplicate by the preserved `event_id`, then apply the result limit. Stored
event IDs remain available to identify at-least-once duplicates in evidence.

Similarity V1 uses deterministic, documented feature weights over trigger,
classification, direction, severity, integrity score, and available component
scores. It compares stored feature evidence only; it makes no outcome or causal
claim. Kafka lag and system DLQ counts are not currently queryable through this
MCP service and are returned as unavailable.

## Troubleshooting

```sh
docker compose logs --tail=100 clickhouse-mcp-user mcp-server
docker compose exec mcp-server python -c 'import socket; socket.create_connection(("clickhouse", 8123), 2).close()'
```

If the reader password changed, rerun its one-shot provisioner and restart MCP:

```sh
docker compose run --rm clickhouse-mcp-user
docker compose restart mcp-server
```

ClickHouse outages return structured errors and the next request creates a new
client connection. The MCP service is not a dependency of the market pipeline,
writer, or Grafana; stopping it does not stop those services.

## Current conditions versus triggered events

`get_market_summary.data.current_market_state` contains the latest calculated
spot/cross-venue metrics and derivatives observations. Spot fields describe one
venue's 30-second window, not consolidated market volume; `spot_metrics_context`
identifies that venue, symbol, timestamp and units. The reference price is a
cross-venue median and has its own timestamp. Derivatives use a one-minute window.

`latest_detected_move` and `latest_triggered_integrity_event` are separate stored
statistical assessments. Each has `event_age_seconds`, `event_status`,
`assessment_scope: triggered_event_only` and `describes_current_market: false`.
An event older than the service's freshness threshold (60 seconds by default)
is historical context, even if it is the latest event. The legacy
`latest_anomaly` and `latest_integrity_event` aliases retain the same metadata.
`triggered_move_state: no_recent_triggered_move` means no fresh event was found
within the bounded queries; it is not an assertion that market integrity is
normal, healthy, or fully observed. Missing scores remain absent/null.

`freshness` separately reports market metrics, cross-venue metrics, derivatives,
anomalies and integrity events, including timestamps, ages and the threshold.
Convenience fields include `market_data_status`, `cross_venue_status`,
`derivatives_status`, `latest_anomaly_status`, and `latest_integrity_event_status`.
States are `fresh`, `stale`, or `no_data`. Summary envelope status concerns only
live metric sources; stale or absent events alone do not make it stale. Stale
or absent derivatives still qualify the summary even when spot is fresh.
`data_as_of` is the newest live metric time, not a guarantee that every source
is equally fresh. `get_data_quality` uses the same distinction for sparse event
tables, with no synthetic epoch timestamp for empty queries. Its freshness is
scoped to the requested lookback, not the full event history.

Cross-venue `confirmation_state` is one of `broad`, `partial`, `fragmented`,
`neutral`, or `insufficient_data`. The first three refer to directional returns;
`neutral` means all usable venue returns are inside the producer's configured
flat threshold (including its boundaries). Existing numeric ratios and the
0.8/0.6 directional cutoffs are unchanged. MCP adds `directional_signal_present`
(false for neutral, null for insufficient data). Legacy persisted zero-ratio
rows are interpreted as neutral using the producer's ratio, without applying a
second potentially different flat threshold. New calculations require enough
usable venue returns as well as active prices. Partial raw-table projections
that omit the evidence needed for this interpretation remain raw.

For AI clients, current calculated conditions appear under
`evidence.calculated_metric`; latest events appear under
`evidence.statistical_inference`. `data_limitations` groups quality flags by source, so historical event limitations
are not mistaken for current feed problems. The envelope flags remain a union.
No narrative trading advice or synthetic integrity assessment is generated.
