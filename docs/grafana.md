# Grafana dashboards

Grafana is provisioned locally at [http://127.0.0.1:3000](http://127.0.0.1:3000).
Start the existing pipeline, ClickHouse, and Grafana with:

```sh
docker compose up -d --build
```

Or start only Grafana and its ClickHouse dependencies with:

```sh
docker compose up -d --build grafana
```

The Grafana container installs the pinned Grafana Labs `grafana-clickhouse-datasource`
plugin at startup and provisions the `Market Integrity ClickHouse` datasource
using the Docker hostname `clickhouse` and native port `9000`. The datasource
uses the dedicated `grafana_reader` account. A one-shot dependency creates or
updates that account, sets it read-only, allows only the query-timeout setting
required by the plugin, removes direct grants, and grants SELECT only on the five
analytical tables. It never receives write or schema privileges.

Set these variables in an ignored `.env` file before starting the stack:

```dotenv
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=
CLICKHOUSE_GRAFANA_USER=grafana_reader
CLICKHOUSE_GRAFANA_PASSWORD=
```

The checked-in `.env.example` has blank password placeholders. Set non-empty
passwords in the ignored `.env` before starting the stack; Compose fails closed
if either password is absent. The Grafana administrator login comes from
`GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD`. Rotate the reader password
by updating `.env` and rerunning:

```sh
docker compose run --rm clickhouse-grafana-user
docker compose restart grafana
```

## Dashboards

The `Market Move Integrity` folder contains seven provisioned dashboards:

- **Market Overview** — reference price, spot metrics, book depth, venue confirmation/dispersion, derivatives evidence, triggered integrity state, and freshness.
- **Market Move Integrity** — score history, confidence, coverage, classifications, component scores/evidence, trigger details, and quality flags.
- **Cross-Venue Analysis** — venue prices/deviations/returns/shares, confirmation, dispersion, venue counts, concentration, and observed lead/lag.
- **Derivatives & Liquidations** — venue-level perpetual/spot prices, basis, volume, open interest, funding, and liquidations.
- **Anomaly Center** — recent events, time series, event/severity/detector breakdowns, and evidence.
- **Historical Event Comparison** — select persisted integrity event IDs and compare their top-level and component evidence. It does not perform similarity search.
- **Streaming Infrastructure Health** — persisted row freshness and event-rate estimates. Kafka lag, worker health, producer errors, DLQ rate, and WebSocket status are explicitly not persisted or represented as zero.

Operational dashboards default to the last 15 minutes and refresh every 2
seconds. The 1-second option is available, but is not the default because the
dashboards contain multiple ClickHouse panels and use `FINAL` for at-least-once
duplicate handling. Other available refresh intervals are 1s, 5s, 10s, 30s,
and 1m. Historical Event Comparison defaults to 30 days and refreshes every 30
seconds. All dashboard SQL uses the Grafana time filter or a bounded recent
window, selects explicit columns, and has a result limit where applicable.
Widening a dashboard time range increases ClickHouse work; use the narrowest
range needed.

Dashboard JSON and datasource/provider configuration live under `grafana/`.
Edit the source JSON or `scripts/generate_grafana_dashboards.py` and rerun that
generator to update the provisioned dashboards. Provisioned dashboards are
read-only in the Grafana UI so runtime edits do not diverge from Git.

## Validation and troubleshooting

Check service health, plugin install, provisioning, and datasource authentication:

```sh
docker compose ps grafana clickhouse clickhouse-grafana-user
docker compose logs --tail=100 grafana clickhouse-grafana-user
```

The Grafana datasource test appears under **Connections → Data sources → Market
Integrity ClickHouse**. If startup reports authentication errors, confirm the
Grafana password matches the ClickHouse reader password and rerun the one-shot
provisioner above. If Grafana starts before configuration changes, restart it
to reload provisioning. Query errors are visible in the panel inspector and
Grafana logs. ClickHouse queries are bounded by the dashboard range; the
historical event selector and data-freshness queries also have fixed recent
windows. Missing integrity events or unavailable evidence remain N/A/empty;
they are not synthesized as zero. Binance derivatives source-health, ClickHouse
writer lag, Kafka lag, and other process health are not stored in the current
analytical tables.
