# Market Inspector

Market Inspector is an open-source, Docker-first crypto market-move integrity and investigation platform. It ingests live market data from multiple exchanges, analyzes spot trades, order books, cross-venue behavior, derivatives, anomalies, integrity, and alerts, stores analytics in ClickHouse, provides Grafana dashboards, exposes a read-only MCP investigation API, and supports deterministic replay and historical evaluation.

This is a market-intelligence research platform, not financial advice or a trading-signal engine. A low integrity score does not prove manipulation. The system does not issue BUY, SELL, LONG, or SHORT commands.

## Quick start

Prerequisites:

- Git
- Docker Desktop or Docker Engine
- Docker Compose v2
- OpenSSL

```sh
git clone https://github.com/smit977/market-inspector.git
cd market-inspector
./scripts/start_stack.sh
```

On the first run, `start_stack.sh` creates a gitignored `.env` from `.env.example` and generates local-only Grafana/ClickHouse/MCP passwords. Secret values are never printed or committed.

After startup:

- Kafka UI: http://127.0.0.1:8080
- Grafana: http://127.0.0.1:3000
- MCP endpoint: http://127.0.0.1:8001/mcp

Useful operations:

```sh
./scripts/status_stack.sh
./scripts/stop_stack.sh
```

Persistent Docker volumes preserve Kafka, ClickHouse, and Grafana state across normal stop/start cycles. Do **not** use `docker compose down -v` unless you intentionally want to delete persisted Docker data.

GitHub contains the reproducible project code and safe configuration only; it does not back up local Kafka, ClickHouse, or Grafana data. Persistent runtime data remains in local Docker volumes. First startup builds and downloads several images and may take a while; allow Docker several gigabytes of memory for the full stack. Public exchange feeds can be unavailable or restricted by region, and Binance derivatives availability varies by location.

## What is included

- Five-venue live spot ingestion
- Binance order-book ingestion and reconstruction
- Multi-window feature engineering
- Cross-venue confirmation and dispersion
- Bybit/OKX derivatives evidence with explicit missing-data semantics
- Move detection and Market Move Integrity scoring
- Real-time alert/incident operations
- ClickHouse analytical persistence
- Grafana live dashboards
- Read-only MCP investigation tools
- Deterministic historical replay and evaluation
- Docker-first local operations and restart-safe services

## Run and verify the complete pipeline

```sh
./scripts/start_stack.sh
docker compose run --rm app python -m pytest -q
docker compose run --rm app python scripts/check_pipeline_health.py
```

All ingestion and processing workers run as dedicated services with
`restart: unless-stopped`. Do not also launch the manual runners below while
their corresponding services are running. The `app` container is now only a
development/test shell. Kafka and ClickHouse retain their existing named volumes.

`PROCESSING_RUN_ID=current` preserves the existing processing groups; the
ClickHouse writer retains its existing `clickhouse-writer-v1-live-v2` group.
Keep these names stable to resume committed positions. Unseen live groups start
at the latest offsets; changing a group name is not a backlog recovery mechanism.

The health command is read-only. It checks active consumer groups and periodic
ClickHouse output freshness (30 seconds maximum). Anomaly and integrity outputs
are event-triggered, so their timestamps are reported without a fixed freshness
requirement. A passing result does not certify full exchange coverage: inspect
`data_quality_flags` and ingestion logs for restricted or unavailable feeds.

Temporary coordinator commit errors receive bounded retries without discarding
pending offsets. Invalid detector/integrity inputs are delivered to `system.dlq`
before acknowledgement. Failed deliveries remain unacknowledged for replay.
Delivery remains at-least-once; ClickHouse deduplication is eventual.
For ClickHouse specifically, a failure record in the DLQ does **not** acknowledge
the input: offsets advance only after every table insert in that batch succeeds.
See [analytical persistence](docs/clickhouse-persistence.md) for migration,
precision, retry/recovery, duplicate handling, and query examples.

## Services

| Service | Purpose |
| --- | --- |
| `app` | Python 3.12-slim, non-root user, `/app` working directory, editable project installation including test dependencies. Sleeps until stopped for development. |
| `kafka` | Official Apache Kafka 4.1.2 image, one combined KRaft broker/controller, no ZooKeeper. Data persists in the `kafka_data` named volume. |
| `kafka-ui` | Kafbat Kafka UI 1.5.0, connected to `kafka:9092`, accessible at `http://localhost:8080` after startup. |

Both app and UI wait for Kafka's readiness check. Kafka is reachable only on the
Compose network; no broker port is published to the host. Clients inside Docker
use `kafka:9092`, never `localhost`. The UI port binds only to `127.0.0.1`.
Automatic topic creation is disabled; topics can be created explicitly in the UI.

## Configuration and validation

A fresh clone should normally use `./scripts/start_stack.sh`. The startup script creates `.env` from `.env.example` when needed and generates the required local-only passwords automatically.

For manual configuration validation:

```sh
./scripts/bootstrap_env.sh
docker compose --env-file .env config --quiet
```

Edit the generated `.env` only when you need local overrides. It is intentionally ignored by Git.

Compose reads `.env` for interpolation and passes the declared settings into
containers. `ENVIRONMENT` defaults to `development` and
`KAFKA_BOOTSTRAP_SERVERS` to `kafka:9092`. `KAFKA_UI_PORT` changes the browser
port. `KAFKA_HEAP_OPTS` controls broker heap size. `KAFKA_CLUSTER_ID` is a stable,
non-secret development identifier; retain it while reusing the Kafka data volume.
Keep credentials out of source control and image build contexts.

## Start later

The following commands start containers; run them only when ready:

```sh
docker compose up --build -d
docker compose ps
docker compose logs -f app kafka
```

The Dockerfile installs all runtime dependencies and the `dev` extra using
container Python. Source and tests are bind-mounted at `/app/src` and `/app/tests`;
source edits take effect without rebuilding. After changing dependencies or
pytest configuration in `pyproject.toml`, rebuild with
`docker compose up --build -d app`.

Run Python or a shell inside the running app container:

```sh
docker compose exec app python --version
docker compose exec app python -m pip check
docker compose exec app sh
```

Once tests exist, run:

```sh
docker compose exec app python -m pytest
```

Tests use mocked network clients and do not require live Binance access.

## Binance raw trade ingestion

The first ingestor subscribes only to BTCUSDT trades on Binance's public
market-data-only WebSocket endpoint. It requires no API key. Set
`BINANCE_WS_BASE_URL` (default `wss://data-stream.binance.vision`) and
`BINANCE_TRADE_SYMBOL` (currently only `BTCUSDT` is supported) in `.env`.
Compose forwards these settings to the app when it is created; for an existing
container, explicit `docker compose exec -e NAME=value ...` overrides also work.

Copy the standalone runner into the existing app container and run it:

```sh
docker compose cp scripts/run_binance_trades.py app:/app/run_binance_trades.py
docker compose exec -T app python /app/run_binance_trades.py --duration 20
```

Omit `--duration` to run until Ctrl-C. The equivalent source-mounted entrypoint is
`docker compose exec -T app python -m integrity_engine.ingestion.binance.trades`.

Each `market.raw.trades` record uses `BTCUSDT` as its key and contains a versioned
wrapper with the unmodified parsed Binance payload and a local UTC receive time.
No normalization, metrics, or cleaned-topic writes occur here. Disconnects retry
with exponential delays from 1 to 30 seconds, resetting after a published trade.
Kafka publishes are sequential and delivery-confirmed in a worker thread; delivery
failure stops ingestion visibly. This small initial implementation applies
backpressure and does not provide replay or gap recovery during disconnections.
The timed runner logs received/delivered counts and fails if nothing was delivered.

## Binance raw order-book deltas

The depth ingestor captures the public `btcusdt@depth@100ms` diff-depth stream
into `market.raw.orderbook`, keyed by `BTCUSDT`. It preserves the complete parsed
Binance payload: `E` (event time), `U`/`u` (first/final update IDs), and `b`/`a`
(bid/ask updates), including price/quantity strings and zero-quantity deletions.
It does not fetch snapshots, maintain local book state, or normalize deltas.

```sh
docker compose cp scripts/run_binance_orderbook.py app:/app/run_binance_orderbook.py
docker compose exec -T app python /app/run_binance_orderbook.py --duration 20
```

The source-mounted equivalent is
`docker compose exec -T app python -m integrity_engine.ingestion.binance.orderbook`.
Omit `--duration` to run continuously. Configuration uses `KAFKA_BOOTSTRAP_SERVERS`,
`BINANCE_WS_BASE_URL`, `BINANCE_ORDERBOOK_SYMBOL=BTCUSDT`, and
`BINANCE_ORDERBOOK_INTERVAL_MS=100` (also supports `1000`). For an existing
container, override settings with `docker compose exec -e NAME=value ...`;
Compose applies `.env` changes when the app container is recreated.

The ingestor logs connections, disconnects, capped exponential reconnect delays
(1–30 seconds), and malformed messages. Kafka delivery failures are logged by the
shared producer and stop ingestion. Reconnection resumes deltas without gap
recovery; these events alone are not a complete order-book snapshot.

## Binance trade normalization

The normalizer consumes `market.raw.trades` using group `trade-normalizer-v1`
and starts at the earliest available offset when that group has no committed
position. Run in a second terminal alongside the ingestor:

```sh
docker compose cp scripts/run_trade_normalizer.py app:/app/run_trade_normalizer.py
docker compose exec -T app python /app/run_trade_normalizer.py --duration 20
```

Omit `--duration` for continuous processing. The source-mounted alternative is
`docker compose exec -T app python -m integrity_engine.normalization.binance`.

Only Binance BTCUSDT trade envelopes are normalized, using canonical `BTC-USDT`
as the cleaned Kafka key. Binance `m` means the buyer is the maker: `true` means
the seller is the aggressor (`sell`), and `false` means `buy`. Price and quantity
are parsed directly as Decimal; quote quantity is their exact Decimal product.
Exchange timestamps use integer epoch milliseconds. The Binance trade ID is
retained as `trade_id` and the symbol-scoped `sequence_id`. Event UUIDs are
deterministic per exchange/symbol/trade ID, making retries identifiable.

Malformed, unsupported, or validation-failing records go to `system.dlq`, with
the original input, source coordinates, safe error details, processor name, and
UTC failure time. Public input is retained; recognizable credential fields are
redacted. Undecodable input is retained as text with escaped invalid bytes.
Unsupported exchange/stream envelopes are explicitly dead-lettered rather than
normalized or silently skipped. This includes the earlier fake smoke-test event.

Automatic commits and offset storage are disabled. Each input offset is committed
only after a confirmed cleaned or DLQ write. Delivery or commit failure stops the
processor. Shutdown finishes the current record before closing the consumer.
This is **at-least-once**, not transactional exactly-once: a crash after delivery
but before commit can produce a duplicate with the same deterministic event ID.

Stop the stack while retaining Kafka data:

```sh
docker compose down
```

## Deployment scope

This is a development foundation, not a production deployment. The single broker
uses plaintext networking and replication factor one; the loopback-only UI has
no authentication. Production deployment will require replicated Kafka,
authentication/TLS, an application entrypoint, and a reviewed dependency lock
and image digests. Dependency ranges are bounded but not fully locked yet.

Image configuration references:
[Apache Kafka Docker](https://kafka.apache.org/41/getting-started/docker/),
[Kafbat UI](https://ui.docs.kafbat.io/overview/getting-started).

## Investigation alerts

The live stack includes a durable alert engine and a lightweight operational-health
collector. Alerts separate market structure, data quality, and pipeline health;
they never modify integrity scores or provide trading actions.

- [Alerting rules, lifecycle, notification and replay guide](docs/alerting.md)
- [Validation results and operating limits](docs/alerting-validation.md)
- [Alert Operations Center](http://127.0.0.1:3000/d/alert-operations)
- Read-only MCP tools: `get_active_alerts` and `get_alert_details`.

Thresholds are controlled by `config/alert_rules.json`. External webhooks are
disabled by default. Preserve `data/alert-state` when stopping or restarting.
