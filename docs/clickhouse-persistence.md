# ClickHouse analytical persistence

The existing official ClickHouse 25.8 service stores only five curated Kafka
topics. Raw trades and raw order-book deltas remain in Kafka. Named volumes hold
database files outside the repository. Host ports 8123 and 9000 bind to loopback.
Core workers do not depend on ClickHouse; only the dedicated writer does.

## Initialize and run

```sh
docker compose config --quiet
docker compose up -d clickhouse
docker compose exec -T app python scripts/init_clickhouse.py
docker compose up -d --build clickhouse-writer
```

Initialization uses `CREATE IF NOT EXISTS`, adds missing columns, and widens the
original numeric columns. It does not drop, truncate, or replay historical data.
Newly added Nullable fields are NULL for old rows. Previously omitted fields or
precision cannot be reconstructed from old rows; historical backfill is separate.
The initializer can safely be rerun. Schema mismatches outside the supported
additive/numeric-widening migration fail explicitly.

`clickhouse-connect` runs inside Docker and uses `CLICKHOUSE_HTTP_PORT` (8123).
`CLICKHOUSE_PORT` (9000) is retained for native clients. All configuration comes
from environment variables; passwords use a redacted settings field. Credentials
are not required with the local-development defaults.

## Delivery and failure semantics

The base group is `clickhouse-writer-v1`. Existing infrastructure scopes live
consumers as `clickhouse-writer-v1-live-v2`; keep that identity stable across
restarts. The first assignment of a new live group uses latest offsets. Existing
groups resume committed positions. A temporary validation group may use a unique
`PROCESSING_RUN_ID`; it does not reset the durable writer group.

The writer flushes at 500 records or 1,000 ms after the first pending record.
Large polls are split into bounded batches. Each table gets a synchronous insert
(`async_insert=0`). Defaults are three retries after the first attempt, with
500 ms exponential backoff capped at 60 seconds. Configurations are validated.
Shutdown flushes pending records and closes all clients.

**Only successful ClickHouse inserts permit acknowledgement.** If any table
fails after retries, the writer publishes safe failure coordinates/identifiers
to `system.dlq` and exits without acknowledging that batch. A DLQ record does not
substitute for database persistence. The existing contiguous partition tracker
prevents a later offset from passing a failed record. Successfully inserted
parts of a failed batch can replay with their original event IDs.

Mapping failures follow the same fail-closed rule. Correct the schema/data or
source issue before restarting; poison records are not silently skipped. During
a prolonged outage, Compose restarts can emit repeated failure records for the
same source coordinates. Pause the writer if necessary to avoid repeated alerts;
Kafka remains the buffer, subject to Kafka retention and available disk.

Every ten seconds, structured logs report consumed events, inserted rows/batches,
insert failures, retries, DLQ records, last successful insert time, and partition
lag. A failed lag observation is reported as unavailable rather than changing
consumer offsets. Counts are per process; after restarts, aggregate process logs.

## Schema, precision, and duplicates

All five tables use monthly partitions and `ReplacingMergeTree`, with ordering
by pair/symbol, exchange where applicable, window, event time, and event ID.
This preserves existing time-series keys and avoids small time partitions.

Financial values use `Nullable(Decimal(76,38))`, with derivatives
`open_interest_change_pct` stored as `Nullable(Decimal(76,50))` to preserve very
small percentage changes from the open-interest source. Float inputs, nonfinite
values, and values that cannot fit exactly are rejected rather than silently rounded.
Missing financial values and missing counts stay NULL; a reported count of zero
remains zero. Nested venue maps and anomaly/integrity evidence are JSON strings
with decimal strings preserved. Use `JSONExtract*` to inspect them.

Existing sorting timestamps stay `DateTime64(3,'UTC')`; `source_timestamp` stores
the full Python microsecond timestamp in `DateTime64(6,'UTC')`. UTC-aware input is
required. `original_event` preserves the complete curated payload. Source topic,
partition and offset make retries inspectable. `is_synthetic` marks explicit
storage-validation fixtures; normal Kafka events default to false.

ReplacingMergeTree deduplicates **the complete ORDER BY key**, not event_id alone,
and does so during background merges. Physical row counts may temporarily include
retries. Use `FINAL` for logical reads and inspect `GROUP BY event_id HAVING
count()>1` without FINAL for currently visible duplicates. Different keys with the
same ID remain a detectable data-quality issue. There is no exactly-once claim,
and merges mean physical duplicate counts are not a permanent retry audit trail.
Source Kafka coordinates and writer logs are retained for diagnostics.

See [query examples](clickhouse-queries.sql). Queries exclude synthetic fixtures;
no dashboard, Fabric, Power BI, MCP, ML, or scoring change is part of this stage.

Client/type reference: https://clickhouse.com/docs/integrations/language-clients/python/advanced-inserting

Engine semantics: https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree
