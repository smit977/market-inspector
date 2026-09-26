# Historical replay

The first implementation is an **isolated in-process pipeline** inside the opt-in
`replay` Compose profile. It calls the same normalizers, spot/cross-venue/derivatives
feature engines, detector, order-book sequencing code, and integrity engine as
live processing. It does not publish to Kafka. Namespaced `replay.*` channels in
output envelopes correspond to the pipeline stages; they are not broker topics
or consumer groups. Live Kafka offsets are never committed by the exporter.

## Setup and commands

```sh
docker build -t market-move-integrity-replay .
docker compose --profile replay config --quiet
# All timestamps must include a timezone; choose a CLOSED historical interval.
docker compose --profile replay run --rm --no-deps replay \
  python scripts/export_kafka_to_parquet.py \
  --start 2026-09-26T21:50:00Z --end 2026-09-26T21:55:00Z \
  --replay-id btc-window --topics market.raw.trades market.raw.orderbook \
  market.raw.derivatives market.cleaned.orderbook

docker compose --profile replay run --rm --no-deps replay \
  python scripts/run_replay.py --manifest data/replay/btc-window/manifest.json \
  --speed 10 --clickhouse
```

The replay container has one CPU and a 2 GiB memory cap. `PROCESSING_MODE=replay`
is mandatory for the replay CLI; live services retain their existing mode.
`--speed 1`, any positive finite multiplier (2, 5, 10, 50), and `--speed max` are
supported. Pacing changes only wall time; payload timestamps and virtual tick
boundaries do not change. Optional `--start`, `--end`, `--exchanges`, `--streams`
select a subset, recorded in configuration. Subset runs start cold; there is no
hidden warmup history. Step mode is not implemented.

## Archive format v1

`data/replay/<replay_id>/exchange=<venue>/stream=<stream>/date=<UTC date>/symbol=<symbol>/<offset-batch-hash>.parquet`

Core columns use typed strings, int32 partitions, int64 offsets, UTC timestamp
microseconds, and lists of quality flags. `raw_payload` and `source_key` are
binary columns preserving the exact original Kafka bytes (including JSON
whitespace and any malformed payload). Python consumers decode with
`json.loads(record.raw_payload)`; no JSON round-trip is imposed on archival.
The original raw exchange payload remains inside those envelope bytes.
`schema_version` describes the envelope; `archive_schema_version=1.0` describes
the Parquet schema. Metadata includes exchange, stream/event type, native symbol,
canonical pair when known, original timestamps, sequence ID, and source offsets.
Zstandard is default; snappy/gzip/none are explicit alternatives.

The exporter uses Kafka record timestamps for `[start,end)` selection, seeks each
partition independently using offsets-for-times, and snapshots upper bounds.
Payload time may precede/follow that interval (for example scheduled funding).
Offset ranges, retention low watermarks, and uncertainty about coverage before
start are recorded. These are **not proof of complete exchange history**.
Batch file names are deterministic from source identities. Completed archive IDs
are immutable: reuse/read them, or choose another ID; reruns cannot overwrite a
manifest. Interrupted exports have no completed manifest. No automatic deletion.

`manifest.json` contains source paths, SHA-256 file hashes, event count, timestamp
bounds, exchange set, schema versions, source partition ranges, archive statistics
and notes. Readers verify file hashes, path containment, Parquet schema, duplicate
source identities, and count. Input loading is currently capped at one million
records in memory; split larger datasets into explicit windows.

## Ordering, clock and book integrity

Records retain source-partition offset order as a hard constraint. A stable heap
merge compares eligible partition heads by exchange timestamp, received timestamp,
source topic, partition, then offset. **Future-dated exchange timestamps** (notably
funding settlement time) use receipt time only as the scheduling key; the original
timestamp is never rewritten and each occurrence is counted in quality metadata.
The replay clock advances monotonically with observed receipt time, never backwards.
Late/timestamp-disordered records remain late and are reported, not repaired.
This is a documented causal replay policy, not a claim of exact live interleaving.

Fixed virtual one-second ticks run features, then evidence caches, then detection
and integrity. All freshness/window/evidence checks receive virtual time. Detector
cooldown accepts an injected monotonic clock; live defaults are unchanged. Integrity
calculation time is injected in replay. Future evidence is not used by earlier ticks.
A short archive starts without the live engine's prior baseline/rolling history.

Book deltas require an **archived full checkpoint** with `payload.lastUpdateId`,
`bids`, and `asks` in the raw book stream, followed by bridgeable deltas. Gaps stop
reconstruction until another archived checkpoint; replay never fetches today's
REST snapshot. Sequence gaps produce isolated DLQ entries. Missing checkpoints
produce explicit quality flags and unavailable book evidence.

Existing live raw topics do not archive REST checkpoints. For real captures,
include `market.cleaned.orderbook`: `book_source=curated` selects those archived
snapshots as the book feature input, while preserving raw deltas in the archive.
It does **not** claim to reconstruct missing full depth. Selecting raw+cleaned
trade/derivative streams prefers raw per venue once to avoid double counting. Filters and
source policy are recorded.

## Outputs and storage isolation

Every stage output has `replay_id`, `is_replay=true`, a `replay.*` channel,
`original_event_time`, and the existing event schema. `outputs.jsonl`, `dlq.json`,
`run.json`, `catalog.json`, and `evaluation.json` live under a unique run directory.
Stage emission counts are called `events_published`; they are local emissions,
not Kafka producer acknowledgements. Raw deltas bypassed for curated books have
an explicit counter. Kafka lag is not applicable to this replay transport.

`--clickhouse` explicitly creates/writes only:

- `replay_metrics`, `replay_cross_venue_metrics`, `replay_derivatives_metrics`
- `replay_anomalies`, `replay_integrity`
- `replay_runs`, `historical_events`

Tables have typed replay/run IDs, event ID, original time, wall-clock replay
persistence time, replay marker, and complete JSON payloads. JSON retains the
existing exact Decimal strings; scalar feature projections can be queried with
ClickHouse JSON functions. ReplacingMergeTree keys isolate runs; use `FINAL` for
idempotent repeated inserts. No live schema/table migration is performed by replay.
MCP allowlists and Grafana queries remain live-only. No new MCP tools are added.
`storage.json` records explicit insert counts and throughput.

## Reproducibility and limitations

Run metadata records Git commit, dirty flag, Python source/configuration content
hash, manifest hash, effective thresholds, schema versions and run times. No
automatic commits. Stable UUID5 IDs and a determinism hash cover ordered outputs,
DLQ contents and quality counts; random run IDs and wall-clock metadata are excluded.
Same data/code/config should match at all speeds. Do not equate replay event counts
with prior live counts: warmup, fixed scheduling and recorded source availability
are intentionally reproducible and can differ from asynchronous live execution.

Use `scripts/check_replay_isolation.py --output <path>` before/during/after runs.
It reads recent live tables, replay-ID collisions, Kafka high offsets, MCP allowlists
and dashboard definitions. Operational source lag and memory still need monitoring
for large archives. Current limits target bounded research windows, not a production
multi-day distributed replay service.

Retention is manual: retain raw archives as source evidence, curated exports as
reconstruction aids, and evaluation outputs plus run manifests for audit. Future
retention tiers may differ; this implementation deletes none of them automatically.

Timing reports distinguish monotonic active elapsed time from UTC start/finish elapsed
time. Host suspension or wall-clock jumps can make these differ; speed ratios use
the monotonic pacing clock. Selected-offset gaps can result from explicit filters
and are not proof of exchange packet loss.

## Shared ClickHouse resource safeguards

Validation exposed intermittent memory-limit failures in existing MCP analytical
reads. The existing MCP read-only settings profile now fixes `max_threads=1`,
`max_block_size=512`, and `preferred_block_size_bytes=262144`. These are server
profile settings, not per-query mutations (which readonly=1 rejects). SELECT
grants are unchanged and replay tables are not granted to MCP. Reprovision with
`scripts/init_mcp_clickhouse_user.py` when deploying the updated source. Integration
tests accept `CLICKHOUSE_MCP_USER` and `CLICKHOUSE_MCP_PASSWORD` separately from
the replay writer credentials to exercise the actual restricted reader.
