# Replay validation — 2026-09-26

REPLAY STATUS: **READY FOR HISTORICAL EVALUATION**

Validated for bounded historical research windows, with explicit source-availability and timing limitations. No automatic commits or archive deletion. No trading signals, new models, exchanges, integrity components, Fabric, or predictions were added.

## Framework and isolation

- Isolated in-process replay using existing normalizers, feature engines, detector, book sequencer, and integrity scorer. No replay traffic is published into live Kafka.
- Replay-only output envelopes and seven dedicated ClickHouse tables: `replay_metrics`, `replay_cross_venue_metrics`, `replay_derivatives_metrics`, `replay_anomalies`, `replay_integrity`, `replay_runs`, `historical_events`.
- Real-time, arbitrary positive accelerated speed, and maximum-speed replay supported. Optional step mode is not implemented.
- Parquet v1 with zstd, exact original Kafka payload/key bytes, typed metadata, partition/offset provenance, immutable manifests and SHA-256 hashes.
- Historical catalog, explicit-window label evaluation, threshold grids, score/classification sensitivity, existing-feature-similarity benchmark and JSON/Markdown incident reconstruction implemented.

## Five-minute real capture

Dataset: `btc-20260926-2150`; broker-time interval `2026-09-26T21:50:00Z` to `2026-09-26T21:55:00Z`.
4,730 archived records; 349,641 Parquet bytes; 3,231,128 raw-payload bytes; compression ratio 9.24:1.
Archive export/write: 0.448s, 10,559 records/s on this small capture.
All five spot venues are represented. Normalized outputs: Binance 1,331, Coinbase 367, Kraken 62, OKX 80, Bybit 474 trades; 335 Binance curated book snapshots; OKX 687 and Bybit 1,137 derivatives records. The raw archive also preserves 1,077 Binance deltas.
Payload exchange timestamps can fall outside the broker capture range: 61 future-dated observations were explicitly scheduled at receipt, without rewriting their timestamps. Original funding settlement timestamps remain intact.

## Replay and performance

| Mode | Active elapsed | Effective speed | Input events/s | Anomalies | Integrity | DLQ |
|---|---:|---:|---:|---:|---:|---:|
| 1x | 301.67s | 0.99x | 15.7 | 108 | 108 | 0 |
| 10x | 31.18s | 9.62x | 151.7 | 108 | 108 | 0 |
| max | 4.54s | 66.06x | 1041.5 | 108 | 108 | 0 |

All modes consumed 4,730 archive records and emitted the same 8,461 stage outputs: 1,200 spot metrics, 900 cross-venue metrics, 1,672 derivatives metrics, 108 anomalies, 108 integrity assessments, plus normalized inputs.
The final ClickHouse write inserted 4,097 rows, including 108 catalog rows, in 0.213s (19,202 rows/s). These are small-batch observations, not capacity guarantees.
Pacing/throughput uses monotonic active elapsed time. During the timed 1x and 10x runs, UTC start/finish elapsed differed substantially; potential host suspension or wall-clock changes were not independently diagnosed. For transparency:
- `real-1x-final`: UTC elapsed 1004.86s; monotonic active elapsed 301.67s.
- `real-10x-final`: UTC elapsed 110.39s; monotonic active elapsed 31.18s.

## Determinism and provenance

Final same-code pair: `audit-final-a` / `audit-final-b`. Git commit `d597066296d5a1763666a744ef3113ced8e95476`, dirty=true. Code, configuration, manifest, deterministic output hashes all match. Exact event IDs, scores, classifications, component scores and output counts match through the full output hash.
Determinism hash: `5702ff6c36375b48cd14eb7ab19e46a44633bcbe5fedb5cb60c35c9b3e0ac06d`.
Run UUIDs, wall-clock metadata and ClickHouse insertion timestamps are intentionally excluded. Earlier timed speed runs also match the final output hash; later changes hardened evaluation validation, book duplicate handling and MCP resource settings. Each run preserves its own code hash. Initial runs before the funding scheduling correction remain explicitly marked superseded.

## Evaluation results

| Shared z threshold | Anomalies |
|---|---:|
| 2.5 | 203 |
| 3.0 | 108 |
| 4.0 | 39 |

No best threshold selected. The real capture has no externally reviewed detection labels, so precision, recall, false-positive/negative counts and onset latency remain null. The labeled synthetic evaluation path is tested.
Stability: 20 recorded assessments, 240 applicable perturbations. Maximum absolute score change for numeric perturbations: 0.3 points. Structural availability changes reached 12.8 points when removing the last available venue; 8 classification flips were retained for review. Missing numeric inputs are skipped, never synthesized as zero.
The rule classifier has no scalar competing-class margin; the report explicitly returns null and exposes actual local perturbation flips and conflicting evidence. This is a known limitation, not a fabricated probability.
Similarity benchmark: 100 anchors, 500 retrieved neighbors; mean feature similarity 94.82/100. Categorical agreement: classification 93.8%, trigger 95.0%, direction 94.8%, severity 95.2%. These neighbors are strongly aligned with stored features in this one sample, but overlapping windows are correlated and no independent human relevance labels exist. No outcome claims.
Incident artifacts: `data/replay/incident-final.json` and `.md`. Final catalog/stability/similarity outputs: `data/replay/runs/audit-final-b/`.

## Live-pipeline isolation and resource findings

- Before/during/after snapshots show advancing live Kafka offsets and live ClickHouse timestamps. Bounded scans found zero replay markers and zero replay event-ID collisions in live tables. Dedicated replay routing is also tested.
- Grafana query definitions and MCP allowlists remain live-only. Final live MCP summary, integrity, anomaly and quality requests succeeded against live tables.
- Replay Kafka lag: not applicable (in-process pipeline). During timed replay, 30 live writer samples showed maximum known per-partition lag 12, zero insert failures, zero writer DLQ records; one lag sample was unavailable.
- New live `system.dlq` offset span since 21:59 UTC: zero on all three partitions. Real replay DLQ count: zero.
- Intermittent ClickHouse total-memory-limit errors occurred during initial integration runs. The existing read-only MCP profile now applies fixed single-thread/small-block limits. An intermediate per-query approach was rejected by readonly permissions and replaced; no additional SELECT grants were introduced. Actual read-only credentials are now used by the integration test. Live endpoint checks and the final suite passed afterward.
- Run large datasets serially and monitor shared ClickHouse memory; the resource limits and these small-window results are not a production-scale throughput guarantee.

## Regression

`docker compose --profile replay config --quiet`: passed. Complete Docker regression including actual read-only MCP integration and replay ClickHouse integration: **440 passed, 0 failed, 0 skipped in 12.93s**. Earlier failed runs were investigated, not counted as successful. `git diff --check`: passed.

## Known limits

- This real archive uses stored curated Binance book snapshots for features because the existing raw topic lacks historical REST checkpoints. Full raw-depth reconstruction requires an archived checkpoint; synthetic delta reconstruction/gap/duplicate tests pass.
- Short windows start with cold rolling state and detector baselines. Missing/stale evidence is preserved; replay is not guaranteed to reproduce the asynchronous live system’s exact interleaving.
- One-million-record in-memory input cap; container limited to one CPU / 2 GiB. Larger research datasets must be explicitly split.
- Broker-time offset export records retention watermarks but cannot prove exchange-history completeness. Filtering can create intentional source-offset gaps.
- No human-labeled real ground truth yet; no automatic threshold selection or classification smoothing.
- Scalar rule-classification margin and step mode are not supplied; their absence is explicit.
- Archives and validation runs are retained locally under ignored `data/replay/`; code and documentation remain uncommitted.

See [replay.md](replay.md) and [evaluation.md](evaluation.md) for commands and methodology.
