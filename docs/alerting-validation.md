# Alerting validation — 2026-09-27

Implementation includes 21 controlled rules: seven market-structure, seven data-quality, and seven pipeline-health rules. The Kafka/SQLite outbox, immutable ClickHouse revisions, logging/optional webhook notifier, Alert Operations Center, Market Overview panel, and read-only MCP alert tools are implemented.

## Regression and scenarios

The full Docker regression suite, including the opt-in ClickHouse replay-storage and read-only MCP integration tests, passed **498 tests** with no skips. Docker Compose configuration and whitespace checks passed. Deterministic tests cover healthy movement, meaningful score deterioration, liquidity collapse, directional fragmentation, derivatives amplification, fresh observed liquidations, concentration, missing/stale evidence, measurable pipeline failure, persistent incidents, escalation, cooldown, recovery, interrupted recovery, future timestamps, storage mapping, delivery-before-commit, outbox restart, exclusive state ownership, stalled-worker supervision, webhook failure isolation and replay notification suppression.

A replay-marked synthetic incident was published through the real Kafka writer and verified in ClickHouse with three distinct revisions: opening, escalation to high urgency, and resolution. It was excluded from live dashboard and active-alert queries. No webhook was enabled or external notification sent.

## Historical replay

The existing `btc-20260926-2150` manifest was rerun through the isolated pipeline as `alert-validation-20260927`; its output was then evaluated with the same alert engine under event time. No thresholds were lowered.

| Measure | Result |
| --- | ---: |
| Observed duration | 299 seconds |
| Incidents | 12 |
| Lifecycle revisions | 62 |
| Initial low / moderate severity | 7 / 5 |
| Duplicate updates suppressed | 1,336 |
| Escalations | 0 |
| Resolutions | 5 |
| Still open at dataset end | 7 |
| Mean resolved duration | 209.6 seconds |
| Incidents/hour extrapolated from short sample | 144.48 |
| External notifications | 0 |

Types: venue_stale 2; insufficient_evidence 4; source_unavailable 4; basis_reference_stale 1; volume_concentration 1. Eleven incidents concern data quality and one concerns market structure. The hourly rate is an extrapolation of a five-minute dataset with startup/missing-feed conditions, not a long-run production rate. Unresolved incidents are right-censored; absent observations were not treated as recovery.

Machine-readable replay evidence: `data/replay/alert-evaluation-final/evaluation.json`; full replay-marked alert revisions: `data/replay/alert-evaluation-final/alerts.jsonl`.

## Interface verification

All 12 Alert Operations Center panels and the compact ACTIVE ALERTS panel returned successful query frames through Grafana's actual datasource API. Both MCP alert tools completed through the deployed HTTP MCP endpoint. SQL, provisioning and returned frames were verified; visual browser inspection was unavailable because computer-use permission was not granted.

During deployment, the monitoring layer detected a stalled trade-normalizer consumer following existing Kafka coordinator timeouts. Restarting that worker with the same consumer group restored committed-offset processing. A subsequent pipeline health check found all eight pre-existing consumer groups stable and periodic ClickHouse data one to two seconds old. Deployment-related incidents and their resolution remain in history; they were not deleted to improve the report.

## Operating limits

- Single local state owner with a durable SQLite volume; distributed failover/ownership is not implemented.
- Acknowledgement is represented in the schema but no acknowledgement API is exposed.
- WebSocket state is not consistently exported, so no WebSocket availability rule is fabricated. MCP availability monitoring checks TCP reachability; separate protocol checks validated the tools.
- ClickHouse probe failures identify an unavailable persistence path, not a proven specific writer-insert failure.
- Missing liquidation observations cannot establish zero liquidations or recovery. Fresh observed evidence is required.
- Webhook retries are bounded and disabled by default. Remote exactly-once delivery is not guaranteed; an outbox replay may repeat an accepted notification.
- Active MCP results are capped at 100 incidents; detail history at 400 revisions with an explicit truncation flag. Complete history remains in ClickHouse. Current-state history scans should gain a materialized projection and pagination before large-scale deployment.
- Evaluation latency excludes SQLite fsync, Kafka publication and notification time. End-to-end lag is measured separately. MCP timings are observational, not a controlled before/after causal benchmark.

## Final five-minute live measurement

The successful watchdog-protected audit ran from **19:07:40 to 19:12:40 UTC**, September 27, with 301 seconds of measured runtime and 11 snapshots. Both alert and persistence consumers were present in every snapshot; every partition had a known committed-lag measurement. No rule thresholds were lowered.

| Measure | Result |
| --- | ---: |
| New DLQ records | 0 |
| Storage query errors | 0 |
| New market-structure incidents in audit window | 0 |
| New data-quality incidents | 2 source_unavailable, moderate |
| Published non-replay lifecycle revisions | 35 (33 open/update, 2 resolved) |
| Duplicate simultaneously active incidents per dedupe key | 0 |
| Alert-engine lag, min / max / final | 1 / 44 / 2 records |
| ClickHouse-writer lag, min / max / final | 0 / 37 / 15 records |
| Mean rule evaluation over logged interval | 0.803 ms |
| Evaluations in logged interval | 1,425 |
| Suppressed updates in logged interval | 374 |
| Notification failures in logged interval | 0 |
| MCP active-alert round-trip median / maximum | 214 / 1,118 ms |
| Existing latest-market-metrics MCP median / maximum | 137 / 429 ms |
| MCP errors in final live audit | 0 |

The evaluation-log interval is 19:07:51–19:12:30 UTC, slightly shorter than the full audit. Its suppression and transition counters therefore need not equal the complete SQL window counts. The worker-lifetime maximum evaluation time was 276.9 ms, including earlier resource contention; it is not a five-minute percentile. At report extraction, spot/cross-venue/derivatives persisted data were 4–5 seconds old and operational health was 11 seconds old. These are observations, not guaranteed latency bounds. The two MCP tools run different queries, so their difference does not isolate the causal latency cost of adding alerting.

Moderate integrity-deterioration, liquidity-shock and volume-concentration incidents were also observed outside this final window. Persistent incidents retained their IDs, and no duplicate active dedupe key was found. The synthetic escalation/resolution probe supplies deterministic coverage beyond what happened naturally during the window.

An earlier audit was **not** accepted as readiness evidence: host pause/resource contention led to Kafka poll-interval expiry and a stalled alert consumer. The implementation now includes a configurable 90-second progress watchdog and durable restart recovery. Earlier deployment failures and their incident history remain recorded. One later full-suite run encountered a transient database error in the pre-existing historical-similarity MCP query; direct retry and the subsequent complete integrated suite passed.

Evidence files:

- `docs/alerting-live-validation.json` — measured live audit, lag, latency and SQL lifecycle checks.
- `docs/alerting-interface-validation.json` — Grafana datasource queries, synthetic lifecycle and deployed MCP calls.
- `docs/alerting-replay-validation.json` — historical alert counts, severities, suppression, resolution and configuration fingerprint.

Readiness is scoped to the running single-host monitoring deployment and the operating limits above. External webhooks remain disabled.

**ALERTING STATUS: READY FOR REAL-TIME MONITORING**
