# Real-time alerting and incident operations

Alerts identify conditions that warrant investigation. Severity measures investigation urgency, not price direction, expected return, or evidence of misconduct. Alerting never changes the integrity score or executes trades.

## Architecture and operation

`market.anomalies`, `market.integrity`, `market.metrics`, `market.metrics.cross_venue`, `market.metrics.derivatives`, and `system.operational_health` feed a single-owner alert engine. It publishes immutable incident revisions to `market.alerts`, keyed by `canonical_pair:alert_type`. The existing partition-safe ClickHouse writer persists them to `market_alerts`. Grafana and read-only MCP query that history.

Run `./scripts/start_stack.sh`. It includes `alert-engine` and `operational-health`. New topics are created explicitly. The writer initializes additive table migrations. Preserve `data/alert-state/state.sqlite` and its WAL files together with existing Kafka volumes and consumer group names. Do not run multiple alert engines against the same database or clone a live state directory for a second consumer. The current implementation is a single-instance service, not distributed incident ownership.

The configuration lives in `config/alert_rules.json`, validated by Pydantic. It accepts controlled thresholds, operators, severity, cooldown and recovery duration. It does not evaluate code, and no MCP write tool is exposed. Configuration changes require restart and review; retain the config hash with replay evaluations. Default monitoring window is 30 seconds. Other upstream windows remain available in source tables.

## Categories and rules

| Category | Rules |
| --- | --- |
| MARKET_STRUCTURE | integrity_deterioration, low_structural_confirmation, liquidity_shock, cross_venue_divergence, derivatives_amplification, liquidation_burst, volume_concentration |
| DATA_QUALITY | venue_stale, source_unavailable, timestamp_disorder, orderbook_sequence_gap, insufficient_evidence, basis_reference_stale, schema_failure |
| PIPELINE_HEALTH | consumer_lag_high, consumer_lag_growing, worker_unhealthy, DLQ_activity, clickhouse_persistence_failure, mcp_unavailable, processing_latency_degradation |

Integrity deterioration requires an absolute and relative decline within the comparison horizon, a meaningful trigger, sufficient confidence, and coverage. Low structural confirmation has its own score and recovery thresholds. Low scores with insufficient confidence become data-quality incidents.

Liquidity requires multiple supporting spread, top-20 depth, and imbalance measurements. Comparisons use the same venue and window. Cross-venue divergence requires directional movement, sufficient active venues, weakened confirmation, and elevated dispersion. Neutral/flat markets do not trigger it. Recovery requires normalized dispersion and recovered confirmation (or neutral conditions).

Derivatives amplification requires participation and supporting basis or return divergence. Missing OI, funding, and liquidation evidence reduce confidence; they are never filled with zero. Liquidation bursts require observed quote volume, count, and a fresh last-liquidation timestamp. Absence of observations cannot prove recovery; such incidents remain open until sufficient observed recovery evidence arrives. Volume concentration requires HHI, largest venue share, and accelerated volume; the description is “unusually concentrated activity.”

Data-quality conditions never modify market scores. Existing upstream flags identify sequence gaps and timestamp disorder. A rejected input creates schema-failure evidence without exposing the raw payload. If an upstream producer does not emit a particular quality signal, this service cannot invent it.

The operational collector persists measured group membership and committed lag, lag growth between polls, DLQ offset increases, analytical output age, ClickHouse query reachability, and MCP TCP reachability. A query failure is a persistence-path probe failure, not proof that a specific insert failed. Worker health covers Kafka consumer membership; it is not Docker process health. MCP TCP reachability is not a complete MCP protocol check. WebSocket connection state is not currently exported consistently by ingestion; no fabricated WebSocket alert is implemented. Collector failures are logged and absent measurements remain unknown.

## Severity, evidence and lifecycle

Severity is `info`, `low`, `moderate`, `high`, or `critical`. Central rules increase urgency for larger threshold exceedances. Evidence below 0.85 confidence/coverage cannot create a high or critical market alert. Every revision includes metric observations, source, previous/baseline values where available, thresholds where applicable, window, freshness, quality, and an interpretation separate from observed values.

Dedupe keys include pair, rule, source, venue, and window (or operational group). One incident retains its ID across updates and escalation. Ordinary updates are coalesced, not emitted every second. Initial, current and maximum severity are separate; escalation timestamps are retained. Recovery uses rule-specific hysteresis and requires continuous fresh healthy observations. Missing/unknown evidence does not resolve an incident, and a long gap resets recovery. Reopening after resolution creates a new incident ID.

The schema supports `open`, `acknowledged`, and `resolved`; only opening and automatic resolution are implemented. Acknowledgement is reserved for a future authenticated API. No arbitrary write tool exists.

SQLite stores the state and input offsets together with a durable output outbox. Kafka publication precedes input acknowledgement. Restart drains the outbox and suppresses already-checkpointed source offsets. Delivery is at least once. ClickHouse uses revision-specific replacement keys, so retried delivery can deduplicate without discarding distinct history revisions. Query latest revision per incident **before** filtering by status, severity, or dashboard time; otherwise resolved incidents can appear open. Current-state queries intentionally include older incidents. History queries are time bounded.

## Notifications and replay

Logging is the default notifier. Optional generic HTTPS webhook delivery requires `ALERT_WEBHOOK_ENABLED=true` and `ALERT_WEBHOOK_URL` in private environment configuration. Never commit or log the URL. The payload contains incident metadata, summary and at most 12 numeric observations, not raw source events or credentials. Three bounded attempts isolate delivery errors; failed notification status remains attached to the published incident. Continued conditions update state without repeating notifications. Cooldown also suppresses quick reopen notifications; escalation can bypass cooldown. Resolution can notify independently.

An outbox crash after a remote webhook accepts a request can repeat that notification. Receivers should deduplicate by alert ID and state; exactly-once external delivery is not promised. Optional webhook retries can delay the single worker briefly; use a separate delivery service before enabling large notification workloads.

Replay constructs the same engine with `is_replay=true`, uses event time, and does not construct a notifier or Kafka producer. Run:

```sh
docker compose --profile replay run --rm --no-deps replay python scripts/run_replay.py --manifest data/replay/btc-20260926-2150/manifest.json --speed max --run-id alert-validation
docker compose --profile replay run --rm --no-deps replay python scripts/evaluate_alert_replay.py --input data/replay/runs/alert-validation/outputs.jsonl --output data/replay/alert-evaluation
```

The evaluator reports incident counts, types, initial severities, revision counts, updates suppressed, escalations, resolutions, alerts/hour, resolved duration and evaluation latency. Incidents still active at dataset end are right-censored, not forced resolved. Archives lacking a last-liquidation timestamp cannot independently establish liquidation freshness; regenerate features from archived observed feeds.

## Interfaces and examples

Grafana’s Alert Operations Center filters pair, category, severity, status and type. It includes current incidents, recent history, escalation/lifecycle timeline, categories, severity, resolution duration and integrity deterioration. Market Overview includes a compact ACTIVE ALERTS table. Replay/validation incidents are excluded from live panels.

MCP `get_active_alerts(canonical_pair?, category?, minimum_severity?)` returns up to 100 current incidents. `get_alert_details(alert_id)` returns current state, up to 400 lifecycle revisions with a truncation flag, related anomaly/integrity events and nearby cross-venue context. These are read-only queries. Large deployments need a materialized current-state projection and paginated history before history scans grow significantly.

Operational example: a spread expansion plus depth decline opens a liquidity incident. Subsequent samples retain its ID; a larger deterioration can escalate urgency. Healthy samples start a recovery timer, and only sustained recovery resolves it. A missing feed instead creates a separate data-quality incident; it neither confirms a liquidity event nor proves recovery.

Progress logs include rules evaluated, opened/updated/escalated/resolved/suppressed alerts, quality suppression, notification outcomes, evaluation latency and consumer lag. `system.operational_health` is stored separately for audit. Validation results and remaining readiness limitations are recorded in `docs/alerting-validation.md`.

Operational lag may be a lower bound when a non-empty partition has never recorded a committed offset. The evidence reports known and unknown partition counts. Such partial evidence can establish a threshold exceedance, but cannot prove recovery. MCP pipeline health now exposes these persisted measurements with timestamps instead of substituting zero for unknown lag.

Future-dated input cannot advance the source watermark and block later valid input. Missing or low-quality samples interrupt market-incident recovery even when the next healthy sample arrives quickly. State ownership is enforced by an exclusive filesystem lock.

A progress watchdog terminates the alert process after 90 seconds without a completed input operation (configurable as `worker_stall_seconds`). This bounds stalls in Kafka commit/rebalance calls following host suspend. Docker restarts the worker; SQLite WAL and the outbox recover the last atomic checkpoint. Termination never acknowledges an input offset. A stopped/suspended host cannot perform real-time monitoring while it is asleep.
