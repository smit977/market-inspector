# Historical evaluation methodology

Evaluation describes detector behavior and integrity evidence. It provides no
trading signals, manipulation probabilities, outcome labels, or price prediction.

## Threshold scenarios

```json
{
  "price_move_threshold": [0.0005, 0.001],
  "volume_z_threshold": [3, 4],
  "liquidity_z_threshold": [3],
  "imbalance_z_threshold": [3],
  "confirmation_count": [2],
  "cooldown_seconds": [10, 20]
}
```

```sh
docker compose --profile replay run --rm --no-deps replay \
  python scripts/evaluate_thresholds.py --manifest data/replay/btc-window/manifest.json \
  --grid data/replay/grid.json --output-dir data/replay/evaluation
```

Price-move threshold is an optional minimum absolute fractional return in addition
to the existing z-score gate. Volume/liquidity/imbalance overrides replace the
shared z threshold only for those dimensions. Unset overrides preserve current
live detection behavior. The existing cooldown policy suppresses low-severity
repeats only; high/moderate repeats are not silently redefined. At most 100 explicit
configurations are accepted. No configuration is selected as best automatically.

Each result reports anomaly counts, per-virtual-minute trigger frequency, exact
signature duplicates, cooldown/quality suppression, and label-scoped detection
latency/precision/recall only when labels exist. An exact duplicate is the same
exchange, symbol, window, trigger type and detection timestamp. This does not
claim that nearby overlapping rolling-window detections are independent incidents.
Quality suppression denominator is metrics evaluated; cooldown denominator is
accepted plus cooldown-suppressed candidate anomalies. Warmup is a separate
limitation: short archives may not have sufficient baseline observations.

## Explicit labeling

Label files are JSON arrays conforming to `LabeledWindow`:

```json
[{
  "label_id": "reviewed-window-1",
  "start_time": "2026-09-26T21:50:00Z",
  "end_time": "2026-09-26T21:51:00Z",
  "label": "volume_anomaly",
  "definition": "A reviewer expects a volume_acceleration trigger within this window using the attached evidence.",
  "evidence": ["reviewed-source-artifact#window-1"],
  "expected_trigger": true,
  "trigger_types": ["volume_acceleration"]
}]
```

This is a structural example, not a label assigned to the real capture. Supported
descriptors: broad_spot_supported, derivatives_amplified, liquidity_shock,
cross_venue_divergence, volume_anomaly, mixed_unknown. Model classifications must
not be copied into labels and called ground truth. Do not use enforcement claims
as model truth; if external confirmed enforcement metadata is later supplied,
keep it explicitly sourced and separate.

Windows must be timezone-aware, nonoverlapping and have explicit definitions and
evidence. Positive and negative expected-trigger windows define the evaluated
domain. The evaluation unit is a **window with any qualifying trigger**: positive
window hit = TP, missed positive = FN, triggered negative = FP. Multiple detections
in a window do not multiply TP. Out-of-domain/type events are reported separately.
Time-to-detection is first qualifying detection minus labeled start, not claimed
causal onset. Precision/recall are null without explicit labels or valid denominators.

## Integrity and classification sensitivity

```sh
docker compose --profile replay run --rm --no-deps replay \
  python scripts/evaluate_replay.py --run-dir data/replay/runs/<run-id>
```

`stability.json` evaluates up to 20 recorded assessments with available matching
inputs. Perturbations cover dispersion, spread, depth loss, volume participation,
venue concentration, liquidation notional, active venue count and missing derivative
fields, exercising all six existing components. It records component/overall deltas,
classification flips, confidence and conflicting evidence. A >5-point score change
or classification flip is flagged for review, not automatically repaired.

The current classifier is rule-based with no continuous competing-class scores.
Therefore a scalar `margin_to_next_classification` is null with an explicit reason;
named perturbations that flip classification are reported as empirical local boundary
evidence. This avoids inventing a probability margin. Tests deliberately exercise
Organic Trend/Derivatives-Amplified, Liquidity Shock/Mixed, and Divergence/Mixed
boundaries; real evidence changes may legitimately flip labels.

## Catalog, similarity and incidents

`HistoricalEvent` catalog rows represent individual triggered assessments, not
merged incidents. They carry direction, severity, classification, confidence,
coverage, maximum absolute return from the first observed trigger-venue trade in
the window, trigger volume z evidence, and observed dispersion. Component minima
use recorded assessments of the same window size within that time interval.
Incomplete archive coverage is explicitly noted. Labeling/incident grouping requires a separate explicit
research definition, not an inferred causal episode.

`similarity.json` invokes the existing MCP weighted feature-similarity function
without a database connection, compares component scores and categorical matches,
and exposes the closest candidates for up to 100 anchors. Human intuitive alignment
is null until reviewed. This benchmark evaluates feature similarity only.

```sh
docker compose --profile replay run --rm --no-deps replay \
  python scripts/generate_replay_incident_report.py --replay-id btc-window \
  --run-dir data/replay/runs/<run-id> --integrity-event-id <event-id> \
  --output data/replay/incident
```

JSON/Markdown reports include trigger, timeline, price/volume/book snapshot,
cross-venue evidence, derivatives/liquidations, integrity components, confidence,
coverage, availability, quality flags and full provenance. Null evidence is not
zero and classification is not a causal assertion.

## Validation and reproducibility

```sh
docker compose --profile replay run --rm --no-deps \
  -e MCP_CLICKHOUSE_INTEGRATION=1 -e REPLAY_CLICKHOUSE_INTEGRATION=1 \
  -e CLICKHOUSE_MCP_USER -e CLICKHOUSE_MCP_PASSWORD \
  replay python -m pytest -q
```

Synthetic tests cover archive fidelity/hashes, ordering, sequence gaps, replay
clock modes, isolated routing, deterministic outputs across speeds, threshold
scenarios, explicit labels, score/classification sensitivity and incidents. Opt-in
integration verifies synthetic outputs in dedicated ClickHouse tables. Replay the
same manifest and config twice and compare `determinism_hash`, event IDs, scores,
classifications and components. Run IDs, start/finish time and insert time are
expected to differ. Always compare code/config/manifest hashes alongside results.

Export the two MCP credential variables from the local environment before the
integration command; never embed them in command logs or reports. Replay storage
uses the separate writer settings inherited by the replay Compose service.
