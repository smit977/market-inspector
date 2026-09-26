"""Descriptive evaluation. Labels define a scoped detection task, never outcomes."""
import itertools
import json
from collections import Counter
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from .archive import stamp
from .models import HistoricalEvent, LabeledWindow


def evaluate_detector(run, outputs, labels=None):
    anomalies = [o["event"] for o in outputs if o["topic"] == "replay.anomalies"]
    signatures = [(a["exchange"], a["symbol"], a["window"], a["event_type"], a["detected_at"]) for a in anomalies]
    duplicates = len(signatures)-len(set(signatures))
    stats = run.statistics
    candidates = len(anomalies)+stats.get("cooldown_suppressed", 0)
    result = {"event_count": len(anomalies), "trigger_frequency_per_virtual_minute": len(anomalies)/(stats["virtual_seconds"]/60),
        "duplicate_event_count": duplicates, "duplicate_event_rate": duplicates/len(anomalies) if anomalies else 0,
        "duplicate_definition": "same exchange/symbol/window/type/detection timestamp",
        "cooldown_suppression_rate": stats.get("cooldown_suppressed", 0)/candidates if candidates else 0,
        "cooldown_suppressed_count": stats.get("cooldown_suppressed", 0),
        "quality_suppressed_count": stats.get("quality_suppressed", 0),
        "quality_suppression_rate": stats.get("quality_suppressed", 0)/stats["metrics_evaluated"] if stats.get("metrics_evaluated") else None,
        "precision": None, "recall": None, "false_positive_count": None, "false_negative_count": None,
        "time_to_detection_seconds": None, "ground_truth_status": "unlabeled_not_computed"}
    if not labels: return result
    labels = sorted([LabeledWindow.model_validate(x) for x in labels], key=lambda x: x.start_time)
    if any(x.start_time < stamp(run.configuration["start"]) or x.end_time > stamp(run.configuration["end"]) for x in labels):
        raise ValueError("labels must be inside the replay observation interval")
    if any(a.end_time > b.start_time for a, b in zip(labels, labels[1:])):
        raise ValueError("evaluation windows must not overlap")
    tp = fp = fn = 0; latency = []; evaluated = 0
    for label in labels:
        found = [a for a in anomalies if label.start_time <= stamp(a["detected_at"]) < label.end_time
                 and (not label.trigger_types or a["event_type"] in label.trigger_types)]
        evaluated += len(found)
        if label.expected_trigger:
            if found:
                tp += 1; latency.append((min(stamp(a["detected_at"]) for a in found)-label.start_time).total_seconds())
            else: fn += 1
        elif found: fp += 1
    result.update(precision=tp/(tp+fp) if tp+fp else None, recall=tp/(tp+fn) if tp+fn else None,
                  false_positive_count=fp, false_negative_count=fn, time_to_detection_seconds=latency,
                  ground_truth_status="explicit_window_labels", evaluation_unit="nonoverlapping labeled window with any qualifying trigger",
                  true_positive_count=tp, unlabeled_or_out_of_scope_events=len(anomalies)-evaluated,
                  label_definitions=[x.model_dump(mode="json") for x in labels])
    return result


def catalog(run, outputs):
    anomalies = {o["event"]["event_id"]: o["event"] for o in outputs if o["topic"] == "replay.anomalies"}
    crosses = [o["event"] for o in outputs if o["topic"] == "replay.metrics.cross_venue"]
    trades = [o["event"] for o in outputs if o["topic"] == "replay.cleaned.trades"]
    assessments = [o["event"] for o in outputs if o["topic"] == "replay.integrity"]
    rows = []
    for output in outputs:
        if output["topic"] != "replay.integrity": continue
        event = output["event"]; trigger = anomalies[event["trigger_event_id"]]
        window = trigger["metrics_snapshot"]
        begin, finish = stamp(window["window_start"]), stamp(trigger["detected_at"])
        history = [e for e in assessments if begin <= stamp(e["calculated_at"]) <= finish and e["move_window"] == event["move_window"]]
        minima = {}
        for previous in history:
            for c in previous["components"]:
                if c["score"] is not None:
                    minima[c["name"]] = min(minima.get(c["name"], float(c["score"])), float(c["score"]))
        observed = sorted([t for t in trades if t["exchange"] == trigger["exchange"] and begin <= stamp(t["exchange_timestamp"]) <= finish], key=lambda t: t["exchange_timestamp"])
        first = Decimal(observed[0]["price"]) if observed else None
        peak = max((abs((Decimal(t["price"])-first)/first) for t in observed), default=None) if first else None
        zs = [abs(e["z_score"]) for e in trigger["evidence"] if e["metric"] in {"quote_volume", "trade_count"} and e["z_score"] is not None]
        dispersion = [float(c["dispersion_bps"]) for c in crosses if c["dispersion_bps"] is not None and begin <= stamp(c["calculated_at"]) <= finish]
        rows.append(HistoricalEvent(event_id=event["event_id"], replay_id=run.replay_id, replay_run_id=run.replay_run_id,
            canonical_pair=event["canonical_pair"], event_start=begin, event_end=finish, trigger_type=event["trigger_type"],
            severity=event["trigger_severity"], direction=event["move_direction"], integrity_score=event["integrity_score"],
            classification=event["classification"], confidence=event["overall_confidence"], coverage=event["evidence_coverage"],
            peak_return=peak, peak_volume_z=max(zs) if zs else None,
            peak_dispersion_bps=max(dispersion) if dispersion else None,
            minimum_liquidity_score=minima.get("Liquidity Stability"), minimum_cross_venue_score=minima.get("Cross-Venue Confirmation"),
            minimum_derivatives_score=minima.get("Derivatives Dependence"),
            notes=["One row per triggered assessment; extrema use archived observations inside its window, not a merged causal incident.",
                   "peak_return is maximum absolute return from the first observed trigger-venue trade in the window; component minima use recorded assessments with the same move window.",
                   "Window begins before archive coverage" if begin < stamp(run.configuration["start"]) else "Window lies inside archive interval"]))
    return rows


def threshold_grid(manifest, output_dir, grid, labels=None, base=None, code_provenance=None):
    from .runner import run_replay
    allowed = {"price_move_threshold", "z_threshold", "volume_z_threshold", "liquidity_z_threshold", "imbalance_z_threshold", "confirmation_count", "cooldown_seconds", "minimum_observations"}
    if not set(grid) <= allowed or not all(isinstance(v, list) and v for v in grid.values()): raise ValueError("invalid threshold grid")
    combinations = list(itertools.product(*(grid[k] for k in sorted(grid))))
    if len(combinations) > 100: raise ValueError("grid capped at 100 explicit scenarios")
    reports = []
    for values in combinations:
        config = dict(base or {}); config["thresholds"] = {**config.get("thresholds", {}), **dict(zip(sorted(grid), values))}
        run, outputs, directory = run_replay(manifest, output_dir, configuration=config, code_provenance=code_provenance)
        reports.append({"replay_run_id": run.replay_run_id, "configuration": config, "determinism_hash": run.determinism_hash,
                        "evaluation": evaluate_detector(run, outputs, labels)})
    return {"scenarios": reports, "selection": None, "objective": "none specified; no best threshold selected"}


def integrity_sensitivity(anomaly, metrics, cross, derivatives):
    from integrity_engine.integrity.scoring import score_integrity
    baseline = score_integrity(anomaly, metrics, cross, derivatives, calculated_at=anomaly.detected_at)
    cases = []
    def changed(obj, **changes): return obj.model_copy(update=changes)
    for delta in (Decimal("-0.01"), Decimal("0.01")):
        cases.extend([
            (f"dispersion_{delta}", metrics, changed(cross, dispersion_bps=max(Decimal(0), (cross.dispersion_bps or Decimal(0))+delta)), derivatives),
            (f"spread_{delta}", changed(metrics, spread_bps=max(Decimal(0), (metrics.spread_bps or Decimal(0))+delta)), cross, derivatives),
            (f"depth_loss_{delta}", changed(metrics, depth_change=(metrics.depth_change or Decimal(0))+delta), cross, derivatives),
            (f"volume_ratio_{delta}", metrics, cross, changed(derivatives, perpetual_to_spot_volume_ratio=max(Decimal(0), (derivatives.perpetual_to_spot_volume_ratio or Decimal(0))+delta))),
            (f"volume_share_{delta}", metrics, changed(cross, largest_venue_volume_share=max(Decimal(0), min(Decimal(1), (cross.largest_venue_volume_share or Decimal(0))+delta))), derivatives),
            (f"liquidation_notional_{delta}", metrics, cross, changed(derivatives, liquidation_quote_volume=max(Decimal(0), (derivatives.liquidation_quote_volume or Decimal(0))*(1+delta))))])
    cases.append(("one_fewer_active_venue", metrics, changed(cross, active_venues=cross.active_venues[:-1], data_quality_flags=cross.data_quality_flags+["perturbed_active_venue_count"]), derivatives))
    cases.append(("missing_derivative_fields", metrics, cross, changed(derivatives, open_interest=None, funding_rate=None, data_quality_flags=derivatives.data_quality_flags+["missing_open_interest", "missing_funding"])))
    base = {c.name: c.score for c in baseline.components}; reports = []; skipped = []
    measured = {"dispersion_": cross.dispersion_bps, "spread_": metrics.spread_bps,
                "depth_loss_": metrics.depth_change, "volume_ratio_": derivatives.perpetual_to_spot_volume_ratio,
                "volume_share_": cross.largest_venue_volume_share, "liquidation_notional_": derivatives.liquidation_quote_volume}
    for name, m, c, d in cases:
        if any(name.startswith(prefix) and value is None for prefix, value in measured.items()):
            skipped.append({"perturbation": name, "reason": "input_unavailable_not_synthesized_as_zero"})
            continue
        result = score_integrity(anomaly, m, c, d, calculated_at=anomaly.detected_at)
        deltas = {x.name: float(x.score-base[x.name]) if x.score is not None and base[x.name] is not None else None for x in result.components}
        reports.append({"perturbation": name, "score": float(result.integrity_score), "score_delta": float(result.integrity_score-baseline.integrity_score),
            "component_score_deltas": deltas, "classification": result.classification,
            "classification_flipped": result.classification != baseline.classification,
            "classification_confidence": float(result.classification_confidence),
            "conflicting_evidence": [x.name+":"+x.status for x in result.components if x.status in {"stressed", "fragmented", "high_dependence", "elevated_dependence"}],
            "review_required": abs(result.integrity_score-baseline.integrity_score)>5 or result.classification != baseline.classification})
    return {"baseline": baseline.model_dump(mode="json"), "perturbations": reports,
        "skipped_unavailable_inputs": skipped,
        "margin_to_next_classification": None,
        "margin_explanation": "Rule classifier has no scalar class logits. Report actual flips under named input perturbations; no fabricated probability margin.",
        "nearest_observed_classification_flips": [x["perturbation"] for x in reports if x["classification_flipped"]]}


def similarity_benchmark(outputs, limit=5):
    from integrity_engine.mcp.tools import MCPTools
    events = [o["event"] for o in outputs if o["topic"] == "replay.integrity"]
    service = MCPTools.__new__(MCPTools)
    index = {e["event_id"]: e for e in events}
    reports = []
    for anchor in events[:100]:
        neighbors = service._historical_similarity(anchor, events)[:limit]
        for n in neighbors:
            other = index[n["event_id"]]
            n["categorical_alignment"] = {key: anchor.get(key) == other.get(key) for key in ("classification", "trigger_type", "move_direction", "trigger_severity")}
        reports.append({"anchor_event_id": anchor["event_id"], "neighbors": neighbors})
    return {"method": "existing MCP deterministic feature similarity", "reports": reports,
            "human_alignment_judgment": None, "limitation": "Feature matches are inspectable; intuitive relevance requires reviewer labels. No outcome prediction."}


def incident_report(run_dir, event_id):
    root = Path(run_dir); run = json.loads((root/"run.json").read_text())
    outputs = [json.loads(line) for line in (root/"outputs.jsonl").read_text().splitlines()]
    event = next((o["event"] for o in outputs if o["topic"] in {"replay.anomalies", "replay.integrity"} and o["event"]["event_id"] == event_id), None)
    if event is None: raise ValueError("event not found in this replay run")
    trigger_id = event.get("trigger_event_id", event_id)
    trigger = next(o["event"] for o in outputs if o["topic"] == "replay.anomalies" and o["event"]["event_id"] == trigger_id)
    assessment = next((o["event"] for o in outputs if o["topic"] == "replay.integrity" and o["event"]["trigger_event_id"] == trigger_id), None)
    start, end = stamp(trigger["metrics_snapshot"]["window_start"]), stamp(trigger["detected_at"])
    timeline = [o for o in outputs if start <= stamp(o["original_event_time"]) <= end and o["topic"] not in {"replay.cleaned.trades", "replay.cleaned.derivatives", "replay.cleaned.orderbook"}]
    return {"replay_id": run["replay_id"], "replay_run_id": run["replay_run_id"], "provenance": run,
        "trigger": trigger, "price_volume_orderbook": trigger["metrics_snapshot"],
        "integrity_assessment": assessment, "timeline": timeline,
        "cross_venue": [o["event"] for o in timeline if o["topic"] == "replay.metrics.cross_venue"],
        "derivatives_and_liquidations": [o["event"] for o in timeline if o["topic"] == "replay.metrics.derivatives"],
        "source_availability": run["statistics"]["quality_counts"],
        "data_quality_issues": sorted({f for o in timeline for f in o["event"].get("data_quality_flags", [])}),
        "key_evidence": trigger["evidence"], "interpretation": "Historical observations and deterministic assessments; no causal or outcome claim."}
