"""Pure rules: missing observations mean unknown, never recovery or numeric zero."""
from datetime import datetime
from .models import Assessment, AlertEvidence, SEVERITIES


def stamp(event):
    return datetime.fromisoformat(str(event.get("calculated_at", event.get("detected_at"))).replace("Z", "+00:00"))


def severity(config, rule, magnitude, confidence=1, coverage=1):
    base = SEVERITIES.index(rule.severity)
    if magnitude >= config.severity_critical_ratio: base = max(base, 4)
    elif magnitude >= config.severity_high_ratio: base = max(base, 3)
    if min(confidence, coverage) < .85: base = min(base, 2)
    return SEVERITIES[base]


def evaluate(topic, event, previous, now, config):
    pair = event.get("canonical_pair", "BTC-USD")
    scope = f"{topic}:{event.get('exchange', 'all')}:{event.get('window', event.get('move_window', 'health'))}"
    age = (now - stamp(event)).total_seconds()
    flags = event.get("data_quality_flags", [])
    exchanges = event.get("active_venues", [event["exchange"]] if event.get("exchange") else [])
    result = []
    fresh = 0 <= age <= config.max_age_seconds
    confidence = coverage = 1.0
    def add(kind, active, recovered, values, category="MARKET_STRUCTURE", magnitude=1, thresholds=None):
        rule = config.rules.get(kind)
        if not rule or not rule.enabled: return
        if thresholds is None:
            thresholds = {
                "low_structural_confirmation":{"integrity_score":rule.threshold},
                "liquidity_shock":{"spread_ratio":config.spread_ratio,"depth_change_fraction":-config.depth_drop,"imbalance":config.imbalance},
                "cross_venue_divergence":{"dispersion_bps":rule.threshold,"confirmation_ratio":config.confirmation_max},
                "derivatives_amplification":{"perpetual_to_spot_volume_ratio":rule.threshold,"basis_bps":config.basis_bps,"spot_perpetual_return_difference":config.return_divergence,"open_interest_change_pct":config.oi_change},
                "liquidation_burst":{"liquidation_quote_volume":rule.threshold},
                "volume_concentration":{"volume_hhi":rule.threshold,"largest_venue_volume_share":config.concentration_share,"volume_acceleration":config.volume_acceleration},
                "insufficient_evidence":{"confidence":config.min_confidence,"coverage":config.min_coverage},
            }.get(kind, {"value":rule.threshold} if category=="PIPELINE_HEALTH" else {})
        evidence = [AlertEvidence(source=topic, metric=k, current_value=v,
                    previous_value=previous.get(k) if previous else None,
                    baseline=previous.get(k) if previous else None,
                    threshold=(thresholds or {}).get(k), window=event.get("window", event.get("move_window")),
                    freshness=age, quality=flags, data_quality=flags,
                    interpretation="Observed measurement; investigation urgency only.", effect=kind)
                    for k,v in values.items()]
        descriptions = {
            "integrity_deterioration":"integrity declined materially with sufficient supporting evidence.",
            "low_structural_confirmation":"structural confirmation is low with sufficient evidence.",
            "liquidity_shock":"liquidity deteriorated across supporting order-book measurements.",
            "cross_venue_divergence":"cross-venue confirmation weakened while price dispersion increased.",
            "derivatives_amplification":"derivatives participation increased with supporting divergence evidence.",
            "liquidation_burst":"observed liquidation activity increased; this warrants investigation.",
            "volume_concentration":"shows unusually concentrated activity with accelerated volume.",
        }
        result.append(Assessment(canonical_pair=pair, category=category, alert_type=kind, scope=scope,
            active=bool(active), recovered=bool(recovered),
            severity=severity(config, rule, magnitude, confidence, coverage),
            summary=f"{pair} " + descriptions.get(kind, kind.replace('_',' ') + " observed; this warrants investigation."),
            evidence=evidence, trigger_event_ids=[str(x) for x in (event.get("event_id"), event.get("trigger_event_id")) if x],
            confidence=confidence, coverage=coverage, exchanges=exchanges, flags=flags,
            integrity_score=event.get("integrity_score"),
            previous_integrity_score=previous.get("integrity_score") if previous else None,
            classification=event.get("classification")))
    if topic == "system.operational_health":
        for kind, observation in event["signals"].items():
            if kind not in config.rules or observation.get("value") is None: continue
            rule = config.rules[kind]; value = observation["value"]
            old_scope = scope; scope = observation.get("scope", scope)
            add(kind, fresh and value >= rule.threshold, fresh and value <= rule.recovery and not observation.get("unknown_partitions"),
                observation, "DATA_QUALITY" if kind == "source_unavailable" else "PIPELINE_HEALTH", value / rule.threshold)
            scope = old_scope
        return result
    # Source-specific quality clears only when that same source supplies healthy evidence.
    quality = {
        "venue_stale": bool(event.get("stale_venues")) or "stale_venue_data" in flags,
        "timestamp_disorder": "timestamp_disorder" in flags or age < 0,
        "orderbook_sequence_gap": any("sequence" in f or "gap" in f for f in flags),
        "basis_reference_stale": "stale_spot_reference" in flags,
        "source_unavailable": not fresh or any(f in flags for f in ("stale_orderbook", "stale_perpetual_price", "no_trades_in_window", "source_unavailable")),
        "schema_failure": "schema_failure" in flags,
        "insufficient_evidence": any("insufficient" in f or "missing_venue" in f for f in flags),
    }
    if topic == "market.integrity":
        confidence = float(event["overall_confidence"]); coverage = float(event["evidence_coverage"])
        quality["insufficient_evidence"] |= confidence < config.min_confidence or coverage < config.min_coverage
    for kind, active in quality.items():
        add(kind, active, fresh and not active, {"flags": flags, "stale_venues": event.get("stale_venues", []),
            "evidence_age_seconds": age, "confidence": confidence, "coverage": coverage}, "DATA_QUALITY")
    if not fresh: return result
    prior_fresh = previous and 0 < (stamp(event)-stamp(previous)).total_seconds() <= config.comparison_seconds
    def number(name):
        value = event.get(name)
        return float(value) if value is not None else None
    def values(*names): return {k:event.get(k) for k in names}
    def ready(*names): return all(event.get(k) is not None for k in names)
    if topic == "market.integrity" and not quality["insufficient_evidence"]:
        score = number("integrity_score"); r = config.rules["low_structural_confirmation"]
        add("low_structural_confirmation", score <= r.threshold, score >= r.recovery,
            values("integrity_score", "overall_confidence", "evidence_coverage", "components"),
            magnitude=r.threshold/max(score, 1))
        if previous and float(previous["overall_confidence"]) >= config.min_confidence and float(previous["evidence_coverage"]) >= config.min_coverage:
            old = float(previous["integrity_score"]); drop = old-score
            r = config.rules["integrity_deterioration"]
            meaningful = event.get("trigger_severity") in {"moderate", "high"}
            add("integrity_deterioration", prior_fresh and meaningful and drop >= r.threshold and drop/max(old,1) >= config.relative_score_drop,
                score >= old-r.recovery, {"integrity_score":score,"score_change":-drop,"comparison_score":old},
                magnitude=drop/r.threshold, thresholds={"score_change":-r.threshold})
    elif topic == "market.metrics" and event.get("data_complete") and previous:
        if ready("spread", "bid_depth_20", "ask_depth_20", "orderbook_imbalance_20") and all(previous.get(k) is not None for k in ("spread", "bid_depth_20", "ask_depth_20")):
            oldspread = float(previous["spread"]); olddepth = float(previous["bid_depth_20"])+float(previous["ask_depth_20"])
            if oldspread > 0 and olddepth > 0:
                spread = number("spread")/oldspread; depth = (number("bid_depth_20")+number("ask_depth_20"))/olddepth
                imbalance = abs(number("orderbook_imbalance_20"))
                support = int(spread >= config.spread_ratio)+int(depth <= 1-config.depth_drop)+int(imbalance >= config.imbalance)
                r = config.rules["liquidity_shock"]
                add("liquidity_shock", support >= r.threshold, support < r.recovery,
                    {"spread_ratio":spread,"depth_change_fraction":depth-1,"imbalance":imbalance, **values("spread", "bid_depth_20", "ask_depth_20", "spread_change", "depth_change")},
                    magnitude=max(spread/config.spread_ratio, (1-depth)/config.depth_drop))
    elif topic == "market.metrics.cross_venue":
        active = len(event["active_venues"])
        coverage = active / max(active+len(event["stale_venues"]), config.minimum_venues)
        confidence = coverage
        if active < config.minimum_venues or coverage < config.min_coverage: return result
        if ready("confirmation_ratio", "dispersion_bps"):
            r = config.rules["cross_venue_divergence"]
            directional = event["confirmation_state"] not in {"neutral", "insufficient_data"} and event.get("up_venue_count",0)+event.get("down_venue_count",0)>0
            dispersion = number("dispersion_bps"); confirmation = number("confirmation_ratio")
            add("cross_venue_divergence", directional and confirmation <= config.confirmation_max and dispersion >= r.threshold,
                dispersion <= r.recovery and (confirmation >= config.confirmation_recovery or event["confirmation_state"] == "neutral"),
                values("confirmation_state","confirmation_ratio","dispersion_bps","active_venues","stale_venues","venue_deviation_bps"), magnitude=dispersion/r.threshold)
        if ready("volume_hhi", "largest_venue_volume_share") and prior_fresh:
            oldvol = sum(float(v) for v in previous.get("venue_quote_volumes",{}).values())
            vol = sum(float(v) for v in event.get("venue_quote_volumes",{}).values())
            if oldvol > 0:
                r = config.rules["volume_concentration"]; hhi=number("volume_hhi")
                add("volume_concentration", hhi >= r.threshold and number("largest_venue_volume_share") >= config.concentration_share and vol/oldvol >= config.volume_acceleration,
                    hhi <= r.recovery, {**values("volume_hhi","largest_venue_volume_share","confirmation_state","confirmation_ratio"),"volume_acceleration":vol/oldvol}, magnitude=hhi/r.threshold)
    elif topic == "market.metrics.derivatives":
        confidence = coverage = max(0, 1 - .1*sum(event.get(k) is None for k in ("open_interest_change_pct", "funding_rate", "liquidation_quote_volume")))
        if confidence < config.min_confidence or coverage < config.min_coverage: return result
        if ready("perpetual_to_spot_volume_ratio", "basis_bps", "spot_perpetual_return_difference", "spot_reference_timestamp"):
            ref = datetime.fromisoformat(event["spot_reference_timestamp"].replace("Z","+00:00"))
            if 0 <= (now-ref).total_seconds() <= config.max_age_seconds and "stale_perpetual_price" not in flags:
                ratio=number("perpetual_to_spot_volume_ratio"); r=config.rules["derivatives_amplification"]
                support = (abs(number("basis_bps")) >= config.basis_bps or abs(number("spot_perpetual_return_difference")) >= config.return_divergence
                    or (number("open_interest_change_pct") is not None and abs(number("open_interest_change_pct")) >= config.oi_change))
                add("derivatives_amplification", ratio >= r.threshold and support, ratio <= r.recovery and not support,
                    values("perpetual_to_spot_volume_ratio","basis","basis_bps","basis_change","spot_perpetual_return_difference","open_interest_change_pct","liquidation_quote_volume"), magnitude=ratio/r.threshold)
        # Non-null observed quote volume and count establish observations, not feed completeness.
        if ready("liquidation_quote_volume", "total_liquidation_volume", "last_liquidation_at") and event.get("liquidation_count",0)>0 and 0 <= (now-datetime.fromisoformat(event["last_liquidation_at"].replace("Z","+00:00"))).total_seconds() <= config.max_age_seconds:
            r=config.rules["liquidation_burst"]; value=number("liquidation_quote_volume")
            add("liquidation_burst", value >= r.threshold, value <= r.recovery,
                values("long_liquidation_volume","short_liquidation_volume","total_liquidation_volume","liquidation_quote_volume","liquidation_count","exchange","window","last_liquidation_at"), magnitude=value/r.threshold)
    return result
