"""Versioned analytical schemas; lossless Decimal and nested event mapping."""
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json

EVENT_TOPIC_TABLE = {
    "market.metrics": "market_metrics",
    "market.metrics.cross_venue": "cross_venue_metrics",
    "market.metrics.derivatives": "derivatives_metrics",
    "market.anomalies": "market_anomalies",
    "market.integrity": "market_integrity",
}
DECIMAL = "Nullable(Decimal(76, 38))"
TIME = "DateTime64(3, 'UTC')"  # Keep existing sorting keys stable during migration.


def columns(time, dimensions, financial="", counts="", extra=None):
    return {"event_id": "String", time: TIME,
            **{n: "LowCardinality(String)" for n in dimensions.split()},
            **{n: DECIMAL for n in financial.split()},
            **{n: "Nullable(UInt64)" for n in counts.split()},
            **(extra or {}), "data_quality_flags": "Array(String)",
            "source_timestamp": "Nullable(DateTime64(6, 'UTC'))",
            "source_topic": "Nullable(String)", "source_partition": "Nullable(Int32)",
            "source_offset": "Nullable(Int64)", "original_event": "String",
            "is_synthetic": "Bool"}


TABLE_TYPES = {
    "market_metrics": columns("timestamp", "exchange symbol window",
        "price_return absolute_return price_range base_volume quote_volume buy_volume sell_volume "
        "trade_imbalance vwap average_trade_size largest_trade_size best_bid best_ask mid_price spread "
        "spread_bps bid_depth_20 ask_depth_20 orderbook_imbalance_20 price_acceleration depth_change",
        "trade_count", {"data_complete": "Bool"}),
    "cross_venue_metrics": columns("timestamp", "canonical_pair window",
        "median_price mean_price min_price max_price dispersion dispersion_bps confirmation_ratio "
        "median_return return_dispersion largest_venue_volume_share volume_hhi",
        "active_venue_count stale_venue_count",
        {"confirmation_state": "LowCardinality(String)", "first_observed_venue": "Nullable(String)",
         **{n: "String" for n in ("venue_prices", "venue_deviation_bps", "venue_returns",
                                  "venue_volume_shares", "venue_quote_volumes", "follow_lag_ms")},
         "data_complete": "Bool"}),
    "derivatives_metrics": columns("timestamp", "exchange canonical_pair window",
        "perpetual_price perpetual_return perpetual_base_volume perpetual_quote_volume perpetual_trade_imbalance "
        "open_interest open_interest_change open_interest_change_pct funding_rate funding_rate_change "
        "long_liquidation_volume short_liquidation_volume total_liquidation_volume liquidation_quote_volume "
        "spot_reference_price spot_quote_volume basis basis_bps basis_change perpetual_to_spot_volume_ratio "
        "spot_perpetual_return_difference", "perpetual_trade_count liquidation_count",
        {"data_complete": "Bool", "volume_unit": "Nullable(String)",
         "open_interest_unit": "Nullable(String)", "open_interest_comparable": "Bool",
         "spot_reference_timestamp": "Nullable(DateTime64(6, 'UTC'))"}),
    "market_anomalies": columns("detected_at", "exchange symbol window event_type severity direction", "", "",
        {"baseline_ready": "Bool", "detectors_triggered": "Array(String)",
         "metrics_snapshot": "String", "evidence": "String"}),
    "market_integrity": columns("calculated_at", "canonical_pair trigger_type trigger_severity move_direction move_window classification",
        "integrity_score overall_confidence evidence_coverage classification_confidence", "",
        {"trigger_event_id": "String", "components": "String"}),
}
TABLE_COLUMNS = {t: list(types) for t, types in TABLE_TYPES.items()}
TABLE_TIME = {t: "detected_at" if t == "market_anomalies" else
              "calculated_at" if t == "market_integrity" else "timestamp" for t in TABLE_TYPES}
TABLE_ORDER = {
    "market_metrics": "symbol, exchange, window, timestamp, event_id",
    "cross_venue_metrics": "canonical_pair, window, timestamp, event_id",
    "derivatives_metrics": "canonical_pair, exchange, window, timestamp, event_id",
    "market_anomalies": "symbol, exchange, window, detected_at, event_id",
    "market_integrity": "canonical_pair, move_window, calculated_at, event_id",
}
TABLE_DDL = {t: f"CREATE TABLE IF NOT EXISTS {t} (" +
             ", ".join(f"`{n}` {typ}" for n, typ in types.items()) +
             f") ENGINE = ReplacingMergeTree ORDER BY ({TABLE_ORDER[t]}) PARTITION BY toYYYYMM({TABLE_TIME[t]})"
             for t, types in TABLE_TYPES.items()}
JSON_FIELDS = {"venue_prices": {}, "venue_deviation_bps": {}, "venue_returns": {},
               "venue_volume_shares": {}, "venue_quote_volumes": {}, "follow_lag_ms": {},
               "metrics_snapshot": {}, "evidence": [], "components": []}


def _json(value):
    return json.dumps(value, default=str, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _decimal(value):
    if value is None:
        return None
    if isinstance(value, (float, bool)):
        raise ValueError("financial values require decimal strings or exact numbers")
    number = Decimal(value)
    if not number.is_finite() or number.copy_abs() >= Decimal('1e38'):
        raise ValueError("financial value outside Decimal(76,38) range")
    with localcontext() as ctx:
        ctx.prec = 80
        if number.quantize(Decimal('1e-38')) != number:
            raise ValueError("financial value exceeds 38 fractional digits")
    return number


def _timestamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamp must include timezone")
    return result.astimezone(timezone.utc)


def event_to_row(topic, event, source=None):
    if not isinstance(event, dict):
        raise ValueError("event must be an object")
    table = EVENT_TOPIC_TABLE[topic]
    if not event.get("event_id"):
        raise ValueError("event_id is required")
    time_name = TABLE_TIME[table]
    timestamp = next((event.get(n) for n in (time_name, "calculated_at", "window_end") if event.get(n) is not None), None)
    timestamp = _timestamp(timestamp)
    fields = dict(event)
    fields[time_name] = timestamp
    fields["source_timestamp"] = timestamp
    fields["original_event"] = _json(event)
    fields["is_synthetic"] = bool(event.get("_storage_validation", False))
    for name in ("topic", "partition", "offset"):
        fields["source_" + name] = getattr(source, name)() if source is not None else None
    if table == "cross_venue_metrics":
        for prefix in ("active", "stale"):
            venues = event.get(prefix + "_venues")
            fields[prefix + "_venue_count"] = len(venues) if venues is not None else None
    row = []
    for name, typ in TABLE_TYPES[table].items():
        value = fields.get(name)
        if typ == DECIMAL:
            value = _decimal(value)
        elif name in JSON_FIELDS:
            value = _json(JSON_FIELDS[name] if value is None else value)
        elif typ.startswith("Array"):
            value = [] if value is None else list(value)
        elif typ == "Nullable(UInt64)" and value is not None:
            if isinstance(value, bool) or int(value) != Decimal(str(value)) or int(value) < 0:
                raise ValueError("invalid count")
            value = int(value)
        elif typ == "Bool":
            value = False if value is None else value
            if not isinstance(value, bool):
                raise ValueError("boolean field must be boolean")
        elif "DateTime64" in typ and value is not None:
            value = _timestamp(value)
        elif name in {"event_id", "trigger_event_id"}:
            if value is None:
                raise ValueError("event identifier is required")
            value = str(value)
        elif value is None and not typ.startswith("Nullable"):
            raise ValueError(f"required field missing: {name}")
        row.append(value)
    return table, row
