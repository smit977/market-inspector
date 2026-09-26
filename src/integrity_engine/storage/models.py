"""ClickHouse table definitions and deterministic event-to-row conversion."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

EVENT_TOPIC_TABLE = {
    "market.metrics": "market_metrics",
    "market.metrics.cross_venue": "cross_venue_metrics",
    "market.metrics.derivatives": "derivatives_metrics",
    "market.anomalies": "market_anomalies",
    "market.integrity": "market_integrity",
}

DECIMAL = "Nullable(Decimal(38, 18))"
COMMON = "event_id String, timestamp DateTime64(3, 'UTC'),"

TABLE_COLUMNS: dict[str, list[str]] = {
    "market_metrics": [
        "event_id", "timestamp", "exchange", "symbol", "window", "price_return", "absolute_return",
        "trade_count", "base_volume", "quote_volume", "buy_volume", "sell_volume", "trade_imbalance", "vwap",
        "best_bid", "best_ask", "mid_price", "spread", "spread_bps", "bid_depth_20", "ask_depth_20",
        "orderbook_imbalance_20", "data_complete", "data_quality_flags",
    ],
    "cross_venue_metrics": [
        "event_id", "timestamp", "canonical_pair", "window", "active_venue_count", "stale_venue_count",
        "median_price", "mean_price", "min_price", "max_price", "dispersion", "dispersion_bps",
        "confirmation_ratio", "confirmation_state", "largest_venue_volume_share", "volume_hhi",
        "venue_prices", "venue_returns", "venue_volume_shares", "venue_deviation_bps", "data_complete", "data_quality_flags",
    ],
    "derivatives_metrics": [
        "event_id", "timestamp", "exchange", "canonical_pair", "window", "perpetual_price", "perpetual_return",
        "perpetual_quote_volume", "perpetual_trade_count", "open_interest", "open_interest_change",
        "open_interest_change_pct", "funding_rate", "liquidation_count", "long_liquidation_volume",
        "short_liquidation_volume", "total_liquidation_volume", "spot_reference_price", "basis", "basis_bps",
        "perpetual_to_spot_volume_ratio", "data_complete", "data_quality_flags",
    ],
    "market_anomalies": [
        "event_id", "detected_at", "exchange", "symbol", "event_type", "severity", "direction", "window",
        "baseline_ready", "detectors_triggered", "evidence", "metrics_snapshot", "data_quality_flags",
    ],
    "market_integrity": [
        "event_id", "calculated_at", "canonical_pair", "trigger_event_id", "trigger_type", "trigger_severity",
        "integrity_score", "overall_confidence", "evidence_coverage", "classification", "classification_confidence",
        "move_direction", "move_window", "components", "data_quality_flags",
    ],
}


def _json(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"), ensure_ascii=False)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _timestamp(event: dict[str, Any], *names: str) -> datetime:
    for name in names:
        value = event.get(name)
        if value is not None:
            if isinstance(value, datetime):
                return value
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    raise ValueError("event has no timestamp")


def event_to_row(topic: str, event: dict[str, Any]) -> tuple[str, list[Any]]:
    """Return (table, row) without mutating the Kafka event or inventing nulls as zero."""
    table = EVENT_TOPIC_TABLE[topic]
    if table == "market_metrics":
        fields = {k: event.get(k) for k in TABLE_COLUMNS[table]}
        fields["timestamp"] = _timestamp(event, "timestamp", "calculated_at", "window_end")
        fields["event_id"] = str(event["event_id"])
        for key in ("price_return", "absolute_return", "base_volume", "quote_volume", "buy_volume", "sell_volume", "trade_imbalance", "vwap", "best_bid", "best_ask", "mid_price", "spread", "spread_bps", "bid_depth_20", "ask_depth_20", "orderbook_imbalance_20"):
            fields[key] = _decimal(fields[key])
        fields["data_quality_flags"] = list(event.get("data_quality_flags") or [])
        fields["trade_count"] = int(event.get("trade_count") or 0)
    elif table == "cross_venue_metrics":
        fields = {k: event.get(k) for k in TABLE_COLUMNS[table]}
        fields["timestamp"] = _timestamp(event, "timestamp", "calculated_at")
        fields["event_id"] = str(event["event_id"])
        fields["active_venue_count"] = len(event.get("active_venues") or [])
        fields["stale_venue_count"] = len(event.get("stale_venues") or [])
        for key in ("median_price", "mean_price", "min_price", "max_price", "dispersion", "dispersion_bps", "confirmation_ratio", "largest_venue_volume_share", "volume_hhi"):
            fields[key] = _decimal(fields[key])
        for key in ("venue_prices", "venue_returns", "venue_volume_shares", "venue_deviation_bps"):
            fields[key] = _json(event.get(key) or {})
        fields["data_quality_flags"] = list(event.get("data_quality_flags") or [])
        fields["active_venue_count"] = int(fields["active_venue_count"] or 0)
        fields["stale_venue_count"] = int(fields["stale_venue_count"] or 0)
    elif table == "derivatives_metrics":
        fields = {k: event.get(k) for k in TABLE_COLUMNS[table]}
        fields["timestamp"] = _timestamp(event, "timestamp", "calculated_at")
        fields["event_id"] = str(event["event_id"])
        for key in ("perpetual_price", "perpetual_return", "perpetual_quote_volume", "open_interest", "open_interest_change", "open_interest_change_pct", "funding_rate", "long_liquidation_volume", "short_liquidation_volume", "total_liquidation_volume", "spot_reference_price", "basis", "basis_bps", "perpetual_to_spot_volume_ratio"):
            fields[key] = _decimal(fields[key])
        fields["data_quality_flags"] = list(event.get("data_quality_flags") or [])
        fields["perpetual_trade_count"] = int(event.get("perpetual_trade_count") or 0)
        fields["liquidation_count"] = int(event.get("liquidation_count") or 0)
    elif table == "market_anomalies":
        fields = {k: event.get(k) for k in TABLE_COLUMNS[table]}
        fields["detected_at"] = _timestamp(event, "detected_at")
        fields["event_id"] = str(event["event_id"])
        fields["detectors_triggered"] = list(event.get("detectors_triggered") or [])
        fields["evidence"] = _json(event.get("evidence") or [])
        fields["metrics_snapshot"] = _json(event.get("metrics_snapshot") or {})
        fields["data_quality_flags"] = list(event.get("data_quality_flags") or [])
    else:
        fields = {k: event.get(k) for k in TABLE_COLUMNS[table]}
        fields["calculated_at"] = _timestamp(event, "calculated_at")
        fields["event_id"] = str(event["event_id"])
        fields["trigger_event_id"] = str(event["trigger_event_id"])
        fields["integrity_score"] = _decimal(event.get("integrity_score"))
        fields["overall_confidence"] = _decimal(event.get("overall_confidence"))
        fields["evidence_coverage"] = _decimal(event.get("evidence_coverage"))
        fields["classification_confidence"] = _decimal(event.get("classification_confidence"))
        fields["components"] = _json(event.get("components") or [])
        fields["data_quality_flags"] = list(event.get("data_quality_flags") or [])
    return table, [fields[name] for name in TABLE_COLUMNS[table]]


TABLE_DDL: dict[str, str] = {
    "market_metrics": f"""CREATE TABLE IF NOT EXISTS market_metrics (
        event_id String, timestamp DateTime64(3, 'UTC'), exchange LowCardinality(String), symbol LowCardinality(String), window LowCardinality(String),
        price_return {DECIMAL}, absolute_return {DECIMAL}, trade_count UInt64, base_volume {DECIMAL}, quote_volume {DECIMAL}, buy_volume {DECIMAL}, sell_volume {DECIMAL}, trade_imbalance {DECIMAL}, vwap {DECIMAL},
        best_bid {DECIMAL}, best_ask {DECIMAL}, mid_price {DECIMAL}, spread {DECIMAL}, spread_bps {DECIMAL}, bid_depth_20 {DECIMAL}, ask_depth_20 {DECIMAL}, orderbook_imbalance_20 {DECIMAL},
        data_complete Bool, data_quality_flags Array(String)
    ) ENGINE = ReplacingMergeTree ORDER BY (symbol, exchange, window, timestamp, event_id) PARTITION BY toYYYYMM(timestamp)""",
    "cross_venue_metrics": f"""CREATE TABLE IF NOT EXISTS cross_venue_metrics (
        event_id String, timestamp DateTime64(3, 'UTC'), canonical_pair LowCardinality(String), window LowCardinality(String), active_venue_count UInt16, stale_venue_count UInt16,
        median_price {DECIMAL}, mean_price {DECIMAL}, min_price {DECIMAL}, max_price {DECIMAL}, dispersion {DECIMAL}, dispersion_bps {DECIMAL}, confirmation_ratio {DECIMAL}, confirmation_state LowCardinality(String),
        largest_venue_volume_share {DECIMAL}, volume_hhi {DECIMAL}, venue_prices String, venue_returns String, venue_volume_shares String, venue_deviation_bps String, data_complete Bool, data_quality_flags Array(String)
    ) ENGINE = ReplacingMergeTree ORDER BY (canonical_pair, window, timestamp, event_id) PARTITION BY toYYYYMM(timestamp)""",
    "derivatives_metrics": f"""CREATE TABLE IF NOT EXISTS derivatives_metrics (
        event_id String, timestamp DateTime64(3, 'UTC'), exchange LowCardinality(String), canonical_pair LowCardinality(String), window LowCardinality(String), perpetual_price {DECIMAL}, perpetual_return {DECIMAL}, perpetual_quote_volume {DECIMAL}, perpetual_trade_count UInt64,
        open_interest {DECIMAL}, open_interest_change {DECIMAL}, open_interest_change_pct {DECIMAL}, funding_rate {DECIMAL}, liquidation_count UInt64, long_liquidation_volume {DECIMAL}, short_liquidation_volume {DECIMAL}, total_liquidation_volume {DECIMAL}, spot_reference_price {DECIMAL}, basis {DECIMAL}, basis_bps {DECIMAL}, perpetual_to_spot_volume_ratio {DECIMAL}, data_complete Bool, data_quality_flags Array(String)
    ) ENGINE = ReplacingMergeTree ORDER BY (canonical_pair, exchange, window, timestamp, event_id) PARTITION BY toYYYYMM(timestamp)""",
    "market_anomalies": """CREATE TABLE IF NOT EXISTS market_anomalies (event_id String, detected_at DateTime64(3, 'UTC'), exchange LowCardinality(String), symbol LowCardinality(String), event_type LowCardinality(String), severity LowCardinality(String), direction LowCardinality(String), window LowCardinality(String), baseline_ready Bool, detectors_triggered Array(String), evidence String, metrics_snapshot String, data_quality_flags Array(String)) ENGINE = ReplacingMergeTree ORDER BY (symbol, exchange, window, detected_at, event_id) PARTITION BY toYYYYMM(detected_at)""",
    "market_integrity": """CREATE TABLE IF NOT EXISTS market_integrity (event_id String, calculated_at DateTime64(3, 'UTC'), canonical_pair LowCardinality(String), trigger_event_id String, trigger_type LowCardinality(String), trigger_severity LowCardinality(String), integrity_score Decimal(9, 4), overall_confidence Decimal(9, 6), evidence_coverage Decimal(9, 6), classification LowCardinality(String), classification_confidence Decimal(9, 6), move_direction LowCardinality(String), move_window LowCardinality(String), components String, data_quality_flags Array(String)) ENGINE = ReplacingMergeTree ORDER BY (canonical_pair, move_window, calculated_at, event_id) PARTITION BY toYYYYMM(calculated_at)""",
}
