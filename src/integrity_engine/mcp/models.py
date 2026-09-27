"""Stable request and response contracts exposed through MCP tools."""
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


CANONICAL_PAIR = "BTC-USD"
WINDOWS = {"10s", "30s", "1m", "5m"}
EXCHANGES = {"binance", "coinbase", "kraken", "okx", "bybit"}
TABLES = {
    "market_alerts": "updated_at",
    "operational_health": "timestamp",
    "market_metrics": "timestamp",
    "cross_venue_metrics": "timestamp",
    "derivatives_metrics": "timestamp",
    "market_anomalies": "detected_at",
    "market_integrity": "calculated_at",
}
TABLE_COLUMNS = {
    "market_metrics": {"event_id", "timestamp", "exchange", "symbol", "window", "price_return", "absolute_return", "price_range", "trade_count", "base_volume", "quote_volume", "buy_volume", "sell_volume", "trade_imbalance", "vwap", "average_trade_size", "largest_trade_size", "best_bid", "best_ask", "mid_price", "spread", "spread_bps", "bid_depth_20", "ask_depth_20", "orderbook_imbalance_20", "depth_change", "price_acceleration", "data_complete", "data_quality_flags"},
    "cross_venue_metrics": {"event_id", "timestamp", "canonical_pair", "window", "active_venue_count", "stale_venue_count", "median_price", "mean_price", "min_price", "max_price", "dispersion", "dispersion_bps", "confirmation_ratio", "confirmation_state", "median_return", "return_dispersion", "largest_venue_volume_share", "volume_hhi", "first_observed_venue", "venue_prices", "venue_deviation_bps", "venue_returns", "venue_volume_shares", "follow_lag_ms", "data_quality_flags"},
    "derivatives_metrics": {"event_id", "timestamp", "exchange", "canonical_pair", "window", "perpetual_price", "perpetual_return", "perpetual_base_volume", "perpetual_quote_volume", "perpetual_trade_count", "perpetual_trade_imbalance", "open_interest", "open_interest_change", "open_interest_change_pct", "funding_rate", "funding_rate_change", "liquidation_count", "long_liquidation_volume", "short_liquidation_volume", "total_liquidation_volume", "spot_reference_price", "basis", "basis_bps", "perpetual_to_spot_volume_ratio", "spot_perpetual_return_difference", "data_complete", "data_quality_flags", "volume_unit", "open_interest_unit", "open_interest_comparable", "spot_reference_timestamp", "source_timestamp"},
    "market_anomalies": {"event_id", "detected_at", "exchange", "symbol", "window", "event_type", "severity", "direction", "baseline_ready", "detectors_triggered", "metrics_snapshot", "evidence", "data_quality_flags"},
    "market_integrity": {"event_id", "calculated_at", "canonical_pair", "trigger_event_id", "trigger_type", "trigger_severity", "move_direction", "move_window", "integrity_score", "overall_confidence", "evidence_coverage", "classification", "classification_confidence", "components", "data_quality_flags"},
}

from integrity_engine.storage.models import TABLE_TYPES as STORAGE_TYPES
for _table in ("market_alerts", "operational_health"):
    TABLE_COLUMNS[_table] = set(STORAGE_TYPES[_table])


class ToolInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LookbackInput(ToolInput):
    canonical_pair: str = CANONICAL_PAIR
    lookback_minutes: int = Field(default=15, ge=1, le=1440)

    @field_validator("canonical_pair")
    @classmethod
    def pair_format(cls, value: str) -> str:
        value = value.strip().upper()
        if not value or len(value) > 24 or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-/" for c in value):
            raise ValueError("invalid canonical_pair")
        return value


class LatestMetricsInput(LookbackInput):
    exchange: str | None = None
    window: str = "30s"

    @field_validator("exchange")
    @classmethod
    def exchange_supported(cls, value: str | None) -> str | None:
        if value is not None and value.lower() not in EXCHANGES:
            raise ValueError("unsupported_exchange")
        return value.lower() if value else None

    @field_validator("window")
    @classmethod
    def window_supported(cls, value: str) -> str:
        if value not in WINDOWS:
            raise ValueError("window must be one of 10s, 30s, 1m, 5m")
        return value


class QueryClickHouseInput(ToolInput):
    table: str
    columns: list[str] = Field(min_length=1, max_length=30)
    lookback_minutes: int = Field(default=15, ge=1, le=43200)
    canonical_pair: str | None = None
    exchange: str | None = None
    limit: int = Field(default=100, ge=1, le=5000)

    @field_validator("table")
    @classmethod
    def table_allowed(cls, value: str) -> str:
        if value not in TABLES:
            raise ValueError("unsupported_table")
        return value

    @field_validator("columns")
    @classmethod
    def columns_allowed(cls, value: list[str], info) -> list[str]:
        table = info.data.get("table")
        if table and any(column not in TABLE_COLUMNS[table] for column in value):
            raise ValueError("unsupported_column")
        if len(value) != len(set(value)):
            raise ValueError("duplicate_columns")
        return value


class EvidenceGroups(BaseModel):
    observed_fact: Any | None = None
    calculated_metric: Any | None = None
    statistical_inference: Any | None = None
    ai_interpretation: Any | None = None


class MCPResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "no_data", "stale_data", "error"]
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data_as_of: datetime | str | None = None
    source_tables: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
    query_duration_ms: float | None = None
    evidence: EvidenceGroups = Field(default_factory=EvidenceGroups)
    data: Any | None = None
    error: dict[str, str] | None = None


def response_dict(**kwargs) -> dict[str, Any]:
    """Serialize models/datetimes/Decimals into transport-safe JSON values."""
    response = MCPResponse(**kwargs)
    return response.model_dump(mode="json")
