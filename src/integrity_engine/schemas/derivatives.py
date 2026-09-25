"""Typed derivatives events and evidence metrics."""
from datetime import datetime
from decimal import Decimal
from uuid import UUID
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field

class DerivativesEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = "1.0"
    event_id: UUID
    event_type: str
    exchange: str
    exchange_symbol: str
    canonical_pair: str = "BTC-USD"
    market_type: Literal["perpetual", "futures"] = "perpetual"
    base_asset: str = "BTC"
    quote_asset: str
    settlement_asset: str | None = None
    exchange_timestamp: datetime
    received_timestamp: datetime

class PerpetualTrade(DerivativesEvent):
    event_type: Literal["perpetual_trade"] = "perpetual_trade"
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    quote_quantity: Decimal | None = Field(default=None, ge=0)
    side: Literal["buy", "sell", "unknown"] = "unknown"
    trade_id: str | None = None
    quantity_unit: str = "base"
    comparable: bool = True
    value_unit: str | None = "USDT"

class FundingRate(DerivativesEvent):
    event_type: Literal["funding_rate"] = "funding_rate"
    funding_rate: Decimal
    funding_timestamp: datetime
    next_funding_timestamp: datetime | None = None
    rate_unit: str = "fraction"

class OpenInterest(DerivativesEvent):
    event_type: Literal["open_interest"] = "open_interest"
    open_interest: Decimal = Field(ge=0)
    open_interest_value: Decimal | None = Field(default=None, ge=0)
    quantity_unit: str
    value_unit: str | None = None
    comparable: bool = False

class LiquidationEvent(DerivativesEvent):
    event_type: Literal["liquidation"] = "liquidation"
    side: Literal["buy", "sell", "unknown"] = "unknown"
    price: Decimal | None = Field(default=None, gt=0)
    quantity: Decimal = Field(gt=0)
    quote_value: Decimal | None = Field(default=None, ge=0)
    quantity_unit: str = "base"
    comparable: bool = True
    value_unit: str | None = "USDT"

class DerivativesMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = "1.0"
    event_id: UUID
    exchange: str
    canonical_pair: str = "BTC-USD"
    window: str
    calculated_at: datetime
    perpetual_price: Decimal | None = None
    perpetual_return: Decimal | None = None
    perpetual_base_volume: Decimal | None = None
    perpetual_quote_volume: Decimal | None = None
    perpetual_trade_count: int = 0
    perpetual_trade_imbalance: Decimal | None = None
    open_interest: Decimal | None = None
    open_interest_unit: str | None = None
    open_interest_comparable: bool = False
    open_interest_change: Decimal | None = None
    open_interest_change_pct: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_rate_change: Decimal | None = None
    long_liquidation_volume: Decimal | None = None
    short_liquidation_volume: Decimal | None = None
    total_liquidation_volume: Decimal | None = None
    liquidation_quote_volume: Decimal | None = None
    volume_unit: str | None = None
    liquidation_count: int = 0
    spot_reference_price: Decimal | None = None
    spot_reference_timestamp: datetime | None = None
    basis: Decimal | None = None
    basis_bps: Decimal | None = None
    basis_change: Decimal | None = None
    spot_quote_volume: Decimal | None = None
    perpetual_to_spot_volume_ratio: Decimal | None = None
    spot_perpetual_return_difference: Decimal | None = None
    data_complete: bool = False
    data_quality_flags: list[str] = Field(default_factory=list)
