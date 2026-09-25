"""Pydantic contract for calculated market microstructure features."""
from datetime import datetime
from decimal import Decimal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

class MarketMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = "1.0"
    event_id: UUID
    exchange: str = "binance"
    symbol: str = "BTC-USDT"
    window: str
    window_start: datetime
    window_end: datetime
    calculated_at: datetime
    price_return: Decimal | None = None
    absolute_return: Decimal | None = None
    price_range: Decimal | None = None
    trade_count: int = 0
    base_volume: Decimal = Decimal(0)
    quote_volume: Decimal = Decimal(0)
    buy_volume: Decimal = Decimal(0)
    sell_volume: Decimal = Decimal(0)
    trade_imbalance: Decimal | None = None
    vwap: Decimal | None = None
    average_trade_size: Decimal | None = None
    largest_trade_size: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    mid_price: Decimal | None = None
    spread: Decimal | None = None
    spread_bps: Decimal | None = None
    bid_depth_1: Decimal | None = None
    ask_depth_1: Decimal | None = None
    bid_depth_5: Decimal | None = None
    ask_depth_5: Decimal | None = None
    bid_depth_10: Decimal | None = None
    ask_depth_10: Decimal | None = None
    bid_depth_20: Decimal | None = None
    ask_depth_20: Decimal | None = None
    orderbook_imbalance_1: Decimal | None = None
    orderbook_imbalance_5: Decimal | None = None
    orderbook_imbalance_10: Decimal | None = None
    orderbook_imbalance_20: Decimal | None = None
    depth_change: Decimal | None = None
    spread_change: Decimal | None = None
    price_acceleration: Decimal | None = None
    trade_event_count: int = 0
    orderbook_event_count: int = 0
    data_complete: bool = False
    data_quality_flags: list[str] = Field(default_factory=list)
