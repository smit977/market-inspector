"""Normalized, exchange-independent local order-book snapshots."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OrderBookLevel(BaseModel):
    """One price level in a normalized book."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    quantity: Decimal = Field(gt=0, allow_inf_nan=False)


class OrderBookSnapshot(BaseModel):
    """A validated, synchronized top-N view of a local book."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = Field(min_length=1)
    event_id: UUID
    event_type: Literal["orderbook"] = "orderbook"
    exchange: Literal["binance"] = "binance"
    symbol: Literal["BTC-USDT"] = "BTC-USDT"
    exchange_timestamp: datetime
    received_timestamp: datetime
    sequence_id: int = Field(ge=0)
    best_bid_price: Decimal = Field(gt=0, allow_inf_nan=False)
    best_bid_quantity: Decimal = Field(gt=0, allow_inf_nan=False)
    best_ask_price: Decimal = Field(gt=0, allow_inf_nan=False)
    best_ask_quantity: Decimal = Field(gt=0, allow_inf_nan=False)
    spread: Decimal = Field(ge=0, allow_inf_nan=False)
    mid_price: Decimal = Field(gt=0, allow_inf_nan=False)
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]

    @field_validator("exchange_timestamp", "received_timestamp")
    @classmethod
    def require_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        from datetime import timezone
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_book(self) -> "OrderBookSnapshot":
        if not self.bids or not self.asks:
            raise ValueError("both sides must contain at least one level")
        if any(level.quantity <= 0 for level in (*self.bids, *self.asks)):
            raise ValueError("zero quantity levels are not valid")
        if len({level.price for level in self.bids}) != len(self.bids):
            raise ValueError("duplicate bid price levels")
        if len({level.price for level in self.asks}) != len(self.asks):
            raise ValueError("duplicate ask price levels")
        if any(self.bids[i].price <= self.bids[i + 1].price for i in range(len(self.bids) - 1)):
            raise ValueError("bids must be strictly descending")
        if any(self.asks[i].price >= self.asks[i + 1].price for i in range(len(self.asks) - 1)):
            raise ValueError("asks must be strictly ascending")
        if self.best_bid_price != self.bids[0].price or self.best_ask_price != self.asks[0].price:
            raise ValueError("best prices must match the first levels")
        if self.best_bid_quantity != self.bids[0].quantity or self.best_ask_quantity != self.asks[0].quantity:
            raise ValueError("best quantities must match the first levels")
        if self.best_bid_price >= self.best_ask_price:
            raise ValueError("book is crossed")
        if self.spread != self.best_ask_price - self.best_bid_price:
            raise ValueError("spread does not match best prices")
        if self.mid_price != (self.best_bid_price + self.best_ask_price) / Decimal(2):
            raise ValueError("mid price does not match best prices")
        return self
