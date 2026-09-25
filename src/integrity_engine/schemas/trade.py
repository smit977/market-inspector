"""Normalized trade event shared by all exchange adapters."""

from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator

from integrity_engine.schemas.base import BaseEvent


class TradeEvent(BaseEvent):
    """A trade with exact amounts and optional exchange identifiers.

    Side describes the aggressor side, or unknown when it is unavailable.
    Amounts accept Decimal, decimal strings, or integers, never binary floats.
    JSON serialization emits decimal strings to preserve precision. Quote
    quantity, when supplied, is retained rather than derived from rounded data.
    """

    event_type: Literal["trade"] = "trade"
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    quantity: Decimal = Field(gt=0, allow_inf_nan=False)
    side: Literal["buy", "sell", "unknown"]
    trade_id: str | None = None
    quote_quantity: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    # Exchange symbol remains in ``symbol`` for backwards compatibility. These
    # fields make cross-venue comparison explicit without claiming USD == USDT.
    exchange_symbol: str | None = None
    canonical_asset: str | None = None
    canonical_quote_family: str | None = None
    base_asset: str | None = None
    quote_asset: str | None = None
    quote_family: str | None = None
    canonical_asset_pair: str | None = None

    @field_validator("price", "quantity", "quote_quantity", mode="before")
    @classmethod
    def reject_inexact_amounts(cls, value: object) -> object:
        if isinstance(value, (float, bool)):
            raise ValueError("use Decimal, a decimal string, or an integer, not float/bool")
        return value
