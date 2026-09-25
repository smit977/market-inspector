"""Shared envelope for normalized market events."""

from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BaseEvent(BaseModel):
    """Common event metadata, with explicit identity and UTC timestamps.

    Adapters supply canonical instrument names (for example BTC-USDT); this
    model normalizes casing, but does not guess exchange-specific symbol maps.
    Schema versions are supplied by the caller and preserved verbatim.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(min_length=1)
    event_id: UUID
    event_type: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange_timestamp: datetime
    received_timestamp: datetime
    sequence_id: Annotated[str, Field(strict=True)] | Annotated[int, Field(strict=True)] | None = None

    @field_validator("schema_version", "event_type")
    @classmethod
    def reject_blank_metadata(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value

    @field_validator("exchange")
    @classmethod
    def normalize_exchange(cls, value: str) -> str:
        value = value.strip().lower()
        if not value:
            raise ValueError("exchange must not be blank")
        return value

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        value = value.strip().upper()
        if not value:
            raise ValueError("symbol must not be blank")
        return value

    @field_validator("exchange_timestamp", "received_timestamp", mode="before")
    @classmethod
    def require_explicit_datetime(cls, value: object) -> object:
        # Avoid silently guessing seconds versus milliseconds for epoch numbers.
        if not isinstance(value, (datetime, str)):
            raise ValueError("use an aware datetime or an ISO timestamp with timezone")
        return value

    @field_validator("exchange_timestamp", "received_timestamp")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(timezone.utc)
