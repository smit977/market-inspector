"""Neutral, measurable cross-venue market features."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field


class CrossVenueMetrics(BaseModel):
    """Observations across spot venues; no ranking or integrity conclusion."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = "1.0"
    event_id: UUID
    canonical_pair: str = "BTC-USD"
    market_type: str = "spot"
    window: str
    calculated_at: datetime
    active_venues: list[str] = Field(default_factory=list)
    stale_venues: list[str] = Field(default_factory=list)
    median_price: Decimal | None = None
    mean_price: Decimal | None = None
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    dispersion: Decimal | None = None
    dispersion_bps: Decimal | None = None
    venue_prices: dict[str, Decimal] = Field(default_factory=dict)
    venue_deviation_bps: dict[str, Decimal] = Field(default_factory=dict)
    venue_returns: dict[str, Decimal | None] = Field(default_factory=dict)
    up_venue_count: int = 0
    down_venue_count: int = 0
    flat_venue_count: int = 0
    same_direction_count: int = 0
    confirmation_ratio: Decimal | None = None
    confirmation_state: str = "insufficient_data"
    median_return: Decimal | None = None
    return_dispersion: Decimal | None = None
    venue_quote_volumes: dict[str, Decimal] = Field(default_factory=dict)
    venue_volume_shares: dict[str, Decimal] = Field(default_factory=dict)
    largest_venue_volume_share: Decimal | None = None
    volume_hhi: Decimal | None = None
    first_observed_venue: str | None = None
    follow_lag_ms: dict[str, int] = Field(default_factory=dict)
    data_complete: bool = False
    data_quality_flags: list[str] = Field(default_factory=list)
