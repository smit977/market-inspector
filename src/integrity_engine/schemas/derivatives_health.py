"""Operational source health, kept separate from market metrics."""
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class DerivativesSourceHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    exchange: str
    feed_type: str
    status: str
    last_event_received_at: datetime | None = None
    last_successful_poll_at: datetime | None = None
    consecutive_errors: int = 0
    last_error_type: str | None = None
