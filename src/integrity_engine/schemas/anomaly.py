"""Neutral statistical market-event schema; this is not an integrity verdict."""
from datetime import datetime
from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

class AnomalyEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    metric: str
    current_value: Any
    baseline_mean: float | None = None
    baseline_std: float | None = None
    z_score: float | None = None
    percentile: float | None = None
    threshold: float | None = None
    reason: str

class MarketAnomaly(BaseModel):
    """Statistically unusual conditions that warrant deeper analysis."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = "1.0"
    event_id: UUID
    event_type: str
    exchange: str
    symbol: str
    detected_at: datetime
    window: str
    severity: Literal["low", "moderate", "high"]
    direction: str = "neutral"
    detectors_triggered: list[str]
    metrics_snapshot: dict[str, Any]
    evidence: list[AnomalyEvidence]
    baseline_ready: bool
    data_quality_flags: list[str] = Field(default_factory=list)
