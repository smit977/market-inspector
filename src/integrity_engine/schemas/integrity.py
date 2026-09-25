"""Machine-readable V1 integrity evidence; this is not manipulation probability."""
from datetime import datetime
from decimal import Decimal
from uuid import UUID
from typing import Any
from pydantic import BaseModel, ConfigDict, Field

class IntegrityEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source: str
    metric: str
    window: str | None = None
    current_value: Any = None
    baseline_value: Any = None
    z_score: Decimal | None = None
    threshold: Decimal | None = None
    data_quality: list[str] = Field(default_factory=list)
    interpretation: str
    effect: str | None = None

class IntegrityComponent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str
    score: Decimal | None = Field(default=None, ge=0, le=100)
    confidence: Decimal = Field(ge=0, le=1)
    weight: Decimal = Field(ge=0, le=1)
    status: str
    availability: str = "available"
    evidence: list[IntegrityEvidence] = Field(default_factory=list)
    data_quality_flags: list[str] = Field(default_factory=list)

class MarketIntegrityEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: str = "1.0"
    event_id: UUID
    exchange_scope: str
    canonical_pair: str = "BTC-USD"
    trigger_event_id: UUID
    trigger_type: str
    trigger_severity: str
    move_direction: str
    move_window: str
    integrity_score: Decimal = Field(ge=0, le=100)
    overall_confidence: Decimal = Field(ge=0, le=1)
    evidence_coverage: Decimal = Field(ge=0, le=1)
    classification: str
    classification_confidence: Decimal = Field(ge=0, le=1)
    components: list[IntegrityComponent]
    data_quality_flags: list[str] = Field(default_factory=list)
    calculated_at: datetime
