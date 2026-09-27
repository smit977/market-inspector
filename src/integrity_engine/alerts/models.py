"""Versioned incident snapshots with explicit observations and interpretation."""
from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, AwareDatetime
from integrity_engine.schemas.integrity import IntegrityEvidence

Severity = Literal["info", "low", "moderate", "high", "critical"]
Category = Literal["MARKET_STRUCTURE", "DATA_QUALITY", "PIPELINE_HEALTH"]
SEVERITIES = ("info", "low", "moderate", "high", "critical")

class AlertEvidence(IntegrityEvidence):
    previous_value: Any = None
    baseline: Any = None
    freshness: float | None = None
    quality: list[str] = Field(default_factory=list)

class AlertEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    schema_version: str = "1.0"
    event_id: str
    alert_id: str
    revision: int = Field(ge=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime
    canonical_pair: str
    category: Category
    alert_type: str
    severity: Severity
    initial_severity: Severity
    maximum_severity: Severity
    status: Literal["open", "acknowledged", "resolved"] = "open"
    title: str
    summary: str
    trigger_event_ids: list[str] = Field(default_factory=list)
    first_detected_at: AwareDatetime
    last_observed_at: AwareDatetime
    resolved_at: AwareDatetime | None = None
    evidence: list[AlertEvidence]
    integrity_score: float | None = None
    previous_integrity_score: float | None = None
    integrity_score_change: float | None = None
    overall_confidence: float = Field(ge=0, le=1)
    evidence_coverage: float = Field(ge=0, le=1)
    classification: str | None = None
    affected_exchanges: list[str] = Field(default_factory=list)
    data_quality_flags: list[str] = Field(default_factory=list)
    dedupe_key: str
    notification_state: Literal["pending", "logged", "sent", "failed", "suppressed", "replay_suppressed"] = "pending"
    is_replay: bool = False
    escalation_timeline: list[dict[str, Any]] = Field(default_factory=list)

class Assessment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    canonical_pair: str
    category: Category
    alert_type: str
    scope: str
    active: bool
    recovered: bool
    severity: Severity = "moderate"
    summary: str
    evidence: list[AlertEvidence]
    trigger_event_ids: list[str] = Field(default_factory=list)
    confidence: float = 1
    coverage: float = 1
    integrity_score: float | None = None
    previous_integrity_score: float | None = None
    classification: str | None = None
    exchanges: list[str] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)
    @property
    def key(self):
        return f"{self.canonical_pair}:{self.alert_type}:{self.scope}"

class HealthObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    value: float | None
    scope: str
    known_partitions: int | None = None
    unknown_partitions: int | None = None
    measurement: str | None = None

class OperationalHealth(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["1.0"] = "1.0"
    event_id: str
    canonical_pair: str = "BTC-USD"
    exchange: str
    calculated_at: AwareDatetime
    signals: dict[str, HealthObservation]
