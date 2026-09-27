"""Validated, controlled rules. No executable expressions or MCP writes."""
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

RULE_NAMES = frozenset("integrity_deterioration low_structural_confirmation liquidity_shock cross_venue_divergence derivatives_amplification liquidation_burst volume_concentration venue_stale source_unavailable timestamp_disorder orderbook_sequence_gap insufficient_evidence basis_reference_stale schema_failure consumer_lag_high consumer_lag_growing worker_unhealthy DLQ_activity clickhouse_persistence_failure mcp_unavailable processing_latency_degradation".split())

class MonitoringRuleDraft(BaseModel):
    """Future controlled authoring contract; not exposed through MCP or executed."""
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    metric: Literal["integrity_score", "dispersion_bps", "spread_bps", "volume_hhi", "consumer_lag"]
    operator: Literal["gt", "gte", "lt", "lte"]
    threshold: float
    window_seconds: int = Field(ge=1, le=3600)
    severity: Literal["info", "low", "moderate", "high", "critical"]
    cooldown_seconds: int = Field(default=300, ge=0)

class RuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    enabled: bool = True
    threshold: float = Field(gt=0)
    recovery: float = Field(ge=0)
    operator: Literal["above", "below"] = "above"
    severity: Literal["info", "low", "moderate", "high", "critical"] = "moderate"
    cooldown_seconds: int = Field(default=300, ge=0)
    recovery_seconds: int = Field(default=30, ge=1)
    @model_validator(mode="after")
    def hysteresis(self):
        if self.operator == "above" and self.recovery >= self.threshold:
            raise ValueError("recovery must be below threshold")
        if self.operator == "below" and self.recovery <= self.threshold:
            raise ValueError("recovery must be above threshold")
        return self

class AlertConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    worker_stall_seconds: int = Field(default=90, ge=30, le=600)
    window: str = "30s"
    max_age_seconds: int = Field(default=15, ge=1)
    min_confidence: float = Field(default=.65, ge=0, le=1)
    min_coverage: float = Field(default=.65, ge=0, le=1)
    comparison_seconds: int = Field(default=300, ge=1)
    relative_score_drop: float = Field(default=.25, gt=0, le=1)
    update_seconds: int = Field(default=30, ge=1)
    minimum_venues: int = Field(default=3, ge=2)
    spread_ratio: float = Field(default=2, gt=1)
    depth_drop: float = Field(default=.5, gt=0, lt=1)
    imbalance: float = Field(default=.8, gt=0, le=1)
    confirmation_max: float = Field(default=.6, ge=0, le=1)
    confirmation_recovery: float = Field(default=.8, ge=0, le=1)
    concentration_share: float = Field(default=.8, gt=0, le=1)
    volume_acceleration: float = Field(default=2, gt=1)
    basis_bps: float = Field(default=30, gt=0)
    return_divergence: float = Field(default=.002, gt=0)
    oi_change: float = Field(default=.02, gt=0)
    severity_high_ratio: float = Field(default=2, gt=1)
    severity_critical_ratio: float = Field(default=3, gt=2)
    rules: dict[str, RuleConfig]

    @model_validator(mode="after")
    def controlled_rules(self):
        if set(self.rules) != RULE_NAMES:
            raise ValueError("Configuration must contain exactly the supported rule names; disable rules explicitly")
        if self.severity_critical_ratio <= self.severity_high_ratio:
            raise ValueError("critical severity ratio must exceed high severity ratio")
        if self.confirmation_recovery <= self.confirmation_max:
            raise ValueError("confirmation recovery must exceed trigger ratio")
        return self

    @classmethod
    def load(cls, path="config/alert_rules.json"):
        return cls.model_validate_json(Path(path).read_text())
