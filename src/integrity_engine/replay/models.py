"""Archive v1: typed UTC metadata plus byte-exact Kafka value and key."""
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

TOPICS = {f"market.{stage}.{stream}" for stage in ("raw", "cleaned") for stream in ("trades", "orderbook", "derivatives")}


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ArchivedEvent(Model):
    archive_schema_version: Literal["1.0"] = "1.0"
    schema_version: str
    exchange: str
    stream: str
    event_type: str | None = None
    exchange_symbol: str
    canonical_pair: str | None = None
    exchange_timestamp: datetime | None = None
    received_timestamp: datetime
    sequence_id: str | None = None
    source_topic: str
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)
    kafka_timestamp: datetime
    raw_payload: bytes
    source_key: bytes | None = None
    quality_flags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_record(self):
        if self.source_topic not in TOPICS:
            raise ValueError("unsupported archive topic")
        for stamp in (self.exchange_timestamp, self.received_timestamp, self.kafka_timestamp):
            if stamp is not None and stamp.utcoffset() is None:
                raise ValueError("archive timestamps must be timezone aware")
        return self

    @property
    def identity(self):
        return (self.source_topic, self.source_partition, self.source_offset)

    @property
    def order_key(self):
        # Future exchange timestamps can describe scheduled funding settlements.
        # Keep payload time intact, but never postpone observation beyond receipt.
        observed = min(self.exchange_timestamp, self.received_timestamp) if self.exchange_timestamp else self.received_timestamp
        return (observed, self.received_timestamp, *self.identity)


class ReplayManifest(Model):
    schema_version: Literal["1.0"] = "1.0"
    replay_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    created_at: datetime
    canonical_pair: str = "BTC-USD"
    start_time: datetime
    end_time: datetime
    source_topics: list[str]
    source_files: list[str]
    exchange_set: list[str]
    event_count: int = Field(ge=0)
    first_event_timestamp: datetime | None = None
    last_event_timestamp: datetime | None = None
    schema_versions: list[str]
    file_hashes: dict[str, str]
    notes: list[str] = Field(default_factory=list)
    partition_ranges: dict[str, dict] = Field(default_factory=dict)
    quality_counts: dict[str, int] = Field(default_factory=dict)
    archive_statistics: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid(self):
        if self.start_time.utcoffset() is None or self.end_time.utcoffset() is None or self.end_time <= self.start_time:
            raise ValueError("require bounded aware start < end")
        if set(self.source_files) != set(self.file_hashes) or len(set(self.source_files)) != len(self.source_files):
            raise ValueError("every unique file requires a hash")
        if not set(self.source_topics) <= TOPICS:
            raise ValueError("unsupported source topics")
        if self.canonical_pair != "BTC-USD":
            raise ValueError("only existing BTC-USD engine supported")
        return self


class ReplayRun(Model):
    replay_run_id: str
    replay_id: str
    git_commit: str
    dirty: bool
    code_hash: str
    started_at: datetime
    finished_at: datetime | None = None
    speed: str
    configuration: dict
    config_hash: str
    manifest_hash: str
    schema_versions: list[str]
    event_count: int = 0
    anomaly_count: int = 0
    integrity_event_count: int = 0
    error_count: int = 0
    DLQ_count: int = 0
    determinism_hash: str | None = None
    statistics: dict = Field(default_factory=dict)


class HistoricalEvent(Model):
    event_id: str
    replay_id: str
    replay_run_id: str
    canonical_pair: str
    event_start: datetime
    event_end: datetime
    trigger_type: str
    severity: str
    direction: str
    integrity_score: float | None
    classification: str | None
    confidence: float | None
    coverage: float | None
    peak_return: float | None = None
    peak_volume_z: float | None = None
    peak_dispersion_bps: float | None = None
    minimum_liquidity_score: float | None = None
    minimum_cross_venue_score: float | None = None
    minimum_derivatives_score: float | None = None
    notes: list[str] = Field(default_factory=list)


class LabeledWindow(Model):
    label_id: str
    start_time: datetime
    end_time: datetime
    label: Literal["broad_spot_supported", "derivatives_amplified", "liquidity_shock", "cross_venue_divergence", "volume_anomaly", "mixed_unknown"]
    definition: str = Field(min_length=1)
    evidence: list[str] = Field(min_length=1)
    expected_trigger: bool
    trigger_types: list[str] = Field(default_factory=list)
    # Evaluation domain must be explicitly covered, including negative windows.
    @model_validator(mode="after")
    def valid(self):
        if self.start_time.utcoffset() is None or self.end_time.utcoffset() is None or self.start_time >= self.end_time:
            raise ValueError("invalid labeled window")
        return self
