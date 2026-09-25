"""Stable envelope sent from curated Kafka topics to Fabric."""
from datetime import datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel, ConfigDict

class FabricEventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str = "1.0"
    source_topic: str
    source_partition: int
    source_offset: int
    event_id: UUID | str
    event_type: str | None = None
    exchange: str | None = None
    canonical_pair: str | None = None
    event_timestamp: datetime | None = None
    received_at: datetime
    payload: dict[str, Any]
