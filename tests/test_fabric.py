import os
from datetime import datetime, timezone
from uuid import uuid4
import pytest
from integrity_engine.fabric.config import FabricSettings
from integrity_engine.schemas.fabric import FabricEventEnvelope
from scripts.run_fabric_exporter import envelope, send_with_retries

class Message:
    def __init__(self, topic="market.integrity", partition=2, offset=41):
        self._topic, self._partition, self._offset = topic, partition, offset
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset

def test_disabled_configuration_is_safe(monkeypatch):
    monkeypatch.delenv("FABRIC_ENABLED", raising=False)
    settings = FabricSettings()
    assert settings.fabric_enabled is False
    settings.validate_enabled()

def test_enabled_configuration_requires_bootstrap(monkeypatch):
    monkeypatch.setenv("FABRIC_ENABLED", "true")
    monkeypatch.delenv("FABRIC_KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(ValueError): FabricSettings().validate_enabled()

def test_envelope_preserves_event_id_source_metadata_and_payload():
    event_id = str(uuid4())
    payload = {"event_id": event_id, "event_type": "rapid_price_move", "exchange": "binance",
               "canonical_pair": "BTC-USD", "detected_at": datetime.now(timezone.utc).isoformat(), "score": "x"}
    result = envelope(Message(), payload)
    parsed = FabricEventEnvelope.model_validate(result)
    assert str(parsed.event_id) == event_id
    assert parsed.source_topic == "market.integrity"
    assert parsed.source_partition == 2 and parsed.source_offset == 41
    assert parsed.payload == payload

def test_routing_is_explicit():
    settings = FabricSettings(fabric_topic_metrics="m", fabric_topic_cross_venue="cv", fabric_topic_derivatives="d", fabric_topic_anomalies="a", fabric_topic_integrity="i")
    assert settings.routing == {"market.metrics":"m", "market.metrics.cross_venue":"cv", "market.metrics.derivatives":"d", "market.anomalies":"a", "market.integrity":"i"}

@pytest.mark.asyncio
async def test_retry_reuses_same_event_id():
    class Fake:
        def __init__(self): self.calls = []
        async def send(self, topic, key, event):
            self.calls.append((topic, key, event["event_id"]))
            if len(self.calls) < 3: raise RuntimeError("temporary")
    fake, counts = Fake(), {"retries": 0}
    settings = FabricSettings(fabric_max_retries=3, fabric_retry_backoff_ms=0)
    await send_with_retries(fake, "fabric-topic", "event-1", {"event_id":"event-1"}, settings, counts)
    assert len(fake.calls) == 3
    assert {call[2] for call in fake.calls} == {"event-1"}
    assert counts["retries"] == 2
