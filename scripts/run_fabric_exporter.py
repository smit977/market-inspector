"""Export curated Kafka events to a Fabric Eventstream Kafka endpoint."""
import asyncio, os
from datetime import datetime, timezone
import structlog
from integrity_engine.fabric.config import FabricSettings
from integrity_engine.fabric.producer import FabricProducer, FabricDeliveryError
from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.schemas.fabric import FabricEventEnvelope

log = structlog.get_logger(__name__)
TOPICS = ["market.metrics", "market.metrics.cross_venue", "market.metrics.derivatives", "market.anomalies", "market.integrity"]

def _payload(message):
    import json
    return json.loads(message.value().decode("utf-8"))

def envelope(message, payload):
    received = datetime.now(timezone.utc)
    return FabricEventEnvelope(
        source_topic=message.topic(), source_partition=message.partition(), source_offset=message.offset(),
        event_id=payload.get("event_id", f"{message.topic()}:{message.partition()}:{message.offset()}"),
        event_type=payload.get("event_type") or payload.get("trigger_type"),
        exchange=payload.get("exchange") or payload.get("exchange_scope"),
        canonical_pair=payload.get("canonical_pair"),
        event_timestamp=payload.get("calculated_at") or payload.get("detected_at") or payload.get("event_timestamp"),
        received_at=received, payload=payload,
    ).model_dump(mode="json")

async def send_with_retries(producer, destination, key, event, settings, counts):
    """Retry the same envelope/key; event_id is never regenerated."""
    for attempt in range(settings.fabric_max_retries + 1):
        try:
            await producer.send(destination, key, event)
            return
        except Exception:
            counts["retries"] += 1
            if attempt >= settings.fabric_max_retries:
                raise
            await asyncio.sleep(settings.fabric_retry_backoff_ms / 1000 * (2 ** attempt))

async def run(duration: float | None = None):
    settings = FabricSettings(); settings.validate_enabled()
    if not settings.fabric_enabled:
        log.info("fabric_exporter_disabled")
        return {"events_read": 0, "events_sent": 0}
    producer = FabricProducer(settings)
    dlq = JsonKafkaProducer(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    consumer = ManualKafkaConsumer(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), TOPICS, settings.fabric_export_group,
                                   os.environ.get("FABRIC_AUTO_OFFSET_RESET", "latest"))
    counts = {"events_read": 0, "events_sent": 0, "send_failures": 0, "retries": 0, "DLQ_exports": 0}
    started = asyncio.get_running_loop().time()
    try:
        while duration is None or asyncio.get_running_loop().time() - started < duration:
            msg = await consumer.poll()
            if msg is None: continue
            counts["events_read"] += 1
            try:
                payload = _payload(msg)
                destination = settings.routing.get(msg.topic(), "")
                if not destination: raise ValueError(f"No Fabric destination configured for {msg.topic()}")
                event = envelope(msg, payload)
                key = str(event["event_id"])
                await send_with_retries(producer, destination, key, event, settings, counts)
                await consumer.commit(msg); counts["events_sent"] += 1
            except Exception as exc:
                counts["send_failures"] += 1
                try:
                    failed_event_id = _payload(msg).get("event_id")
                except Exception:
                    failed_event_id = None
                dlq_event = {"source_topic": msg.topic(), "source_partition": msg.partition(), "source_offset": msg.offset(),
                             "event_id": failed_event_id, "processor": "fabric-exporter-v1",
                             "error_type": type(exc).__name__, "error_message": str(exc), "failed_timestamp": datetime.now(timezone.utc).isoformat()}
                await dlq.publish("system.dlq", msg.topic(), dlq_event); counts["DLQ_exports"] += 1
                log.error("fabric_export_failed", **{k: v for k, v in dlq_event.items() if k != "event_id"})
    finally:
        await consumer.close(); await producer.close(); await dlq.close()
    log.info("fabric_exporter_stopped", **counts)
    return counts

def main():
    import argparse
    p=argparse.ArgumentParser(); p.add_argument("--duration", type=float)
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True), structlog.processors.JSONRenderer()])
    print(asyncio.run(run(p.parse_args().duration)))
if __name__ == "__main__": main()
