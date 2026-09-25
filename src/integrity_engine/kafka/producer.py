"""Bounded ordered batches; delivery confirmation precedes acknowledgements."""
import asyncio
import json
import time
from typing import Any
import structlog
from confluent_kafka import KafkaException, Producer
log = structlog.get_logger(__name__)

class KafkaDeliveryError(RuntimeError):
    pass

def serialize_event(event: dict[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8")

class JsonKafkaProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers, "enable.idempotence": True,
            "acks": "all", "message.timeout.ms": 10000, "linger.ms": 0,
            "queue.buffering.max.kbytes": 16384, "allow.auto.create.topics": False,
        })
        self.delivered = 0
        self._lock = asyncio.Lock()
        self.delivery_timeout = 12

    def _publish_batch(self, records):
        outstanding = len(records)
        errors = []
        def delivered(error, message):
            nonlocal outstanding
            outstanding -= 1
            if error is not None:
                errors.append(error)
            else:
                self.delivered += 1
        for topic, key, value in records:
            self._producer.produce(topic, key=key.encode(), value=value, on_delivery=delivered)
        deadline = time.monotonic() + self.delivery_timeout
        while outstanding and time.monotonic() < deadline:
            self._producer.poll(.01)
        if outstanding:
            raise TimeoutError("Kafka batch delivery timed out")
        if errors:
            raise KafkaException(errors[0])

    async def publish_batch(self, records) -> None:
        async with self._lock:
            for start in range(0, len(records), 256):
                try:
                    batch = [(t, k, serialize_event(e)) for t, k, e in records[start:start + 256]]
                    task = asyncio.create_task(asyncio.to_thread(self._publish_batch, batch))
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        await task
                        raise
                except Exception as error:
                    log.error("kafka_delivery_failed", error_type=type(error).__name__)
                    raise KafkaDeliveryError(str(error)) from error

    async def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        await self.publish_batch([(topic, key, event)])

    async def close(self) -> None:
        async with self._lock:
            if await asyncio.to_thread(self._producer.flush, 12):
                raise KafkaDeliveryError("Kafka shutdown flush left undelivered records")
