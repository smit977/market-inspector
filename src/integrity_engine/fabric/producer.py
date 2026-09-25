"""Kafka-compatible Fabric producer with bounded, acknowledged sends."""
import asyncio
from typing import Any
from confluent_kafka import Producer, KafkaException
from integrity_engine.kafka.producer import serialize_event

class FabricDeliveryError(RuntimeError):
    pass

class FabricProducer:
    def __init__(self, settings):
        if not settings.fabric_kafka_bootstrap_servers.strip():
            raise ValueError("Fabric bootstrap servers must be configured")
        conf = {
            "bootstrap.servers": settings.fabric_kafka_bootstrap_servers,
            "security.protocol": settings.fabric_security_protocol,
            "sasl.mechanisms": settings.fabric_sasl_mechanism,
            "sasl.username": settings.fabric_kafka_username,
            "sasl.password": settings.fabric_kafka_password,
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 10000,
        }
        self._producer = Producer(conf)
        self.sent = 0

    def _send(self, topic: str, key: str, value: bytes) -> None:
        result = []
        self._producer.produce(topic, key=key.encode(), value=value,
                               on_delivery=lambda error, message: result.append(error))
        self._producer.flush(12)
        if not result or result[0] is not None:
            raise KafkaException(result[0] if result else "Fabric delivery timeout")
        self.sent += 1

    async def send(self, topic: str, key: str, event: dict[str, Any]) -> None:
        try:
            await asyncio.to_thread(self._send, topic, key, serialize_event(event))
        except Exception as exc:
            raise FabricDeliveryError(str(exc)) from exc

    async def close(self) -> None:
        await asyncio.to_thread(self._producer.flush, 12)
