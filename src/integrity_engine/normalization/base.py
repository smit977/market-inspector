"""Normalizer interface and delivery-before-commit processing."""

import asyncio
import json
import math
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import structlog
from confluent_kafka import Message
from pydantic import ValidationError

from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.schemas.trade import TradeEvent

log = structlog.get_logger(__name__)
PROCESSOR_NAME = "trade-normalizer-v1"
SOURCE_TOPIC = "market.raw.trades"
CLEANED_TOPIC = "market.cleaned.trades"
DLQ_TOPIC = "system.dlq"


class NormalizationError(ValueError):
    """Invalid or unsupported public market data; messages must omit input values."""


class TradeNormalizer(ABC):
    @abstractmethod
    def normalize(self, raw: dict[str, Any]) -> TradeEvent:
        """Convert one supported raw envelope, or raise a validation error."""


def _redact(value: Any) -> Any:
    """Keep public input intact while removing common accidental credentials."""
    secret_keys = {"apikey", "secret", "apisecret", "secretkey", "password",
                   "authorization", "token", "accesstoken", "refreshtoken", "signature"}
    if isinstance(value, dict):
        return {key: "[REDACTED]" if re.sub(r"[^a-z]", "", key.lower()) in secret_keys
                else _redact(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r"(?i)\bBearer\s+[^\s\"',}]+", "Bearer [REDACTED]", value)
        return re.sub(
            r'(?i)([\"\']?(?:api[_-]?key|api[_-]?secret|secret|password|authorization|'
            r'access[_-]?token|refresh[_-]?token|token|signature)[\"\']?\s*[:=]\s*)'
            r'([\"\'][^\"\']*[\"\']|[^\s,}]+)',
            r'\1"[REDACTED]"', value,
        )
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def make_dlq_event(original: Any, message: Message, error: Exception) -> dict[str, Any]:
    """Include source coordinates and safe diagnostics, never exception traces/inputs."""
    if isinstance(error, ValidationError):
        details = "; ".join(
            f"{'.'.join(map(str, item['loc']))}: {item['type']}"
            for item in error.errors(include_input=False, include_context=False,
                                     include_url=False)
        )
    elif isinstance(error, NormalizationError):
        details = str(error)
    else:
        details = "Raw event must contain valid UTF-8 JSON"
    return {
        "original_raw_event": _redact(original),
        "exchange": original.get("exchange") if isinstance(original, dict) else None,
        "exchange_symbol": (original.get("exchange_symbol", original.get("symbol"))
                            if isinstance(original, dict) else None),
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "processor_name": PROCESSOR_NAME,
        "error_type": type(error).__name__,
        "error_message": details,
        "failed_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _reject_constant(value: str) -> None:
    raise NormalizationError("Non-finite JSON numbers are not supported")


class TradeNormalizationProcessor:
    """At-least-once processing: acknowledge output, then commit source offset.

    A crash between those operations may duplicate output, but never intentionally
    skips input. Unsupported envelopes go to DLQ, not to another exchange adapter.
    """

    def __init__(self, normalizer: TradeNormalizer, consumer: ManualKafkaConsumer,
                 producer: JsonKafkaProducer) -> None:
        self.normalizer = normalizer
        self.consumer = consumer
        self.producer = producer
        self.cleaned = 0
        self.dead_lettered = 0

    def prepare(self, message: Message):
        data = message.value()
        original = data.decode("utf-8", errors="backslashreplace") if data is not None else None
        try:
            if data is None:
                raise NormalizationError("Raw event is a tombstone, not an envelope")
            original = json.loads(data.decode("utf-8"), parse_constant=_reject_constant)
            if not isinstance(original, dict):
                raise NormalizationError("Raw event must be a JSON object")
            event = self.normalizer.normalize(original)
        except (ValueError, UnicodeError) as error:
            dlq = make_dlq_event(original, message, error)
            log.warning("normalization_dlq", source_offset=message.offset(),
                        error_type=dlq["error_type"], error_message=dlq["error_message"])
            return (DLQ_TOPIC, f"{message.topic()}:{message.partition()}:{message.offset()}", dlq)
        else:
            return (CLEANED_TOPIC, event.symbol, event.model_dump(mode="json"))

    async def process(self, message: Message) -> None:
        output = self.prepare(message)
        await self.producer.publish(*output)
        self.cleaned += output[0] == CLEANED_TOPIC
        self.dead_lettered += output[0] == DLQ_TOPIC
        # Publish exceptions propagate and terminate processing before any commit.
        await self.consumer.commit(message)

    async def run(self, stop: asyncio.Event) -> None:
        next_log = time.monotonic() + 10
        while not stop.is_set():
            messages = await self.consumer.poll_batch()
            if messages:
                outputs = [self.prepare(message) for message in messages]
                await self.producer.publish_batch(outputs)
                await self.consumer.commit_batch(messages)
                self.cleaned += sum(o[0] == CLEANED_TOPIC for o in outputs)
                self.dead_lettered += sum(o[0] == DLQ_TOPIC for o in outputs)
            if time.monotonic() >= next_log:
                log.info("normalizer_progress", cleaned=self.cleaned, dlq=self.dead_lettered)
                next_log = time.monotonic() + 10
