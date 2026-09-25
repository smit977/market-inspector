"""Offline normalization, DLQ, and commit-order contract tests."""

import copy
import json
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException
from pydantic import ValidationError

from integrity_engine.kafka import consumer as consumer_module
from integrity_engine.kafka.producer import KafkaDeliveryError, serialize_event
from integrity_engine.normalization.base import (
    CLEANED_TOPIC, DLQ_TOPIC, NormalizationError, TradeNormalizationProcessor,
    make_dlq_event,
)
from integrity_engine.normalization.binance import BinanceTradeNormalizer
from integrity_engine.schemas.trade import TradeEvent


@pytest.fixture
def raw():
    return {
        "schema_version": "1.0", "exchange": "binance", "stream": "trades",
        "symbol": "BTCUSDT", "received_timestamp": "2026-09-19T14:32:33.145781+00:00",
        "payload": {"e": "trade", "E": 1789828353037, "s": "BTCUSDT",
                    "t": 6694573558, "p": "81602.56000000", "q": "0.00106000",
                    "T": 1789828353036, "m": True, "M": True},
    }


def record(value):
    message = MagicMock()
    message.value.return_value = value
    message.topic.return_value = "market.raw.trades"
    message.partition.return_value = 0
    message.offset.return_value = 12
    message.error.return_value = None
    return message


def test_valid_normalization_and_serialization(raw):
    original = copy.deepcopy(raw)
    event = BinanceTradeNormalizer().normalize(raw)
    assert raw == original
    assert event.schema_version == "1.0"
    assert event.exchange == "binance"
    assert event.symbol == "BTC-USDT"
    assert event.event_type == "trade"
    assert event.trade_id == "6694573558"
    assert event.sequence_id == 6694573558
    assert event.price == Decimal("81602.56000000")
    assert event.quantity == Decimal("0.00106000")
    assert event.quote_quantity == Decimal("86.4987136000000000")
    assert event.exchange_timestamp == datetime(2026, 9, 19, 14, 32, 33, 36000, tzinfo=timezone.utc)
    assert event.received_timestamp == datetime.fromisoformat(raw["received_timestamp"])
    wire = serialize_event(event.model_dump(mode="json"))
    assert json.loads(wire)["price"] == "81602.56000000"
    assert TradeEvent.model_validate_json(wire) == event
    assert BinanceTradeNormalizer().normalize(raw).event_id == event.event_id


@pytest.mark.parametrize("maker,side", [(True, "sell"), (False, "buy")])
def test_aggressor_side(raw, maker, side):
    raw["payload"]["m"] = maker
    assert BinanceTradeNormalizer().normalize(raw).side == side


def test_decimal_product_ignores_low_ambient_precision(raw):
    raw["payload"]["p"] = "123456789012345678901234567890.123456789"
    raw["payload"]["q"] = "0.000000000000000000123456789"
    with localcontext() as ctx:
        ctx.prec = 100
        expected = Decimal(raw["payload"]["p"]) * Decimal(raw["payload"]["q"])
    with localcontext() as ctx:
        ctx.prec = 6
        event = BinanceTradeNormalizer().normalize(raw)
    assert event.quote_quantity == expected


@pytest.mark.parametrize("field", ["s", "p", "q", "t", "T", "m", "e"])
def test_missing_required_field(raw, field):
    del raw["payload"][field]
    with pytest.raises(NormalizationError):
        BinanceTradeNormalizer().normalize(raw)


@pytest.mark.parametrize("field,value", [
    ("m", "true"), ("m", 1), ("p", 0.1), ("q", True), ("p", "NaN"),
    ("q", "0"), ("p", "-1"), ("p", "nonsense"), ("s", "ETHUSDT"),
    ("t", True), ("T", 1.5), ("T", 10**30), ("e", "aggTrade"),
])
def test_malformed_binance_fields(raw, field, value):
    raw["payload"][field] = value
    with pytest.raises(NormalizationError):
        BinanceTradeNormalizer().normalize(raw)


@pytest.mark.parametrize("changes", [
    {"payload": []}, {"exchange": "other"}, {"stream": "orderbook"},
    {"schema_version": "2.0"}, {"symbol": "ETHUSDT"},
])
def test_unsupported_envelopes(raw, changes):
    with pytest.raises(NormalizationError):
        BinanceTradeNormalizer().normalize(raw | changes)


def test_naive_received_timestamp_fails_pydantic(raw):
    raw["received_timestamp"] = "2026-09-19T14:32:33"
    with pytest.raises(ValidationError):
        BinanceTradeNormalizer().normalize(raw)


def test_dlq_event_preserves_public_input_and_redacts_credentials(raw):
    raw["payload"]["api_key"] = "do-not-publish"
    dlq = make_dlq_event(raw, record(b""), NormalizationError("Missing required Binance trade field"))
    assert dlq["source_topic"] == "market.raw.trades"
    assert dlq["source_offset"] == 12
    assert dlq["processor_name"] == "trade-normalizer-v1"
    assert dlq["error_type"] == "NormalizationError"
    assert dlq["original_raw_event"]["payload"]["p"] == raw["payload"]["p"]
    assert dlq["original_raw_event"]["payload"]["api_key"] == "[REDACTED]"
    assert "do-not-publish" not in json.dumps(dlq)
    assert "traceback" not in dlq
    assert datetime.fromisoformat(dlq["failed_timestamp"]).utcoffset().total_seconds() == 0


@pytest.mark.parametrize("invalid", [False, True])
async def test_commit_only_after_confirmed_output(raw, invalid):
    consumer, producer = AsyncMock(), AsyncMock()
    actions = []
    if invalid:
        del raw["payload"]["p"]

    async def publish(topic, key, event):
        actions.append("delivered")

    async def commit(message):
        assert actions == ["delivered"]
        actions.append("committed")

    producer.publish.side_effect = publish
    consumer.commit.side_effect = commit
    processor = TradeNormalizationProcessor(BinanceTradeNormalizer(), consumer, producer)
    await processor.process(record(json.dumps(raw).encode()))
    assert actions == ["delivered", "committed"]
    topic, key, event = producer.publish.call_args.args
    assert topic == (DLQ_TOPIC if invalid else CLEANED_TOPIC)
    if not invalid:
        assert key == "BTC-USDT"
        TradeEvent.model_validate(event)
    else:
        assert event["original_raw_event"] == raw


@pytest.mark.parametrize("invalid", [False, True])
async def test_failed_cleaned_or_dlq_delivery_does_not_commit(raw, invalid):
    consumer, producer = AsyncMock(), AsyncMock()
    producer.publish.side_effect = KafkaDeliveryError("unavailable")
    processor = TradeNormalizationProcessor(BinanceTradeNormalizer(), consumer, producer)
    with pytest.raises(KafkaDeliveryError):
        await processor.process(record(b"not-json" if invalid else json.dumps(raw).encode()))
    consumer.commit.assert_not_awaited()
    producer.publish.assert_awaited_once()


@pytest.mark.parametrize("data", [b"not-json", b"\xff", b"[]", None,
                                  b'{"x":NaN}', b'{"token":"private-token",broken'])
async def test_unparseable_record_goes_to_dlq(data):
    consumer, producer = AsyncMock(), AsyncMock()
    processor = TradeNormalizationProcessor(BinanceTradeNormalizer(), consumer, producer)
    await processor.process(record(data))
    topic, key, event = producer.publish.call_args.args
    assert topic == DLQ_TOPIC
    assert "private-token" not in serialize_event(event).decode()
    consumer.commit.assert_awaited_once()


async def test_pydantic_error_dlq_does_not_leak_input(raw):
    raw["received_timestamp"] = "secret-input-value"
    consumer, producer = AsyncMock(), AsyncMock()
    processor = TradeNormalizationProcessor(BinanceTradeNormalizer(), consumer, producer)
    await processor.process(record(json.dumps(raw).encode()))
    event = producer.publish.call_args.args[2]
    assert event["error_type"] == "ValidationError"
    assert "secret-input-value" not in event["error_message"]
    assert "received_timestamp" in event["error_message"]


async def test_consumer_manual_offsets_and_commit_errors(monkeypatch):
    client = MagicMock()
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(consumer_module, "Consumer", factory)
    consumer = consumer_module.ManualKafkaConsumer("broker:9092", "market.raw.trades", "trade-normalizer-v1")
    config = factory.call_args.args[0]
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["group.id"] == "trade-normalizer-v1"
    assert config["auto.offset.reset"] == "earliest"
    message = record(b"{}")
    client.consume.return_value = [message]
    assert await consumer.poll() is message
    client.commit.return_value = [MagicMock(error=None)]
    await consumer.commit(message)
    await consumer.flush_commits()
    assert client.commit.call_args.kwargs["asynchronous"] is False
    assert client.commit.call_args.kwargs["offsets"][0].offset == message.offset()+1
    consumer.tracker.observe(message)
    consumer.tracker.acknowledge(message)
    client.commit.return_value = [MagicMock(error=KafkaError(KafkaError._STATE))]
    with pytest.raises(KafkaException):
        await consumer.flush_commits()
    client.commit.return_value = [MagicMock(error=None)]
    await consumer.close()
    client.close.assert_called_once()

async def test_consumer_offset_reset_can_start_at_latest(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(consumer_module, "Consumer", MagicMock(return_value=client))
    consumer_module.ManualKafkaConsumer("broker:9092", "market.raw.derivatives", "validation", "latest")
    config = consumer_module.Consumer.call_args.args[0]
    assert config["auto.offset.reset"] == "latest"


async def test_commit_failure_stops_processing_after_output(raw):
    consumer, producer = AsyncMock(), AsyncMock()
    consumer.commit.side_effect = RuntimeError("commit failed")
    processor = TradeNormalizationProcessor(BinanceTradeNormalizer(), consumer, producer)
    with pytest.raises(RuntimeError, match="commit failed"):
        await processor.process(record(json.dumps(raw).encode()))
    producer.publish.assert_awaited_once()
