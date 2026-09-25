"""Offline depth ingestion tests; no Binance or Kafka access."""

import asyncio
import copy
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from integrity_engine.ingestion.binance import orderbook
from integrity_engine.kafka.producer import KafkaDeliveryError, serialize_event


@pytest.fixture
def settings(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "test-broker:9092")
    for name in ("BINANCE_WS_BASE_URL", "BINANCE_ORDERBOOK_SYMBOL", "BINANCE_ORDERBOOK_INTERVAL_MS"):
        monkeypatch.delenv(name, raising=False)
    return orderbook.BinanceOrderBookSettings()


@pytest.fixture
def payload():
    return {"e": "depthUpdate", "E": 1789828353037, "s": "BTCUSDT", "U": 100, "u": 105,
            "b": [["81500.01000000", "0.00000000"], ["81499.00000000", "1.20000000"]],
            "a": [["81501.00000000", "0.04000000"]], "future_field": {"keep": True}}


def test_configuration_defaults_and_overrides(settings, monkeypatch):
    assert settings.kafka_bootstrap_servers == "test-broker:9092"
    assert settings.stream_url == "wss://data-stream.binance.vision/ws/btcusdt@depth@100ms"
    monkeypatch.setenv("BINANCE_WS_BASE_URL", "wss://stream.binance.com:443/")
    monkeypatch.setenv("BINANCE_ORDERBOOK_SYMBOL", "btcusdt")
    monkeypatch.setenv("BINANCE_ORDERBOOK_INTERVAL_MS", "1000")
    assert orderbook.BinanceOrderBookSettings().stream_url == "wss://stream.binance.com:443/ws/btcusdt@depth"


@pytest.mark.parametrize("name,value", [
    ("KAFKA_BOOTSTRAP_SERVERS", " "), ("BINANCE_ORDERBOOK_SYMBOL", "ETHUSDT"),
    ("BINANCE_ORDERBOOK_INTERVAL_MS", "500"), ("BINANCE_WS_BASE_URL", "http://example.com"),
    ("BINANCE_WS_BASE_URL", "wss://user:secret@example.com"),
])
def test_invalid_configuration(settings, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        orderbook.BinanceOrderBookSettings()


def test_envelope_preserves_all_fields(payload):
    original = copy.deepcopy(payload)
    received = datetime.now(timezone.utc)
    event = orderbook.raw_orderbook_event(payload, "BTCUSDT", received)
    assert event == {"schema_version": "1.0", "exchange": "binance", "stream": "orderbook",
                     "symbol": "BTCUSDT", "received_timestamp": received.isoformat(), "payload": original}
    assert json.loads(serialize_event(event))["payload"] == original
    assert payload == original
    with pytest.raises(ValueError):
        orderbook.raw_orderbook_event(payload, "BTCUSDT", datetime(2026, 9, 19))


async def test_mock_websocket_timestamp_capture_and_publish(settings, payload, monkeypatch):
    publisher = AsyncMock()
    ingestor = orderbook.BinanceOrderBookIngestor(settings, publisher)
    times = {}

    class WebSocket:
        def __aiter__(self):
            return self.messages()

        async def messages(self):
            times["before"] = datetime.now(timezone.utc)
            yield json.dumps(payload)
            raise asyncio.CancelledError

    async def publish(topic, key, event):
        times["after"] = datetime.now(timezone.utc)

    publisher.publish.side_effect = publish
    connection = AsyncMock()
    connection.__aenter__.return_value = WebSocket()
    mocked_connect = MagicMock(return_value=connection)
    monkeypatch.setattr(orderbook, "connect", mocked_connect)
    with pytest.raises(asyncio.CancelledError):
        await ingestor.run()
    topic, key, event = publisher.publish.call_args.args
    assert (topic, key) == ("market.raw.orderbook", "BTCUSDT")
    assert event["payload"] == payload
    timestamp = datetime.fromisoformat(event["received_timestamp"])
    assert times["before"] <= timestamp <= times["after"]
    assert timestamp.utcoffset().total_seconds() == 0
    assert ingestor.received == 1
    assert mocked_connect.call_args.args == (settings.stream_url,)
    connection.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("field,value", [
    ("e", "trade"), ("s", "ETHUSDT"), ("E", None), ("U", True), ("u", 99),
    ("a", None), ("b", [[1.1, "1"]]), ("a", [["1"]]),
])
async def test_malformed_depth_logged_and_not_published(settings, payload, monkeypatch, field, value):
    payload[field] = value
    publisher, logger = AsyncMock(), MagicMock()
    monkeypatch.setattr(orderbook, "log", logger)
    ingestor = orderbook.BinanceOrderBookIngestor(settings, publisher)
    assert not await ingestor.handle_message(json.dumps(payload), datetime.now(timezone.utc))
    publisher.publish.assert_not_awaited()
    logger.error.assert_called_once()
    assert ingestor.malformed == 1


@pytest.mark.parametrize("message", ["not-json", "[]", b"\xff", '{"E":NaN}'])
async def test_malformed_json(settings, message):
    publisher = AsyncMock()
    ingestor = orderbook.BinanceOrderBookIngestor(settings, publisher)
    assert not await ingestor.handle_message(message, datetime.now(timezone.utc))
    publisher.publish.assert_not_awaited()


async def test_empty_sides_and_zero_quantity_preserved(settings, payload):
    publisher = AsyncMock()
    ingestor = orderbook.BinanceOrderBookIngestor(settings, publisher)
    payload["a"] = []
    assert await ingestor.handle_message(json.dumps(payload), datetime.now(timezone.utc))
    assert publisher.publish.call_args.args[2]["payload"] == payload


@pytest.mark.parametrize("normal_close", [True, False])
async def test_reconnect_backoff_capped(settings, monkeypatch, normal_close):
    connection = AsyncMock()
    if normal_close:
        connection.__aenter__.return_value.__aiter__.return_value = []
    else:
        connection.__aenter__.side_effect = OSError("offline")
    monkeypatch.setattr(orderbook, "connect", MagicMock(return_value=connection))
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 7:
            raise asyncio.CancelledError

    monkeypatch.setattr(orderbook.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await orderbook.BinanceOrderBookIngestor(settings, AsyncMock()).run()
    assert delays == [1, 2, 4, 8, 16, 30, 30]


async def test_kafka_failure_is_fatal(settings, payload):
    publisher = AsyncMock()
    publisher.publish.side_effect = KafkaDeliveryError("delivery failed")
    with pytest.raises(KafkaDeliveryError):
        await orderbook.BinanceOrderBookIngestor(settings, publisher).handle_message(
            json.dumps(payload), datetime.now(timezone.utc))
