"""Offline tests for raw ingestion and Kafka delivery, with network clients mocked."""

import asyncio
import copy
import json
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from integrity_engine.ingestion.binance import trades
from integrity_engine.kafka import producer as kafka


@pytest.fixture
def payload():
    return {"e": "trade", "E": 1700000000000, "s": "BTCUSDT", "t": 123,
            "p": "100000.00000000", "q": "0.00100000", "T": 1700000000000,
            "m": True, "M": True, "future_field": {"value": [1, "unchanged"]}}


@pytest.fixture
def settings(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example:9092")
    monkeypatch.delenv("BINANCE_WS_BASE_URL", raising=False)
    monkeypatch.delenv("BINANCE_TRADE_SYMBOL", raising=False)
    return trades.BinanceTradeSettings()


def test_configuration_defaults(settings):
    assert settings.kafka_bootstrap_servers == "broker.example:9092"
    assert settings.stream_url == "wss://data-stream.binance.vision/ws/btcusdt@trade"


def test_configuration_environment(settings, monkeypatch):
    monkeypatch.setenv("BINANCE_WS_BASE_URL", "wss://stream.binance.com:443/")
    monkeypatch.setenv("BINANCE_TRADE_SYMBOL", "btcusdt")
    configured = trades.BinanceTradeSettings()
    assert configured.stream_url == "wss://stream.binance.com:443/ws/btcusdt@trade"


@pytest.mark.parametrize("env,value", [
    ("BINANCE_TRADE_SYMBOL", "ETHUSDT"), ("BINANCE_WS_BASE_URL", "http://example.com"),
    ("BINANCE_WS_BASE_URL", "wss://user:secret@example.com"),
    ("KAFKA_BOOTSTRAP_SERVERS", " "),
])
def test_configuration_invalid(settings, monkeypatch, env, value):
    monkeypatch.setenv(env, value)
    with pytest.raises(ValidationError):
        trades.BinanceTradeSettings()


def test_configuration_requires_kafka(settings, monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS")
    with pytest.raises(ValidationError):
        trades.BinanceTradeSettings()


def test_raw_envelope_and_serialization(payload):
    original = copy.deepcopy(payload)
    received = datetime(2026, 9, 19, 17, tzinfo=timezone(timedelta(hours=5)))
    event = trades.raw_trade_event(payload, "BTCUSDT", received)
    assert event == {
        "schema_version": "1.0", "exchange": "binance", "stream": "trades",
        "symbol": "BTCUSDT", "received_timestamp": "2026-09-19T12:00:00+00:00",
        "payload": original,
    }
    assert payload == original
    assert json.loads(kafka.serialize_event(event)) == event


def test_reject_naive_received_timestamp(payload):
    with pytest.raises(ValueError):
        trades.raw_trade_event(payload, "BTCUSDT", datetime(2026, 9, 19))


async def test_mocked_websocket_timestamp_and_message(settings, payload, monkeypatch):
    publisher = AsyncMock()
    ingestor = trades.BinanceTradeIngestor(settings, publisher)
    seen = {}

    class WebSocket:
        def __aiter__(self):
            return self.messages()

        async def messages(self):
            seen["before"] = datetime.now(timezone.utc)
            yield json.dumps(payload)
            raise asyncio.CancelledError

    connection = AsyncMock()
    connection.__aenter__.return_value = WebSocket()
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(trades, "connect", connect)

    async def publish(topic, key, event):
        seen["after"] = datetime.now(timezone.utc)
        seen["event"] = event

    publisher.publish.side_effect = publish
    with pytest.raises(asyncio.CancelledError):
        await ingestor.run()
    publisher.publish.assert_awaited_once()
    topic, key, event = publisher.publish.call_args.args
    assert (topic, key) == ("market.raw.trades", "BTCUSDT")
    assert event["payload"] == payload
    received = datetime.fromisoformat(event["received_timestamp"])
    assert received.utcoffset() == timedelta(0)
    assert seen["before"] <= received <= seen["after"]
    assert connect.call_args.args == (settings.stream_url,)
    connection.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("message", ["not json", "[]", '{"e":"trade","s":"ETHUSDT"}',
                                     '{"e":"depthUpdate","s":"BTCUSDT"}'])
async def test_unexpected_messages_are_not_published(settings, message):
    publisher = AsyncMock()
    ingestor = trades.BinanceTradeIngestor(settings, publisher)
    assert not await ingestor.handle_message(message, datetime.now(timezone.utc))
    publisher.publish.assert_not_awaited()


async def test_exponential_backoff_is_capped(settings, monkeypatch):
    connection = AsyncMock()
    connection.__aenter__.side_effect = OSError("offline")
    monkeypatch.setattr(trades, "connect", MagicMock(return_value=connection))
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 7:
            raise asyncio.CancelledError

    monkeypatch.setattr(trades.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await trades.BinanceTradeIngestor(settings, AsyncMock()).run()
    assert delays == [1, 2, 4, 8, 16, 30, 30]


async def test_kafka_failure_propagates(settings, payload):
    publisher = AsyncMock()
    publisher.publish.side_effect = kafka.KafkaDeliveryError("broker unavailable")
    with pytest.raises(kafka.KafkaDeliveryError):
        await trades.BinanceTradeIngestor(settings, publisher).handle_message(
            json.dumps(payload), datetime.now(timezone.utc))


@pytest.mark.parametrize("failure", [None, "delivery", "timeout", "queue"])
async def test_kafka_delivery_and_worker_thread(monkeypatch, payload, failure):
    main_thread = threading.get_ident()
    client = MagicMock()
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(kafka, "Producer", factory)
    pending = {}

    def produce(topic, **kwargs):
        assert threading.get_ident() != main_thread
        if failure == "queue":
            raise BufferError("queue full")
        pending.update(kwargs)

    def flush(timeout):
        if failure == "timeout":
            return 1
        if "on_delivery" in pending:
            callback = pending.pop("on_delivery")
            from confluent_kafka import KafkaError
            error = KafkaError(KafkaError._MSG_TIMED_OUT) if failure == "delivery" else None
            callback(error, MagicMock())
        return 0

    client.produce.side_effect = produce
    client.poll.side_effect = flush
    client.flush.return_value = 0
    publisher = kafka.JsonKafkaProducer("configured-broker:9092")
    publisher.delivery_timeout = .02
    if failure:
        with pytest.raises(kafka.KafkaDeliveryError):
            await publisher.publish("market.raw.trades", "BTCUSDT", payload)
        assert publisher.delivered == 0
    else:
        await publisher.publish("market.raw.trades", "BTCUSDT", payload)
        assert pending["key"] == b"BTCUSDT"
        assert json.loads(pending["value"]) == payload
        assert publisher.delivered == 1
        await publisher.close()
    assert factory.call_args.args[0]["bootstrap.servers"] == "configured-broker:9092"
