"""Offline Coinbase ingestion and normalization tests."""

import copy
import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from integrity_engine.ingestion.coinbase.trades import CoinbaseTradeIngestor, CoinbaseTradeSettings, raw_trade_event
from integrity_engine.normalization.base import NormalizationError
from integrity_engine.normalization.coinbase import CoinbaseTradeNormalizer
from integrity_engine.normalization.multi import MultiExchangeTradeNormalizer
from integrity_engine.kafka.producer import serialize_event


@pytest.fixture
def raw():
    return {
        "schema_version": "1.0", "exchange": "coinbase", "stream": "trades", "symbol": "BTC-USD",
        "received_timestamp": "2026-09-19T14:32:33.145781+00:00",
        "payload": {"type": "match", "trade_id": "12345", "sequence": 99,
                    "product_id": "BTC-USD", "maker_order_id": "m", "taker_order_id": "t",
                    "maker_user_id": "", "taker_user_id": "", "taker_profile_id": "",
                    "side": "buy", "size": "0.00100000", "price": "100000.12345678",
                    "time": "2026-09-19T14:32:33.123456Z"},
    }


def test_coinbase_normalization_precision_and_mapping(raw):
    original = copy.deepcopy(raw)
    event = CoinbaseTradeNormalizer().normalize(raw)
    assert raw == original
    assert event.exchange == "coinbase" and event.symbol == "BTC-USD"
    assert event.exchange_symbol == "BTC-USD"
    assert event.canonical_asset == "BTC" and event.canonical_quote_family == "USD"
    assert event.price == Decimal("100000.12345678")
    assert event.quantity == Decimal("0.00100000")
    assert event.quote_quantity == Decimal("100.0001234567800000")
    assert event.side == "buy" and event.trade_id == "12345" and event.sequence_id == 99
    assert event.exchange_timestamp == datetime(2026, 9, 19, 14, 32, 33, 123456, tzinfo=timezone.utc)
    assert event == CoinbaseTradeNormalizer().normalize(raw)


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_coinbase_side_is_aggressor_side(raw, side):
    raw["payload"]["side"] = side
    assert CoinbaseTradeNormalizer().normalize(raw).side == side


@pytest.mark.parametrize("field", ["product_id", "trade_id", "price", "size", "side", "time"])
def test_missing_coinbase_field_goes_to_error(raw, field):
    del raw["payload"][field]
    with pytest.raises(NormalizationError):
        CoinbaseTradeNormalizer().normalize(raw)


def test_multi_exchange_dispatch_and_serialization(raw):
    event = MultiExchangeTradeNormalizer().normalize(raw)
    wire = serialize_event(event.model_dump(mode="json"))
    assert json.loads(wire)["exchange"] == "coinbase"


def test_raw_envelope_preserves_payload_and_utc():
    payload = {"type": "match", "price": "1"}
    original = copy.deepcopy(payload)
    event = raw_trade_event(payload, "BTC-USD", datetime(2026, 9, 19, 17, tzinfo=timezone.utc))
    assert event["payload"] == original and event["received_timestamp"].endswith("+00:00")


def test_coinbase_configuration(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    settings = CoinbaseTradeSettings()
    assert settings.coinbase_trade_symbol == "BTC-USD"
    monkeypatch.setenv("COINBASE_TRADE_SYMBOL", "eth-usd")
    assert CoinbaseTradeSettings().coinbase_trade_symbol == "ETH-USD"


async def test_mocked_coinbase_message_publishes_raw(raw):
    settings = CoinbaseTradeSettings(kafka_bootstrap_servers="broker:9092")
    producer = AsyncMock()
    ingestor = CoinbaseTradeIngestor(settings, producer)
    assert await ingestor.handle_message(json.dumps(raw["payload"]), datetime.now(timezone.utc))
    producer.publish.assert_awaited_once()
    topic, key, envelope = producer.publish.call_args.args
    assert (topic, key) == ("market.raw.trades", "BTC-USD")
    assert envelope["payload"] == raw["payload"]


async def test_malformed_coinbase_message_not_published():
    settings = CoinbaseTradeSettings(kafka_bootstrap_servers="broker:9092")
    producer = AsyncMock()
    ingestor = CoinbaseTradeIngestor(settings, producer)
    assert not await ingestor.handle_message('{"type":"match"}', datetime.now(timezone.utc))
    producer.publish.assert_not_awaited()
