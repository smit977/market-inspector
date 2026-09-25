"""Validate normalized trades without network or exchange connections."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from integrity_engine.schemas.trade import TradeEvent


@pytest.fixture
def trade_data() -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "event_id": UUID("7d41aee8-e373-48a6-91a8-2fb2c55f7452"),
        "event_type": "trade",
        "exchange": "test",
        "symbol": "BTC-USDT",
        "exchange_timestamp": datetime(2026, 9, 19, 12, tzinfo=timezone.utc),
        "received_timestamp": datetime(2026, 9, 19, 12, 0, 1, tzinfo=timezone.utc),
        "price": Decimal("100000.1234567890123456789"),
        "quantity": Decimal("0.001"),
        "side": "buy",
    }


def test_valid_trade(trade_data):
    event = TradeEvent(**trade_data)
    assert event.price == trade_data["price"]
    assert isinstance(event.price, Decimal)
    assert isinstance(event.quantity, Decimal)
    assert event.event_type == "trade"
    assert event.trade_id is None
    assert event.quote_quantity is None


@pytest.mark.parametrize("field,value", [
    ("price", "-1"), ("price", "0"),
    ("quantity", "0"), ("quantity", "-0.001"),
    ("quote_quantity", "-1"),
])
def test_invalid_amounts(trade_data, field, value):
    with pytest.raises(ValidationError) as error:
        TradeEvent(**(trade_data | {field: value}))
    assert error.value.errors()[0]["loc"] == (field,)


@pytest.mark.parametrize("field", ["price", "quantity", "quote_quantity"])
@pytest.mark.parametrize("value", [0.1, True, "NaN", "Infinity", "-Infinity"])
def test_reject_inexact_or_nonfinite_amounts(trade_data, field, value):
    with pytest.raises(ValidationError):
        TradeEvent(**(trade_data | {field: value}))


@pytest.mark.parametrize("field", ["exchange_timestamp", "received_timestamp"])
def test_timestamps_normalized_to_utc(trade_data, field):
    original = datetime(2026, 9, 19, 17, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    event = TradeEvent(**(trade_data | {field: original}))
    timestamp = getattr(event, field)
    assert timestamp.tzinfo is timezone.utc
    assert timestamp == datetime(2026, 9, 19, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("field", ["exchange_timestamp", "received_timestamp"])
@pytest.mark.parametrize("value", [datetime(2026, 9, 19), "2026-09-19T12:00:00", 1789819200])
def test_reject_ambiguous_timestamps(trade_data, field, value):
    with pytest.raises(ValidationError):
        TradeEvent(**(trade_data | {field: value}))


def test_symbol_normalization(trade_data):
    assert TradeEvent(**(trade_data | {"symbol": " btc-usdt "})).symbol == "BTC-USDT"


def test_exchange_normalization(trade_data):
    assert TradeEvent(**(trade_data | {"exchange": " TeSt "})).exchange == "test"


@pytest.mark.parametrize("sequence_id", [None, "000123", 123])
def test_optional_sequence_id(trade_data, sequence_id):
    event = TradeEvent(**(trade_data | {"sequence_id": sequence_id}))
    assert event.sequence_id == sequence_id
    assert type(event.sequence_id) is type(sequence_id)


def test_sequence_id_defaults_to_none(trade_data):
    assert TradeEvent(**trade_data).sequence_id is None


@pytest.mark.parametrize("value", [True, 1.5])
def test_sequence_id_rejects_coercion(trade_data, value):
    with pytest.raises(ValidationError):
        TradeEvent(**(trade_data | {"sequence_id": value}))


def test_json_round_trip(trade_data):
    event = TradeEvent(**(trade_data | {
        "schema_version": "2.0.0-preview",
        "sequence_id": "000123",
        "trade_id": "trade-001",
        "quote_quantity": Decimal("100.0001234567890123456789"),
    }))
    serialized = event.model_dump_json()
    decoded = json.loads(serialized)
    assert decoded["price"] == str(event.price)
    assert decoded["quantity"] == str(event.quantity)
    assert decoded["quote_quantity"] == str(event.quote_quantity)
    assert decoded["exchange_timestamp"].endswith("Z")
    assert decoded["received_timestamp"].endswith("Z")
    restored = TradeEvent.model_validate_json(serialized)
    assert restored == event
    assert restored.schema_version == "2.0.0-preview"
    assert isinstance(restored.event_id, UUID)
    assert isinstance(restored.price, Decimal)


@pytest.mark.parametrize("side", ["buy", "sell", "unknown"])
def test_supported_sides(trade_data, side):
    assert TradeEvent(**(trade_data | {"side": side})).side == side


@pytest.mark.parametrize("changes", [
    {"event_type": "ticker"}, {"side": "bid"}, {"exchange": " "},
    {"symbol": " "}, {"schema_version": " "}, {"exchange_specific": "value"},
])
def test_reject_invalid_metadata(trade_data, changes):
    with pytest.raises(ValidationError):
        TradeEvent(**(trade_data | changes))


def test_validated_event_is_immutable(trade_data):
    event = TradeEvent(**trade_data)
    with pytest.raises(ValidationError):
        event.price = Decimal("-1")
