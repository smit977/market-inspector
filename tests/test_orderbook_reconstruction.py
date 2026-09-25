"""Unit tests for Binance snapshot reconstruction without live services."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from integrity_engine.orderbook.binance import (
    BinanceOrderBookReconstructor, LocalOrderBook, OrderBookSettings, SequenceGap,
    build_snapshot, parse_depth_event,
)
from integrity_engine.schemas.orderbook import OrderBookLevel, OrderBookSnapshot


def snap(last=100):
    return {"lastUpdateId": last, "bids": [["100", "2"], ["99", "1"]],
            "asks": [["101", "3"], ["102", "4"]]}


def raw(first, final, bids=None, asks=None, event_time=1700000000000):
    return {"schema_version": "1.0", "exchange": "binance", "stream": "orderbook",
            "symbol": "BTCUSDT", "received_timestamp": "2026-09-19T00:00:00+00:00",
            "payload": {"e": "depthUpdate", "E": event_time, "s": "BTCUSDT",
                        "U": first, "u": final, "b": bids or [], "a": asks or []}}


def event(first, final, bids=None, asks=None):
    return parse_depth_event(raw(first, final, bids, asks))


def test_rest_snapshot_initialization_and_levels():
    book = LocalOrderBook(); book.initialize(snap(100))
    assert book.last_update_id == 100
    assert book.bids == {Decimal("100"): Decimal("2"), Decimal("99"): Decimal("1")}
    assert book.asks == {Decimal("101"): Decimal("3"), Decimal("102"): Decimal("4")}
    assert not book.synchronized


def test_bid_and_ask_insert_update_delete():
    book = LocalOrderBook(); book.initialize(snap(100)); assert book.bridge(event(101, 101, [["98", "4"]], [["103", "5"]]))
    book.apply(event(102, 102, [["100", "5"]], [["101", "0"]]))
    assert book.bids[Decimal("100")] == Decimal("5")
    assert Decimal("101") not in book.asks
    book.apply(event(103, 103, [["98", "0"]], [["100.5", "1"]]))
    assert Decimal("98") not in book.bids and book.asks[Decimal("100.5")] == Decimal("1")


def test_bridge_stale_and_correct_event():
    book = LocalOrderBook(); book.initialize(snap(100))
    assert not book.bridge(event(90, 100))
    assert book.bridge(event(99, 101, [["98", "1"]]))
    assert book.last_update_id == 101 and book.synchronized


def test_gap_detection():
    book = LocalOrderBook(); book.initialize(snap(100)); assert book.bridge(event(101, 101))
    with pytest.raises(SequenceGap): book.apply(event(103, 103))
    assert not book.synchronized


def test_top_sorting_and_best_values():
    book = LocalOrderBook(); book.initialize({"lastUpdateId": 1,
        "bids": [["99", "1"], ["101", "2"], ["100", "3"]],
        "asks": [["104", "1"], ["102", "4"], ["103", "2"]]})
    book.bridge(event(2, 2)); bids, asks = book.top_levels(2)
    assert [x.price for x in bids] == [Decimal("101"), Decimal("100")]
    assert [x.price for x in asks] == [Decimal("102"), Decimal("103")]
    snapshot = build_snapshot(book, event(3, 3), datetime.now(timezone.utc), 2)
    assert snapshot.best_bid_price == Decimal("101")
    assert snapshot.best_ask_price == Decimal("102")
    assert snapshot.spread == Decimal("1")
    assert snapshot.mid_price == Decimal("101.5")


def test_decimal_precision():
    book = LocalOrderBook(); book.initialize({"lastUpdateId": 1,
        "bids": [["100.123456789012345678901", "1.000000000000000000001"]],
        "asks": [["100.223456789012345678901", "2"]]})
    book.bridge(event(2, 2)); snapshot = build_snapshot(book, event(2, 2), datetime.now(timezone.utc), 20)
    assert snapshot.spread == Decimal("0.1")
    assert snapshot.bids[0].quantity == Decimal("1.000000000000000000001")


def test_snapshot_model_rejects_crossed_duplicate_unsorted_and_zero():
    base = dict(schema_version="1.0", event_id="11111111-1111-4111-8111-111111111111",
        event_type="orderbook", exchange="binance", symbol="BTC-USDT",
        exchange_timestamp="2026-09-19T00:00:00Z", received_timestamp="2026-09-19T00:00:01Z",
        sequence_id=1, best_bid_price="100", best_bid_quantity="1", best_ask_price="101",
        best_ask_quantity="1", spread="1", mid_price="100.5",
        bids=[{"price": "100", "quantity": "1"}], asks=[{"price": "101", "quantity": "1"}])
    assert OrderBookSnapshot(**base).spread == Decimal("1")
    for change in ({"best_ask_price": "99", "spread": "-1", "mid_price": "99.5"},
                   {"bids": [{"price": "100", "quantity": "1"}, {"price": "100", "quantity": "1"}]},
                   {"bids": [{"price": "99", "quantity": "1"}, {"price": "100", "quantity": "1"}]},
                   {"bids": [{"price": "100", "quantity": "0"}]}):
        with pytest.raises(ValidationError): OrderBookSnapshot(**(base | change))


def test_snapshot_serialization_round_trip():
    level = OrderBookLevel(price=Decimal("100.00"), quantity=Decimal("2.00"))
    assert level.model_dump_json() == '{"price":"100.00","quantity":"2.00"}'


def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    settings = OrderBookSettings()
    assert settings.binance_orderbook_snapshot_limit == 1000
    assert settings.orderbook_publish_depth == 20
    assert settings.orderbook_publish_interval_ms == 250
    assert settings.snapshot_url == "https://data-api.binance.vision/api/v3/depth"


@pytest.mark.asyncio
async def test_snapshot_fetch_and_resynchronization(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    settings = OrderBookSettings()
    response = MagicMock(); response.raise_for_status.return_value = None
    response.json.side_effect = [snap(200), snap(202)]
    client = AsyncMock(); client.get.return_value = response
    consumer, producer = AsyncMock(), AsyncMock()
    reconstructor = BinanceOrderBookReconstructor(settings, consumer, producer, client)
    await reconstructor.bootstrap(); assert reconstructor.snapshots_fetched == 1
    await reconstructor.process_raw(raw(201, 201, [["100", "4"]]), datetime.now(timezone.utc))
    assert reconstructor.deltas_applied == 1
    await reconstructor.process_raw(raw(203, 203), datetime.now(timezone.utc))
    assert reconstructor.gaps_detected == 1
    assert reconstructor.snapshots_fetched == 2
    assert reconstructor.book.synchronized


@pytest.mark.asyncio
async def test_publish_top_n_and_stale(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("ORDERBOOK_PUBLISH_DEPTH", "1")
    settings = OrderBookSettings(); consumer, producer = AsyncMock(), AsyncMock()
    reconstructor = BinanceOrderBookReconstructor(settings, consumer, producer)
    reconstructor.book.initialize(snap(10)); reconstructor.book.bridge(event(11, 11))
    await reconstructor.process_raw(raw(10, 10), datetime.now(timezone.utc))
    assert reconstructor.stale_ignored == 1
    await reconstructor.process_raw(raw(12, 12, [["100.5", "2"]], [["101.5", "1"]]), datetime.now(timezone.utc))
    assert producer.publish.await_count == 1
    published = producer.publish.call_args.args[2]
    assert len(published["bids"]) == 1 and len(published["asks"]) == 1
