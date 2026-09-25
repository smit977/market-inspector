from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from integrity_engine.features.cross_venue import CrossVenueFeatureEngine
from integrity_engine.schemas.cross_venue import CrossVenueMetrics
from integrity_engine.schemas.trade import TradeEvent

T = datetime(2026, 9, 19, 20, 0, tzinfo=timezone.utc)

def trade(exchange: str, price: str, seconds: int = 0, qty: str = "1") -> TradeEvent:
    symbols = {"binance": ("BTC-USDT", "BTCUSDT", "USDT"), "coinbase": ("BTC-USD", "BTC-USD", "USD"),
               "kraken": ("BTC-USD", "BTC/USD", "USD"), "okx": ("BTC-USDT", "BTC-USDT", "USDT"), "bybit": ("BTC-USDT", "BTCUSDT", "USDT")}
    symbol, exchange_symbol, quote = symbols[exchange]
    return TradeEvent(schema_version="1.0", event_id=uuid4(), exchange=exchange, symbol=symbol,
        exchange_timestamp=T + timedelta(seconds=seconds), received_timestamp=T + timedelta(seconds=seconds),
        sequence_id=seconds, price=Decimal(price), quantity=Decimal(qty), side="buy", trade_id=str(seconds),
        quote_quantity=Decimal(price) * Decimal(qty), exchange_symbol=exchange_symbol,
        canonical_asset="BTC", canonical_quote_family="USD", base_asset="BTC", quote_asset=quote,
        quote_family="USD", canonical_asset_pair="BTC-USD")

def populated_engine():
    engine = CrossVenueFeatureEngine()
    for venue, price in zip(("binance", "coinbase", "kraken", "okx", "bybit"), ("100", "101", "99", "100", "100")):
        engine.add_trade(trade(venue, price, 0)); engine.add_trade(trade(venue, str(Decimal(price) + 1), 5))
    return engine

def test_reference_price_dispersion_and_deviation():
    metric = populated_engine().calculate("10s", T + timedelta(seconds=5))
    assert metric.median_price == Decimal("101")
    assert metric.min_price == Decimal("100") and metric.max_price == Decimal("102")
    assert metric.dispersion == Decimal("2")
    assert metric.venue_prices["coinbase"] == Decimal("102")
    assert metric.venue_deviation_bps["coinbase"] == Decimal("99.00990099009900990099009901")

def test_confirmation_and_returns():
    metric = populated_engine().calculate("10s", T + timedelta(seconds=5))
    assert metric.active_venues == ["binance", "coinbase", "kraken", "okx", "bybit"]
    assert metric.confirmation_ratio == Decimal("1")
    assert metric.confirmation_state == "broad"
    assert metric.up_venue_count == 5

def test_volume_share_and_hhi():
    metric = populated_engine().calculate("10s", T + timedelta(seconds=5))
    assert metric.largest_venue_volume_share is not None
    assert metric.volume_hhi is not None and Decimal("0") < metric.volume_hhi <= Decimal("1")
    assert sum(metric.venue_volume_shares.values()) == Decimal("1")

def test_stale_and_minimum_venue_quality():
    engine = CrossVenueFeatureEngine(stale_after_ms=1000, minimum_active_venues=3)
    for venue in ("binance", "coinbase"):
        engine.add_trade(trade(venue, "100"))
    metric = engine.calculate("10s", T + timedelta(seconds=3))
    assert metric.confirmation_state == "insufficient_data"
    assert "insufficient_active_venues" in metric.data_quality_flags
    assert "stale_venue_data" in metric.data_quality_flags

def test_bounded_state_and_serialization():
    engine = populated_engine()
    for i in range(100): engine.add_trade(trade("binance", "100", i + 10))
    assert len(engine.trades["binance"]) <= 61
    metric = engine.calculate("1m", T + timedelta(seconds=109))
    assert CrossVenueMetrics.model_validate_json(metric.model_dump_json()) == metric
