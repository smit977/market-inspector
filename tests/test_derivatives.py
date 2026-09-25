from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from integrity_engine.normalization.derivatives import normalize_derivative
from integrity_engine.features.derivatives import DerivativesFeatureEngine
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent
from integrity_engine.schemas.derivatives_health import DerivativesSourceHealth

R=datetime.fromtimestamp(1789850000,tz=timezone.utc).isoformat()
def raw(exchange,kind,payload,symbol):return {"schema_version":"1.0","exchange":exchange,"stream":"derivatives","event_type":kind,"exchange_symbol":symbol,"received_timestamp":R,"payload":payload}
def test_exchange_trade_normalizers_preserve_decimal_and_units():
    cases=[raw("binance","perpetual_trade",{"p":"100","q":"2","T":1789850000000,"a":1,"m":False},"BTCUSDT"),raw("bybit","perpetual_trade",{"data":[{"p":"100","v":"2","T":1789850000000,"i":"2","S":"Sell"}]},"BTCUSDT"),raw("okx","perpetual_trade",{"data":[{"px":"100","sz":"2","ts":"1789850000000","tradeId":"3","side":"buy"}]},"BTC-USDT-SWAP")]
    events=[normalize_derivative(c) for c in cases]
    assert all(e.price==Decimal("100") and e.quantity==Decimal("2") for e in events)
    assert [e.side for e in events]==["buy","sell","buy"]
    assert all(e.market_type=="perpetual" for e in events)
def test_funding_oi_liquidation_and_feature_aggregation():
    engine=DerivativesFeatureEngine(); trade=normalize_derivative(raw("binance","perpetual_trade",{"p":"100","q":"2","T":1789850000000,"a":1,"m":False},"BTCUSDT"));engine.add(trade)
    oi=normalize_derivative(raw("binance","open_interest",{"openInterest":"10","time":1789850000000},"BTCUSDT"));engine.add(oi)
    funding=normalize_derivative(raw("binance","funding_rate",{"lastFundingRate":"0.0001","fundingTime":1789850000000},"BTCUSDT"));engine.add(funding)
    liq=normalize_derivative(raw("binance","liquidation",{"o":{"T":1789850000000,"p":"100","q":"1","S":"SELL","i":"x"}},"BTCUSDT"));engine.add(liq)
    metric=engine.calculate("10s",datetime.fromtimestamp(1789850000,tz=timezone.utc))
    assert metric.perpetual_quote_volume==Decimal("200") and metric.open_interest==Decimal("10")
    assert metric.long_liquidation_volume==Decimal("1") and metric.liquidation_count==1
    assert metric.funding_rate==Decimal("0.0001")
def test_serialization():
    event=normalize_derivative(raw("bybit","open_interest",{"data":{"openInterest":"12","timestamp":"1789850000000"}},"BTCUSDT"))
    assert event.model_validate_json(event.model_dump_json())==event

def test_cross_venue_json_decimal_values_support_basis_calculation():
    engine=DerivativesFeatureEngine()
    engine.add(normalize_derivative(raw("binance","perpetual_trade",{"p":"100","q":"2","T":1789850000000,"a":1,"m":False},"BTCUSDT")))
    engine.add_spot_metric({"calculated_at":R,"window":"10s","active_venues":["binance"],"median_price":"80","median_return":"0.01","venue_quote_volumes":{"binance":"1000"}})
    metric=engine.calculate("10s",datetime.fromtimestamp(1789850000,tz=timezone.utc))
    assert metric.basis == Decimal("20")
    assert metric.basis_bps == Decimal("2500")
    assert metric.perpetual_to_spot_volume_ratio == Decimal("0.2")
    assert metric.spot_perpetual_return_difference == Decimal("-0.01")

def test_restricted_source_health_and_missing_values_are_explicit():
    health=DerivativesSourceHealth(exchange="binance",feed_type="rest_derivatives",status="restricted",consecutive_errors=1,last_error_type="HTTP451")
    assert health.status=="restricted"
    engine=DerivativesFeatureEngine(); engine.add(normalize_derivative(raw("bybit","perpetual_trade",{"data":[{"p":"100","v":"1","T":1789850000000,"i":"1","S":"Buy"}]},"BTCUSDT")))
    metric=engine.calculate("10s",datetime.fromtimestamp(1789850000,tz=timezone.utc))
    assert metric.open_interest is None and metric.funding_rate is None
    assert "missing_open_interest" in metric.data_quality_flags
    assert "missing_funding" in metric.data_quality_flags
