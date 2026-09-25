from integrity_engine.normalization.base import NormalizationError
from integrity_engine.normalization.derivatives.common import dec, metadata, quote, ts
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent

def normalize_bybit(raw):
    p=raw.get("payload"); symbol=raw.get("exchange_symbol",raw.get("symbol","BTCUSDT")); received=raw.get("received_timestamp"); row=(p.get("data",[{}])[0] if isinstance(p,dict) and isinstance(p.get("data"),list) else p.get("data",p) if isinstance(p,dict) else p)
    if not isinstance(row,dict): raise NormalizationError("Bybit derivatives payload must be an object")
    kind=raw.get("event_type")
    if kind in {"perpetual_trade","trade"}:
        price=dec(row.get("p"),"price");qty=dec(row.get("v"),"quantity");return PerpetualTrade(**metadata("bybit",symbol,"USDT",received,"perpetual_trade",ts(row.get("T")),row.get("i")),price=price,quantity=qty,quote_quantity=quote(price,qty),side="buy" if row.get("S")=="Buy" else "sell" if row.get("S")=="Sell" else "unknown",trade_id=str(row.get("i")),quantity_unit="base")
    if kind=="funding_rate":
        stamp=ts(row.get("fundingRateTimestamp",row.get("ts",p.get("ts",p.get("timestamp")))));return FundingRate(**metadata("bybit",symbol,"USDT",received,kind,stamp,f"funding:{stamp.isoformat()}"),funding_rate=dec(row.get("fundingRate"),"funding",False,signed=True),funding_timestamp=stamp,next_funding_timestamp=ts(row["nextFundingTime"]) if row.get("nextFundingTime") else None)
    if kind=="open_interest":
        stamp=ts(row.get("timestamp",row.get("ts")));return OpenInterest(**metadata("bybit",symbol,"USDT",received,kind,stamp,f"oi:{stamp.isoformat()}"),open_interest=dec(row.get("openInterest"),"open_interest",False),quantity_unit="BTC",value_unit=None)
    if kind=="liquidation":
        stamp=ts(row.get("T"));price=dec(row.get("p",row.get("P")),"price");qty=dec(row.get("v"),"quantity");return LiquidationEvent(**metadata("bybit",symbol,"USDT",received,kind,stamp,f"liq:{row.get('id',stamp.isoformat())}"),side="sell" if row.get("S")=="Buy" else "buy" if row.get("S")=="Sell" else "unknown",price=price,quantity=qty,quote_value=quote(price,qty),quantity_unit="base")
    raise NormalizationError("unsupported Bybit derivatives event")
