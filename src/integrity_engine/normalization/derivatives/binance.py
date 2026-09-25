from integrity_engine.normalization.base import NormalizationError
from integrity_engine.normalization.derivatives.common import dec, metadata, quote, ts
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent

def normalize_binance(raw):
    p=raw.get("payload"); received=raw.get("received_timestamp"); symbol=raw.get("exchange_symbol", raw.get("symbol", "BTCUSDT"))
    if not isinstance(p,dict): raise NormalizationError("Binance derivatives payload must be an object")
    p = p.get("data", p)
    kind=raw.get("event_type")
    if kind in {"perpetual_trade", "trade"}:
        price=dec(p.get("p"),"price"); qty=dec(p.get("q"),"quantity"); base=metadata("binance",symbol,"USDT",received,"perpetual_trade",ts(p.get("T")),p.get("a",p.get("t")))
        return PerpetualTrade(**base,price=price,quantity=qty,quote_quantity=quote(price,qty),side="sell" if p.get("m") is True else "buy" if p.get("m") is False else "unknown",trade_id=str(p.get("a",p.get("t"))),quantity_unit="base")
    if kind == "funding_rate":
        stamp=ts(p.get("fundingTime",p.get("T",p.get("time")))); return FundingRate(**metadata("binance",symbol,"USDT",received,kind,stamp,f"funding:{stamp.isoformat()}"), funding_rate=dec(p.get("r",p.get("lastFundingRate")),"funding",False,signed=True), funding_timestamp=stamp, next_funding_timestamp=ts(p["nextFundingTime"]) if p.get("nextFundingTime") else None)
    if kind == "open_interest":
        stamp=ts(p.get("time",p.get("E"))); return OpenInterest(**metadata("binance",symbol,"USDT",received,kind,stamp,f"oi:{stamp.isoformat()}"), open_interest=dec(p.get("openInterest"),"open_interest",False), open_interest_value=dec(p["openInterestValue"],"open_interest_value",False) if p.get("openInterestValue") else None, quantity_unit="base_as_reported", value_unit="USDT" if p.get("openInterestValue") else None)
    if kind == "liquidation":
        o=p.get("o",p); stamp=ts(o.get("T",p.get("E"))); price=dec(o.get("p"),"price"); qty=dec(o.get("q"),"quantity"); return LiquidationEvent(**metadata("binance",symbol,"USDT",received,kind,stamp,f"liq:{o.get('i',stamp.isoformat())}"),side="sell" if o.get("S")=="SELL" else "buy" if o.get("S")=="BUY" else "unknown",price=price,quantity=qty,quote_value=quote(price,qty),quantity_unit="base")
    raise NormalizationError("unsupported Binance derivatives event")
