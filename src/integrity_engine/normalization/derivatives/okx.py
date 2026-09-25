from integrity_engine.normalization.base import NormalizationError
from integrity_engine.normalization.derivatives.common import dec, metadata, quote, ts
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent

def normalize_okx(raw):
    p=raw.get("payload"); symbol=raw.get("exchange_symbol",raw.get("symbol","BTC-USDT-SWAP"));received=raw.get("received_timestamp");row=(p.get("data",[{}])[0] if isinstance(p,dict) and isinstance(p.get("data"),list) else p.get("data",p) if isinstance(p,dict) else p)
    if not isinstance(row,dict):raise NormalizationError("OKX derivatives payload must be an object")
    kind=raw.get("event_type")
    if kind in {"perpetual_trade","trade"}:
        price=dec(row.get("px"),"price");qty=dec(row.get("sz"),"quantity");return PerpetualTrade(**metadata("okx",symbol,"USDT",received,"perpetual_trade",ts(row.get("ts")),row.get("tradeId")),price=price,quantity=qty,quote_quantity=None,comparable=False,value_unit=None,side=row.get("side") if row.get("side") in {"buy","sell"} else "unknown",trade_id=str(row.get("tradeId")),quantity_unit="contracts_as_reported")
    if kind=="funding_rate":
        stamp=ts(row.get("fundingTime",row.get("ts",p.get("ts"))));return FundingRate(**metadata("okx",symbol,"USDT",received,kind,stamp,f"funding:{stamp.isoformat()}"),funding_rate=dec(row.get("fundingRate"),"funding",False,signed=True),funding_timestamp=stamp,next_funding_timestamp=ts(row["nextFundingTime"]) if row.get("nextFundingTime") else None)
    if kind=="open_interest":
        stamp=ts(row.get("ts",p.get("ts")));return OpenInterest(**metadata("okx",symbol,"USDT",received,kind,stamp,f"oi:{stamp.isoformat()}"),open_interest=dec(row.get("oi"),"open_interest",False),open_interest_value=dec(row["oiCcy"],"open_interest_value",False) if row.get("oiCcy") else None,quantity_unit="contracts_as_reported",value_unit="BTC" if row.get("oiCcy") else None)
    if kind=="liquidation":
        detail=(row.get("details") or [{}])[0] if isinstance(row.get("details"),list) else row
        stamp=ts(detail.get("ts",row.get("ts")));price=dec(detail.get("bkPx",detail.get("px")),"price");qty=dec(detail.get("sz"),"quantity");return LiquidationEvent(**metadata("okx",symbol,"USDT",received,kind,stamp,f"liq:{detail.get('ordId',stamp.isoformat())}"),side=detail.get("side") if detail.get("side") in {"buy","sell"} else "unknown",price=price,quantity=qty,quote_value=None,comparable=False,value_unit=None,quantity_unit="contracts_as_reported")
    raise NormalizationError("unsupported OKX derivatives event")
