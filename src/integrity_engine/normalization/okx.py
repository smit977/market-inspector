"""OKX v5 trade normalization; ``side`` is the taker/aggressor side."""
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from integrity_engine.normalization.base import NormalizationError, TradeNormalizer
from integrity_engine.schemas.symbols import market_mapping
from integrity_engine.schemas.trade import TradeEvent

def _d(v:Any,name:str)->Decimal:
    if isinstance(v,bool) or not isinstance(v,(str,int,Decimal)):raise NormalizationError(f"Invalid OKX {name}")
    try:x=Decimal(v)
    except DecimalException as e:raise NormalizationError(f"Invalid OKX {name}") from e
    if not x.is_finite() or x<=0:raise NormalizationError(f"Invalid OKX {name}")
    return x
class OKXTradeNormalizer(TradeNormalizer):
    def normalize(self,raw:dict[str,Any])->TradeEvent:
        if raw.get("exchange")!="okx" or raw.get("stream")!="trades":raise NormalizationError("Unsupported envelope: expected OKX trades")
        if raw.get("schema_version")!="1.0":raise NormalizationError("Unsupported raw envelope schema_version")
        env=raw.get("payload");rows=env.get("data") if isinstance(env,dict) else None
        if not isinstance(rows,list) or not rows or not isinstance(rows[0],dict):raise NormalizationError("Missing OKX trade data")
        p=rows[0]; req=("instId","tradeId","px","sz","side","ts")
        if any(k not in p for k in req):raise NormalizationError("Missing required OKX trade field")
        if p["instId"]!="BTC-USDT" or raw.get("symbol")!="BTC-USDT":raise NormalizationError("Expected OKX BTC-USDT")
        if p["side"] not in {"buy","sell"}:raise NormalizationError("Invalid OKX aggressor side")
        try: ms=int(p["ts"]); ts=datetime.fromtimestamp(ms/1000,tz=timezone.utc)
        except (TypeError,ValueError,OverflowError) as e:raise NormalizationError("Invalid OKX millisecond timestamp") from e
        price,qty=_d(p["px"],"price"),_d(p["sz"],"quantity")
        with localcontext() as c:c.prec=max(28,len(price.as_tuple().digits)+len(qty.as_tuple().digits));quote=price*qty
        m=market_mapping("okx","BTC-USDT");tid=str(p["tradeId"])
        return TradeEvent(schema_version="1.0",event_id=uuid5(NAMESPACE_URL,f"okx:spot:BTC-USDT:trade:{tid}"),event_type="trade",exchange="okx",symbol=m.symbol,exchange_timestamp=ts,received_timestamp=raw.get("received_timestamp"),sequence_id=tid,price=price,quantity=qty,side=p["side"],trade_id=tid,quote_quantity=quote,exchange_symbol=m.exchange_symbol,canonical_asset=m.canonical_asset,canonical_quote_family=m.canonical_quote_family,base_asset=m.base_asset,quote_asset=m.quote_asset,quote_family=m.canonical_quote_family,canonical_asset_pair=m.canonical_asset_pair)
