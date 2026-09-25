"""Kraken v2 trade normalization; ``side`` is the taker/aggressor side."""
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from integrity_engine.normalization.base import NormalizationError, TradeNormalizer
from integrity_engine.schemas.symbols import market_mapping
from integrity_engine.schemas.trade import TradeEvent

def _d(v: Any, name: str) -> Decimal:
    # Kraken v2 may encode numeric fields as JSON numbers. Convert their
    # shortest textual representation to Decimal; no binary arithmetic occurs.
    if isinstance(v, bool) or not isinstance(v, (str, int, float, Decimal)): raise NormalizationError(f"Invalid Kraken {name}")
    try: x=Decimal(str(v)) if isinstance(v, float) else Decimal(v)
    except (TypeError, DecimalException) as e: raise NormalizationError(f"Invalid Kraken {name}") from e
    if not x.is_finite() or x<=0: raise NormalizationError(f"Invalid Kraken {name}")
    return x

class KrakenTradeNormalizer(TradeNormalizer):
    def normalize(self, raw: dict[str, Any]) -> TradeEvent:
        if raw.get("exchange")!="kraken" or raw.get("stream")!="trades": raise NormalizationError("Unsupported envelope: expected Kraken trades")
        if raw.get("schema_version")!="1.0": raise NormalizationError("Unsupported raw envelope schema_version")
        envelope=raw.get("payload"); rows=envelope.get("data") if isinstance(envelope,dict) else None
        if not isinstance(rows,list) or not rows or not isinstance(rows[0],dict): raise NormalizationError("Missing Kraken trade data")
        p=rows[0]; required=("symbol","price","qty","side","timestamp","trade_id")
        if any(k not in p for k in required): raise NormalizationError("Missing required Kraken trade field")
        if p["symbol"]!="BTC/USD" or raw.get("symbol")!="BTC/USD": raise NormalizationError("Expected Kraken BTC/USD")
        if p["side"] not in {"buy","sell"}: raise NormalizationError("Invalid Kraken aggressor side")
        try: ts=datetime.fromisoformat(str(p["timestamp"]).replace("Z","+00:00"))
        except (TypeError,ValueError) as e: raise NormalizationError("Invalid Kraken timestamp") from e
        if ts.tzinfo is None or ts.utcoffset() is None: raise NormalizationError("Kraken timestamp must be timezone-aware")
        price,qty=_d(p["price"],"price"),_d(p["qty"],"quantity")
        with localcontext() as c: c.prec=max(28,len(price.as_tuple().digits)+len(qty.as_tuple().digits)); quote=price*qty
        m=market_mapping("kraken","BTC/USD"); tid=str(p["trade_id"])
        return TradeEvent(schema_version="1.0", event_id=uuid5(NAMESPACE_URL,f"kraken:spot:BTC/USD:trade:{tid}"), event_type="trade", exchange="kraken", symbol=m.symbol, exchange_timestamp=ts.astimezone(timezone.utc), received_timestamp=raw.get("received_timestamp"), sequence_id=p["trade_id"], price=price, quantity=qty, side=p["side"], trade_id=tid, quote_quantity=quote, exchange_symbol=m.exchange_symbol, canonical_asset=m.canonical_asset, canonical_quote_family=m.canonical_quote_family, base_asset=m.base_asset, quote_asset=m.quote_asset, quote_family=m.canonical_quote_family, canonical_asset_pair=m.canonical_asset_pair)
