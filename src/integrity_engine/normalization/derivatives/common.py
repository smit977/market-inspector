from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from integrity_engine.normalization.base import NormalizationError
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent

def dec(value: Any, label: str, positive: bool = True, signed: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)): raise NormalizationError(f"invalid derivatives {label}")
    try: value = str(value) if isinstance(value, float) else value; out = Decimal(value)
    except (DecimalException, ValueError) as error: raise NormalizationError(f"invalid derivatives {label}") from error
    if not out.is_finite() or (not signed and ((positive and out <= 0) or (not positive and out < 0))): raise NormalizationError(f"invalid derivatives {label}")
    return out

def ts(value: Any, unit: str = "ms") -> datetime:
    try:
        if isinstance(value, str) and ("T" in value or value.endswith("Z")):
            out = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            scale = {"s": 1, "ms": 1000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
            out = datetime.fromtimestamp(int(value) / scale, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, KeyError) as error: raise NormalizationError("invalid derivatives timestamp") from error
    if out.tzinfo is None or out.utcoffset() is None: raise NormalizationError("derivatives timestamp must be timezone-aware")
    return out.astimezone(timezone.utc)

def quote(price: Decimal, quantity: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = max(28, len(price.as_tuple().digits) + len(quantity.as_tuple().digits))
        return price * quantity

def metadata(exchange, symbol, quote_asset, received, event_type, stamp, event_key):
    return dict(schema_version="1.0", event_id=uuid5(NAMESPACE_URL, f"{exchange}:perpetual:{event_key}"), event_type=event_type, exchange=exchange, exchange_symbol=symbol, canonical_pair="BTC-USD", market_type="perpetual", base_asset="BTC", quote_asset=quote_asset, settlement_asset=quote_asset, exchange_timestamp=stamp, received_timestamp=received)
