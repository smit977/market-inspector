"""Coinbase public match-feed normalization."""

from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from integrity_engine.normalization.base import NormalizationError, TradeNormalizer
from integrity_engine.schemas.symbols import market_mapping
from integrity_engine.schemas.trade import TradeEvent


def _decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise NormalizationError(f"Coinbase {label} requires a decimal string or exact number")
    try:
        result = Decimal(value)
    except DecimalException as error:
        raise NormalizationError(f"Invalid Coinbase {label}") from error
    if not result.is_finite() or result <= 0:
        raise NormalizationError(f"Coinbase {label} must be finite and positive")
    return result


class CoinbaseTradeNormalizer(TradeNormalizer):
    """Map Coinbase match messages. Coinbase ``side`` is the taker order side,
    so it already describes aggressor direction: buy=buy and sell=sell.
    """

    def normalize(self, raw: dict[str, Any]) -> TradeEvent:
        if raw.get("exchange") != "coinbase" or raw.get("stream") != "trades":
            raise NormalizationError("Unsupported envelope: expected Coinbase trades")
        if raw.get("schema_version") != "1.0":
            raise NormalizationError("Unsupported raw envelope schema_version")
        payload = raw.get("payload")
        required = ("product_id", "trade_id", "price", "size", "side", "time")
        if not isinstance(payload, dict) or any(field not in payload for field in required):
            raise NormalizationError("Missing required Coinbase trade field")
        symbol = payload["product_id"]
        if symbol != raw.get("symbol") or symbol != "BTC-USD":
            raise NormalizationError("Expected matching Coinbase BTC-USD symbols")
        if not isinstance(payload["trade_id"], (str, int)) or isinstance(payload["trade_id"], bool):
            raise NormalizationError("Coinbase trade ID must be a string or integer")
        if payload["side"] not in {"buy", "sell"}:
            raise NormalizationError("Coinbase side must be buy or sell")
        try:
            exchange_timestamp = datetime.fromisoformat(str(payload["time"]).replace("Z", "+00:00"))
        except (TypeError, ValueError) as error:
            raise NormalizationError("Invalid Coinbase trade timestamp") from error
        if exchange_timestamp.tzinfo is None or exchange_timestamp.utcoffset() is None:
            raise NormalizationError("Coinbase trade timestamp must be timezone-aware")
        price, quantity = _decimal(payload["price"], "price"), _decimal(payload["size"], "size")
        with localcontext() as context:
            context.prec = max(28, len(price.as_tuple().digits) + len(quantity.as_tuple().digits))
            quote_quantity = price * quantity
        mapping = market_mapping("coinbase", symbol)
        trade_id = str(payload["trade_id"])
        sequence = payload.get("sequence")
        if sequence is not None and (not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0):
            raise NormalizationError("Coinbase sequence must be a nonnegative integer")
        return TradeEvent(
            schema_version="1.0", event_id=uuid5(NAMESPACE_URL, f"coinbase:spot:{symbol}:trade:{trade_id}"),
            event_type="trade", exchange="coinbase", symbol=mapping.symbol,
            exchange_timestamp=exchange_timestamp.astimezone(timezone.utc),
            received_timestamp=raw.get("received_timestamp"), sequence_id=sequence,
            price=price, quantity=quantity, side=payload["side"], trade_id=trade_id,
            quote_quantity=quote_quantity, exchange_symbol=mapping.exchange_symbol,
            canonical_asset=mapping.canonical_asset, canonical_quote_family=mapping.canonical_quote_family,
            base_asset=mapping.base_asset, quote_asset=mapping.quote_asset,
            quote_family=mapping.canonical_quote_family, canonical_asset_pair=mapping.canonical_asset_pair,
        )
