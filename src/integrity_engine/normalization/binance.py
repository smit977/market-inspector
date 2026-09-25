"""Binance spot trade normalization and its single-consumer runner."""

import argparse
import asyncio
import os
import signal
from datetime import datetime, timedelta, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import structlog

from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.normalization.base import (
    NormalizationError, PROCESSOR_NAME, SOURCE_TOPIC, TradeNormalizer,
    TradeNormalizationProcessor,
)
from integrity_engine.schemas.trade import TradeEvent
from integrity_engine.schemas.symbols import market_mapping

log = structlog.get_logger(__name__)


def _amount(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise NormalizationError("Trade amounts require decimal strings or exact numbers")
    try:
        amount = Decimal(value)
    except DecimalException as error:
        raise NormalizationError("Invalid decimal trade amount") from error
    if not amount.is_finite() or amount <= 0:
        raise NormalizationError("Trade amounts must be finite and positive")
    return amount


class BinanceTradeNormalizer(TradeNormalizer):
    """Map BTCUSDT trades only. Binance m means buyer-is-maker:

    m=True => resting buyer, aggressive seller => sell.
    m=False => aggressive buyer => buy. Never coerce truthy strings/numbers.
    Trade ID is the symbol-scoped sequence identifier; it isn't exchange-global.
    """

    def normalize(self, raw: dict[str, Any]) -> TradeEvent:
        if raw.get("exchange") != "binance" or raw.get("stream") != "trades":
            raise NormalizationError("Unsupported envelope: expected Binance trades")
        if raw.get("schema_version") != "1.0":
            raise NormalizationError("Unsupported raw envelope schema_version")
        payload = raw.get("payload")
        if not isinstance(payload, dict):
            raise NormalizationError("Binance payload must be an object")
        if any(field not in payload for field in ("s", "p", "q", "t", "T", "m", "e")):
            raise NormalizationError("Missing required Binance trade field")
        if "received_timestamp" not in raw:
            raise NormalizationError("Missing envelope received_timestamp")
        if payload["e"] != "trade":
            raise NormalizationError("Expected Binance trade event type")
        if payload["s"] != "BTCUSDT" or raw.get("symbol") != "BTCUSDT":
            raise NormalizationError("Expected matching BTCUSDT envelope and payload symbols")
        if type(payload["m"]) is not bool:
            raise NormalizationError("Binance maker flag must be a boolean")
        if type(payload["t"]) is not int or payload["t"] < 0:
            raise NormalizationError("Binance trade ID must be a nonnegative integer")
        if type(payload["T"]) is not int or payload["T"] < 0:
            raise NormalizationError("Binance trade timestamp must be integer epoch milliseconds")
        try:
            # Integer arithmetic preserves milliseconds without float conversion.
            exchange_timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                milliseconds=payload["T"])
        except OverflowError as error:
            raise NormalizationError("Binance trade timestamp is outside datetime range") from error
        price, quantity = _amount(payload["p"]), _amount(payload["q"])
        try:
            with localcontext() as context:
                # Default Decimal precision is 28; preserve the full product instead.
                context.prec = max(28, len(price.as_tuple().digits) + len(quantity.as_tuple().digits))
                quote_quantity = price * quantity
        except DecimalException as error:
            raise NormalizationError("Trade amount product exceeds Decimal range") from error
        return TradeEvent(
            schema_version="1.0",
            event_id=uuid5(NAMESPACE_URL, f"binance:spot:BTCUSDT:trade:{payload['t']}"),
            event_type="trade", exchange="binance", symbol="BTC-USDT",
            exchange_timestamp=exchange_timestamp,
            received_timestamp=raw["received_timestamp"],
            sequence_id=payload["t"], price=price, quantity=quantity,
            side="sell" if payload["m"] else "buy",
            trade_id=str(payload["t"]), quote_quantity=quote_quantity,
            exchange_symbol=market_mapping("binance", "BTCUSDT").exchange_symbol,
            canonical_asset=market_mapping("binance", "BTCUSDT").canonical_asset,
            canonical_quote_family=market_mapping("binance", "BTCUSDT").canonical_quote_family,
            base_asset="BTC", quote_asset="USDT", quote_family="USD", canonical_asset_pair="BTC-USD",
        )


async def run_normalizer(duration: float | None) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
    producer = JsonKafkaProducer(bootstrap)
    consumer = ManualKafkaConsumer(bootstrap, SOURCE_TOPIC, PROCESSOR_NAME)
    processor = TradeNormalizationProcessor(BinanceTradeNormalizer(), consumer, producer)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    timer = loop.call_later(duration, stop.set) if duration is not None else None
    try:
        log.info("normalizer_started", group=PROCESSOR_NAME)
        await processor.run(stop)
    finally:
        if timer is not None:
            timer.cancel()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        try:
            await consumer.close()
        finally:
            await producer.close()
        log.info("normalizer_stopped", cleaned=processor.cleaned, dlq=processor.dead_lettered)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, help="Stop after this many seconds")
    args = parser.parse_args()
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be positive")
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True),
                                   structlog.processors.JSONRenderer()])
    try:
        asyncio.run(run_normalizer(args.duration))
    except Exception as error:
        log.error("normalizer_failed", error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
