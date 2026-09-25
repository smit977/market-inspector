"""Dispatch normalized raw trades to the supported venue adapter."""

import argparse
import asyncio
import os
import signal
import structlog
from typing import Any

from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.normalization.base import (CLEANED_TOPIC, PROCESSOR_NAME,
    SOURCE_TOPIC, NormalizationError, TradeNormalizer, TradeNormalizationProcessor)
from integrity_engine.normalization.binance import BinanceTradeNormalizer
from integrity_engine.normalization.coinbase import CoinbaseTradeNormalizer
from integrity_engine.normalization.kraken import KrakenTradeNormalizer
from integrity_engine.normalization.okx import OKXTradeNormalizer
from integrity_engine.normalization.bybit import BybitTradeNormalizer
from integrity_engine.schemas.trade import TradeEvent


class MultiExchangeTradeNormalizer(TradeNormalizer):
    def __init__(self) -> None:
        self._normalizers = {"binance": BinanceTradeNormalizer(), "coinbase": CoinbaseTradeNormalizer(),
                             "kraken": KrakenTradeNormalizer(), "okx": OKXTradeNormalizer(),
                             "bybit": BybitTradeNormalizer()}

    def normalize(self, raw: dict[str, Any]) -> TradeEvent:
        exchange = raw.get("exchange")
        try:
            normalizer = self._normalizers[exchange]
        except (KeyError, TypeError) as error:
            raise NormalizationError("Unsupported trade exchange") from error
        return normalizer.normalize(raw)


async def run_normalizer(duration: float | None) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
    producer = JsonKafkaProducer(bootstrap)
    consumer = ManualKafkaConsumer(bootstrap, SOURCE_TOPIC, PROCESSOR_NAME)
    processor = TradeNormalizationProcessor(MultiExchangeTradeNormalizer(), consumer, producer)
    stop = asyncio.Event(); loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, stop.set)
    timer = loop.call_later(duration, stop.set) if duration is not None else None
    try:
        await processor.run(stop)
    finally:
        if timer: timer.cancel()
        for sig in (signal.SIGINT, signal.SIGTERM): loop.remove_signal_handler(sig)
        await consumer.close(); await producer.close()
        structlog.get_logger().info("multi_normalizer_stopped", cleaned=processor.cleaned, dlq=processor.dead_lettered)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float)
    args = parser.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True), structlog.processors.JSONRenderer()])
    try:
        asyncio.run(run_normalizer(args.duration))
    except Exception as error:
        structlog.get_logger().error("multi_normalizer_failed", error_type=type(error).__name__, error=str(error))
        return 1
    return 0
