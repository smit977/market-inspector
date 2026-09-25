"""Capture Binance spot depth deltas without building local order-book state."""

import argparse
import asyncio
import json
import math
import signal
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlsplit

import structlog
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from integrity_engine.ingestion.base import ExchangeIngestor
from integrity_engine.kafka.producer import JsonKafkaProducer

TOPIC = "market.raw.orderbook"
log = structlog.get_logger(__name__)


class BinanceOrderBookSettings(BaseSettings):
    """Public diff-depth configuration; only BTCUSDT is supported initially."""

    model_config = SettingsConfigDict(extra="ignore")
    kafka_bootstrap_servers: str = Field(min_length=1)
    binance_ws_base_url: str = "wss://data-stream.binance.vision"
    binance_orderbook_symbol: Literal["BTCUSDT"] = "BTCUSDT"
    binance_orderbook_interval_ms: Literal["100", "1000"] = "100"

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_bootstrap(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be blank")
        return value.strip()

    @field_validator("binance_orderbook_symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("binance_ws_base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        value = value.rstrip("/")
        url = urlsplit(value)
        if (url.scheme != "wss" or not url.hostname or url.path or url.query
                or url.fragment or url.username or url.password):
            raise ValueError("BINANCE_WS_BASE_URL must be a wss origin without credentials or path")
        return value

    @property
    def stream_url(self) -> str:
        suffix = "@100ms" if self.binance_orderbook_interval_ms == "100" else ""
        return f"{self.binance_ws_base_url}/ws/{self.binance_orderbook_symbol.lower()}@depth{suffix}"


def raw_orderbook_event(payload: dict[str, Any], symbol: str,
                        received_timestamp: datetime) -> dict[str, Any]:
    """Preserve all original fields, price strings, zero quantities, and level order."""
    if received_timestamp.utcoffset() is None:
        raise ValueError("received_timestamp must be timezone-aware")
    return {
        "schema_version": "1.0", "exchange": "binance", "stream": "orderbook",
        "symbol": symbol,
        "received_timestamp": received_timestamp.astimezone(timezone.utc).isoformat(),
        "payload": payload,
    }


def _reject_constant(value: str) -> None:
    raise ValueError("Non-finite JSON constants are not supported")


def _validate_depth(payload: Any, symbol: str) -> None:
    """Check message structure only; never apply deltas or infer sequence gaps."""
    if not isinstance(payload, dict) or payload.get("e") != "depthUpdate" or payload.get("s") != symbol:
        raise ValueError("Expected a BTCUSDT depthUpdate object")
    for field in ("E", "U", "u"):
        if type(payload.get(field)) is not int or payload[field] < 0:
            raise ValueError(f"Missing or invalid integer depth field: {field}")
    if payload["U"] > payload["u"]:
        raise ValueError("First update ID exceeds final update ID")
    for field in ("b", "a"):
        levels = payload.get(field)
        if not isinstance(levels, list) or any(
            not isinstance(level, list) or len(level) != 2
            or not all(isinstance(value, str) for value in level)
            for level in levels
        ):
            raise ValueError(f"Missing or invalid depth levels: {field}")


class BinanceOrderBookIngestor(ExchangeIngestor):
    def __init__(self, settings: BinanceOrderBookSettings, producer: JsonKafkaProducer) -> None:
        self.settings = settings
        self.producer = producer
        self.received = 0
        self.malformed = 0

    async def handle_message(self, message: str | bytes, received_timestamp: datetime) -> bool:
        try:
            payload = json.loads(message, parse_constant=_reject_constant)
            _validate_depth(payload, self.settings.binance_orderbook_symbol)
        except (ValueError, UnicodeError) as error:
            self.malformed += 1
            log.error("binance_orderbook_malformed_message", error_type=type(error).__name__,
                      error=str(error))
            return False
        self.received += 1
        if self.received <= 3:
            log.info("binance_depth_received", first_update_id=payload["U"],
                     final_update_id=payload["u"], symbol=payload["s"])
        event = raw_orderbook_event(payload, self.settings.binance_orderbook_symbol, received_timestamp)
        # Shared producer logs delivery failures and raises; don't silently retry/drop.
        await self.producer.publish(TOPIC, self.settings.binance_orderbook_symbol, event)
        return True

    async def run(self) -> None:
        delay = 1.0
        while True:
            log.info("binance_orderbook_connecting", symbol=self.settings.binance_orderbook_symbol)
            try:
                async with connect(self.settings.stream_url, open_timeout=10,
                                   close_timeout=3, ping_interval=20, ping_timeout=20,
                                   max_queue=32) as websocket:
                    log.info("binance_orderbook_connected", symbol=self.settings.binance_orderbook_symbol)
                    try:
                        async for message in websocket:
                            received_timestamp = datetime.now(timezone.utc)
                            if await self.handle_message(message, received_timestamp):
                                delay = 1.0
                    finally:
                        log.info("binance_orderbook_disconnected")
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError) as error:
                log.error("binance_orderbook_connection_error", error=str(error))
            log.info("binance_orderbook_reconnect_attempt", delay_seconds=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def run_ingestor(duration: float | None) -> None:
    settings = BinanceOrderBookSettings()
    producer = JsonKafkaProducer(settings.kafka_bootstrap_servers)
    ingestor = BinanceOrderBookIngestor(settings, producer)
    task = asyncio.create_task(ingestor.run())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    timer = loop.call_later(duration, task.cancel) if duration is not None else None
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        if timer is not None:
            timer.cancel()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await producer.close()
        log.info("orderbook_ingestion_stopped", received=ingestor.received,
                 delivered=producer.delivered, malformed=ingestor.malformed)
    if duration is not None and producer.delivered == 0:
        raise RuntimeError("No Binance depth events were delivered during the timed run")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, help="Stop after this many seconds")
    args = parser.parse_args()
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0):
        parser.error("--duration must be finite and positive")
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True),
                                   structlog.processors.JSONRenderer()])
    try:
        asyncio.run(run_ingestor(args.duration))
    except Exception as error:
        log.error("orderbook_ingestion_failed", error_type=type(error).__name__, error=str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
