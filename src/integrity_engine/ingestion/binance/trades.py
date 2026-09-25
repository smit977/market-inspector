"""Binance single-symbol raw trade ingestion; no normalized schema mapping."""

import argparse
import asyncio
import json
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

TOPIC = "market.raw.trades"
log = structlog.get_logger(__name__)


class BinanceTradeSettings(BaseSettings):
    """Environment configuration; this first adapter supports only BTCUSDT."""

    model_config = SettingsConfigDict(extra="ignore")

    kafka_bootstrap_servers: str = Field(min_length=1)
    binance_ws_base_url: str = "wss://data-stream.binance.vision"
    binance_trade_symbol: Literal["BTCUSDT"] = "BTCUSDT"

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_bootstrap(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be blank")
        return value.strip()

    @field_validator("binance_trade_symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("binance_ws_base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        value = value.rstrip("/")
        url = urlsplit(value)
        if (url.scheme != "wss" or not url.hostname or url.path
                or url.query or url.fragment or url.username or url.password):
            raise ValueError("BINANCE_WS_BASE_URL must be a wss origin without credentials or path")
        return value

    @property
    def stream_url(self) -> str:
        return f"{self.binance_ws_base_url}/ws/{self.binance_trade_symbol.lower()}@trade"


def raw_trade_event(payload: dict[str, Any], symbol: str,
                    received_timestamp: datetime, exchange_symbol: str | None = None) -> dict[str, Any]:
    """Wrap the original parsed payload without altering its fields or values."""
    if received_timestamp.utcoffset() is None:
        raise ValueError("received_timestamp must be timezone-aware")
    event = {
        "schema_version": "1.0",
        "exchange": "binance",
        "stream": "trades",
        "symbol": symbol,
        "received_timestamp": received_timestamp.astimezone(timezone.utc).isoformat(),
        "payload": payload,
    }
    if exchange_symbol is not None:
        event["exchange_symbol"] = exchange_symbol
    return event


class BinanceTradeIngestor(ExchangeIngestor):
    def __init__(self, settings: BinanceTradeSettings, producer: JsonKafkaProducer) -> None:
        self.settings = settings
        self.producer = producer
        self.received = 0

    async def handle_message(self, message: str | bytes, received_timestamp: datetime) -> bool:
        """Publish only trade messages for the configured symbol; retain payload as-is."""
        try:
            payload = json.loads(message)
        except (ValueError, UnicodeError) as error:
            log.error("binance_invalid_json", error=str(error))
            return False
        if (not isinstance(payload, dict) or payload.get("e") != "trade"
                or payload.get("s") != self.settings.binance_trade_symbol):
            log.warning("binance_unexpected_message")
            return False
        self.received += 1
        if self.received <= 3:
            log.info("binance_trade_received", trade_id=payload.get("t"),
                     symbol=self.settings.binance_trade_symbol)
        event = raw_trade_event(payload, self.settings.binance_trade_symbol, received_timestamp,
                                exchange_symbol=self.settings.binance_trade_symbol)
        await self.producer.publish(TOPIC, self.settings.binance_trade_symbol, event)
        return True

    async def run(self) -> None:
        delay = 1.0
        while True:
            log.info("binance_connecting", symbol=self.settings.binance_trade_symbol)
            try:
                async with connect(self.settings.stream_url, open_timeout=10,
                                   close_timeout=3, ping_interval=20, ping_timeout=20,
                                   max_queue=32) as websocket:
                    log.info("binance_connected", symbol=self.settings.binance_trade_symbol)
                    try:
                        async for message in websocket:
                            received_timestamp = datetime.now(timezone.utc)
                            # Capture time immediately, before parsing or Kafka I/O.
                            if await self.handle_message(message, received_timestamp):
                                delay = 1.0
                    finally:
                        log.info("binance_disconnected")
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError) as error:
                # Kafka delivery failures are fatal, not silent WebSocket retries.
                log.error("binance_connection_error", error=str(error))
            log.info("binance_retry", delay_seconds=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def run_ingestor(duration: float | None) -> None:
    settings = BinanceTradeSettings()
    producer = JsonKafkaProducer(settings.kafka_bootstrap_servers)
    ingestor = BinanceTradeIngestor(settings, producer)
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
        log.info("ingestion_stopped", received=ingestor.received, delivered=producer.delivered)
    if duration is not None and producer.delivered == 0:
        raise RuntimeError("No Binance trade events were delivered during the timed run")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, help="Stop after this many seconds")
    args = parser.parse_args()
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be positive")
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True),
                                   structlog.processors.JSONRenderer()])
    try:
        asyncio.run(run_ingestor(args.duration))
    except Exception as error:
        log.error("ingestion_failed", error=str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
