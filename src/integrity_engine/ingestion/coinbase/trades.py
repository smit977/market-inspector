"""Coinbase Exchange public match-feed trade ingestion."""

import argparse
import asyncio
import json
import signal
from datetime import datetime, timezone
from typing import Any
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


class CoinbaseTradeSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    kafka_bootstrap_servers: str = Field(min_length=1)
    coinbase_ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    coinbase_trade_symbol: str = "BTC-USD"

    @property
    def stream_url(self) -> str:
        return self.coinbase_ws_url

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_bootstrap(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be blank")
        return value.strip()

    @field_validator("coinbase_trade_symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("coinbase_ws_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        value = value.rstrip("/")
        url = urlsplit(value)
        if url.scheme != "wss" or not url.hostname or url.path or url.query or url.fragment or url.username or url.password:
            raise ValueError("COINBASE_WS_URL must be a wss origin without credentials or path")
        return value


def raw_trade_event(payload: dict[str, Any], symbol: str, received_timestamp: datetime,
                    exchange_symbol: str | None = None) -> dict[str, Any]:
    if received_timestamp.tzinfo is None or received_timestamp.utcoffset() is None:
        raise ValueError("received_timestamp must be timezone-aware")
    event = {
        "schema_version": "1.0", "exchange": "coinbase", "stream": "trades",
        "symbol": symbol, "received_timestamp": received_timestamp.astimezone(timezone.utc).isoformat(),
        "payload": payload,
    }
    if exchange_symbol is not None:
        event["exchange_symbol"] = exchange_symbol
    return event


class CoinbaseTradeIngestor(ExchangeIngestor):
    def __init__(self, settings: CoinbaseTradeSettings, producer: JsonKafkaProducer) -> None:
        self.settings, self.producer, self.received = settings, producer, 0

    @property
    def subscription(self) -> dict[str, Any]:
        return {"type": "subscribe", "product_ids": [self.settings.coinbase_trade_symbol], "channels": ["matches"]}

    async def handle_message(self, message: str | bytes, received_timestamp: datetime) -> bool:
        try:
            payload = json.loads(message)
        except (ValueError, UnicodeError) as error:
            log.error("coinbase_malformed_message", error=str(error)); return False
        if not isinstance(payload, dict):
            log.warning("coinbase_malformed_message", reason="message is not an object"); return False
        if payload.get("type") not in {"match", "last_match"}:
            if payload.get("type") == "subscriptions":
                log.info("coinbase_subscription_confirmed", symbol=self.settings.coinbase_trade_symbol)
            return False
        required = ("product_id", "trade_id", "price", "size", "side", "time")
        if any(field not in payload for field in required) or payload["product_id"] != self.settings.coinbase_trade_symbol:
            log.warning("coinbase_malformed_message", reason="missing or mismatched trade fields"); return False
        if payload["side"] not in {"buy", "sell"}:
            log.warning("coinbase_malformed_message", reason="invalid side"); return False
        self.received += 1
        event = raw_trade_event(payload, self.settings.coinbase_trade_symbol, received_timestamp,
                                exchange_symbol=self.settings.coinbase_trade_symbol)
        await self.producer.publish(TOPIC, self.settings.coinbase_trade_symbol, event)
        return True

    async def run(self) -> None:
        delay = 1.0
        while True:
            log.info("coinbase_connecting", symbol=self.settings.coinbase_trade_symbol)
            try:
                async with connect(self.settings.coinbase_ws_url, open_timeout=10, close_timeout=3,
                                   ping_interval=20, ping_timeout=20, max_queue=32) as websocket:
                    await websocket.send(json.dumps(self.subscription))
                    log.info("coinbase_connected", symbol=self.settings.coinbase_trade_symbol)
                    async for message in websocket:
                        received_timestamp = datetime.now(timezone.utc)
                        if await self.handle_message(message, received_timestamp):
                            delay = 1.0
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError) as error:
                log.error("coinbase_connection_error", error=str(error))
            log.info("coinbase_retry", delay_seconds=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def run_ingestor(duration: float | None) -> None:
    settings = CoinbaseTradeSettings(); producer = JsonKafkaProducer(settings.kafka_bootstrap_servers)
    ingestor = CoinbaseTradeIngestor(settings, producer); task = asyncio.create_task(ingestor.run())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, task.cancel)
    timer = loop.call_later(duration, task.cancel) if duration is not None else None
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        if timer: timer.cancel()
        for sig in (signal.SIGINT, signal.SIGTERM): loop.remove_signal_handler(sig)
        await producer.close()
        log.info("coinbase_ingestion_stopped", received=ingestor.received, delivered=producer.delivered)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--duration", type=float)
    args = parser.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True), structlog.processors.JSONRenderer()])
    try: asyncio.run(run_ingestor(args.duration))
    except Exception as error: log.error("coinbase_ingestion_failed", error_type=type(error).__name__, error=str(error)); return 1
    return 0


if __name__ == "__main__": raise SystemExit(main())
