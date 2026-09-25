"""Kraken v2 public trade WebSocket ingestion."""

import argparse, asyncio, json, signal
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

class KrakenTradeSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    kafka_bootstrap_servers: str = Field(min_length=1)
    kraken_ws_url: str = "wss://ws.kraken.com/v2"
    kraken_trade_symbol: str = "BTC/USD"

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def bootstrap(cls, value: str) -> str:
        if not value.strip(): raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be blank")
        return value.strip()

    @field_validator("kraken_trade_symbol", mode="before")
    @classmethod
    def symbol(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("kraken_ws_url")
    @classmethod
    def url(cls, value: str) -> str:
        value = value.rstrip("/"); parsed = urlsplit(value)
        if parsed.scheme != "wss" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("KRAKEN_WS_URL must be a credential-free wss URL")
        return value

def raw_trade_event(payload: dict[str, Any], symbol: str, received_timestamp: datetime) -> dict[str, Any]:
    if received_timestamp.tzinfo is None or received_timestamp.utcoffset() is None: raise ValueError("timestamp must be timezone-aware")
    return {"schema_version":"1.0","exchange":"kraken","stream":"trades","symbol":symbol,
            "exchange_symbol":symbol,"received_timestamp":received_timestamp.astimezone(timezone.utc).isoformat(),"payload":payload}

class KrakenTradeIngestor(ExchangeIngestor):
    def __init__(self, settings: KrakenTradeSettings, producer: JsonKafkaProducer) -> None:
        self.settings, self.producer, self.received = settings, producer, 0
    @property
    def subscription(self) -> dict[str, Any]:
        return {"method":"subscribe","params":{"channel":"trade","symbol":[self.settings.kraken_trade_symbol],"snapshot":False}}
    async def handle_message(self, message: str | bytes, received_timestamp: datetime) -> bool:
        try: payload = json.loads(message)
        except (ValueError, UnicodeError) as error: log.error("kraken_malformed_payload", error=str(error)); return False
        if not isinstance(payload, dict): log.warning("kraken_malformed_payload", reason="not_object"); return False
        if payload.get("channel") != "trade":
            if payload.get("method") == "subscribe" and payload.get("success") is True: log.info("kraken_subscribed")
            return False
        rows = payload.get("data")
        if not isinstance(rows, list) or not rows: log.warning("kraken_malformed_payload", reason="missing_data"); return False
        valid = False
        for row in rows:
            if not isinstance(row, dict) or row.get("symbol") != self.settings.kraken_trade_symbol: continue
            if any(k not in row for k in ("price","qty","side","timestamp","trade_id")): log.warning("kraken_malformed_payload", reason="missing_trade_field"); continue
            if row.get("side") not in {"buy","sell"}: log.warning("kraken_malformed_payload", reason="invalid_side"); continue
            valid = True; self.received += 1
            event = raw_trade_event({**payload, "data":[row]}, self.settings.kraken_trade_symbol, received_timestamp)
            await self.producer.publish(TOPIC, self.settings.kraken_trade_symbol, event)
        return valid
    async def run(self) -> None:
        delay = 1.0
        while True:
            try:
                async with connect(self.settings.kraken_ws_url, open_timeout=10, close_timeout=3, ping_interval=20, ping_timeout=20, max_queue=32) as websocket:
                    await websocket.send(json.dumps(self.subscription)); log.info("kraken_connected", symbol=self.settings.kraken_trade_symbol)
                    async for message in websocket:
                        if await self.handle_message(message, datetime.now(timezone.utc)): delay = 1.0
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError) as error: log.error("kraken_connection_error", error=str(error))
            log.info("kraken_retry", delay_seconds=delay); await asyncio.sleep(delay); delay=min(delay*2,30.0)

async def run_ingestor(duration: float | None) -> None:
    settings=KrakenTradeSettings(); producer=JsonKafkaProducer(settings.kafka_bootstrap_servers); ingestor=KrakenTradeIngestor(settings,producer); task=asyncio.create_task(ingestor.run()); loop=asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(sig,task.cancel)
    timer=loop.call_later(duration,task.cancel) if duration else None
    try: await task
    except asyncio.CancelledError: pass
    finally:
        if timer: timer.cancel()
        for sig in (signal.SIGINT,signal.SIGTERM): loop.remove_signal_handler(sig)
        await producer.close(); log.info("kraken_ingestion_stopped",received=ingestor.received,delivered=producer.delivered)

def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--duration",type=float); args=parser.parse_args(); structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()])
    try: asyncio.run(run_ingestor(args.duration))
    except Exception as error: log.error("kraken_ingestion_failed",error_type=type(error).__name__,error=str(error)); return 1
    return 0
if __name__ == "__main__": raise SystemExit(main())
