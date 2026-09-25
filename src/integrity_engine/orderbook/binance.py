"""Binance REST snapshot plus diff-depth reconstruction for BTCUSDT."""

import argparse
import asyncio
import json
import math
import os
import signal
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any, Callable
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

import httpx
import structlog
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.schemas.orderbook import OrderBookLevel, OrderBookSnapshot

RAW_TOPIC = "market.raw.orderbook"
CLEAN_TOPIC = "market.cleaned.orderbook"
log = structlog.get_logger(__name__)


class OrderBookSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    kafka_bootstrap_servers: str = Field(min_length=1)
    # Binance's public market-data host avoids trading API restrictions and
    # serves snapshots from the same global market-data venue as the stream.
    binance_rest_base_url: str = "https://data-api.binance.vision"
    binance_orderbook_symbol: str = "BTCUSDT"
    binance_orderbook_snapshot_limit: int = Field(default=1000, ge=5, le=5000)
    orderbook_publish_depth: int = Field(default=20, ge=1, le=5000)
    orderbook_publish_interval_ms: int = Field(default=250, ge=1, le=60000)

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be blank")
        return value.strip()

    @field_validator("binance_orderbook_symbol", mode="before")
    @classmethod
    def symbol(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("binance_orderbook_symbol")
    @classmethod
    def only_btcusdt(cls, value: str) -> str:
        if value != "BTCUSDT":
            raise ValueError("only BTCUSDT is supported")
        return value

    @field_validator("binance_rest_base_url")
    @classmethod
    def rest_origin(cls, value: str) -> str:
        value = value.rstrip("/")
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname or parsed.path or parsed.query or parsed.fragment:
            raise ValueError("BINANCE_REST_BASE_URL must be an https origin")
        return value

    @property
    def snapshot_url(self) -> str:
        return f"{self.binance_rest_base_url}/api/v3/depth"


def _decimal(value: Any, *, allow_zero: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("book price and quantity must be decimal strings")
    try:
        result = Decimal(value)
    except DecimalException as error:
        raise ValueError("invalid decimal book level") from error
    if not result.is_finite() or (result < 0 if allow_zero else result <= 0):
        raise ValueError("invalid book level value")
    return result


class DepthEvent(BaseModel):
    event_time: int
    first_update_id: int
    final_update_id: int
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]


def parse_depth_event(raw: dict[str, Any]) -> DepthEvent:
    payload = raw.get("payload")
    if raw.get("exchange") != "binance" or raw.get("stream") != "orderbook" or not isinstance(payload, dict):
        raise ValueError("unsupported raw order-book envelope")
    if payload.get("e") != "depthUpdate" or payload.get("s") != "BTCUSDT":
        raise ValueError("unsupported Binance depth payload")
    try:
        event = DepthEvent(event_time=payload["E"], first_update_id=payload["U"],
                           final_update_id=payload["u"], bids=payload["b"], asks=payload["a"])
    except Exception as error:
        raise ValueError("malformed Binance depth payload") from error
    if event.first_update_id > event.final_update_id or event.event_time < 0:
        raise ValueError("invalid Binance depth sequence")
    for levels in (event.bids, event.asks):
        for level in levels:
            if len(level) != 2:
                raise ValueError("malformed Binance book level")
            _decimal(level[0]); _decimal(level[1], allow_zero=True)
    return event


class LocalOrderBook:
    """Mutable Decimal levels with strict synchronization and sequencing."""

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id: int | None = None
        self.synchronized = False

    def initialize(self, snapshot: dict[str, Any]) -> None:
        self.bids, self.asks = {}, {}
        self._apply_levels(self.bids, snapshot.get("bids", []))
        self._apply_levels(self.asks, snapshot.get("asks", []))
        last = snapshot.get("lastUpdateId")
        if type(last) is not int or last < 0 or not self.bids or not self.asks:
            raise ValueError("invalid empty or malformed REST snapshot")
        self.last_update_id = last
        self.synchronized = False

    @staticmethod
    def _apply_levels(side: dict[Decimal, Decimal], levels: Any) -> None:
        if not isinstance(levels, list):
            raise ValueError("snapshot levels must be arrays")
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) != 2:
                raise ValueError("malformed snapshot level")
            price, quantity = _decimal(level[0]), _decimal(level[1], allow_zero=True)
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def bridge(self, event: DepthEvent) -> bool:
        if self.last_update_id is None:
            raise RuntimeError("book is not initialized")
        if event.final_update_id <= self.last_update_id:
            return False
        if event.first_update_id > self.last_update_id + 1 or event.final_update_id < self.last_update_id + 1:
            return False
        self.apply(event)
        self.synchronized = True
        return True

    def apply(self, event: DepthEvent) -> None:
        if not self.synchronized and self.last_update_id is not None:
            self._apply_levels(self.bids, event.bids)
            self._apply_levels(self.asks, event.asks)
            self.last_update_id = event.final_update_id
            return
        if self.last_update_id is None or event.first_update_id > self.last_update_id + 1:
            self.synchronized = False
            raise SequenceGap(self.last_update_id, event)
        if event.final_update_id <= self.last_update_id:
            return
        self._apply_levels(self.bids, event.bids)
        self._apply_levels(self.asks, event.asks)
        self.last_update_id = event.final_update_id

    def top_levels(self, depth: int) -> tuple[list[OrderBookLevel], list[OrderBookLevel]]:
        bids = [OrderBookLevel(price=p, quantity=self.bids[p]) for p in sorted(self.bids, reverse=True)[:depth]]
        asks = [OrderBookLevel(price=p, quantity=self.asks[p]) for p in sorted(self.asks)[:depth]]
        return bids, asks


class SequenceGap(RuntimeError):
    def __init__(self, previous: int | None, event: DepthEvent) -> None:
        super().__init__(f"sequence gap: previous={previous}, first={event.first_update_id}, final={event.final_update_id}")


def build_snapshot(book: LocalOrderBook, event: DepthEvent, received_timestamp: datetime,
                   depth: int) -> OrderBookSnapshot:
    if not book.synchronized or book.last_update_id is None:
        raise ValueError("book is not synchronized")
    bids, asks = book.top_levels(depth)
    if not bids or not asks:
        raise ValueError("book side is empty")
    best_bid, best_ask = bids[0], asks[0]
    spread = best_ask.price - best_bid.price
    mid = (best_bid.price + best_ask.price) / Decimal(2)
    if best_bid.price >= best_ask.price or spread < 0:
        raise ValueError("crossed or invalid book")
    return OrderBookSnapshot(
        schema_version="1.0", event_id=uuid5(NAMESPACE_URL, f"binance:BTCUSDT:orderbook:{book.last_update_id}"),
        event_type="orderbook", exchange="binance", symbol="BTC-USDT",
        exchange_timestamp=datetime.fromtimestamp(event.event_time / 1000, tz=timezone.utc),
        received_timestamp=received_timestamp.astimezone(timezone.utc), sequence_id=book.last_update_id,
        best_bid_price=best_bid.price, best_bid_quantity=best_bid.quantity,
        best_ask_price=best_ask.price, best_ask_quantity=best_ask.quantity,
        spread=spread, mid_price=mid, bids=bids, asks=asks,
    )


async def fetch_snapshot(settings: OrderBookSettings, client: httpx.AsyncClient) -> dict[str, Any]:
    response = await client.get(settings.snapshot_url, params={"symbol": settings.binance_orderbook_symbol,
                                                               "limit": settings.binance_orderbook_snapshot_limit})
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("Binance REST snapshot was not an object")
    return data


class BinanceOrderBookReconstructor:
    """Buffer-before-snapshot, bridge, strict gap recovery, and interval publishing."""

    def __init__(self, settings: OrderBookSettings, consumer: ManualKafkaConsumer,
                 producer: JsonKafkaProducer, http_client: httpx.AsyncClient | None = None) -> None:
        self.settings, self.consumer, self.producer = settings, consumer, producer
        self.book = LocalOrderBook()
        self.buffer: deque[tuple[DepthEvent, datetime]] = deque()
        self.http_client = http_client
        self.snapshots_fetched = self.deltas_received = self.deltas_applied = 0
        self.stale_ignored = self.gaps_detected = self.resync_count = 0
        self.invalid_books = self.published = 0
        self._last_publish = 0.0

    async def _fetch_and_rebuild(self) -> None:
        client = self.http_client or httpx.AsyncClient(timeout=10)
        close = self.http_client is None
        try:
            data = await fetch_snapshot(self.settings, client)
            self.book.initialize(data)
            self.snapshots_fetched += 1
            log.info("orderbook_snapshot_fetched", last_update_id=self.book.last_update_id)
            buffered = list(self.buffer)
            self.buffer.clear()
            for event, received in buffered:
                if event.final_update_id <= self.book.last_update_id:
                    self.stale_ignored += 1
                    continue
                if not self.book.synchronized:
                    if not self.book.bridge(event):
                        self.gaps_detected += 1
                        log.error("orderbook_sequence_gap", previous=self.book.last_update_id,
                                  first=event.first_update_id, final=event.final_update_id)
                        self.buffer.append((event, received))
                        self.book.synchronized = False
                        return
                else:
                    self.book.apply(event)
                self.deltas_applied += 1
        finally:
            if close:
                await client.aclose()

    async def process_raw(self, raw: dict[str, Any], received: datetime | None = None) -> None:
        received = received or datetime.now(timezone.utc)
        event = parse_depth_event(raw)
        self.deltas_received += 1
        if self.book.last_update_id is None:
            self.buffer.append((event, received))
            return
        if not self.book.synchronized:
            if event.final_update_id <= self.book.last_update_id:
                self.stale_ignored += 1
                return
            if self.book.bridge(event):
                self.deltas_applied += 1
                await self._publish_if_due(event, received)
                return
            self.gaps_detected += 1
            self.buffer.clear()
            self.buffer.append((event, received))
            log.error("orderbook_sequence_gap", previous=self.book.last_update_id,
                      first=event.first_update_id, final=event.final_update_id)
            await self._fetch_and_rebuild()
            self.resync_count += 1
            return
        if event.final_update_id <= self.book.last_update_id:
            self.stale_ignored += 1
            return
        try:
            self.book.apply(event)
            self.deltas_applied += 1
        except SequenceGap:
            self.gaps_detected += 1
            self.book.synchronized = False
            self.buffer.clear()
            self.buffer.append((event, received))
            log.error("orderbook_sequence_gap", previous=self.book.last_update_id,
                      first=event.first_update_id, final=event.final_update_id)
            await self._fetch_and_rebuild()
            self.resync_count += 1
            return
        await self._publish_if_due(event, received)

    async def _publish_if_due(self, event: DepthEvent, received: datetime) -> None:
        now = asyncio.get_running_loop().time()
        if now - self._last_publish < self.settings.orderbook_publish_interval_ms / 1000:
            return
        try:
            snapshot = build_snapshot(self.book, event, received, self.settings.orderbook_publish_depth)
        except ValueError as error:
            self.invalid_books += 1
            self.book.synchronized = False
            log.error("orderbook_invalid_book", error=str(error))
            return
        await self.producer.publish(CLEAN_TOPIC, "BTC-USDT", snapshot.model_dump(mode="json"))
        self._last_publish = now
        self.published += 1
        if self.published % 100 == 0:
            log.info("orderbook_progress", snapshots=self.published, raw=self.deltas_received,
                     gaps=self.gaps_detected, resyncs=self.resync_count, invalid=self.invalid_books)

    async def bootstrap(self) -> None:
        await self._fetch_and_rebuild()
        if self.book.synchronized:
            log.info("orderbook_synchronized", sequence_id=self.book.last_update_id)

    async def run(self, stop: asyncio.Event) -> None:
        await self.bootstrap()
        while not stop.is_set():
            message = await self.consumer.poll()
            if message is None:
                continue
            try:
                raw = json.loads(message.value().decode("utf-8"))
                await self.process_raw(raw, datetime.now(timezone.utc))
            except (ValueError, UnicodeError) as error:
                log.error("orderbook_raw_message_invalid", error=str(error))
                # malformed raw records do not mutate state; they can be committed here.
            await self.consumer.commit(message)


async def run_reconstructor(duration: float | None) -> None:
    settings = OrderBookSettings()
    consumer = ManualKafkaConsumer(settings.kafka_bootstrap_servers, RAW_TOPIC, "orderbook-reconstructor-v1")
    producer = JsonKafkaProducer(settings.kafka_bootstrap_servers)
    reconstructor = BinanceOrderBookReconstructor(settings, consumer, producer)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    timer = loop.call_later(duration, stop.set) if duration else None
    try:
        await reconstructor.run(stop)
    finally:
        if timer: timer.cancel()
        for sig in (signal.SIGINT, signal.SIGTERM): loop.remove_signal_handler(sig)
        await consumer.close(); await producer.close()
        log.info("orderbook_reconstructor_stopped", snapshots_fetched=reconstructor.snapshots_fetched,
                 deltas_received=reconstructor.deltas_received, deltas_applied=reconstructor.deltas_applied,
                 stale_ignored=reconstructor.stale_ignored, gaps_detected=reconstructor.gaps_detected,
                 resync_count=reconstructor.resync_count, invalid_books=reconstructor.invalid_books,
                 published=reconstructor.published)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--duration", type=float)
    args = parser.parse_args()
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0): parser.error("duration must be positive")
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True), structlog.processors.JSONRenderer()])
    try:
        asyncio.run(run_reconstructor(args.duration))
    except Exception as error:
        log.error("orderbook_reconstructor_failed", error_type=type(error).__name__, error=str(error)); return 1
    return 0


if __name__ == "__main__": raise SystemExit(main())
