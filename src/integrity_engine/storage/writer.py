"""Kafka to ClickHouse persistence with bounded batches and at-least-once commits."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import structlog

from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.storage.clickhouse import ClickHouseStore
from integrity_engine.storage.models import EVENT_TOPIC_TABLE, event_to_row

log = structlog.get_logger(__name__)
TOPICS = list(EVENT_TOPIC_TABLE)


def storage_dlq(message: Any, event_id: str | None, table: str | None, error: Exception) -> dict[str, Any]:
    return {
        "source_topic": message.topic(), "source_partition": message.partition(), "source_offset": message.offset(),
        "event_id": event_id, "target_table": table, "processor_name": "clickhouse-writer-v1",
        "error_type": type(error).__name__, "error_message": str(error)[:500],
        "failed_timestamp": datetime.now(timezone.utc).isoformat(),
    }


class ClickHouseWriter:
    def __init__(self, store: ClickHouseStore, consumer: Any, producer: Any,
                 batch_size: int = 500, flush_interval_ms: int = 1000,
                 max_retries: int = 3) -> None:
        self.store, self.consumer, self.producer = store, consumer, producer
        self.batch_size, self.flush_interval = batch_size, flush_interval_ms / 1000
        self.max_retries = max_retries
        self.events_consumed = self.rows_inserted = self.batches_inserted = 0
        self.insert_failures = self.retries = self.dlq_records = 0
        self.last_successful_insert: str | None = None

    async def _insert(self, table: str, rows: list[list[Any]]) -> None:
        for attempt in range(self.max_retries + 1):
            try:
                await asyncio.to_thread(self.store.insert_rows, table, rows)
                return
            except Exception:
                self.insert_failures += 1
                if attempt >= self.max_retries:
                    raise
                self.retries += 1
                await asyncio.sleep(min(2 ** attempt * .1, 1.0))

    async def flush(self, messages: list[Any]) -> None:
        if not messages:
            return
        grouped: dict[str, list[list[Any]]] = defaultdict(list)
        table_messages: dict[str, list[Any]] = defaultdict(list)
        dead: list[tuple[Any, str | None, str | None, Exception]] = []
        for message in messages:
            try:
                event = json.loads(message.value())
                table, row = event_to_row(message.topic(), event)
                grouped[table].append(row)
                table_messages[table].append(message)
                self.events_consumed += 1
            except Exception as error:
                dead.append((message, None, EVENT_TOPIC_TABLE.get(message.topic()), error))
        failed_tables: set[str] = set()
        for table, rows in grouped.items():
            try:
                await self._insert(table, rows)
                self.rows_inserted += len(rows)
                self.batches_inserted += 1
                self.last_successful_insert = datetime.now(timezone.utc).isoformat()
            except Exception as error:
                failed_tables.add(table)
                for message in table_messages[table]:
                    dead.append((message, None, table, error))
        if dead:
            dlq_records = []
            for message, event_id, table, error in dead:
                try:
                    raw = json.loads(message.value())
                    event_id = str(raw.get("event_id")) if raw.get("event_id") is not None else None
                except Exception:
                    pass
                record = storage_dlq(message, event_id, table, error)
                dlq_records.append(("system.dlq", f"{message.topic()}:{message.partition()}:{message.offset()}", record))
            await self.producer.publish_batch(dlq_records)
            self.dlq_records += len(dlq_records)
        # Every message has either durable ClickHouse output or a durable DLQ record.
        await self.consumer.commit_batch(messages)

    async def run(self, duration: float | None = None) -> None:
        self.store.initialize()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        timer = loop.call_later(duration, stop.set) if duration else None
        pending: list[Any] = []
        deadline = loop.time() + self.flush_interval
        try:
            while not stop.is_set():
                batch = await self.consumer.poll_batch()
                pending.extend(batch)
                if len(pending) >= self.batch_size or (pending and loop.time() >= deadline):
                    await self.flush(pending)
                    pending.clear()
                    deadline = loop.time() + self.flush_interval
                elif not batch:
                    await asyncio.sleep(.01)
            if pending:
                await self.flush(pending)
        finally:
            if timer:
                timer.cancel()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
            await self.consumer.close()
            self.store.close()
            await self.producer.close()
            log.info("clickhouse_writer_stopped", events_consumed=self.events_consumed,
                     rows_inserted=self.rows_inserted, batches_inserted=self.batches_inserted,
                     insert_failures=self.insert_failures, retries=self.retries, dlq_records=self.dlq_records,
                     last_successful_insert=self.last_successful_insert)


async def run_from_env(duration: float | None = None) -> None:
    if os.getenv("CLICKHOUSE_ENABLED", "true").lower() not in {"1", "true", "yes"}:
        log.info("clickhouse_writer_disabled")
        return
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    store = ClickHouseStore()
    consumer = ManualKafkaConsumer(bootstrap, TOPICS, os.getenv("CLICKHOUSE_CONSUMER_GROUP_ID", "clickhouse-writer-v1"))
    producer = JsonKafkaProducer(bootstrap)
    writer = ClickHouseWriter(store, consumer, producer, int(os.getenv("CLICKHOUSE_BATCH_SIZE", "500")), int(os.getenv("CLICKHOUSE_FLUSH_INTERVAL_MS", "1000")))
    await writer.run(duration)
