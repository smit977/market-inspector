"""Bounded at-least-once persistence: a DLQ write never substitutes for insertion."""
import asyncio
import json
import os
import signal
from collections import defaultdict
from datetime import datetime, timezone

import structlog

from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.storage.clickhouse import ClickHouseSettings, ClickHouseStore
from integrity_engine.storage.models import EVENT_TOPIC_TABLE, event_to_row

log = structlog.get_logger(__name__)
TOPICS = list(EVENT_TOPIC_TABLE)


class StorageBatchError(RuntimeError):
    """Input remains unacknowledged and must replay after recovery."""


def storage_dlq(message, event_id, table, error):
    # Exception text can contain connection URLs, credentials, or event contents.
    # Use safe diagnostics and coordinates; Kafka retains the original event.
    return {
        "source_topic": message.topic(), "source_partition": message.partition(),
        "source_offset": message.offset(), "event_id": event_id, "target_table": table,
        "processor": "clickhouse-writer-v1", "processor_name": "clickhouse-writer-v1",
        "error_type": type(error).__name__,
        "error_message": "Storage mapping or insert failed; source offset retained for recovery",
        "failed_timestamp": datetime.now(timezone.utc).isoformat(),
    }


class ClickHouseWriter:
    def __init__(self, store, consumer, producer, batch_size=500, flush_interval_ms=1000,
                 max_retries=3, retry_backoff_ms=500):
        settings = ClickHouseSettings(batch_size=batch_size, flush_interval_ms=flush_interval_ms,
                                      max_retries=max_retries, retry_backoff_ms=retry_backoff_ms)
        self.store, self.consumer, self.producer = store, consumer, producer
        self.batch_size, self.flush_interval = settings.batch_size, settings.flush_interval_ms / 1000
        self.max_retries, self.retry_backoff = settings.max_retries, settings.retry_backoff_ms / 1000
        self.events_consumed = self.rows_inserted = self.batches_inserted = 0
        self.insert_failures = self.retries = self.dlq_records = 0
        self.last_successful_insert_at = None

    def summary(self):
        return {"events_consumed": self.events_consumed, "rows_inserted": self.rows_inserted,
                "batches_inserted": self.batches_inserted, "insert_failures": self.insert_failures,
                "retries": self.retries, "DLQ_records": self.dlq_records,
                "last_successful_insert_at": self.last_successful_insert_at}

    async def _insert(self, table, rows):
        for attempt in range(self.max_retries + 1):
            try:
                await asyncio.to_thread(self.store.insert_rows, table, rows)
                return
            except Exception:
                self.insert_failures += 1
                if attempt == self.max_retries:
                    raise
                self.retries += 1
                await asyncio.sleep(min(self.retry_backoff * 2 ** attempt, 60))

    async def _dead_letter(self, failed):
        records = []
        for message, event_id, table, error in failed:
            key = f"{message.topic()}:{message.partition()}:{message.offset()}"
            records.append(("system.dlq", key, storage_dlq(message, event_id, table, error)))
        await self.producer.publish_batch(records)
        self.dlq_records += len(records)

    async def flush(self, messages):
        if not messages:
            return
        self.events_consumed += len(messages)
        grouped, origins = defaultdict(list), defaultdict(list)
        failed = []
        for message in messages:
            event_id = None
            try:
                event = json.loads(message.value())
                if isinstance(event, dict) and event.get("event_id") is not None:
                    event_id = str(event["event_id"])
                table, row = event_to_row(message.topic(), event, message)
                grouped[table].append(row)
                origins[table].append((message, event_id))
            except Exception as error:
                failed.append((message, event_id, EVENT_TOPIC_TABLE.get(message.topic()), error))
        if failed:
            await self._dead_letter(failed)
            raise StorageBatchError("mapping failed; batch offsets retained")
        for table, rows in grouped.items():
            try:
                await self._insert(table, rows)
                self.rows_inserted += len(rows)
                self.batches_inserted += 1
                self.last_successful_insert_at = datetime.now(timezone.utc).isoformat()
            except Exception as error:
                await self._dead_letter([(message, event_id, table, error) for message, event_id in origins[table]])
                raise StorageBatchError("insert retries exhausted; batch offsets retained") from None
        # Conservative whole-batch acknowledgement: no failed record can be skipped.
        # Earlier tables in a partially successful batch may replay with identical IDs.
        await self.consumer.commit_batch(messages)

    async def run(self, duration=None):
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        timer = loop.call_later(duration, stop.set) if duration is not None else None
        pending = []
        deadline = None
        next_log = loop.time() + 10
        try:
            await asyncio.to_thread(self.store.initialize)
            while not stop.is_set():
                batch = await self.consumer.poll_batch()
                if batch and not pending:
                    deadline = loop.time() + self.flush_interval
                pending.extend(batch)
                while len(pending) >= self.batch_size:
                    await self.flush(pending[:self.batch_size])
                    del pending[:self.batch_size]
                    if not pending:
                        deadline = None
                if pending and loop.time() >= deadline:
                    await self.flush(pending)
                    pending.clear()
                    deadline = None
                if loop.time() >= next_log:
                    # Read-only lag inspection, serialized with consumer polling.
                    lag = await self.consumer.lag_snapshot()
                    log.info("clickhouse_writer_progress", **self.summary(), consumer_lag=lag)
                    next_log = loop.time() + 10
            if pending:
                await self.flush(pending)
        finally:
            if timer:
                timer.cancel()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
            try:
                await self.consumer.close()
            finally:
                try:
                    self.store.close()
                finally:
                    try:
                        await self.producer.close()
                    finally:
                        log.info("clickhouse_writer_stopped", **self.summary())


async def run_from_env(duration=None):
    settings = ClickHouseSettings()
    if not settings.enabled:
        log.info("clickhouse_writer_disabled")
        return
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    store = ClickHouseStore(settings=settings)
    consumer = ManualKafkaConsumer(bootstrap, TOPICS, os.getenv("CLICKHOUSE_CONSUMER_GROUP_ID", "clickhouse-writer-v1"))
    producer = JsonKafkaProducer(bootstrap)
    writer = ClickHouseWriter(store, consumer, producer, settings.batch_size, settings.flush_interval_ms,
                              settings.max_retries, settings.retry_backoff_ms)
    await writer.run(duration)
