"""Bounded polling and partition-contiguous explicit acknowledgements."""
import asyncio
import os
import time
from collections import OrderedDict, deque
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

class OffsetTracker:
    """Contiguity is over delivered records; Kafka numeric offsets may have gaps."""
    def __init__(self):
        self.pending = {}
        self.ready = {}
    def observe(self, message):
        key = (message.topic(), message.partition())
        self.pending.setdefault(key, OrderedDict()).setdefault(message.offset(), False)
    def acknowledge(self, message):
        key = (message.topic(), message.partition())
        rows = self.pending.get(key, {})
        if message.offset() not in rows:
            raise RuntimeError("cannot acknowledge an unobserved offset")
        rows[message.offset()] = True
        while rows and next(iter(rows.values())):
            offset, _ = rows.popitem(last=False)
            self.ready[key] = offset + 1
    def offsets(self):
        return [TopicPartition(t, p, o) for (t, p), o in self.ready.items()]

class ManualKafkaConsumer:
    def __init__(self, bootstrap_servers: str, topic: str | list[str], group_id: str,
                 auto_offset_reset: str = "earliest") -> None:
        if not bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
        mode = os.getenv("PROCESSING_MODE", "replay")
        if mode not in {"live", "replay"}:
            raise ValueError("PROCESSING_MODE must be live or replay")
        if mode == "live":
            group_id += "-live-" + os.getenv("PROCESSING_RUN_ID", "v2")
            auto_offset_reset = "latest"
        self.group_id = group_id
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers, "group.id": group_id,
            "auto.offset.reset": auto_offset_reset, "enable.auto.commit": False,
            "enable.auto.offset.store": False, "allow.auto.create.topics": False,
            "queued.max.messages.kbytes": 16384,
        })
        self.tracker = OffsetTracker()
        self.buffer = deque()
        self.batch_size = 256
        self.handled = self.consumed = 0
        self._since_commit = 0
        self._last_commit = self._last_health = time.monotonic()
        self._consumer.subscribe([topic] if isinstance(topic, str) else topic,
                                 on_revoke=self._revoke)
        structlog.get_logger().info("consumer_started", group=group_id, mode=mode)

    def _flush(self):
        offsets = self.tracker.offsets()
        if not offsets:
            return
        for attempt in range(6):
            try:
                committed = self._consumer.commit(offsets=offsets, asynchronous=False)
                for offset in committed or []:
                    if offset.error is not None:
                        raise KafkaException(offset.error)
                break
            except KafkaException as exc:
                error = exc.args[0]
                transient = error.retriable() or error.code() in {
                    KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
                    KafkaError.COORDINATOR_NOT_AVAILABLE,
                    KafkaError.NOT_COORDINATOR,
                }
                if not transient or attempt == 5:
                    raise
                structlog.get_logger().warning("offset_commit_retry", group=self.group_id,
                                               attempt=attempt + 1, error=str(error))
                time.sleep(min(.25 * 2 ** attempt, 2))
        self.tracker.ready.clear()

    def _revoke(self, consumer, partitions):
        self._flush()
        self.tracker = OffsetTracker()
        self.buffer.clear()

    async def poll_batch(self) -> list[Message]:
        if self.buffer:
            messages = list(self.buffer)
            self.buffer.clear()
            return messages
        messages = await asyncio.to_thread(self._consumer.consume, self.batch_size, .1)
        for message in messages:
            if message.error():
                raise KafkaException(message.error())
            self.tracker.observe(message)
        self.consumed += len(messages)
        if time.monotonic() - self._last_health >= 10:
            structlog.get_logger().info("consumer_progress", group=self.group_id,
                                       consumed=self.consumed, handled=self.handled)
            self._last_health = time.monotonic()
        return messages

    async def poll(self) -> Message | None:
        if not self.buffer:
            self.buffer.extend(await self.poll_batch())
        return self.buffer.popleft() if self.buffer else None

    async def commit(self, message: Message) -> None:
        self.tracker.acknowledge(message)
        self.handled += 1
        self._since_commit += 1
        if self._since_commit >= self.batch_size or time.monotonic() - self._last_commit >= .5:
            await self.flush_commits()

    async def commit_batch(self, messages) -> None:
        for message in messages:
            self.tracker.acknowledge(message)
        self.handled += len(messages)
        await self.flush_commits()

    async def flush_commits(self):
        await asyncio.to_thread(self._flush)
        self._since_commit = 0
        self._last_commit = time.monotonic()

    async def close(self) -> None:
        try:
            await self.flush_commits()
        finally:
            await asyncio.to_thread(self._consumer.close)
            structlog.get_logger().info("consumer_stopped", group=self.group_id,
                                       consumed=self.consumed, handled=self.handled)
