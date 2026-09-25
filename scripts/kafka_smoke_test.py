"""One fake trade round trip. Run with Python inside the app container."""

import json
import os
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, Producer


TOPIC = "market.raw.trades"
TIMEOUT_SECONDS = 30


def main() -> int:
    consumer = None
    try:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        if not bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")

        event = {
            "exchange": "test",
            "symbol": "BTC-USDT",
            "event_type": "trade",
            "price": 100000.0,
            "quantity": 0.001,
            "event_id": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "message.timeout.ms": TIMEOUT_SECONDS * 1000,
        })
        metadata = producer.list_topics(timeout=TIMEOUT_SECONDS)
        topic = metadata.topics.get(TOPIC)
        if topic is None or topic.error is not None:
            raise RuntimeError(f"Topic {TOPIC} is missing or unavailable")
        print("PASS: producer connected successfully")

        delivery = []

        def on_delivery(error, message):
            delivery.append((error, message))

        producer.produce(
            TOPIC,
            key=event["event_id"].encode("utf-8"),
            value=json.dumps(event, allow_nan=False).encode("utf-8"),
            on_delivery=on_delivery,
        )
        if producer.flush(TIMEOUT_SECONDS + 5) or not delivery:
            raise TimeoutError("Producer did not receive delivery confirmation")
        error, written = delivery[0]
        if error is not None:
            raise KafkaException(error)
        print(f"PASS: event written to {TOPIC} "
              f"partition={written.partition()} offset={written.offset()}")

        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": "market-integrity-smoke-test",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        consumer.subscribe([TOPIC])
        deadline = time.monotonic() + TIMEOUT_SECONDS
        connected = False
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=1.0)
            if consumer.assignment() and not connected:
                print("PASS: consumer connected and assigned topic partitions")
                connected = True
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            # Skip unrelated records, including prior smoke-test runs.
            if message.key() != event["event_id"].encode("utf-8"):
                continue
            consumed = json.loads(message.value().decode("utf-8"))
            if consumed != event:
                raise RuntimeError("Consumed event does not match the produced event")
            consumer.commit(message=message, asynchronous=False)
            print("PASS: same event consumed successfully")
            print(json.dumps(consumed, indent=2, allow_nan=False))
            return 0
        raise TimeoutError(f"Did not consume event {event['event_id']} within "
                           f"{TIMEOUT_SECONDS} seconds")
    except (KafkaException, ValueError, RuntimeError, TimeoutError,
            BufferError, UnicodeError, AttributeError) as error:
        print(f"FAIL: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    sys.exit(main())
