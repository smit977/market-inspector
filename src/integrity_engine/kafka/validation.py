"""Quarantine invalid inputs without acknowledging failed deliveries."""
import json

from pydantic import ValidationError

from integrity_engine.normalization.base import make_dlq_event


async def validated_event(message, schema, producer, consumer, processor):
    original = None
    try:
        original = json.loads(message.value())
        return schema.model_validate(original)
    except (ValidationError, json.JSONDecodeError, UnicodeError) as error:
        if original is None:
            original = message.value().decode("utf-8", errors="backslashreplace")
        record = make_dlq_event(original, message, error)
        record["processor_name"] = processor
        key = f"{message.topic()}:{message.partition()}:{message.offset()}"
        await producer.publish("system.dlq", key, record)
        await consumer.commit(message)
        return None
