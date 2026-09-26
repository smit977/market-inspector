import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from integrity_engine.kafka.validation import validated_event
from integrity_engine.schemas.metrics import MarketMetrics


@pytest.mark.parametrize("payload", [b'{', b'\xff', b'null', json.dumps({"event_id": "old-record"}).encode()])
async def test_invalid_metrics_delivered_to_dlq_before_commit(payload):
    message = MagicMock()
    message.value.return_value = payload
    producer, consumer = AsyncMock(), AsyncMock()
    actions = []
    producer.publish.side_effect = lambda *args: actions.append("dlq")
    consumer.commit.side_effect = lambda *args: actions.append("commit")
    result = await validated_event(message, MarketMetrics, producer, consumer, "detector")
    assert result is None
    assert actions == ["dlq", "commit"]
    assert producer.publish.call_args.args[2]["processor_name"] == "detector"


async def test_dlq_failure_does_not_commit_invalid_metrics():
    message = MagicMock()
    message.value.return_value = b'{}'
    producer, consumer = AsyncMock(), AsyncMock()
    producer.publish.side_effect = RuntimeError("delivery failed")
    with pytest.raises(RuntimeError, match="delivery failed"):
        await validated_event(message, MarketMetrics, producer, consumer, "detector")
    consumer.commit.assert_not_awaited()
