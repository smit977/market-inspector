"""Commit failures must preserve acknowledged offsets until durable commit."""
import asyncio
from unittest.mock import MagicMock
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from integrity_engine.kafka import consumer as module


def consumer(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(module, "Consumer", lambda config: client)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    result = module.ManualKafkaConsumer("broker", "topic", "worker")
    result.tracker.ready = {("topic", 0): 12}
    return result, client


@pytest.mark.parametrize("per_partition", [False, True])
def test_transient_commit_recovers_without_advancing_offset(monkeypatch, per_partition):
    worker, client = consumer(monkeypatch)
    error = KafkaError(KafkaError.COORDINATOR_LOAD_IN_PROGRESS)
    failure = [SimpleNamespace(error=error)] if per_partition else KafkaException(error)
    client.commit.side_effect = [failure, []]
    worker._flush()
    assert client.commit.call_count == 2
    assert all(call.kwargs["offsets"][0].offset == 12 for call in client.commit.call_args_list)
    assert not worker.tracker.ready


@pytest.mark.parametrize("code,attempts", [(KafkaError.COORDINATOR_LOAD_IN_PROGRESS, 6),
                                          (KafkaError.TOPIC_AUTHORIZATION_FAILED, 1)])
def test_failed_commit_retains_offsets_and_bounds_retries(monkeypatch, code, attempts):
    worker, client = consumer(monkeypatch)
    client.commit.side_effect = KafkaException(KafkaError(code))
    with pytest.raises(KafkaException):
        worker._flush()
    assert client.commit.call_count == attempts
    assert worker.tracker.ready == {("topic", 0): 12}


async def test_lag_snapshot_is_read_only(monkeypatch):
    from confluent_kafka import TopicPartition
    worker, client = consumer(monkeypatch)
    client.assignment.return_value = [TopicPartition("topic", 0)]
    client.committed.return_value = [TopicPartition("topic", 0, 12)]
    client.position.return_value = [TopicPartition("topic", 0, 15)]
    client.get_watermark_offsets.return_value = (0, 17)
    assert await worker.lag_snapshot() == {"topic:0": 5}
    client.commit.assert_not_called()
    assert worker.tracker.ready == {("topic", 0): 12}


async def test_lag_failure_is_unavailable_not_a_processing_failure(monkeypatch):
    worker, client = consumer(monkeypatch)
    client.assignment.side_effect = RuntimeError("broker unavailable")
    assert await worker.lag_snapshot() is None
    client.commit.assert_not_called()


async def test_sustained_empty_assignment_recreates_consumer(monkeypatch):
    monkeypatch.setenv("PROCESSING_MODE", "live")
    monkeypatch.setenv("PROCESSING_RUN_ID", "current")
    old, fresh = MagicMock(), MagicMock()
    old.assignment.return_value = []
    old.consume.return_value = []
    monkeypatch.setattr(module, "Consumer", MagicMock(side_effect=[old, fresh]))
    worker = module.ManualKafkaConsumer("broker", ["topic", "other"], "worker",
                                        assignment_recovery_seconds=30)
    worker._last_assignment_activity = 0
    monkeypatch.setattr(module.time, "monotonic", lambda: 31)

    assert await worker.poll_batch() == []

    old.close.assert_called_once_with()
    assert worker._consumer is fresh
    fresh.subscribe.assert_called_once()
    assert fresh.subscribe.call_args.args[0] == ["topic", "other"]
    assert worker._consumer_config["group.id"] == "worker-live-current"


async def test_assigned_consumer_is_not_recreated_when_idle(monkeypatch):
    monkeypatch.setenv("PROCESSING_MODE", "live")
    monkeypatch.setenv("PROCESSING_RUN_ID", "current")
    client = MagicMock()
    client.assignment.return_value = [object()]
    client.consume.return_value = []
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(module, "Consumer", factory)
    worker = module.ManualKafkaConsumer("broker", "topic", "worker",
                                        assignment_recovery_seconds=30)
    monkeypatch.setattr(module.time, "monotonic", lambda: 31)
    worker._last_assignment_activity = 0

    assert await worker.poll_batch() == []

    assert factory.call_count == 1
    client.close.assert_not_called()
