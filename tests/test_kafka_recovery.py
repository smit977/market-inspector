"""Commit failures must preserve acknowledged offsets until durable commit."""
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
