import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from integrity_engine.storage.clickhouse import ClickHouseStore
from integrity_engine.storage.models import EVENT_TOPIC_TABLE, TABLE_COLUMNS, TABLE_DDL, event_to_row
from integrity_engine.storage.writer import ClickHouseWriter


def msg(topic, offset=1, payload=None, partition=0):
    return SimpleNamespace(topic=lambda: topic, offset=lambda: offset, partition=lambda: partition,
                           value=lambda: json.dumps(payload or {
                               "event_id": str(uuid4()), "calculated_at": "2026-09-25T12:00:00+00:00",
                               "exchange": "binance", "symbol": "BTC-USDT", "window": "30s",
                           }).encode())


def test_routes_all_curated_topics():
    assert set(EVENT_TOPIC_TABLE) == {"market.metrics", "market.metrics.cross_venue", "market.metrics.derivatives", "market.anomalies", "market.integrity"}


def test_market_metrics_preserves_decimal_and_nullable_values():
    event = {"event_id": str(uuid4()), "calculated_at": "2026-09-25T12:00:00+00:00", "exchange": "binance", "symbol": "BTC-USDT", "window": "30s", "price_return": "0.000000000000000123", "data_quality_flags": ["missing_orderbook"]}
    table, row = event_to_row("market.metrics", event)
    assert table == "market_metrics"
    assert row[TABLE_COLUMNS[table].index("price_return")] == Decimal("0.000000000000000123")
    assert row[TABLE_COLUMNS[table].index("best_bid")] is None
    assert row[TABLE_COLUMNS[table].index("data_quality_flags")] == ["missing_orderbook"]


def test_cross_venue_structures_are_machine_readable_json():
    event = {"event_id": str(uuid4()), "calculated_at": "2026-09-25T12:00:00+00:00", "canonical_pair": "BTC-USD", "window": "1m", "active_venues": ["binance"], "venue_prices": {"binance": "100"}}
    table, row = event_to_row("market.metrics.cross_venue", event)
    assert table == "cross_venue_metrics"
    assert json.loads(row[TABLE_COLUMNS[table].index("venue_prices")]) == {"binance": "100"}
    assert row[TABLE_COLUMNS[table].index("active_venue_count")] == 1


def test_anomaly_and_integrity_ids_are_not_regenerated():
    anomaly_id, trigger_id = str(uuid4()), str(uuid4())
    anomaly = {"event_id": anomaly_id, "detected_at": "2026-09-25T12:00:00+00:00", "exchange": "binance", "symbol": "BTC-USDT", "event_type": "move", "severity": "high", "window": "30s", "evidence": [{"metric": "x"}]}
    _, arow = event_to_row("market.anomalies", anomaly)
    integrity = {"event_id": str(uuid4()), "calculated_at": "2026-09-25T12:00:00+00:00", "canonical_pair": "BTC-USD", "trigger_event_id": trigger_id, "integrity_score": "42.1234", "overall_confidence": "0.5", "evidence_coverage": "0.75", "classification_confidence": "0.6"}
    _, irow = event_to_row("market.integrity", integrity)
    assert arow[0] == anomaly_id and irow[3] == trigger_id


def test_schema_initialization_is_idempotent():
    commands = []
    store = ClickHouseStore(client=SimpleNamespace(command=commands.append))
    store.initialize(); store.initialize()
    assert sum("CREATE DATABASE" in c for c in commands) == 2
    assert all("CREATE TABLE IF NOT EXISTS" in c for c in commands if "CREATE TABLE" in c)
    assert set(TABLE_DDL) == set(TABLE_COLUMNS)


def test_query_selects_database_without_unsupported_keyword():
    class Client:
        database = "default"
        def query(self, sql):
            assert self.database == "market_integrity"
            assert sql == "SELECT 1"
            return 1
    assert ClickHouseStore(client=Client()).query("SELECT 1") == 1


class FakeStore:
    def __init__(self, fail=False): self.fail, self.calls = fail, []
    def insert_rows(self, table, rows):
        self.calls.append((table, rows))
        if self.fail: raise RuntimeError("insert unavailable")
    def initialize(self): pass
    def close(self): pass


class FakeConsumer:
    def __init__(self): self.committed = []
    async def commit_batch(self, messages): self.committed.extend(messages)


class FakeProducer:
    def __init__(self): self.records = []
    async def publish_batch(self, records): self.records.extend(records)
    async def close(self): pass


@pytest.mark.asyncio
async def test_batch_insert_commits_only_after_success():
    consumer, producer, store = FakeConsumer(), FakeProducer(), FakeStore()
    writer = ClickHouseWriter(store, consumer, producer, max_retries=0)
    await writer.flush([msg("market.metrics")])
    assert len(store.calls) == 1 and len(consumer.committed) == 1
    assert writer.rows_inserted == 1


@pytest.mark.asyncio
async def test_failed_insert_is_retried_and_sent_to_dlq_before_commit():
    consumer, producer, store = FakeConsumer(), FakeProducer(), FakeStore(fail=True)
    writer = ClickHouseWriter(store, consumer, producer, max_retries=1)
    await writer.flush([msg("market.metrics", offset=11)])
    assert len(store.calls) == 2
    assert len(producer.records) == 1 and producer.records[0][0] == "system.dlq"
    assert len(consumer.committed) == 1 and writer.dlq_records == 1
