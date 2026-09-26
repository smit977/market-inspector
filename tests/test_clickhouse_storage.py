"""Storage correctness, migration and partition-aware delivery guarantees (offline)."""
import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from integrity_engine.kafka.consumer import OffsetTracker
from integrity_engine.storage.clickhouse import ClickHouseSettings, ClickHouseStore
from integrity_engine.storage.models import EVENT_TOPIC_TABLE, TABLE_COLUMNS, TABLE_DDL, TABLE_TYPES, DECIMAL, HIGH_SCALE_DECIMAL, event_to_row
from integrity_engine.storage.writer import ClickHouseWriter, StorageBatchError, storage_dlq
from tests.test_detection import metric
from tests.test_integrity import anomaly, cross, deriv, spot
from integrity_engine.integrity.scoring import score_integrity


def event(topic="market.metrics"):
    models = {"market.metrics": metric(), "market.metrics.cross_venue": cross(),
              "market.metrics.derivatives": deriv(), "market.anomalies": anomaly(),
              "market.integrity": score_integrity(anomaly(), spot(), cross(), deriv())}
    return models[topic].model_dump(mode="json")


def msg(topic="market.metrics", offset=1, payload=None, partition=0):
    data = event(topic) if payload is None else payload
    return SimpleNamespace(topic=lambda: topic, offset=lambda: offset, partition=lambda: partition,
                           value=lambda: json.dumps(data).encode())


def row_dict(topic, data):
    table, row = event_to_row(topic, data)
    return dict(zip(TABLE_COLUMNS[table], row))


def test_configuration_defaults_and_secret_redaction(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "test-only-secret")
    settings = ClickHouseSettings()
    assert settings.batch_size == 500 and settings.max_retries == 3
    assert settings.retry_backoff_ms == 500 and settings.http_port == 8123
    assert "test-only-secret" not in repr(settings)


@pytest.mark.parametrize("config", [{"batch_size": 0}, {"flush_interval_ms": 0}, {"max_retries": -1},
                                    {"retry_backoff_ms": -1}, {"database": "bad`; DROP TABLE x"}, {"http_port": 0}])
def test_configuration_rejects_invalid_values(config):
    with pytest.raises(ValidationError): ClickHouseSettings(**config)


def test_http_client_uses_configured_http_port(monkeypatch):
    import clickhouse_connect
    factory = MagicMock()
    monkeypatch.setattr(clickhouse_connect, "get_client", factory)
    ClickHouseStore(http_port=8124, port=9001, user="reader", password="test-only")
    assert factory.call_args.kwargs["port"] == 8124
    assert factory.call_args.kwargs["password"] == "test-only"


@pytest.mark.parametrize("topic,table", EVENT_TOPIC_TABLE.items())
def test_routes_and_preserves_identifiers(topic, table):
    data = event(topic)
    actual, row = event_to_row(topic, data)
    assert actual == table and row[0] == data['event_id']
    assert len(row) == len(TABLE_COLUMNS[table])
    assert json.loads(row[TABLE_COLUMNS[table].index('original_event')]) == data


def test_only_curated_topics():
    assert len(EVENT_TOPIC_TABLE) == 5
    with pytest.raises(KeyError): event_to_row('market.raw.trades', event())


def test_decimal_precision_and_nullable_values():
    data = event(); value = '12345678901234567890.1234567890123456789012345678'
    data.update(quote_volume=value, price_acceleration=None, trade_count=None)
    row = row_dict('market.metrics', data)
    assert row['quote_volume'] == Decimal(value)
    assert row['price_acceleration'] is None and row['trade_count'] is None
    assert TABLE_TYPES['market_metrics']['quote_volume'] == DECIMAL


def test_tiny_open_interest_percentage_keeps_full_scale():
    value = "0." + "0" * 35 + "123456789012345"
    data = event("market.metrics.derivatives")
    data["open_interest_change_pct"] = value
    row = row_dict("market.metrics.derivatives", data)
    assert row["open_interest_change_pct"] == Decimal(value)
    assert TABLE_TYPES["derivatives_metrics"]["open_interest_change_pct"] == HIGH_SCALE_DECIMAL
    data["open_interest_change_pct"] = value + "123456"
    with pytest.raises(ValueError, match="50 fractional digits"):
        row_dict("market.metrics.derivatives", data)


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '1e39', '1e-39', 0.1, True])
def test_inexact_or_unrepresentable_financial_values_rejected(value):
    data = event();data['quote_volume'] = value
    with pytest.raises(ValueError):row_dict('market.metrics',data)


def test_timestamp_utc_conversion_and_microseconds():
    data = event();data['calculated_at'] = '2026-09-26T12:30:00.123456+02:00'
    row = row_dict('market.metrics', data)
    assert row['timestamp'] == datetime(2026,9,26,10,30,0,123456,tzinfo=timezone.utc)
    assert row['source_timestamp'] == row['timestamp']


def test_naive_timestamp_rejected():
    data = event();data['calculated_at'] = '2026-09-26T12:30:00'
    with pytest.raises(ValueError, match='timezone'):row_dict('market.metrics', data)


@pytest.mark.parametrize('topic,field', [('market.anomalies','evidence'),('market.integrity','components'),
                                       ('market.metrics.cross_venue','venue_prices')])
def test_nested_evidence_roundtrips(topic, field):
    data = event(topic);row = row_dict(topic, data)
    assert json.loads(row[field]) == data[field]


def test_all_required_new_fields_preserved():
    examples = {'market.metrics': ['price_range','average_trade_size','largest_trade_size','price_acceleration'],
                'market.metrics.cross_venue': ['median_return','return_dispersion'],
                'market.metrics.derivatives': ['perpetual_base_volume','perpetual_trade_imbalance',
                    'funding_rate_change','basis_change','spot_perpetual_return_difference']}
    for topic, fields in examples.items():
        data = event(topic)
        for field in fields:data[field] = '0.1234567890123456789012345678'
        row = row_dict(topic,data)
        assert all(row[field] == Decimal(data[field]) for field in fields)


def test_source_coordinates_and_synthetic_label():
    data=event();data['_storage_validation']=True
    table,row=event_to_row('market.metrics',data,msg(offset=17,partition=2))
    result=dict(zip(TABLE_COLUMNS[table],row))
    assert (result['source_partition'],result['source_offset'],result['is_synthetic']) == (2,17,True)


class SchemaClient:
    def __init__(self, legacy=False):self.commands=[];self.legacy=legacy
    def command(self, sql):self.commands.append(sql)
    def query(self, sql):
        table=sql.split('`')[1]
        types=dict(TABLE_TYPES[table])
        if self.legacy:
            types.pop('original_event')
            if 'quote_volume' in types:types['quote_volume']='Nullable(Decimal(38, 18))'
            if 'open_interest_change_pct' in types:types['open_interest_change_pct']='Nullable(Decimal(76, 38))'
        return SimpleNamespace(result_rows=list(types.items()))


def test_schema_initialization_is_idempotent():
    client=SchemaClient();store=ClickHouseStore(client=client)
    store.initialize();store.initialize()
    assert len(client.commands)==12
    assert all('IF NOT EXISTS' in cmd for cmd in client.commands)
    assert not any('DROP' in cmd or 'TRUNCATE' in cmd for cmd in client.commands)


def test_existing_schema_migrates_by_adding_and_widening_only():
    client=SchemaClient(legacy=True);ClickHouseStore(client=client).initialize()
    alters=[c for c in client.commands if c.startswith('ALTER')]
    assert len(alters)==5
    assert all('ADD COLUMN IF NOT EXISTS `original_event` String' in c for c in alters)
    assert any('MODIFY COLUMN `quote_volume` Nullable(Decimal(76, 38))' in c for c in alters)
    assert any('MODIFY COLUMN `open_interest_change_pct` Nullable(Decimal(76, 50))' in c for c in alters)
    assert not any('DROP' in c or 'TRUNCATE' in c for c in client.commands)


def test_query_uses_database_without_unsupported_keyword():
    client=MagicMock();client.query.return_value=1
    store=ClickHouseStore(client=client)
    assert store.query('SELECT 1')==1
    client.query.assert_called_once_with('SELECT 1')
    assert client.database=='market_integrity'


class FakeStore:
    def __init__(self, failures=0):self.failures=failures;self.calls=[];self.closed=False
    def insert_rows(self,table,rows):
        self.calls.append((table,rows))
        if self.failures:
            self.failures-=1
            raise RuntimeError('password=test-only-secret')
    def initialize(self):pass
    def close(self):self.closed=True


class FakeConsumer:
    def __init__(self, messages=()):
        self.tracker=OffsetTracker();self.committed=[];self.offsets=[];self.closed=False;self.batches=[]
        for message in messages:self.tracker.observe(message)
    async def commit_batch(self,messages):
        for message in messages:self.tracker.acknowledge(message)
        self.committed.extend(messages)
        self.offsets=self.tracker.offsets()
    async def poll_batch(self):
        await asyncio.sleep(.002)
        return self.batches.pop(0) if self.batches else []
    async def lag_snapshot(self):return {}
    async def close(self):self.closed=True


class FakeProducer:
    def __init__(self):self.records=[];self.closed=False
    async def publish_batch(self,records):self.records.extend(records)
    async def close(self):self.closed=True


async def test_successful_insert_precedes_commit():
    message=msg();consumer=FakeConsumer([message]);store=FakeStore();producer=FakeProducer()
    async def commit(messages):
        assert len(store.calls)==1
        consumer.committed.extend(messages)
    consumer.commit_batch=commit
    writer=ClickHouseWriter(store,consumer,producer)
    await writer.flush([message])
    assert writer.rows_inserted==1 and writer.batches_inserted==1
    assert consumer.committed==[message]


async def test_retry_preserves_rows_and_event_ids():
    message=msg();store=FakeStore(2);consumer=FakeConsumer([message]);producer=FakeProducer()
    writer=ClickHouseWriter(store,consumer,producer,retry_backoff_ms=0)
    await writer.flush([message])
    assert len(store.calls)==3 and store.calls[0]==store.calls[1]==store.calls[2]
    assert writer.retries==2 and writer.insert_failures==2
    assert consumer.committed==[message] and producer.records==[]


async def test_permanent_insert_failure_dlq_never_commits():
    message=msg();store=FakeStore(10);consumer=FakeConsumer([message]);producer=FakeProducer()
    writer=ClickHouseWriter(store,consumer,producer,max_retries=2,retry_backoff_ms=0)
    with pytest.raises(StorageBatchError):await writer.flush([message])
    assert len(store.calls)==3 and consumer.committed==[]
    assert writer.dlq_records==1 and writer.insert_failures==3
    record=producer.records[0][2]
    assert record['event_id']==json.loads(message.value())['event_id']
    assert record['source_offset']==1 and record['target_table']=='market_metrics'
    assert 'test-only-secret' not in json.dumps(record)


async def test_failed_offset_blocks_later_record_in_same_partition():
    first,later,other=msg(offset=10),msg(offset=11),msg(offset=5,partition=1)
    consumer=FakeConsumer([first,later,other]);store=FakeStore(1);producer=FakeProducer()
    writer=ClickHouseWriter(store,consumer,producer,max_retries=0)
    with pytest.raises(StorageBatchError):await writer.flush([first])
    await writer.flush([later,other])
    assert [(p.partition,p.offset) for p in consumer.offsets]==[(1,6)]
    await writer.flush([first])
    assert {(p.partition,p.offset) for p in consumer.offsets}=={(0,12),(1,6)}


async def test_partial_table_success_never_acknowledges_failed_batch():
    messages=[msg(),msg('market.anomalies')];consumer=FakeConsumer(messages);store=FakeStore()
    def insert(table, rows):
        if table=='market_anomalies':raise RuntimeError('unavailable')
    store.insert_rows=insert
    writer=ClickHouseWriter(store,consumer,FakeProducer(),max_retries=0)
    with pytest.raises(StorageBatchError):await writer.flush(messages)
    assert writer.rows_inserted==1 and consumer.committed==[]


@pytest.mark.parametrize('payload',[{},[],None,'invalid'])
async def test_invalid_mapping_dlq_and_no_commit(payload):
    message=msg(payload={}) if payload is None else msg(payload=payload)
    consumer=FakeConsumer([message]);producer=FakeProducer();store=FakeStore()
    writer=ClickHouseWriter(store,consumer,producer)
    with pytest.raises(StorageBatchError):await writer.flush([message])
    assert consumer.committed==[] and store.calls==[] and len(producer.records)==1


async def test_dlq_failure_does_not_commit():
    message=msg(payload={});consumer=FakeConsumer([message]);producer=FakeProducer()
    producer.publish_batch=AsyncMock(side_effect=RuntimeError('delivery failed'))
    with pytest.raises(RuntimeError):await ClickHouseWriter(FakeStore(),consumer,producer).flush([message])
    assert consumer.committed==[]


@pytest.mark.parametrize('batch_size,interval,expected_sizes',[(3,1000,[3,2]),(100,5,[5])])
async def test_accumulation_size_timer_and_shutdown_flush(batch_size,interval,expected_sizes):
    messages=[msg(offset=i) for i in range(5)];consumer=FakeConsumer(messages)
    consumer.batches=[messages[:2],messages[2:]]
    store,producer=FakeStore(),FakeProducer()
    writer=ClickHouseWriter(store,consumer,producer,batch_size=batch_size,flush_interval_ms=interval)
    await writer.run(.04)
    assert [len(rows) for _,rows in store.calls]==expected_sizes
    assert len(consumer.committed)==5 and writer.events_consumed==5
    assert store.closed and consumer.closed and producer.closed


async def test_cleanup_all_resources_after_insert_failure():
    message=msg();consumer=FakeConsumer([message]);consumer.batches=[[message]]
    store,producer=FakeStore(10),FakeProducer()
    writer=ClickHouseWriter(store,consumer,producer,batch_size=1,max_retries=0)
    with pytest.raises(StorageBatchError):await writer.run(.1)
    assert store.closed and consumer.closed and producer.closed and consumer.committed==[]


async def test_disabled_writer_constructs_no_clients(monkeypatch):
    from integrity_engine.storage import writer as module
    monkeypatch.setenv('CLICKHOUSE_ENABLED','false')
    factory=MagicMock(side_effect=AssertionError('must not connect'))
    monkeypatch.setattr(module,'ClickHouseStore',factory)
    await module.run_from_env(.1)
    factory.assert_not_called()
