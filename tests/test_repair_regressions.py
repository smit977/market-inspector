"""Regression evidence for the 2026-09-25 correctness/lag repair."""
import asyncio
import json
from datetime import timedelta
from decimal import Decimal as D
from unittest.mock import AsyncMock, MagicMock
import pytest
from integrity_engine.kafka.consumer import OffsetTracker, ManualKafkaConsumer
from integrity_engine.kafka import consumer as consumer_module
from integrity_engine.kafka import producer as producer_module
from integrity_engine.kafka.producer import KafkaDeliveryError
from integrity_engine.normalization.derivatives import normalize_derivative_batch, normalize_derivative
from integrity_engine.features.derivatives import DerivativesFeatureEngine
from integrity_engine.integrity import components
from integrity_engine.integrity.scoring import score_integrity
from integrity_engine.detection.detectors import detect
from integrity_engine.detection.engine import MoveDetector
from tests.test_derivatives import raw, R
from tests.test_detection import metric, baselines
from tests.test_integrity import anomaly, spot, cross, deriv, T
from scripts.run_derivatives_features import handle_batch

def message(offset,partition=0,topic="market.raw.derivatives",value=None):
    m=MagicMock()
    m.offset.return_value=offset;m.partition.return_value=partition;m.topic.return_value=topic
    m.error.return_value=None;m.value.return_value=json.dumps(value).encode()
    return m

def test_contiguous_commit_hole_and_partition_independence():
    tracker=OffsetTracker()
    a,b,c=[message(i) for i in (10,11,12)];other=message(20,1)
    for m in (a,b,c,other):tracker.observe(m)
    tracker.acknowledge(a);tracker.acknowledge(c);tracker.acknowledge(other)
    assert tracker.ready=={("market.raw.derivatives",0):11,("market.raw.derivatives",1):21}
    tracker.acknowledge(b)
    assert tracker.ready[("market.raw.derivatives",0)]==13

def test_numeric_offset_gaps_do_not_stall_compacted_topics():
    t=OffsetTracker();a,b=message(10),message(13)
    for m in (a,b):t.observe(m);t.acknowledge(m)
    assert t.ready[("market.raw.derivatives",0)]==14

@pytest.mark.parametrize("mode,reset", [("live","latest"),("replay","earliest")])
def test_live_and_replay_groups_do_not_reset_old_offsets(monkeypatch,mode,reset):
    factory=MagicMock();monkeypatch.setattr(consumer_module,"Consumer",factory)
    monkeypatch.setenv("PROCESSING_MODE",mode);monkeypatch.setenv("PROCESSING_RUN_ID","audit")
    ManualKafkaConsumer("broker","topic","worker")
    conf=factory.call_args.args[0]
    assert conf["auto.offset.reset"]==reset
    assert conf["group.id"]==("worker-live-audit" if mode=="live" else "worker")

async def test_producer_batch_order_confirmation_no_individual_flush(monkeypatch):
    client=MagicMock();pending=[];sent=[]
    def produce(topic,**kw):sent.append(kw["key"]);pending.append(kw["on_delivery"])
    def poll(_):
        for callback in pending:callback(None,MagicMock())
        pending.clear()
    client.produce.side_effect=produce;client.poll.side_effect=poll
    monkeypatch.setattr(producer_module,"Producer",lambda conf:client)
    p=producer_module.JsonKafkaProducer("broker")
    await p.publish_batch([("topic",str(i),{"i":i}) for i in range(600)])
    assert p.delivered==600 and sent==[str(i).encode() for i in range(600)]
    client.flush.assert_not_called()

def envelope(exchange,kind="perpetual_trade",n=3):
    rows=[]
    for i in range(n):
        if kind=="perpetual_trade":
            row={"bybit":{"p":"100","v":"2","T":1789850000000+i,"i":str(i),"S":"Buy"},
                 "okx":{"px":"100","sz":"2","ts":str(1789850000000+i),"tradeId":str(i),"side":"buy"},
                 "binance":{"p":"100","q":"2","T":1789850000000+i,"a":i,"m":False}}[exchange]
        elif kind=="funding_rate":
            row={"bybit":{"fundingRate":"-0.0001","ts":1789850000000+i},
                 "okx":{"fundingRate":"-0.0001","fundingTime":str(1789850000000+i)},
                 "binance":{"lastFundingRate":"-0.0001","fundingTime":1789850000000+i}}[exchange]
        elif kind=="open_interest":
            row={"bybit":{"openInterest":"2","timestamp":str(1789850000000+i)},
                 "okx":{"oi":"2","oiCcy":"0.02","ts":str(1789850000000+i)},
                 "binance":{"openInterest":"2","time":1789850000000+i}}[exchange]
        else:
            row={"bybit":{"p":"100","v":"2","T":1789850000000+i,"S":"Sell"},
                 "okx":{"details":[{"bkPx":"100","sz":"2","ts":str(1789850000000+i),"side":"sell"}]},
                 "binance":{"o":{"p":"100","q":"2","T":1789850000000+i,"S":"SELL","i":i}}}[exchange]
        rows.append(row)
    return raw(exchange,kind,{"data":rows},"BTC-USDT-SWAP" if exchange=="okx" else "BTCUSDT")

@pytest.mark.parametrize("exchange",["bybit","okx","binance"])
@pytest.mark.parametrize("kind",["perpetual_trade","funding_rate","open_interest","liquidation"])
@pytest.mark.parametrize("n",[1,3])
def test_derivatives_all_rows_types_order_and_ids(exchange,kind,n):
    source=envelope(exchange,kind,n)
    rows=list(normalize_derivative_batch(source))
    assert len(rows)==n and all(row[3] is None for row in rows)
    events=[r[2] for r in rows]
    assert len({e.event_id for e in events})==n
    assert [e.exchange_timestamp for e in events]==sorted(e.exchange_timestamp for e in events)
    assert [e.event_id for e in events]==[r[2].event_id for r in normalize_derivative_batch(source)]

@pytest.mark.parametrize("exchange",["bybit","okx","binance"])
def test_invalid_batch_sibling_isolated(exchange):
    source=envelope(exchange);source["payload"]["data"][1]={}
    rows=list(normalize_derivative_batch(source))
    assert [r[2] is not None for r in rows]==[True,False,True]
    assert rows[1][3] is not None

def test_nested_okx_liquidation_fanout():
    source=envelope("okx","liquidation",1)
    detail=source["payload"]["data"][0]["details"][0]
    source["payload"]["data"][0]["details"]=[detail,dict(detail,sz="3")]
    rows=list(normalize_derivative_batch(source))
    assert [r[2].quantity for r in rows]==[D(2),D(3)]

@pytest.mark.parametrize("fail",[False,True])
async def test_derivative_batch_delivery_before_commit_and_row_dlq(fail):
    source=envelope("bybit");source["payload"]["data"][1]={}
    p,c=AsyncMock(),AsyncMock();actions=[]
    async def publish(records):
        actions.append("delivered")
        if fail:raise KafkaDeliveryError("failed")
    p.publish_batch.side_effect=publish
    c.commit_batch.side_effect=lambda _:actions.append("commit")
    stats=dict(cleaned=0,dlq=0,spot_trades=0,late=0)
    engine=DerivativesFeatureEngine()
    if fail:
        with pytest.raises(KafkaDeliveryError):
            await handle_batch([message(10,value=source)],p,c,{"bybit":engine},stats)
        c.commit_batch.assert_not_awaited()
    else:
        await handle_batch([message(10,value=source)],p,c,{"bybit":engine},stats)
        assert actions==["delivered","commit"]
        outputs=p.publish_batch.call_args.args[0] # list is drained after confirmation
        assert stats["cleaned"]==2 and stats["dlq"]==1 and len(engine.events)==2

def test_okx_contracts_not_mislabeled_as_base_or_quote():
    event=list(normalize_derivative_batch(envelope("okx",n=1)))[0][2]
    assert event.quantity==2 and event.quantity_unit=="contracts_as_reported"
    assert not event.comparable and event.quote_quantity is None
    engine=DerivativesFeatureEngine();engine.add(event)
    result=engine.calculate("10s",event.exchange_timestamp)
    assert result.perpetual_base_volume is None and result.perpetual_quote_volume is None

def test_oi_venue_and_unit_mixing_rejected():
    a=list(normalize_derivative_batch(envelope("bybit","open_interest",1)))[0][2]
    b=list(normalize_derivative_batch(envelope("okx","open_interest",1)))[0][2]
    engine=DerivativesFeatureEngine();engine.add(a)
    with pytest.raises(ValueError):engine.add(b)
    with pytest.raises(ValueError):engine.add(a.model_copy(update={"quantity_unit":"contracts"}))
    assert engine.calculate("10s",a.exchange_timestamp).open_interest_comparable is False

@pytest.mark.parametrize("age,window,active",[(6,"10s",True),(0,"1m",True),(0,"10s",False)])
def test_stale_wrong_window_or_inactive_spot_reference_not_used(age,window,active):
    event=list(normalize_derivative_batch(envelope("bybit",n=1)))[0][2]
    engine=DerivativesFeatureEngine();engine.add(event)
    engine.add_spot_metric({"window":window,"calculated_at":(event.exchange_timestamp-timedelta(seconds=age)).isoformat(),"active_venues":["bybit"] if active else [],"median_price":"90"})
    metric=engine.calculate("10s",event.exchange_timestamp)
    assert metric.spot_reference_price is None and metric.basis is None and metric.basis_bps is None
    assert "stale_spot_reference" in metric.data_quality_flags

def test_liquidation_component_uses_notional_not_base_quantity():
    metric=deriv().model_copy(update={"total_liquidation_volume":D(100),"liquidation_quote_volume":None})
    assert components.liquidation(metric).score is None
    component=components.liquidation(metric.model_copy(update={"liquidation_quote_volume":D(1),"perpetual_quote_volume":D(100)}))
    assert component.score==95

@pytest.mark.parametrize("fn,arg",[(components.cross,None),(components.spot,None),(components.liquidity,None),(components.volume,None),(components.derivatives,None),(components.liquidation,None)])
def test_missing_whole_component_has_no_score(fn,arg):
    component=fn(arg)
    assert component.score is None and component.availability=="unavailable" and component.confidence==0

def test_empty_present_evidence_cannot_fabricate_perfect_scores():
    empty=spot().model_copy(update={"spread_bps":None,"depth_change":None,"trade_count":0,"quote_volume":D(0)})
    empty_cross=cross().model_copy(update={"active_venues":[],"confirmation_ratio":None,"largest_venue_volume_share":None,"venue_volume_shares":{}})
    for c in [components.spot(empty),components.liquidity(empty),components.cross(empty_cross),components.volume(empty,empty_cross),components.derivatives(deriv().model_copy(update={"perpetual_to_spot_volume_ratio":None}))]:
        assert c.score is None and c.availability=="unavailable"

def test_partial_liquidity_reduces_confidence_and_coverage():
    complete=score_integrity(anomaly(),spot(),cross(),deriv())
    partial=score_integrity(anomaly(),spot().model_copy(update={"depth_change":None}),cross(),deriv())
    c=next(c for c in partial.components if c.name=="Liquidity Stability")
    assert c.availability=="partial" and c.score is not None
    assert partial.evidence_coverage<complete.evidence_coverage and partial.overall_confidence<complete.overall_confidence

@pytest.mark.parametrize("field",["spread_bps","depth_change","orderbook_imbalance_20","trade_imbalance","absolute_return","price_range","price_acceleration"])
def test_optional_detector_inputs_suppress_only_affected_evidence(field):
    bs=baselines()
    for b in bs.values():
        for i in range(20):b.add(D(1)+D(i)/100)
    m=metric(absolute_return=D(10),quote_volume=D(1000),trade_count=1000)
    result=detect(m.model_copy(update={field:None}),bs)
    assert result
    assert all(e.metric!=field for event in result for e in event.evidence)

async def test_detector_delivery_failure_exits_before_next_record(monkeypatch):
    from integrity_engine.detection import engine as module
    c=AsyncMock();c.poll.side_effect=[message(10,value=json.loads(metric().model_dump_json())),message(11)]
    p=AsyncMock();p.publish.side_effect=KafkaDeliveryError("injected")
    monkeypatch.setattr(module,"ManualKafkaConsumer",lambda *a,**kw:c)
    engine=MoveDetector(p);engine.process=lambda m:[anomaly()]
    with pytest.raises(KafkaDeliveryError):await engine.run(.2)
    assert c.poll.await_count==1
    c.commit.assert_not_awaited()

def test_broad_support_exceeds_severe_weak_structure():
    broad=score_integrity(anomaly(),spot(),cross(),deriv())
    weak=score_integrity(anomaly(),spot().model_copy(update={"spread_bps":D(20),"depth_change":D("-.8")}),cross().model_copy(update={"confirmation_ratio":D(".2"),"dispersion_bps":D(20),"largest_venue_volume_share":D(".95")}),deriv(ratio=D(20)))
    assert broad.integrity_score>weak.integrity_score

@pytest.mark.parametrize("exchange",["bybit","okx","binance"])
def test_all_invalid_rows_are_individually_reported(exchange):
    source=envelope(exchange);source['payload']['data']=[{},None,{}]
    rows=list(normalize_derivative_batch(source))
    assert len(rows)==3 and all(r[2] is None and r[3] is not None for r in rows)

@pytest.mark.parametrize("field",["quote_volume","trade_count"])
def test_missing_volume_input_does_not_crash_other_detectors(field):
    bs=baselines()
    for b in bs.values():
        for i in range(20):b.add(D(1)+D(i)/100)
    result=detect(metric(absolute_return=D(10),price_range=D(10)).model_copy(update={field:None}),bs)
    assert result and all(e.metric!=field for a in result for e in a.evidence)

async def test_spot_batch_delivery_failure_never_acknowledges_inputs():
    from integrity_engine.normalization.base import TradeNormalizationProcessor
    normalizer=MagicMock();normalizer.normalize.return_value=MagicMock(symbol='BTC-USDT')
    normalizer.normalize.return_value.model_dump.return_value={'event_id':'stable'}
    producer,consumer=AsyncMock(),AsyncMock()
    consumer.poll_batch.return_value=[message(10,value={}),message(11,value={})]
    producer.publish_batch.side_effect=KafkaDeliveryError('failed')
    processor=TradeNormalizationProcessor(normalizer,consumer,producer)
    with pytest.raises(KafkaDeliveryError):await processor.run(asyncio.Event())
    consumer.commit_batch.assert_not_awaited()

async def test_feature_output_failure_keeps_input_unacknowledged(monkeypatch):
    from integrity_engine.features import engine as module
    from tests.test_features import trade
    consumer,producer=AsyncMock(),AsyncMock()
    event=trade('100','1','buy')
    consumer.poll_batch.return_value=[message(10,topic='market.cleaned.trades',value=event.model_dump(mode='json'))]
    producer.publish_batch.side_effect=KafkaDeliveryError('failed')
    monkeypatch.setattr(module,'ManualKafkaConsumer',lambda *a,**kw:consumer)
    with pytest.raises(KafkaDeliveryError):await module.FeatureEngine(producer).run(.1)
    consumer.commit_batch.assert_not_awaited()

async def test_cross_output_failure_keeps_input_unacknowledged(monkeypatch):
    from scripts import run_cross_venue_features as module
    from tests.test_features import trade
    consumer,producer=AsyncMock(),AsyncMock()
    event=trade('100','1','buy')
    consumer.poll_batch.return_value=[message(10,topic='market.cleaned.trades',value=event.model_dump(mode='json'))]
    producer.publish_batch.side_effect=KafkaDeliveryError('failed')
    monkeypatch.setattr(module,'ManualKafkaConsumer',lambda *a,**kw:consumer)
    monkeypatch.setattr(module,'JsonKafkaProducer',lambda *a,**kw:producer)
    with pytest.raises(KafkaDeliveryError):await module.run(.1)
    consumer.commit_batch.assert_not_awaited()

async def test_integrity_failure_exits_without_advancing_offsets(monkeypatch):
    from integrity_engine.integrity import engine as module
    consumer,producer=AsyncMock(),AsyncMock()
    consumer.poll.return_value=message(11,topic='market.anomalies',value=anomaly().model_dump(mode='json'))
    producer.publish.side_effect=KafkaDeliveryError('failed')
    monkeypatch.setattr(module,'ManualKafkaConsumer',lambda *a,**kw:consumer)
    with pytest.raises(KafkaDeliveryError):await module.IntegrityEngine(producer).run(.1)
    consumer.commit.assert_not_awaited()
    assert consumer.poll.await_count==1

async def test_rebalance_commits_only_contiguous_success(monkeypatch):
    client=MagicMock();client.commit.return_value=[]
    monkeypatch.setattr(consumer_module,'Consumer',lambda conf:client)
    consumer=ManualKafkaConsumer('broker','topic','group')
    rows=[message(i) for i in (10,11,12)]
    for row in rows:consumer.tracker.observe(row)
    consumer.tracker.acknowledge(rows[0]);consumer.tracker.acknowledge(rows[2])
    consumer._revoke(client,[])
    assert client.commit.call_args.kwargs['offsets'][0].offset==11
    assert not consumer.tracker.pending

@pytest.mark.parametrize('control',[{'event':'subscribe','arg':{'channel':'trades','instId':'BTC-USDT-SWAP'}},{'event':'error','arg':{'channel':'trades'},'code':'1'},{'arg':{'channel':'trades'},'data':[]}])
def test_okx_control_messages_are_not_market_events(control):
    from integrity_engine.ingestion.okx.derivatives import classifier
    assert classifier(control) is None

@pytest.mark.parametrize('position_side,execution_side',[('Buy','sell'),('Sell','buy')])
def test_bybit_liquidated_position_side_maps_to_closing_trade(position_side,execution_side):
    source=envelope('bybit','liquidation',1)
    source['payload']['data'][0]['S']=position_side
    event=list(normalize_derivative_batch(source))[0][2]
    assert event.side==execution_side
