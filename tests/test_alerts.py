"""Deterministic rules, incident transitions, delivery boundaries and replay isolation."""
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import httpx
import pytest
from integrity_engine.alerts.config import AlertConfig
from integrity_engine.alerts.models import Assessment, AlertEvidence, AlertEvent
from integrity_engine.alerts.rules import evaluate, severity
from integrity_engine.alerts.state import AlertState
from integrity_engine.alerts.engine import AlertEngine, AlertWorker
from integrity_engine.alerts.notifiers import NotificationConfig, WebhookNotifier
from integrity_engine.storage.models import event_to_row, TABLE_COLUMNS, TABLE_DDL

NOW=datetime(2026,9,27,12,tzinfo=timezone.utc)
@pytest.fixture
def config(): return AlertConfig.load()

def event(**kw):
    return dict(event_id=str(uuid4()),calculated_at=NOW.isoformat(),canonical_pair="BTC-USD",window="30s",data_quality_flags=[],**kw)

def integrity(score=40,**kw):
    return event(integrity_score=score,overall_confidence=kw.get('confidence',.95),evidence_coverage=kw.get('coverage',.95),
        components=[],trigger_severity="high",trigger_event_id=str(uuid4()),trigger_type="price_move",exchange_scope="binance",
        move_direction="up",move_window="30s",classification="weak",classification_confidence=.9)

def assess(level='moderate',active=True,recovered=False):
    return Assessment(canonical_pair='BTC-USD',category='MARKET_STRUCTURE',alert_type='liquidity_shock',scope='binance',
        active=active,recovered=recovered,severity=level,summary='Liquidity has deteriorated.',
        evidence=[AlertEvidence(source='market.metrics',metric='spread',current_value=2,interpretation='Observed expansion.')])

def active(rows,kind): return next((a for a in rows if a.alert_type==kind and a.active),None)
def previous(e): return dict(e,calculated_at=(NOW-timedelta(seconds=10)).isoformat())

def test_healthy_broad_move(config):
    e=event(active_venues=['a','b','c'],stale_venues=[],confirmation_state='confirmed',confirmation_ratio=1,
        dispersion_bps=2,up_venue_count=3,down_venue_count=0)
    assert not any(a.active and a.category=='MARKET_STRUCTURE' for a in evaluate('market.metrics.cross_venue',e,None,NOW,config))

def test_integrity_drop_and_low_score(config):
    e=integrity(35); old=previous(integrity(78))
    rows=evaluate('market.integrity',e,old,NOW,config)
    assert active(rows,'integrity_deterioration') and active(rows,'low_structural_confirmation')
    assert not active(evaluate('market.integrity',integrity(76),old,NOW,config),'integrity_deterioration')

@pytest.mark.parametrize('confidence,coverage',[(.2,.9),(.9,.2),(.1,.1)])
def test_low_quality_is_not_market_signal(config,confidence,coverage):
    rows=evaluate('market.integrity',integrity(10,confidence=confidence,coverage=coverage),previous(integrity(80)),NOW,config)
    assert active(rows,'insufficient_evidence')
    assert not any(a.active and a.category=='MARKET_STRUCTURE' for a in rows)

def test_liquidity_multiple_measurements(config):
    e=event(exchange='binance',data_complete=True,spread=5,bid_depth_20=1,ask_depth_20=1,orderbook_imbalance_20=.9)
    old=previous(dict(e,spread=1,bid_depth_20=5,ask_depth_20=5))
    alert=active(evaluate('market.metrics',e,old,NOW,config),'liquidity_shock')
    assert alert
    assert next(x for x in alert.evidence if x.metric=='spread_ratio').threshold==config.spread_ratio
    assert next(x for x in alert.evidence if x.metric=='spread').previous_value==1
    e.update(bid_depth_20=5,ask_depth_20=5,orderbook_imbalance_20=.1)
    assert not active(evaluate('market.metrics',e,old,NOW,config),'liquidity_shock')

@pytest.mark.parametrize('state,count,expected',[('fragmented',3,True),('neutral',0,False),('insufficient_data',0,False)])
def test_cross_directional_semantics(config,state,count,expected):
    e=event(active_venues=['a','b','c'],stale_venues=[],confirmation_state=state,confirmation_ratio=.3,
        dispersion_bps=50,up_venue_count=count,down_venue_count=count)
    assert bool(active(evaluate('market.metrics.cross_venue',e,None,NOW,config),'cross_venue_divergence'))==expected

def test_derivatives_missing_optional_not_zero(config):
    e=event(exchange='bybit',perpetual_to_spot_volume_ratio=5,basis_bps=40,spot_perpetual_return_difference=.005,
        spot_reference_timestamp=NOW.isoformat(),open_interest_change_pct=None,funding_rate=None,liquidation_quote_volume=None)
    a=active(evaluate('market.metrics.derivatives',e,None,NOW,config),'derivatives_amplification')
    assert a and a.confidence==pytest.approx(.7) and a.severity=='moderate'
    assert next(x for x in a.evidence if x.metric=='open_interest_change_pct').current_value is None

def test_liquidations_require_fresh_observations(config):
    e=event(exchange='okx',liquidation_quote_volume=2000000,total_liquidation_volume=20,
        long_liquidation_volume=15,short_liquidation_volume=5,liquidation_count=8,last_liquidation_at=NOW.isoformat())
    assert active(evaluate('market.metrics.derivatives',e,None,NOW,config),'liquidation_burst')
    e['last_liquidation_at']=(NOW-timedelta(seconds=60)).isoformat()
    assert not active(evaluate('market.metrics.derivatives',e,None,NOW,config),'liquidation_burst')
    e['liquidation_quote_volume']=None
    assert not active(evaluate('market.metrics.derivatives',e,None,NOW,config),'liquidation_burst')

def test_volume_concentration(config):
    e=event(active_venues=['a','b','c'],stale_venues=[],volume_hhi=.8,largest_venue_volume_share=.9,venue_quote_volumes={'a':1000})
    assert active(evaluate('market.metrics.cross_venue',e,previous(dict(e,venue_quote_volumes={'a':100})),NOW,config),'volume_concentration')

@pytest.mark.parametrize('kind,flags',[('venue_stale',['stale_venue_data']),('source_unavailable',['source_unavailable']),
    ('timestamp_disorder',['timestamp_disorder']),('orderbook_sequence_gap',['orderbook_sequence_gap']),
    ('insufficient_evidence',['insufficient_active_venues']),('basis_reference_stale',['stale_spot_reference']),('schema_failure',['schema_failure'])])
def test_quality_category(config,kind,flags):
    e=event();e['data_quality_flags']=flags
    a=active(evaluate('market.anomalies',e,None,NOW,config),kind)
    assert a and a.category=='DATA_QUALITY'

@pytest.mark.parametrize('kind',['consumer_lag_high','consumer_lag_growing','worker_unhealthy','DLQ_activity','clickhouse_persistence_failure','mcp_unavailable','processing_latency_degradation'])
def test_measured_pipeline_category(config,kind):
    e=event(signals={kind:{'value':config.rules[kind].threshold*2,'scope':'worker'}})
    assert active(evaluate('system.operational_health',e,None,NOW,config),kind).category=='PIPELINE_HEALTH'
    e['signals'][kind]['value']=None
    assert not evaluate('system.operational_health',e,None,NOW,config)

def test_stale_market_suppressed(config):
    e=integrity(5)
    rows=evaluate('market.integrity',e,None,NOW+timedelta(seconds=60),config)
    assert active(rows,'source_unavailable')
    assert not any(a.active and a.category=='MARKET_STRUCTURE' for a in rows)

def test_persistent_escalation_recovery(config):
    s=AlertState(); first=s.transition(assess(),NOW,config)
    assert first.status=='open'
    assert s.transition(assess(),NOW+timedelta(seconds=1),config) is None
    high=s.transition(assess('high'),NOW+timedelta(seconds=2),config)
    assert high.alert_id==first.alert_id and high.maximum_severity=='high' and high.initial_severity=='moderate'
    assert high.notification_state=='pending' and len(high.escalation_timeline)==1
    assert s.transition(assess(active=False,recovered=True),NOW+timedelta(seconds=3),config) is None
    # One bad sample resets recovery timer.
    s.transition(assess(),NOW+timedelta(seconds=20),config)
    assert s.transition(assess(active=False,recovered=True),NOW+timedelta(seconds=35),config) is None
    for seconds in (45,55):
        assert s.transition(assess(active=False,recovered=True),NOW+timedelta(seconds=seconds),config) is None
    resolved=s.transition(assess(active=False,recovered=True),NOW+timedelta(seconds=66),config)
    assert resolved.status=='resolved' and resolved.alert_id==first.alert_id
    reopened=s.transition(assess(),NOW+timedelta(seconds=67),config)
    assert reopened.alert_id!=first.alert_id and reopened.notification_state=='suppressed'

def test_unknown_never_resolves(config):
    s=AlertState();s.transition(assess(),NOW,config)
    assert s.transition(assess(active=False),NOW+timedelta(hours=1),config) is None

def test_restart_outbox_and_offsets(tmp_path,config):
    path=str(tmp_path/'state.sqlite');s=AlertState(path)
    e=s.transition(assess(),NOW,config);s.checkpoint('topic:0',5,[e]);s.close()
    s=AlertState(path)
    assert s.seen('topic:0',5) and not s.seen('topic:1',5)
    assert s.pending()[0].event_id==e.event_id
    update=s.transition(assess(),NOW+timedelta(seconds=40),config)
    assert update.alert_id==e.alert_id
    s.delivered(e.event_id);assert not s.pending()

def test_storage_history(config):
    s=AlertState();a=s.transition(assess(),NOW,config);b=s.transition(assess('high'),NOW+timedelta(seconds=2),config)
    for e in (a,b):
        table,row=event_to_row('market.alerts',e.model_dump(mode='json'))
        assert table=='market_alerts' and dict(zip(TABLE_COLUMNS[table],row))['alert_id']==a.alert_id
    assert a.event_id!=b.event_id and 'revision' in TABLE_DDL['market_alerts']

async def test_webhook_failure_isolated_and_secret(config):
    e=AlertState().transition(assess(),NOW,config)
    n=WebhookNotifier(NotificationConfig(enabled=True,url='https://example.invalid/secret'),httpx.MockTransport(lambda r:httpx.Response(503)))
    assert await n.notify(e)=='failed'
    e.is_replay=True
    assert await n.notify(e)=='replay_suppressed'

def test_invalid_configuration(config):
    with pytest.raises(ValueError): NotificationConfig(enabled=True,url='http://bad')
    data=config.model_dump();data['rules']['liquidity_shock']['recovery']=10
    with pytest.raises(ValueError): AlertConfig.model_validate(data)

@pytest.mark.parametrize('ratio,confidence,expected',[(1,1,'moderate'),(2,1,'high'),(3,1,'critical'),(3,.7,'moderate')])
def test_severity(config,ratio,confidence,expected):
    assert severity(config,config.rules['liquidity_shock'],ratio,confidence,1)==expected

async def test_publish_before_commit_and_recovery(config):
    s=AlertState();e=s.transition(assess(),NOW,config);s.checkpoint('topic:0',5,[e]);calls=[]
    class P:
        async def publish(self,*args):
            calls.append('publish');raise RuntimeError('delivery')
    class C:
        async def commit(self,m): calls.append('commit')
    w=AlertWorker(AlertEngine(s,config),C(),P())
    with pytest.raises(RuntimeError): await w.drain()
    assert calls==['publish'] and len(s.pending())==1

def test_replay_notifications(config):
    e=AlertState().transition(assess(),NOW,config,replay=True)
    assert e.is_replay and e.notification_state=='replay_suppressed'

async def test_worker_typed_input_publish_and_commit(config):
    from integrity_engine.schemas.integrity import MarketIntegrityEvent
    now=datetime.now(timezone.utc)
    e=integrity(20);e.pop('window');e['calculated_at']=now.isoformat()
    MarketIntegrityEvent.model_validate(e)
    calls=[]
    class Message:
        def topic(self): return 'market.integrity'
        def partition(self): return 0
        def offset(self): return 1
        def value(self): return json.dumps(e).encode()
    class Producer:
        async def publish(self,topic,key,event):
            assert topic=='market.alerts' and key=='BTC-USD:low_structural_confirmation'
            assert event['notification_state']=='logged';calls.append('publish')
    class Consumer:
        async def commit(self,m): calls.append('commit')
    w=AlertWorker(AlertEngine(config=config),Consumer(),Producer())
    await w.handle(Message());await w.handle(Message())
    assert calls==['publish','commit','commit']

async def test_invalid_integrity_schema_generates_quality(config):
    calls=[]
    class Message:
        def topic(self): return 'market.integrity'
        def partition(self): return 0
        def offset(self): return 1
        def value(self): return b'{"password":"do-not-expose"}'
    class Producer:
        async def publish(self,topic,key,event): calls.append(event)
    class Consumer:
        async def commit(self,m): pass
    await AlertWorker(AlertEngine(config=config),Consumer(),Producer()).handle(Message())
    assert calls[0]['category']=='DATA_QUALITY' and calls[0]['alert_type']=='schema_failure'
    assert 'do-not-expose' not in json.dumps(calls)

def test_recovery_gap_does_not_resolve(config):
    s=AlertState();s.transition(assess(),NOW,config)
    s.transition(assess(active=False,recovered=True),NOW+timedelta(seconds=1),config)
    assert s.transition(assess(active=False,recovered=True),NOW+timedelta(seconds=100),config) is None

def test_mcp_alert_tools_validation_and_queries():
    from integrity_engine.mcp.tools import MCPTools
    from integrity_engine.mcp.queries import QueryResult
    class Runner:
        def __init__(self): self.calls=[]
        def run(self,sql,params,limit):
            self.calls.append((sql,params,limit));return QueryResult([],1)
    r=Runner();m=MCPTools(runner=r)
    assert m.invoke('get_active_alerts',category='BAD')['status']=='error'
    assert m.invoke('get_active_alerts',minimum_severity='BAD')['status']=='error'
    assert m.invoke('get_alert_details','invalid')['status']=='error'
    assert m.invoke('get_active_alerts',canonical_pair='BTC-USD',category='DATA_QUALITY',minimum_severity='high')['status']=='no_data'
    sql,params,limit=r.calls[-1]
    assert params['severities']==['high','critical'] and 'LIMIT 1 BY alert_id' in sql
    assert sql.index('LIMIT 1 BY alert_id')<sql.index("WHERE status IN")
    assert m.invoke('get_alert_details',str(uuid4()))['status']=='no_data'

async def test_webhook_payload_bounded(config):
    e=AlertState().transition(assess(),NOW,config)
    def handler(request):
        payload=json.loads(request.content)
        assert set(payload)=={'alert_id','category','alert_type','canonical_pair','severity','status','summary','evidence'}
        assert len(payload['evidence'])<=12
        return httpx.Response(200)
    n=WebhookNotifier(NotificationConfig(enabled=True,url='https://example.invalid/token'),httpx.MockTransport(handler))
    assert await n.notify(e)=='sent'

def test_topic_initialization_is_idempotent(monkeypatch):
    from integrity_engine.alerts import health
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    admin=MagicMock();admin.list_topics.return_value=SimpleNamespace(topics={'market.alerts':None,'system.operational_health':None})
    monkeypatch.setattr(health,'AdminClient',lambda cfg:admin)
    health.ensure_topics('kafka:9092')
    admin.create_topics.assert_not_called()

def test_state_enforces_single_owner(tmp_path):
    path=str(tmp_path/'state.sqlite');s=AlertState(path)
    try:
        with pytest.raises(BlockingIOError): AlertState(path)
    finally:s.close()

def test_partial_lag_cannot_prove_recovery(config):
    e=event(signals={'consumer_lag_high':{'value':0,'scope':'worker','unknown_partitions':1,'known_partitions':2}})
    a=evaluate('system.operational_health',e,None,NOW,config)[0]
    assert not a.active and not a.recovered
    e['signals']['consumer_lag_high']['value']=30000
    assert evaluate('system.operational_health',e,None,NOW,config)[0].active

def test_rules_reject_missing_or_unknown_names(config):
    data=config.model_dump();data['rules'].pop('liquidity_shock')
    with pytest.raises(ValueError): AlertConfig.model_validate(data)

def test_future_event_does_not_poison_watermark(config):
    engine=AlertEngine(config=config,is_replay=True)
    bad=integrity(30);bad.pop('window');bad['calculated_at']=(NOW+timedelta(days=1)).isoformat()
    assert any(a.alert_type=='timestamp_disorder' for a in engine.process('market.integrity',bad,NOW))
    good=integrity(30);good.pop('window')
    assert any(a.alert_type=='low_structural_confirmation' for a in engine.process('market.integrity',good,NOW))

def test_quality_gap_resets_market_recovery_in_engine(config):
    engine=AlertEngine(config=config,is_replay=True)
    base=event(active_venues=['a','b','c'],stale_venues=[],confirmation_state='fragmented',confirmation_ratio=.3,
        dispersion_bps=40,up_venue_count=2,down_venue_count=1)
    assert any(a.alert_type=='cross_venue_divergence' for a in engine.process('market.metrics.cross_venue',base,NOW))
    for second in (10,15,20,25,30,35,40):
        now=NOW+timedelta(seconds=second)
        current=dict(base,event_id=str(uuid4()),calculated_at=now.isoformat(),dispersion_bps=5,confirmation_ratio=1)
        if second==20:current['active_venues']=['a']
        outputs=engine.process('market.metrics.cross_venue',current,now)
        assert not any(a.alert_type=='cross_venue_divergence' and a.status=='resolved' for a in outputs)

def test_stalled_worker_watchdog_bounds_recovery():
    from integrity_engine.alerts.watchdog import ProgressWatchdog
    clock=[0];exits=[]
    w=ProgressWatchdog(90,clock=lambda:clock[0],terminate=exits.append)
    clock[0]=89;assert w.check() and not exits
    w.touch();clock[0]=100;assert w.check()
    clock[0]=180;assert not w.check() and exits==[1]
