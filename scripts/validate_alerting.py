"""Five-minute read-only live audit plus an explicitly replay-marked lifecycle probe."""
import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
from confluent_kafka import Consumer, TopicPartition
import httpx
from integrity_engine.alerts.config import AlertConfig
from integrity_engine.alerts.models import Assessment, AlertEvidence
from integrity_engine.alerts.state import AlertState
from integrity_engine.alerts.health import HealthCollector
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.storage.clickhouse import ClickHouseStore

async def synthetic():
    now=datetime.now(timezone.utc);state=AlertState();config=AlertConfig.load()
    assessment=Assessment(canonical_pair='BTC-USD',category='MARKET_STRUCTURE',alert_type='liquidity_shock',
        scope='synthetic-validation',active=True,recovered=False,summary='Synthetic lifecycle validation.',
        evidence=[AlertEvidence(source='synthetic-validation',metric='spread_ratio',current_value=4,
            freshness=0,interpretation='Deterministic synthetic fixture, not a live market observation.')])
    opened=state.transition(assessment,now,config,True)
    escalation=state.transition(assessment.model_copy(update={'severity':'high'}),now+timedelta(seconds=1),config,True)
    good=assessment.model_copy(update={'active':False,'recovered':True})
    resolved=None
    for second in (2,12,22,32): resolved=state.transition(good,now+timedelta(seconds=second),config,True)
    producer=JsonKafkaProducer(os.environ['KAFKA_BOOTSTRAP_SERVERS'])
    try:
        await producer.publish_batch([('market.alerts',f'{e.canonical_pair}:{e.alert_type}',e.model_dump(mode='json')) for e in (opened,escalation,resolved)])
    finally: await producer.close()
    return opened.alert_id

class Audit:
    def __init__(self):
        bootstrap=os.environ['KAFKA_BOOTSTRAP_SERVERS']
        self.health=HealthCollector(bootstrap)
        self.reader=Consumer({'bootstrap.servers':bootstrap,'group.id':'alert-validation-reader','enable.auto.commit':False})
    def snapshot(self):
        result={'at':datetime.now(timezone.utc).isoformat()}
        try:
            metadata=self.health.admin.list_topics('system.dlq',timeout=5)
            result['dlq_high_watermark']=sum(self.reader.get_watermark_offsets(TopicPartition('system.dlq',p),timeout=2)[1] for p in metadata.topics['system.dlq'].partitions)
        except Exception as error: result['dlq_error']=type(error).__name__
        result['health']=self.health.collect()
        store=None
        try:
            store=ClickHouseStore()
            result['alerts']=store.query("SELECT category, alert_type, severity, status, count() FROM (SELECT * FROM market_alerts WHERE NOT is_replay ORDER BY revision DESC LIMIT 1 BY alert_id) GROUP BY category, alert_type, severity, status").result_rows
            result['revisions']=store.query("SELECT count(), uniqExact(alert_id), max(updated_at) FROM market_alerts WHERE NOT is_replay").result_rows
            result['operational_freshness_seconds']=store.query("SELECT dateDiff('second',max(timestamp),now()) FROM operational_health").result_rows[0][0]
        except Exception as error: result['storage_error']=type(error).__name__
        finally:
            if store: store.close()
        result['mcp']={}
        for name in ('get_active_alerts','get_latest_market_metrics'):
            start=time.perf_counter()
            try:
                with httpx.Client(timeout=20) as client:
                    response=client.post('http://mcp-server:8001/mcp',headers={'Accept':'application/json, text/event-stream'},
                        json={'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':name,'arguments':{}}})
                    response.raise_for_status();payload=response.json()
                    result['mcp'][name]={'latency_ms':(time.perf_counter()-start)*1000,'response':payload}
            except Exception as error: result['mcp'][name]={'error':type(error).__name__}
        return result
    def close(self): self.reader.close();self.health.close()

def main():
    p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=300);p.add_argument('--output',default='/tmp/alert-live-validation.json');a=p.parse_args()
    audit=Audit();report={'started_at':datetime.now(timezone.utc).isoformat(),'samples':[]}
    start=time.monotonic()
    try:
        report['synthetic_alert_id']=asyncio.run(synthetic())
        while True:
            sample=audit.snapshot();report['samples'].append(sample)
            Path(a.output).write_text(json.dumps(report,indent=2,default=str))
            print(json.dumps({'sample':len(report['samples']),'elapsed_seconds':round(time.monotonic()-start),
                'storage_error':sample.get('storage_error'),'dlq':sample.get('dlq_high_watermark'),
                'alerts':sample.get('alerts')},default=str),flush=True)
            if time.monotonic()-start>=a.seconds: break
            time.sleep(min(30,a.seconds-(time.monotonic()-start)))
        report['elapsed_seconds']=time.monotonic()-start
        values=[s['dlq_high_watermark'] for s in report['samples'] if 'dlq_high_watermark' in s]
        report['new_dlq_records']=values[-1]-values[0] if len(values)>1 else None
    finally:
        audit.close();Path(a.output).write_text(json.dumps(report,indent=2,default=str))

if __name__=='__main__': main()
