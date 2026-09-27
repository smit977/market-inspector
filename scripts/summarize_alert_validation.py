"""Summarize a completed live audit with persisted incident lifecycle evidence."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import median
from integrity_engine.storage.clickhouse import ClickHouseStore

def main():
    p=argparse.ArgumentParser();p.add_argument('--audit',required=True);p.add_argument('--output',required=True);args=p.parse_args()
    audit=json.loads(Path(args.audit).read_text())
    if audit.get('elapsed_seconds',0)<300:raise ValueError('audit has not completed five minutes')
    report={'started_at':audit['started_at'],'ended_at':audit['samples'][-1]['at'],
        'elapsed_seconds':audit['elapsed_seconds'],'samples':len(audit['samples']),'new_dlq_records':audit['new_dlq_records'],
        'storage_errors':[s.get('storage_error') for s in audit['samples'] if s.get('storage_error')],
        'lag':{},'mcp':{}}
    for group in ('alert-engine-v1-live-current','clickhouse-writer-v1-live-v2'):
        observations=[h for s in audit['samples'] for h in s['health'] if h['exchange']==group]
        lags=[h['signals']['consumer_lag_high']['value'] for h in observations if h['signals'].get('consumer_lag_high',{}).get('value') is not None]
        report['lag'][group]={'minimum':min(lags) if lags else None,'maximum':max(lags) if lags else None,
            'final':lags[-1] if lags else None,'observations':len(lags),
            'unknown_partition_counts':[h['signals']['consumer_lag_high'].get('unknown_partitions') for h in observations if 'consumer_lag_high' in h['signals']],
            'unhealthy_samples':sum(h['signals'].get('worker_unhealthy',{}).get('value',0) for h in observations)}
    for tool in ('get_active_alerts','get_latest_market_metrics'):
        results=[s['mcp'][tool] for s in audit['samples']]
        latencies=[r['latency_ms'] for r in results if 'latency_ms' in r]
        errors=[]
        for r in results:
            payload=r.get('response',{});data=payload.get('result',{})
            if r.get('error') or payload.get('error') or data.get('isError'):errors.append(r)
            for content in data.get('content',[]):
                if content.get('type')=='text':
                    try:
                        if json.loads(content['text']).get('status')=='error': errors.append(r)
                    except (ValueError,AttributeError):pass
        report['mcp'][tool]={'median_ms':median(latencies),'max_ms':max(latencies),'errors':len(errors)}
    store=ClickHouseStore()
    store.client.database=store.database
    try:
        at=datetime.fromisoformat(audit['started_at']).astimezone(timezone.utc).replace(tzinfo=None)
        end=datetime.fromisoformat(audit['samples'][-1]['at']).astimezone(timezone.utc).replace(tzinfo=None)
        params={'start':at,'end':end}
        report['new_incidents']=store.client.query("SELECT category, alert_type, initial_severity, uniqExact(alert_id) FROM market_alerts WHERE NOT is_replay AND created_at BETWEEN {start:DateTime64(3)} AND {end:DateTime64(3)} GROUP BY category, alert_type, initial_severity",parameters=params).result_rows
        report['lifecycle_revisions']=store.client.query("SELECT status, uniqExact(event_id) FROM market_alerts WHERE NOT is_replay AND updated_at BETWEEN {start:DateTime64(3)} AND {end:DateTime64(3)} GROUP BY status",parameters=params).result_rows
        report['duplicate_active_incidents']=store.query("SELECT dedupe_key, count() FROM (SELECT alert_id, dedupe_key, status FROM market_alerts WHERE NOT is_replay ORDER BY revision DESC LIMIT 1 BY alert_id) WHERE status != 'resolved' GROUP BY dedupe_key HAVING count()>1").result_rows
        report['source_freshness_seconds']={table:store.query(f"SELECT dateDiff('second',max(timestamp),now()) FROM {table}").result_rows[0][0] for table in ('market_metrics','cross_venue_metrics','derivatives_metrics','operational_health')}
    finally:store.close()
    Path(args.output).write_text(json.dumps(report,indent=2,default=str));print(json.dumps(report,indent=2,default=str))

if __name__=='__main__':main()
