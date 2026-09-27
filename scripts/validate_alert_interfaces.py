"""Read-only Grafana query and MCP protocol validation against the local stack."""
import json
import os
from pathlib import Path
import time
import httpx
from integrity_engine.storage.clickhouse import ClickHouseStore


def main():
    report={'grafana':{},'mcp':{}}
    with httpx.Client(base_url='http://grafana:3000',auth=(os.environ['GRAFANA_ADMIN_USER'],os.environ['GRAFANA_ADMIN_PASSWORD']),timeout=30) as client:
        for uid in ('alert-operations','market-overview'):
            response=client.get(f'/api/dashboards/uid/{uid}');response.raise_for_status()
            dashboard=response.json()['dashboard'];panels={}
            for panel in dashboard['panels']:
                if uid=='market-overview' and panel['title']!='ACTIVE ALERTS':continue
                sql=panel['targets'][0]['rawSql']
                for name in ('canonical_pair','category','severity','status','alert_type'):
                    sql=sql.replace('${'+name+'}','BTC-USD' if name=='canonical_pair' else 'all')
                query=dict(panel['targets'][0],rawSql=sql,intervalMs=10000,maxDataPoints=1000)
                started=time.perf_counter()
                result=client.post('/api/ds/query',json={'queries':[query],'from':str(int((time.time()-900)*1000)),'to':str(int(time.time()*1000))})
                payload=result.json();errors=[v.get('error') for v in payload.get('results',{}).values() if v.get('error')]
                panels[panel['title']]={'http_status':result.status_code,'errors':errors,'latency_ms':round((time.perf_counter()-started)*1000,2),
                    'frame_count':sum(len(v.get('frames',[])) for v in payload.get('results',{}).values())}
            report['grafana'][uid]=panels
    store=ClickHouseStore()
    try:
        ids=store.query('SELECT alert_id FROM market_alerts ORDER BY updated_at DESC LIMIT 1').result_rows
        report['synthetic_lifecycle']=store.query("SELECT alert_id, countDistinct(revision), groupUniqArray(status), groupUniqArray(severity) FROM market_alerts WHERE is_replay GROUP BY alert_id").result_rows
        report['unhealthy_scopes']=store.query("SELECT dedupe_key, summary FROM (SELECT * FROM market_alerts WHERE NOT is_replay ORDER BY revision DESC LIMIT 1 BY alert_id) WHERE status != 'resolved' AND alert_type='worker_unhealthy'").result_rows
    finally:store.close()
    with httpx.Client(timeout=30) as client:
        for name,args in [('get_active_alerts',{}),('get_alert_details',{'alert_id':ids[0][0]})]:
            started=time.perf_counter()
            response=client.post('http://mcp-server:8001/mcp',headers={'Accept':'application/json, text/event-stream'},
                json={'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':name,'arguments':args}})
            report['mcp'][name]={'http_status':response.status_code,'latency_ms':(time.perf_counter()-started)*1000,'response':response.json()}
    Path('/tmp/alert-interfaces.json').write_text(json.dumps(report,indent=2,default=str))
    print(json.dumps({'grafana':report['grafana'],'synthetic_lifecycle':report['synthetic_lifecycle'],'unhealthy_scopes':report['unhealthy_scopes'],
        'mcp':{k:{'http_status':v['http_status'],'latency_ms':v['latency_ms'],'error':v['response'].get('error'),'isError':v['response'].get('result',{}).get('isError')} for k,v in report['mcp'].items()}},indent=2))

if __name__=='__main__':main()
