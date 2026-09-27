"""Regenerate Alert Operations Center and the compact overview panel."""
import json
import os
import shutil
from pathlib import Path
from generate_grafana_dashboards import panel, dashboard, variables, OUT

if os.getenv("ALERT_DASHBOARD_OUTPUT"):
    source=OUT
    OUT=Path(os.environ["ALERT_DASHBOARD_OUTPUT"])
    OUT.mkdir(parents=True,exist_ok=True)
    shutil.copy(source/'market-overview.json',OUT/'market-overview.json')
LATEST="(SELECT updated_at, canonical_pair, category, alert_type, severity, status, summary, first_detected_at, resolved_at, alert_id, revision, integrity_score, integrity_score_change FROM market_alerts WHERE NOT is_replay AND NOT is_synthetic ORDER BY revision DESC, updated_at DESC LIMIT 1 BY alert_id)"
FILTER=" AND ".join(f"('${{{k}}}' = 'all' OR {col} = '${{{k}}}')" for k,col in [("canonical_pair","canonical_pair"),("category","category"),("severity","severity"),("status","status"),("alert_type","alert_type")])
vars=variables(exchange=False,window=False)
for name,values in [("category","all,MARKET_STRUCTURE,DATA_QUALITY,PIPELINE_HEALTH"),("severity","all,info,low,moderate,high,critical"),("status","all,open,acknowledged,resolved"),("alert_type","all,"+','.join(json.loads(Path('config/alert_rules.json').read_text())["rules"]))]:
    vars.append({"name":name,"label":name.replace('_',' ').title(),"type":"custom","query":values,"current":{"text":"all","value":"all"},"includeAll":False,"multi":False,"options":[]})
panels=[]
for title,extra in [("Open alerts","status != 'resolved'"),("Critical/high alerts","status != 'resolved' AND severity IN ('critical','high')"),("Resolved alerts","status = 'resolved'")]:
    panels.append(panel(title,f"SELECT count() AS alerts FROM {LATEST} WHERE {FILTER} AND {extra}","stat",w=8,h=4))
for title,extra in [("Recent alerts","1"),("Data-quality alerts","category = 'DATA_QUALITY'"),("Pipeline-health alerts","category = 'PIPELINE_HEALTH'")]:
    panels.append(panel(title,f"SELECT updated_at, canonical_pair, severity, status, alert_type, summary FROM {LATEST} WHERE {FILTER} AND {extra} ORDER BY updated_at DESC LIMIT 100","table",w=24,h=7))
panels.append(panel("Alert timeline",f"SELECT updated_at, alert_id, revision, severity, status, summary FROM market_alerts FINAL WHERE $__timeFilter(updated_at) AND NOT is_replay AND NOT is_synthetic AND {FILTER} ORDER BY updated_at DESC LIMIT 300","table",w=24))
for title,column in [("Alerts by category","category"),("Alerts by type","alert_type"),("Alert severity","severity")]:
    panels.append(panel(title,f"SELECT {column}, count() AS alerts FROM {LATEST} WHERE {FILTER} GROUP BY {column}","table",w=8,h=6))
panels.append(panel("Average duration",f"SELECT avgOrNull(dateDiff('second', first_detected_at, resolved_at)) AS duration FROM {LATEST} WHERE {FILTER} AND status = 'resolved' AND $__timeFilter(resolved_at)","stat",unit="s",w=8,h=5))
panels.append(panel("Integrity deterioration history",f"SELECT updated_at, integrity_score, integrity_score_change, severity, status FROM market_alerts FINAL WHERE $__timeFilter(updated_at) AND NOT is_replay AND NOT is_synthetic AND {FILTER} AND alert_type = 'integrity_deterioration' ORDER BY updated_at DESC LIMIT 100","table",w=16,h=5))
d=dashboard("alert-operations","Alert Operations Center",panels,vars,description="Investigation urgency. Data quality and pipeline health remain separate from market structure.")
d["refresh"]="10s"
for i,p in enumerate(d["panels"],1): p["id"]=i
(OUT/'alert-operations.json').write_text(json.dumps(d,indent=2)+'\n')
p=OUT/'market-overview.json'; overview=json.loads(p.read_text())
overview['panels']=[x for x in overview['panels'] if x['title']!='ACTIVE ALERTS']
a=panel("ACTIVE ALERTS",f"SELECT severity, alert_type, dateDiff('second', first_detected_at, now()) AS age_seconds, summary FROM {LATEST} WHERE status != 'resolved' AND canonical_pair = '${{canonical_pair}}' ORDER BY indexOf(['critical','high','moderate','low','info'], severity), updated_at DESC LIMIT 8","table",w=24,h=5)
a['id']=max(x.get('id',0) for x in overview['panels'])+1
a['gridPos']['y']=max(x['gridPos']['y']+x['gridPos']['h'] for x in overview['panels'])
overview['panels'].append(a);p.write_text(json.dumps(overview,indent=2)+'\n')
