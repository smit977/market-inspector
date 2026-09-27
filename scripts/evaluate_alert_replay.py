"""Offline alert evaluation over isolated replay output; no Kafka or notifier constructed."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
from integrity_engine.alerts.engine import AlertEngine, MODELS
from integrity_engine.alerts.rules import stamp


def evaluate_file(source,output):
    engine=AlertEngine(is_replay=True)
    counts=Counter();types=Counter();levels=Counter();incidents={};first=last=None
    target=Path(output);target.mkdir(parents=True,exist_ok=True)
    with Path(source).open() as inp, (target/'alerts.jsonl').open('w') as out:
        for line in inp:
            row=json.loads(line);topic=row['topic'].replace('replay.','market.',1)
            if topic not in MODELS: continue
            event=row['event']; now=stamp(event)
            first=min(first,now) if first else now;last=max(last,now) if last else now
            for alert in engine.process(topic,event,now):
                assert alert.is_replay and alert.notification_state=='replay_suppressed'
                out.write(alert.model_dump_json()+'\n');counts['revisions']+=1
                if alert.alert_id not in incidents:
                    types[alert.alert_type]+=1; levels[alert.initial_severity]+=1
                incidents[alert.alert_id]=alert
    hours=(last-first).total_seconds()/3600 if first and last else 0
    durations=[(a.resolved_at-a.first_detected_at).total_seconds() for a in incidents.values() if a.resolved_at]
    code_root=Path(__file__).resolve().parents[1]/'src/integrity_engine/alerts'
    code_hash=hashlib.sha256(b''.join(p.read_bytes() for p in sorted(code_root.glob('*.py')))).hexdigest()
    report={'alert_code_sha256':code_hash,'source':str(source),'source_sha256':hashlib.sha256(Path(source).read_bytes()).hexdigest(),
        'config_sha256':hashlib.sha256(engine.config.model_dump_json().encode()).hexdigest(),
        'hours':hours,'alerts_generated':len(incidents),'types':dict(types),'initial_severities':dict(levels),
        'revisions':counts['revisions'],'alerts_per_hour':len(incidents)/hours if hours else None,
        'duplicates_suppressed':engine.state.stats['alerts_suppressed'],
        'escalations':engine.state.stats['alerts_escalated'],'resolutions':engine.state.stats['alerts_resolved'],
        'average_resolved_duration_seconds':sum(durations)/len(durations) if durations else None,
        'open_at_end':sum(a.status!='resolved' for a in incidents.values()),
        'evaluation_mean_ms':engine.total_ms/max(engine.evaluations,1),'evaluation_max_ms':engine.max_ms,
        'external_notifications':0,'stats':dict(engine.state.stats)}
    (target/'evaluation.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',required=True);p.add_argument('--output',required=True)
    args=p.parse_args();evaluate_file(args.input,args.output)
