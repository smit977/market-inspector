"""Durable single-owner state and outbox; publish before acknowledging input."""
import json
import sqlite3
from collections import Counter
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5
from .models import AlertEvent, SEVERITIES

class AlertState:
    def __init__(self, path=":memory:"):
        self.lock = None
        if path != ":memory:":
            import fcntl
            self.lock = open(path + ".lock", "a")
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS offsets (key TEXT PRIMARY KEY, offset INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        """)
        row = self.db.execute("SELECT value FROM state WHERE key='snapshot'").fetchone()
        self.data = json.loads(row[0]) if row else {"incidents":{}, "baseline":{}, "latest":{}, "recovery":{}, "notified":{}, "stats":{}}
        self.stats = Counter(self.data["stats"])
        self.data.setdefault("recovery_last", {})
    def seen(self, key, offset):
        row = self.db.execute("SELECT offset FROM offsets WHERE key=?", (key,)).fetchone()
        return row is not None and offset <= row[0]
    def checkpoint(self, key, offset, events):
        self.data["stats"] = dict(self.stats)
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO state VALUES ('snapshot',?)", (json.dumps(self.data),))
            self.db.execute("INSERT OR REPLACE INTO offsets VALUES (?,?)", (key, offset))
            for e in events:
                self.db.execute("INSERT OR IGNORE INTO outbox VALUES (?,?)", (e.event_id,e.model_dump_json()))
    def pending(self):
        return [AlertEvent.model_validate_json(r[0]) for r in self.db.execute("SELECT payload FROM outbox ORDER BY rowid")]
    def delivered(self, event_id):
        with self.db: self.db.execute("DELETE FROM outbox WHERE id=?", (event_id,))
    def transition(self, a, now, config, replay=False):
        self.stats["rules_evaluated"] += 1
        if a.category == "DATA_QUALITY" and a.active: self.stats["quality_suppressed"] += 1
        key = ("replay:" if replay else "live:") + a.key
        stored = self.data["incidents"].get(key)
        current = AlertEvent.model_validate(stored) if stored else None
        rule=config.rules[a.alert_type]
        if current and now < current.last_observed_at: return None
        if not a.active:
            if not current or current.status == "resolved": return None
            if not a.recovered:
                self.data["recovery"].pop(key,None)
                return None
            last_good=self.data["recovery_last"].get(key)
            if last_good and (now-datetime.fromisoformat(last_good)).total_seconds()>config.max_age_seconds:
                self.data["recovery"].pop(key,None)
            self.data["recovery_last"][key]=now.isoformat()
            since=self.data["recovery"].setdefault(key,now.isoformat())
            if (now-datetime.fromisoformat(since)).total_seconds()<rule.recovery_seconds: return None
            current.status="resolved"; current.resolved_at=now
            current.summary=f"{a.canonical_pair} {a.alert_type.replace('_',' ')} recovered for {rule.recovery_seconds} seconds."
            self.stats["alerts_resolved"]+=1
            notify=True
        else:
            self.data["recovery"].pop(key,None)
            if current is None or current.status == "resolved":
                identity=str(uuid5(NAMESPACE_URL, key+":"+now.isoformat()+":"+','.join(a.trigger_event_ids)))
                current=AlertEvent(event_id=identity,alert_id=identity,revision=1,created_at=now,updated_at=now,
                    canonical_pair=a.canonical_pair,category=a.category,alert_type=a.alert_type,
                    severity=a.severity,initial_severity=a.severity,maximum_severity=a.severity,
                    title=a.alert_type.replace('_',' ').capitalize(),summary=a.summary,
                    first_detected_at=now,last_observed_at=now,evidence=a.evidence,
                    overall_confidence=a.confidence,evidence_coverage=a.coverage,dedupe_key=a.key,is_replay=replay)
                self.stats["alerts_opened"]+=1
                notify=True
            else:
                escalating=SEVERITIES.index(a.severity)>SEVERITIES.index(current.maximum_severity)
                current.last_observed_at=now
                if not escalating and (now-current.updated_at).total_seconds()<config.update_seconds:
                    self.stats["alerts_suppressed"]+=1
                    self.data["incidents"][key]=current.model_dump(mode="json")
                    return None
                if escalating:
                    current.escalation_timeline.append({"at":now.isoformat(),"from":current.severity,"to":a.severity})
                    current.maximum_severity=a.severity
                    self.stats["alerts_escalated"]+=1
                else: self.stats["alerts_updated"]+=1
                current.severity=a.severity
                notify=escalating
                current.summary=a.summary
        if stored and current.alert_id == stored["alert_id"]: current.revision+=1
        current.updated_at=now; current.last_observed_at=now
        current.evidence=a.evidence; current.trigger_event_ids=list(dict.fromkeys(current.trigger_event_ids+a.trigger_event_ids))[-64:]
        current.overall_confidence=a.confidence; current.evidence_coverage=a.coverage
        current.integrity_score=a.integrity_score; current.previous_integrity_score=a.previous_integrity_score
        current.integrity_score_change=(a.integrity_score-a.previous_integrity_score if a.integrity_score is not None and a.previous_integrity_score is not None else None)
        current.classification=a.classification; current.affected_exchanges=a.exchanges; current.data_quality_flags=a.flags
        last=self.data["notified"].get(key)
        in_cooldown=last and (now-datetime.fromisoformat(last)).total_seconds()<rule.cooldown_seconds
        escalated=bool(current.escalation_timeline and current.escalation_timeline[-1]["at"]==now.isoformat())
        notify=notify and (not in_cooldown or escalated or current.status=="resolved")
        current.notification_state="replay_suppressed" if replay else "pending" if notify else "suppressed"
        if notify: self.data["notified"][key]=now.isoformat()
        current.event_id=str(uuid5(NAMESPACE_URL,current.alert_id+":"+str(current.revision)))
        self.data["incidents"][key]=current.model_dump(mode="json")
        return current
    def close(self):
        self.db.close()
        if self.lock: self.lock.close()
