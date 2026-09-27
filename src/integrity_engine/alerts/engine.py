"""Typed inputs, event-time rules and crash-recoverable Kafka output."""
import asyncio
import json
import os
import signal
import time
from datetime import datetime, timezone
from pydantic import ValidationError
import structlog
from integrity_engine.schemas.metrics import MarketMetrics
from integrity_engine.schemas.cross_venue import CrossVenueMetrics
from integrity_engine.schemas.derivatives import DerivativesMetrics
from integrity_engine.schemas.integrity import MarketIntegrityEvent
from integrity_engine.schemas.anomaly import MarketAnomaly
from .config import AlertConfig
from .state import AlertState
from .models import OperationalHealth, AlertEvidence, Assessment
from .rules import evaluate, stamp
from .notifiers import LoggingNotifier, WebhookNotifier, NotificationConfig
from .watchdog import ProgressWatchdog

log=structlog.get_logger(__name__)
MODELS={"market.metrics":MarketMetrics,"market.metrics.cross_venue":CrossVenueMetrics,
        "market.metrics.derivatives":DerivativesMetrics,"market.integrity":MarketIntegrityEvent,"market.anomalies":MarketAnomaly,"system.operational_health":OperationalHealth}
TOPICS=list(MODELS)

class AlertEngine:
    def __init__(self,state=None,config=None,is_replay=False):
        self.state=state or AlertState(); self.config=config or AlertConfig.load(); self.is_replay=is_replay
        self.evaluations=0; self.total_ms=0.; self.max_ms=0.
    def process(self,topic,event,now):
        start=time.perf_counter()
        if topic in MODELS:
            event=MODELS[topic].model_validate(event).model_dump(mode="json")
        if event.get("schema_version","1.0")!="1.0": raise ValueError("unsupported schema")
        window=event.get("window",event.get("move_window"))
        if window and window!=self.config.window: return []
        key=f"{topic}:{event.get('canonical_pair','BTC-USD')}:{event.get('exchange','all')}:{window}"
        previous=self.state.data["baseline"].get(key)
        latest=self.state.data["latest"].get(key)
        if stamp(event).tzinfo is None: raise ValueError("timezone required")
        if stamp(event)>now:
            return [output for a in evaluate(topic,event,None,now,self.config)
                    if a.category == "DATA_QUALITY"
                    if (output:=self.state.transition(a,now,self.config,self.is_replay))]
        if latest and stamp(event)<=stamp(latest):
            self.state.stats["alerts_suppressed"]+=1
            if stamp(event)==stamp(latest): return []
            disorder=dict(event, data_quality_flags=[*event.get("data_quality_flags",[]),"timestamp_disorder"])
            return [output for a in evaluate(topic,disorder,None,now,self.config)
                    if a.alert_type == "timestamp_disorder"
                    if (output:=self.state.transition(a,now,self.config,self.is_replay))]
        assessments=evaluate(topic,event,previous,now,self.config)
        events=[]
        assessed={a.key for a in assessments}
        scope=f"{topic}:{event.get('exchange','all')}:{window}"
        for incident_key,incident in self.state.data["incidents"].items():
            if (incident["category"]=="MARKET_STRUCTURE" and incident["status"]!="resolved"
                and incident["canonical_pair"]==event.get("canonical_pair","BTC-USD")
                and incident["dedupe_key"].endswith(scope) and incident["dedupe_key"] not in assessed):
                self.state.data["recovery"].pop(incident_key,None)
                self.state.data["recovery_last"].pop(incident_key,None)
        for assessment in assessments:
            if assessment.category == "MARKET_STRUCTURE":
                for cached in self.state.data["latest"].values():
                    if not 0 <= (now-stamp(cached)).total_seconds() <= self.config.max_age_seconds: continue
                    if cached.get("canonical_pair", "BTC-USD") != assessment.canonical_pair: continue
                    if cached.get("event_type") == "liquidity_shock" and assessment.alert_type == "liquidity_shock":
                        assessment.trigger_event_ids.append(str(cached["event_id"]))
                        assessment.evidence.append(AlertEvidence(source="market.anomalies",metric="liquidity_shock_anomaly",
                            current_value=cached["event_id"],freshness=(now-stamp(cached)).total_seconds(),
                            interpretation="Related observed anomaly; measurements independently determine the alert."))
                    component_name={"liquidity_shock":"Liquidity Stability", "derivatives_amplification":"Derivatives Dependence", "liquidation_burst":"Liquidation Dependence"}.get(assessment.alert_type)
                    for component in cached.get("components", []):
                        if component["name"] == component_name:
                            assessment.evidence.append(AlertEvidence(source="market.integrity",metric=component_name,
                                current_value=component.get("score"),quality=component.get("data_quality_flags",[]),
                                freshness=(now-stamp(cached)).total_seconds(),window=cached.get("move_window"),
                                interpretation="Related component assessment, separate from observed measurements."))
            output=self.state.transition(assessment,now,self.config,self.is_replay)
            if output: events.append(output)
        # Retain a comparison anchor through active market incidents; normal anchors roll at window boundary.
        active=any(v["status"]!="resolved" and v["category"]=="MARKET_STRUCTURE" and v["dedupe_key"].endswith(f"{topic}:{event.get('exchange','all')}:{window}") for v in self.state.data["incidents"].values())
        if not previous or (not active and (stamp(event)-stamp(previous)).total_seconds()>=self.config.comparison_seconds):
            self.state.data["baseline"][key]=event
        self.state.data["latest"][key]=event
        ms=(time.perf_counter()-start)*1000; self.evaluations+=1; self.total_ms+=ms; self.max_ms=max(ms,self.max_ms)
        return events

class AlertWorker:
    def __init__(self,engine,consumer,producer,notifier=None):
        self.engine,self.consumer,self.producer=engine,consumer,producer
        self.notifier=notifier or LoggingNotifier()
    async def drain(self):
        for event in self.engine.state.pending():
            # Incident generation is durably queued before notification; delivery failures are isolated.
            if event.notification_state=="pending":
                try: event.notification_state=await self.notifier.notify(event)
                except Exception: event.notification_state="failed"
                self.engine.state.stats["notification_failures" if event.notification_state=="failed" else "notifications_sent"]+=1
            await self.producer.publish("market.alerts",f"{event.canonical_pair}:{event.alert_type}",event.model_dump(mode="json"))
            self.engine.state.delivered(event.event_id)
    async def handle(self,message):
        state=self.engine.state; key=f"{message.topic()}:{message.partition()}"
        if not state.seen(key,message.offset()):
            try:
                event=json.loads(message.value())
                now=datetime.now(timezone.utc)
                outputs=self.engine.process(message.topic(),event,now)
                recovery=Assessment(canonical_pair="BTC-USD",category="DATA_QUALITY",alert_type="schema_failure",
                    scope=key,active=False,recovered=True,summary="Input schema is valid.",
                    evidence=[AlertEvidence(source=key,metric="schema_valid",current_value=True,freshness=0,
                        interpretation="The source partition supplied a valid typed event.")])
                resolved=state.transition(recovery,now,self.engine.config,self.engine.is_replay)
                if resolved: outputs.append(resolved)
            except (ValidationError,ValueError,TypeError,KeyError) as error:
                # Safe diagnostics; never include credentials or arbitrary raw payloads.
                now=datetime.now(timezone.utc)
                event={"event_id":f"{key}:{message.offset()}","calculated_at":now.isoformat(),"data_quality_flags":["schema_failure"]}
                a=Assessment(canonical_pair="BTC-USD",category="DATA_QUALITY",alert_type="schema_failure",
                    scope=key,active=True,recovered=False,summary="Input schema validation failed; evidence is unavailable.",
                    evidence=[AlertEvidence(source=key,metric="schema_failure",current_value=1,
                        interpretation="Input rejected without changing market integrity.",freshness=0)],
                    trigger_event_ids=[event["event_id"]])
                out=state.transition(a,now,self.engine.config,self.engine.is_replay)
                outputs=[out] if out else []
                log.warning("alert_schema_failure",source=key,offset=message.offset(),error_type=type(error).__name__)
            state.checkpoint(key,message.offset(),outputs)
        await self.drain()
        await self.consumer.commit(message)

async def run():
    if os.getenv("PROCESSING_MODE", "live") != "live":
        raise ValueError("Use the offline alert replay evaluator; live alert publication is disabled in replay mode")
    from integrity_engine.kafka.consumer import ManualKafkaConsumer
    from integrity_engine.kafka.producer import JsonKafkaProducer
    from .health import ensure_topics
    bootstrap=os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    await asyncio.to_thread(ensure_topics,bootstrap)
    state=AlertState(os.getenv("ALERT_STATE_PATH","/app/alert-state/state.sqlite"))
    engine=AlertEngine(state,AlertConfig.load(os.getenv("ALERT_RULES_PATH","config/alert_rules.json")))
    consumer=ManualKafkaConsumer(bootstrap,TOPICS,"alert-engine-v1")
    producer=JsonKafkaProducer(bootstrap)
    config=NotificationConfig.from_env()
    worker=AlertWorker(engine,consumer,producer,WebhookNotifier(config) if config.enabled else LoggingNotifier())
    stop=asyncio.Event(); loop=asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(sig,stop.set)
    next_log=loop.time()+10
    watchdog=ProgressWatchdog(engine.config.worker_stall_seconds)
    watchdog.start()
    try:
        await worker.drain()
        while not stop.is_set():
            for message in await consumer.poll_batch():
                await worker.handle(message)
                watchdog.touch()
            if loop.time()>=next_log:
                log.info("alert_engine_progress",**dict(state.stats),evaluations=engine.evaluations,
                    evaluation_mean_ms=engine.total_ms/max(1,engine.evaluations),evaluation_max_ms=engine.max_ms,
                    consumer_lag=await consumer.lag_snapshot())
                next_log=loop.time()+10
    finally:
        try:
            await consumer.close(); await producer.close(); state.close()
        finally: watchdog.close()
