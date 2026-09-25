import json,os,asyncio,signal
from datetime import datetime,timezone
import structlog
from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.schemas.anomaly import MarketAnomaly
from integrity_engine.schemas.metrics import MarketMetrics
from integrity_engine.schemas.cross_venue import CrossVenueMetrics
from integrity_engine.schemas.derivatives import DerivativesMetrics
from integrity_engine.integrity.scoring import score_integrity

class IntegrityEngine:
    def __init__(self,producer,max_age_seconds=5): self.producer=producer;self.max_age=max_age_seconds;self.metrics={};self.cross={};self.derivatives={};self.produced=0;self.stale=0
    def _fresh(self,value,now): return value if value and abs((now-value.calculated_at).total_seconds())<=self.max_age else None
    def process(self,anomaly):
        key=(anomaly.exchange,anomaly.symbol,anomaly.window);now=anomaly.detected_at
        candidates=[v for (exchange,window),v in self.derivatives.items() if window==anomaly.window and self._fresh(v,now) and v.perpetual_to_spot_volume_ratio is not None]
        derivative=max(candidates,key=lambda v:(v.calculated_at,v.exchange)) if candidates else None
        result=score_integrity(anomaly,self._fresh(self.metrics.get(key),now),self._fresh(self.cross.get(anomaly.window),now),derivative,self.max_age);return result
    async def run(self,duration=None):
        b=os.environ["KAFKA_BOOTSTRAP_SERVERS"];c=ManualKafkaConsumer(b,["market.anomalies","market.metrics","market.metrics.cross_venue","market.metrics.derivatives"],os.getenv("INTEGRITY_CONSUMER_GROUP_ID","market-integrity-engine-v1"));stop=asyncio.Event();loop=asyncio.get_running_loop();[loop.add_signal_handler(s,stop.set) for s in (signal.SIGINT,signal.SIGTERM)];timer=loop.call_later(duration,stop.set) if duration else None
        try:
            while not stop.is_set():
                m=await c.poll()
                if not m:continue
                try:
                    raw=json.loads(m.value())
                    if m.topic()=="market.anomalies":
                        event=self.process(MarketAnomaly.model_validate(raw));await self.producer.publish("market.integrity",f"BTC-USD:{event.trigger_event_id}",event.model_dump(mode="json"));self.produced+=1
                    elif m.topic()=="market.metrics":
                        metric=MarketMetrics.model_validate(raw);self.metrics[(metric.exchange,metric.symbol,metric.window)]=metric
                    elif m.topic()=="market.metrics.cross_venue": self.cross[raw["window"]]=CrossVenueMetrics.model_validate(raw)
                    else:self.derivatives[(raw["exchange"],raw["window"])]=DerivativesMetrics.model_validate(raw)
                    await c.commit(m)
                except Exception as error:
                    structlog.get_logger().error("integrity_input_error",error_type=type(error).__name__)
                    raise
        finally:
            if timer:timer.cancel()
            [loop.remove_signal_handler(s) for s in (signal.SIGINT,signal.SIGTERM)];await c.close();await self.producer.close();structlog.get_logger().info("integrity_engine_stopped",integrity_events=self.produced)
