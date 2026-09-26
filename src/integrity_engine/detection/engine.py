"""Kafka-backed rolling baseline and move detector."""
import argparse, asyncio, json, os, signal, time
from datetime import datetime, timezone
import structlog
from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.kafka.validation import validated_event
from integrity_engine.schemas.metrics import MarketMetrics
from integrity_engine.detection.baselines import Baseline
from integrity_engine.detection.detectors import DetectionConfig, detect
log=structlog.get_logger(__name__)
METRICS=("absolute_return","quote_volume","trade_count","price_range","spread_bps","depth_change","orderbook_imbalance_20","trade_imbalance")
class MoveDetector:
 def __init__(self,producer,config=DetectionConfig(), clock=None):
  self.producer=producer;self.config=config;self.clock=clock or time.monotonic;self.baselines={};self.last_emitted={};self.metrics_consumed=self.produced=self.cooldown_suppressed=self.quality_suppressed=0
 def process(self,metric):
  key=(metric.exchange,metric.symbol,metric.window); bs=self.baselines.setdefault(key,{n:Baseline( max_observations=config_obs(self.config),minimum_observations=self.config.minimum_observations) for n in METRICS})
  ready=all(b.ready for b in bs.values() if b.values)
  metric=metric.model_copy(update={})
  anomalies=detect(metric,bs,self.config) if ready else []
  for n in METRICS: bs[n].add(getattr(metric,n,None))
  self.metrics_consumed+=1
  if metric.data_quality_flags:self.quality_suppressed+=1
  return anomalies if ready else []
 def eligible(self,metric):
  accepted=[]
  for anomaly in self.process(metric):
   key=(anomaly.event_type,anomaly.window); now=self.clock(); previous=self.last_emitted.get(key)
   if previous is not None and now-previous<self.config.cooldown_seconds and anomaly.severity=="low":self.cooldown_suppressed+=1;continue
   self.last_emitted[key]=now;accepted.append(anomaly)
  return accepted
 async def handle(self,metric):
  for anomaly in self.eligible(metric):
   await self.producer.publish("market.anomalies",f"{anomaly.exchange}:{anomaly.symbol}:{anomaly.event_type}",anomaly.model_dump(mode="json"));self.produced+=1
 async def run(self,duration=None):
  c=ManualKafkaConsumer(os.environ["KAFKA_BOOTSTRAP_SERVERS"],"market.metrics","market-move-detector-v1");stop=asyncio.Event();loop=asyncio.get_running_loop()
  for s in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(s,stop.set)
  timer=loop.call_later(duration,stop.set) if duration else None;next_log=loop.time()+10
  try:
   while not stop.is_set():
    m=await c.poll()
    if m:
     try:
      metric = await validated_event(m, MarketMetrics, self.producer, c, "market-move-detector-v1")
      if metric is not None:
       await self.handle(metric)
       await c.commit(m)
     except Exception as e:
      log.error("move_detector_input_error",error_type=type(e).__name__)
      raise
    if loop.time()>=next_log:
     log.info("detector_progress",metrics=self.metrics_consumed,quality_suppressed=self.quality_suppressed,anomalies=self.produced,cooldown_suppressed=self.cooldown_suppressed);next_log=loop.time()+10
  finally:
   if timer:timer.cancel()
   await c.close();await self.producer.close();ready=sum(1 for b in self.baselines.values() if all(x.ready for x in b.values()));log.info("move_detector_stopped",metrics_consumed=self.metrics_consumed,baseline_observations=sum(len(x.values) for b in self.baselines.values() for x in b.values()),baseline_ready=ready,anomalies_produced=self.produced,cooldown_suppressed=self.cooldown_suppressed,quality_suppressed=self.quality_suppressed)
def config_obs(c):return int(c.baseline_minutes*60/10)
def main():
 p=argparse.ArgumentParser();p.add_argument("--duration",type=float);a=p.parse_args();structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()]);asyncio.run(MoveDetector(JsonKafkaProducer(os.environ["KAFKA_BOOTSTRAP_SERVERS"])).run(a.duration))
if __name__=="__main__":main()
