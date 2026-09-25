"""Event-time rolling feature engine for Binance BTC-USDT."""
import argparse, asyncio, json, os, signal
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
import structlog
from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.schemas.trade import TradeEvent
from integrity_engine.schemas.orderbook import OrderBookSnapshot
from integrity_engine.schemas.metrics import MarketMetrics
from integrity_engine.features.trades import trade_features
from integrity_engine.features.orderbook import orderbook_features
log=structlog.get_logger(__name__)
WINDOWS={"10s":10,"30s":30,"1m":60,"5m":300}

class FeatureEngine:
 def __init__(self, producer, interval_ms=1000):
  self.producer=producer; self.interval=interval_ms/1000; self.trades=deque(); self.books=deque(); self.previous_book=None
  self.trade_processed=self.book_processed=self.produced=self.malformed=self.warnings=0; self.late_events=self.timestamp_disorder=self.outside_retained=0; self.previous_short_return=None; self.latest_trade_ts=None; self.latest_book_ts=None
 def add_trade(self,e):
  if len(self.trades)>=200000:raise RuntimeError("feature trade state capacity exceeded")
  if self.latest_trade_ts and e.exchange_timestamp < self.latest_trade_ts: self.timestamp_disorder+=1; log.warning("late_trade_event",event_timestamp=e.exchange_timestamp.isoformat(),latest_timestamp=self.latest_trade_ts.isoformat())
  self.latest_trade_ts=max(self.latest_trade_ts or e.exchange_timestamp,e.exchange_timestamp); self.trades.append((e.exchange_timestamp,e)); self.trade_processed+=1
 def add_book(self,e):
  if len(self.books)>=200000:raise RuntimeError("feature book state capacity exceeded")
  if self.latest_book_ts and e.exchange_timestamp < self.latest_book_ts: self.timestamp_disorder+=1; log.warning("late_orderbook_event",event_timestamp=e.exchange_timestamp.isoformat(),latest_timestamp=self.latest_book_ts.isoformat())
  self.latest_book_ts=max(self.latest_book_ts or e.exchange_timestamp,e.exchange_timestamp); self.books.append((e.exchange_timestamp,e)); self.book_processed+=1
 def expire(self, now):
  cutoff=now-timedelta(seconds=300)
  self.trades=deque((t,e) for t,e in self.trades if t>=cutoff)
  self.books=deque((t,e) for t,e in self.books if t>=cutoff)
 def calculate(self, now=None):
  now=now or datetime.now(timezone.utc); self.expire(now); outputs=[]
  short_return=None
  for name,seconds in WINDOWS.items():
   start=now-timedelta(seconds=seconds); ts=[e for t,e in self.trades if start<=t<=now]; bs=[e for t,e in self.books if start<=t<=now]
   disordered=any(ts[i].exchange_timestamp<ts[i-1].exchange_timestamp for i in range(1,len(ts)))
   ts.sort(key=lambda e:e.exchange_timestamp);bs.sort(key=lambda e:e.exchange_timestamp)
   tf=trade_features(ts); ob=orderbook_features(bs,self.previous_book)
   flags=[]
   if not ts: flags.append("no_trades_in_window")
   if not bs: flags.append("insufficient_orderbook_samples")
   if bs and (now-bs[-1].exchange_timestamp).total_seconds()>5: flags.append("stale_orderbook")
   if disordered: flags.append("timestamp_disorder")
   if name=="10s": short_return=tf.get("price_return")
   acceleration=(short_return-self.previous_short_return) if name=="10s" and short_return is not None and self.previous_short_return is not None else None
   if flags: self.warnings += len(flags)
   data=dict(schema_version="1.0",event_id=uuid4(),exchange="binance",symbol="BTC-USDT",window=name,window_start=start,window_end=now,calculated_at=now,price_acceleration=acceleration,trade_event_count=len(ts),orderbook_event_count=len(bs),data_complete=not flags,data_quality_flags=flags,**tf,**ob)
   outputs.append(MarketMetrics(**data))
  if self.books:self.previous_book=orderbook_features([self.books[-1][1]])
  self.previous_short_return=short_return
  return outputs
 async def publish(self,now):
  metrics=self.calculate(now)
  await self.producer.publish_batch([("market.metrics",f"binance:BTC-USDT:{m.window}",m.model_dump(mode="json")) for m in metrics])
  self.produced+=len(metrics)
 async def run(self,duration=None):
  stop=asyncio.Event(); loop=asyncio.get_running_loop()
  for s in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(s,stop.set)
  consumer=ManualKafkaConsumer(os.environ["KAFKA_BOOTSTRAP_SERVERS"],["market.cleaned.trades","market.cleaned.orderbook"],os.getenv("FEATURE_CONSUMER_GROUP_ID","market-feature-engine-v1"))
  timer=loop.call_later(duration,stop.set) if duration else None; next_calc=loop.time();next_log=loop.time()+10
  pending=[];failed=True
  try:
   while not stop.is_set():
    if len(pending)>=8192:
     await asyncio.sleep(max(0,next_calc-loop.time()))
     messages=[]
    else:messages=await consumer.poll_batch()
    for msg in messages:
      kind="trade" if msg.topic()=="market.cleaned.trades" else "book"
      raw=json.loads(msg.value())
      e=TradeEvent.model_validate(raw) if kind=="trade" else OrderBookSnapshot.model_validate(raw)
      if e.exchange == "binance" and e.symbol == "BTC-USDT":
       self.add_trade(e) if kind=="trade" else self.add_book(e)
      pending.append(msg)
    if loop.time()>=next_calc:
     await self.publish(datetime.now(timezone.utc))
     await consumer.commit_batch(pending);pending.clear();next_calc=loop.time()+self.interval
    if loop.time()>=next_log:
     log.info("feature_progress",trades=self.trade_processed,books=self.book_processed,metrics=self.produced,malformed=self.malformed);next_log=loop.time()+10
   failed=False
  except Exception as error:
   log.error("feature_engine_failed",error_type=type(error).__name__)
   raise
  finally:
   if timer:timer.cancel()
   if not failed and pending:
    await self.publish(datetime.now(timezone.utc));await consumer.commit_batch(pending)
   await consumer.close();await self.producer.close();log.info("feature_engine_stopped",trade_messages=self.trade_processed,orderbook_snapshots=self.book_processed,metric_events=self.produced,malformed=self.malformed,data_quality_warnings=self.warnings,late_events=self.late_events,timestamp_disorder=self.timestamp_disorder)

def main():
 p=argparse.ArgumentParser();p.add_argument("--duration",type=float);a=p.parse_args();structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()]);asyncio.run(FeatureEngine(JsonKafkaProducer(os.environ["KAFKA_BOOTSTRAP_SERVERS"]),int(os.getenv("FEATURE_CALCULATION_INTERVAL_MS","1000"))).run(a.duration))
if __name__=="__main__":main()
