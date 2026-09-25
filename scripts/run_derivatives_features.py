"""Batched row-safe derivatives normalization and timer-driven rolling metrics."""
import argparse, asyncio, json, os, signal
from datetime import datetime, timezone
import structlog
from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.normalization.derivatives import normalize_derivative_batch
from integrity_engine.normalization.base import make_dlq_event
from integrity_engine.features.derivatives import DerivativesFeatureEngine, WINDOWS
from integrity_engine.schemas.trade import TradeEvent

async def handle_batch(messages, producer, consumer, engines, stats, commit=True):
    outputs=[]; staged=[]
    async def deliver():
        if outputs:
            await producer.publish_batch(outputs)
            for event in staged: engines[event.exchange].add(event)
            outputs.clear(); staged.clear()
    for msg in messages:
        original=None
        try:
            original=json.loads(msg.value())
            if msg.topic()=="market.metrics.cross_venue":
                for engine in engines.values():engine.add_spot_metric(original)
            elif msg.topic()=="market.cleaned.trades":
                spot=TradeEvent.model_validate(original)
                if spot.canonical_asset_pair=="BTC-USD":
                    for engine in engines.values():engine.add_spot(spot)
                    stats["spot_trades"]+=1
            else:
                for index,row,event,error in normalize_derivative_batch(original):
                    if error is not None:
                        dlq=make_dlq_event(row,msg,error);dlq["processor_name"]="derivatives-normalizer-v1";dlq["row_index"]=index
                        outputs.append(("system.dlq",f"{msg.topic()}:{msg.partition()}:{msg.offset()}:{index}",dlq));stats["dlq"]+=1
                    else:
                        outputs.append(("market.cleaned.derivatives",event.exchange_symbol,event.model_dump(mode="json")))
                        staged.append(event);stats["cleaned"]+=1
                        if (datetime.now(timezone.utc)-event.exchange_timestamp).total_seconds()>5:stats["late"]+=1
                    if len(outputs)>=256:await deliver()
        except (json.JSONDecodeError, UnicodeError) as error:
            dlq=make_dlq_event(original,msg,error);dlq["processor_name"]="derivatives-normalizer-v1"
            outputs.append(("system.dlq",f"{msg.topic()}:{msg.partition()}:{msg.offset()}",dlq));stats["dlq"]+=1
    await deliver()
    if commit:await consumer.commit_batch(messages)

async def run(duration):
    b=os.environ["KAFKA_BOOTSTRAP_SERVERS"];producer=JsonKafkaProducer(b)
    consumer=ManualKafkaConsumer(b,["market.raw.derivatives","market.cleaned.trades","market.metrics.cross_venue"],os.getenv("DERIVATIVES_CONSUMER_GROUP_ID","derivatives-normalizer-v1"))
    engines={e:DerivativesFeatureEngine() for e in ("binance","bybit","okx")}
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for s in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(s,stop.set)
    timer=loop.call_later(duration,stop.set) if duration else None
    next_calc=loop.time();next_log=loop.time()+10
    stats=dict(cleaned=0,dlq=0,metrics=0,spot_trades=0,late=0,stale=0)
    structlog.get_logger().info("derivatives_features_started")
    pending=[];failed=True
    try:
        while not stop.is_set():
            if len(pending)>=8192:
                await asyncio.sleep(max(0,next_calc-loop.time()));messages=[]
            else:messages=await consumer.poll_batch()
            if messages:
                await handle_batch(messages,producer,consumer,engines,stats,commit=False)
                pending.extend(messages)
            if loop.time()>=next_calc:
                now=datetime.now(timezone.utc);outputs=[]
                for exchange,engine in engines.items():
                    engine.expire(now)
                    if not engine.latest_perpetual and not engine.latest_oi:continue
                    for window in WINDOWS:
                        metric=engine.calculate(window,now)
                        outputs.append(("market.metrics.derivatives",f"{exchange}:BTC-USD:{window}",metric.model_dump(mode="json")))
                        stats["metrics"]+=1
                        stats["stale"]+=int(any(f.startswith("stale") for f in metric.data_quality_flags))
                await producer.publish_batch(outputs)
                await consumer.commit_batch(pending);pending.clear()
                next_calc=loop.time()+1
            if loop.time()>=next_log:
                structlog.get_logger().info("derivatives_features_progress",**stats);next_log=loop.time()+10
        failed=False
    finally:
        if timer:timer.cancel()
        for s in (signal.SIGINT,signal.SIGTERM):loop.remove_signal_handler(s)
        # Unacknowledged final state replays safely; normalized IDs are deterministic.
        await consumer.close();await producer.close()
        structlog.get_logger().info("derivatives_features_stopped",**stats)

def main():
    p=argparse.ArgumentParser();p.add_argument("--duration",type=float);a=p.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()])
    asyncio.run(run(a.duration))
if __name__=="__main__":main()
