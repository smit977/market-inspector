"""Cross-venue rolling metrics; input commits follow confirmed metric batches."""
import argparse,asyncio,json,os,signal
from datetime import datetime,timezone
import structlog
from integrity_engine.kafka.consumer import ManualKafkaConsumer
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.schemas.trade import TradeEvent
from integrity_engine.features.cross_venue import CrossVenueFeatureEngine,WINDOWS

async def run(duration):
    b=os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    engine=CrossVenueFeatureEngine(stale_after_ms=int(os.getenv("CROSS_VENUE_STALE_AFTER_MS","5000")),minimum_active_venues=int(os.getenv("CROSS_VENUE_MIN_ACTIVE_VENUES","3")))
    producer=JsonKafkaProducer(b);consumer=ManualKafkaConsumer(b,"market.cleaned.trades","cross-venue-feature-engine-v1")
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for s in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(s,stop.set)
    timer=loop.call_later(duration,stop.set) if duration else None
    next_calc=loop.time();next_log=loop.time()+10;produced=0;pending=[]
    try:
        while not stop.is_set():
            if len(pending)>=8192:
                await asyncio.sleep(max(0,next_calc-loop.time()));messages=[]
            else:messages=await consumer.poll_batch()
            for m in messages:
                engine.add_trade(TradeEvent.model_validate_json(m.value()));pending.append(m)
            if loop.time()>=next_calc:
                now=datetime.now(timezone.utc)
                metrics=[engine.calculate(w,now) for w in WINDOWS]
                await producer.publish_batch([("market.metrics.cross_venue",f"BTC-USD:{m.window}",m.model_dump(mode="json")) for m in metrics])
                await consumer.commit_batch(pending);pending.clear();produced+=len(metrics)
                next_calc=loop.time()+int(os.getenv("CROSS_VENUE_CALCULATION_INTERVAL_MS","1000"))/1000
            if loop.time()>=next_log:
                structlog.get_logger().info("cross_venue_progress",trades=engine.processed,metrics=produced,malformed=engine.malformed);next_log=loop.time()+10
    finally:
        if timer:timer.cancel()
        for s in (signal.SIGINT,signal.SIGTERM):loop.remove_signal_handler(s)
        await consumer.close();await producer.close()
        structlog.get_logger().info("cross_venue_features_stopped",trades=engine.processed,metrics=produced,malformed=engine.malformed,timestamp_disorder=engine.timestamp_disorder)

def main():
    p=argparse.ArgumentParser();p.add_argument("--duration",type=float);a=p.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()])
    try:asyncio.run(run(a.duration))
    except Exception as error:
        structlog.get_logger().error("cross_venue_features_failed",error_type=type(error).__name__);return 1
    return 0
if __name__=="__main__":raise SystemExit(main())
