"""Public perpetual sources; restrictions explicit, delivery failures fatal."""
import argparse,asyncio,os,signal
from datetime import datetime,timezone
import httpx,structlog
from integrity_engine.ingestion.binance.derivatives import ingestor as binance_ingestor
from integrity_engine.ingestion.bybit.derivatives import ingestor as bybit_ingestor
from integrity_engine.ingestion.okx.derivatives import ingestor as okx_ingestor
from integrity_engine.ingestion.derivatives.base import raw_event
from integrity_engine.kafka.producer import JsonKafkaProducer,KafkaDeliveryError
log=structlog.get_logger(__name__)

async def poll_rest(exchange,producer,stop,interval):
    async with httpx.AsyncClient(timeout=8) as client:
        while not stop.is_set():
            now=datetime.now(timezone.utc)
            try:
                if exchange=="binance":
                    specs=[("https://fapi.binance.com/fapi/v1/openInterest",{"symbol":"BTCUSDT"}),
                           ("https://fapi.binance.com/fapi/v1/premiumIndex",{"symbol":"BTCUSDT"})]
                elif exchange=="bybit":
                    specs=[("https://api.bybit.com/v5/market/open-interest",{"category":"linear","symbol":"BTCUSDT","intervalTime":"5min"}),
                           ("https://api.bybit.com/v5/market/tickers",{"category":"linear","symbol":"BTCUSDT"})]
                else:
                    specs=[("https://www.okx.com/api/v5/public/open-interest",{"instType":"SWAP","instId":"BTC-USDT-SWAP"}),
                           ("https://www.okx.com/api/v5/public/funding-rate",{"instId":"BTC-USDT-SWAP"})]
                responses=await asyncio.gather(*(client.get(url,params=params) for url,params in specs))
                restricted=[r.status_code for r in responses if r.status_code in {403,451}]
                if restricted:
                    log.warning("source_unavailable",exchange=exchange,feed_type="rest_derivatives",status="restricted",http_status=max(restricted))
                else:
                    for r in responses:r.raise_for_status()
                    oi,funding=[r.json() for r in responses]
                    if exchange=="bybit":
                        if oi.get("retCode") or funding.get("retCode"):raise ValueError("REST source returned error")
                        oi={"data":oi["result"]["list"],"timestamp":oi.get("time")}
                        funding={"data":funding["result"]["list"],"ts":funding.get("time")}
                    symbol="BTC-USDT-SWAP" if exchange=="okx" else "BTCUSDT"
                    await producer.publish_batch([("market.raw.derivatives",symbol,raw_event(exchange,symbol,"open_interest",oi,now)),
                                                  ("market.raw.derivatives",symbol,raw_event(exchange,symbol,"funding_rate",funding,now))])
            except KafkaDeliveryError:
                raise
            except (httpx.HTTPError,ValueError,KeyError,IndexError) as error:
                log.warning("derivatives_rest_poll_error",exchange=exchange,error_type=type(error).__name__)
            try:await asyncio.wait_for(stop.wait(),timeout=interval)
            except asyncio.TimeoutError:pass

async def run(duration):
    producer=JsonKafkaProducer(os.environ["KAFKA_BOOTSTRAP_SERVERS"]);stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(sig,stop.set)
    webs=[binance_ingestor(producer),bybit_ingestor(producer),okx_ingestor(producer)]
    for w in webs:w.no_event_timeout=int(os.getenv("DERIVATIVES_NO_TRADE_TIMEOUT_MS","15000"))/1000
    tasks=[asyncio.create_task(w.run(stop)) for w in webs]
    tasks += [asyncio.create_task(poll_rest(e,producer,stop,int(os.getenv("DERIVATIVES_OI_POLL_INTERVAL_MS","5000"))/1000)) for e in ("binance","bybit","okx")]
    stop_task=asyncio.create_task(stop.wait());timer=loop.call_later(duration,stop.set) if duration else None
    log.info("derivatives_ingestion_started")
    try:
        done,_=await asyncio.wait([*tasks,stop_task],return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is not stop_task:
                task.result()
                if not stop.is_set():raise RuntimeError("derivatives source task exited unexpectedly")
    finally:
        if timer:timer.cancel()
        for task in [*tasks,stop_task]:task.cancel()
        await asyncio.gather(*tasks,stop_task,return_exceptions=True)
        await producer.close()
        log.info("derivatives_ingestion_stopped",websocket_events=sum(w.received for w in webs),malformed=sum(w.errors for w in webs),source_health={w.exchange:{"status":w.status,"events":w.received} for w in webs})

def main():
    p=argparse.ArgumentParser();p.add_argument("--duration",type=float);a=p.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()])
    asyncio.run(run(a.duration))
if __name__=="__main__":main()
