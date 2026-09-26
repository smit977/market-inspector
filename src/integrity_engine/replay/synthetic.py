"""Small deterministic five-venue fixture with a documented injected discontinuity."""
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from .archive import archived_record, write_archive


def synthetic_archive(root, replay_id="synthetic", seconds=45):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc); rows = []; offsets = Counter()
    def add(topic, raw, at):
        n=offsets[topic]; offsets[topic]+=1
        rows.append(archived_record(topic,0,n,json.dumps(raw).encode(),at))
    def raw(exchange, stream, symbol, payload, at, kind=None):
        data={"schema_version":"1.0","exchange":exchange,"stream":stream,"symbol":symbol,"exchange_symbol":symbol,"received_timestamp":at.isoformat(),"payload":payload}
        if kind:data["event_type"]=kind
        return data
    add("market.raw.orderbook",raw("binance","orderbook","BTCUSDT",{"lastUpdateId":100,"bids":[["99.99","10"]],"asks":[["100.01","10"]]},start),start)
    old=Decimal("100")
    for i in range(seconds):
        at=start+timedelta(seconds=i,milliseconds=100); ms=int(at.timestamp()*1000)
        price=Decimal("100")+Decimal(i%5)/100+(Decimal("5") if i>=30 else 0)
        quantity="100" if i==30 else str(1+i%3)
        symbols={"binance":"BTCUSDT","coinbase":"BTC-USD","kraken":"BTC/USD","okx":"BTC-USDT","bybit":"BTCUSDT"}
        payloads={
            "binance":{"e":"trade","s":"BTCUSDT","p":str(price),"q":quantity,"t":i,"T":ms,"m":bool(i%2)},
            "coinbase":{"type":"match","product_id":"BTC-USD","price":str(price),"size":quantity,"trade_id":i,"sequence":i,"side":"sell","time":at.isoformat()},
            "kraken":{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","price":str(price),"qty":quantity,"side":"buy","trade_id":i,"timestamp":at.isoformat()}]},
            "okx":{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":str(i),"px":str(price),"sz":quantity,"side":"buy","ts":str(ms)}]},
            "bybit":{"topic":"publicTrade.BTCUSDT","data":[{"T":ms,"s":"BTCUSDT","S":"Buy","p":str(price),"v":quantity,"i":str(i)}]}}
        for venue in symbols:add("market.raw.trades",raw(venue,"trades",symbols[venue],payloads[venue],at),at)
        depth="1" if i>=30 else str(10+i%3)
        p={"e":"depthUpdate","s":"BTCUSDT","E":ms,"U":101+i,"u":101+i,
           "b":[[str(old-Decimal('.01')),"0"],[str(price-Decimal('.01')),depth]],
           "a":[[str(old+Decimal('.01')),"0"],[str(price+Decimal('.01')),depth]]}
        add("market.raw.orderbook",raw("binance","orderbook","BTCUSDT",p,at),at);old=price
        add("market.raw.derivatives",raw("bybit","derivatives","BTCUSDT",{"data":[{"p":str(price),"v":quantity,"T":ms,"i":str(i),"S":"Buy"}]},at,"perpetual_trade"),at)
    return write_archive(rows,root,replay_id,start,start+timedelta(seconds=seconds),list(offsets),notes=["Synthetic discontinuity at +30s; not empirical ground truth."])
