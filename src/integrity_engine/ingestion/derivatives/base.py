import json
import asyncio
from datetime import datetime, timezone
from typing import Any
import structlog
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake
from integrity_engine.kafka.producer import JsonKafkaProducer, KafkaDeliveryError

log = structlog.get_logger(__name__)

def raw_event(exchange: str, symbol: str, event_type: str, payload: dict[str, Any], received: datetime) -> dict[str, Any]:
    return {"schema_version":"1.0","exchange":exchange,"stream":"derivatives","event_type":event_type,"exchange_symbol":symbol,"symbol":symbol,"received_timestamp":received.astimezone(timezone.utc).isoformat(),"payload":payload}

class DerivativesWebsocket:
    def __init__(self, exchange, url, symbol, subscription, classifier, producer, no_event_timeout_ms=15000):
        self.exchange,self.url,self.symbol,self.subscription,self.classifier,self.producer=exchange,url,symbol,subscription,classifier,producer;self.received=0;self.errors=0;self.no_event_timeout=no_event_timeout_ms/1000;self.last_event_received_at=None;self.status="disconnected"
    async def run(self, stop):
        delay=1.0
        while not stop.is_set():
            try:
                async with connect(self.url,open_timeout=10,close_timeout=3,ping_interval=20,ping_timeout=20,max_queue=128) as ws:
                    await ws.send(json.dumps(self.subscription));log.info("derivatives_connected",exchange=self.exchange)
                    self.status="healthy"
                    while not stop.is_set():
                        try: message=await asyncio.wait_for(ws.recv(),timeout=self.no_event_timeout)
                        except asyncio.TimeoutError:
                            if self.received == 0:
                                self.status="unavailable";log.warning("derivatives_source_unavailable",exchange=self.exchange,reason="no_trade_or_liquidation_event_timeout")
                            break
                        if stop.is_set(): break
                        received=datetime.now(timezone.utc)
                        try:
                            payload=json.loads(message); kind=self.classifier(payload)
                            if kind is None: continue
                            await self.producer.publish("market.raw.derivatives",self.symbol,raw_event(self.exchange,self.symbol,kind,payload,received));self.received+=1;self.last_event_received_at=received;self.status="healthy";delay=1.0
                        except KafkaDeliveryError:
                            raise
                        except (ValueError, TypeError, KeyError) as error: self.errors+=1;log.warning("derivatives_malformed_payload",exchange=self.exchange,error_type=type(error).__name__)
            except (ConnectionClosed,InvalidHandshake,OSError,TimeoutError) as error: self.status="disconnected";log.error("derivatives_connection_error",exchange=self.exchange,error=str(error))
            if not stop.is_set(): await __import__('asyncio').sleep(delay);delay=min(delay*2,30)
