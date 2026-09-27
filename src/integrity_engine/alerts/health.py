"""Measured operational signals only. No Docker socket or inferred WebSocket status."""
import asyncio
import os
import signal
import socket
from datetime import datetime, timezone
from uuid import uuid4
import structlog
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from integrity_engine.storage.clickhouse import ClickHouseStore
from integrity_engine.kafka.producer import JsonKafkaProducer

log=structlog.get_logger(__name__)

def ensure_topics(bootstrap):
    admin=AdminClient({"bootstrap.servers":bootstrap})
    names=["market.alerts","system.operational_health"]
    existing=admin.list_topics(timeout=10).topics
    missing=[NewTopic(n,1,1,config={"retention.ms":"604800000"}) for n in names if n not in existing]
    if not missing: return
    from confluent_kafka import KafkaException, KafkaError
    for future in admin.create_topics(missing).values():
        try: future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS: raise

class HealthCollector:
    def __init__(self,bootstrap):
        self.admin=AdminClient({"bootstrap.servers":bootstrap})
        self.reader=Consumer({"bootstrap.servers":bootstrap,"group.id":"alert-health-observer","enable.auto.commit":False})
        self.previous_lag={}; self.previous_dlq=None
        suffix="-live-"+os.getenv("PROCESSING_RUN_ID","current")
        self.groups=[x+suffix for x in ("trade-normalizer-v1","orderbook-reconstructor-v1","market-feature-engine-v1",
            "cross-venue-feature-engine-v1","derivatives-normalizer-v1","market-move-detector-v1","market-integrity-engine-v1","alert-engine-v1")]
        self.groups.append("clickhouse-writer-v1-live-"+os.getenv("CLICKHOUSE_PROCESSING_RUN_ID","v2"))
    def event(self,scope,signals):
        return {"schema_version":"1.0","event_id":str(uuid4()),"canonical_pair":"BTC-USD","exchange":scope,
                "calculated_at":datetime.now(timezone.utc).isoformat(),
                "signals":{k:({"scope":scope, **v} if isinstance(v,dict) else {"value":v,"scope":scope}) for k,v in signals.items()}}
    def collect(self):
        output=[]
        try:
            descriptions=self.admin.describe_consumer_groups(self.groups,request_timeout=5)
            for name,future in descriptions.items():
                group=future.result(); partitions=[p for m in group.members for p in m.assignment.topic_partitions]
                signals={"worker_unhealthy":int(not group.members)}
                if partitions:
                    # A separate read-only observer never subscribes, seeks, or commits.
                    from confluent_kafka import ConsumerGroupTopicPartitions
                    result=self.admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(name)],request_timeout=5)[name].result()
                    lag=0; known=0; unknown=0
                    for p in result.topic_partitions:
                        _,high=self.reader.get_watermark_offsets(TopicPartition(p.topic,p.partition),timeout=2)
                        if p.offset<0 and high>0:
                            unknown+=1; continue
                        known+=1
                        lag+=max(0,high-max(0,p.offset))
                    if known:
                        signals["consumer_lag_high"]={"value":lag,"known_partitions":known,"unknown_partitions":unknown,
                            "measurement":"committed_lag_lower_bound" if unknown else "committed_lag_total"}
                        previous=self.previous_lag.get(name)
                        if previous and previous[1:] == (known,unknown):
                            signals["consumer_lag_growing"]={**signals["consumer_lag_high"],"value":max(0,lag-previous[0])}
                        self.previous_lag[name]=(lag,known,unknown)
                output.append(self.event(name,signals))
            meta=self.admin.list_topics("system.dlq",timeout=5)
            high=sum(self.reader.get_watermark_offsets(TopicPartition("system.dlq",p),timeout=2)[1] for p in meta.topics["system.dlq"].partitions)
            if self.previous_dlq is not None: output.append(self.event("system.dlq",{"DLQ_activity":max(0,high-self.previous_dlq)}))
            self.previous_dlq=high
        except Exception as error:
            log.warning("operational_kafka_measurement_unavailable",error_type=type(error).__name__)
        store=None
        try:
            store=ClickHouseStore()
            age=store.query("SELECT dateDiff('second', max(timestamp), now()) FROM market_metrics").result_rows[0][0]
            output.append(self.event("clickhouse",{"clickhouse_persistence_failure":0,"processing_latency_degradation":max(0,age)}))
            for table in ("market_metrics", "cross_venue_metrics", "derivatives_metrics"):
                age=store.query(f"SELECT dateDiff('second', max(timestamp), now()) FROM {table}").result_rows[0][0]
                output.append(self.event(table,{"source_unavailable":int(age>30)}))
        except Exception as error:
            # Reachability/query failure is measured; it does not prove a writer insert failed.
            output.append(self.event("clickhouse",{"clickhouse_persistence_failure":1}))
            log.warning("operational_store_probe_failed",error_type=type(error).__name__)
        finally:
            if store: store.close()
        try:
            with socket.create_connection(("mcp-server",int(os.getenv("MCP_PORT","8001"))),timeout=2): pass
            unavailable=0
        except OSError: unavailable=1
        output.append(self.event("mcp_tcp",{"mcp_unavailable":unavailable}))
        return output
    def close(self): self.reader.close()

async def run():
    bootstrap=os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    await asyncio.to_thread(ensure_topics,bootstrap)
    collector=HealthCollector(bootstrap); producer=JsonKafkaProducer(bootstrap)
    stop=asyncio.Event(); loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT): loop.add_signal_handler(sig,stop.set)
    try:
        while not stop.is_set():
            events=await asyncio.to_thread(collector.collect)
            await producer.publish_batch([("system.operational_health",e["exchange"],e) for e in events])
            log.info("operational_health_collected",observations=len(events))
            try: await asyncio.wait_for(stop.wait(),10)
            except TimeoutError: pass
    finally:
        collector.close(); await producer.close()
