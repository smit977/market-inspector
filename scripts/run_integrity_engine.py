import argparse,asyncio,os,structlog
from integrity_engine.kafka.producer import JsonKafkaProducer
from integrity_engine.integrity.engine import IntegrityEngine
def main():
 p=argparse.ArgumentParser();p.add_argument("--duration",type=float);a=p.parse_args();structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso",utc=True),structlog.processors.JSONRenderer()]);b=os.environ["KAFKA_BOOTSTRAP_SERVERS"];asyncio.run(IntegrityEngine(JsonKafkaProducer(b),int(os.getenv("INTEGRITY_MAX_EVIDENCE_AGE_SECONDS","5"))).run(a.duration))
if __name__=="__main__":main()
