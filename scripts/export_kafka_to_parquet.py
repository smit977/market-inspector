"""Export a closed Kafka timestamp interval without committing consumer offsets."""
import argparse
import os
from integrity_engine.replay.archive import export_kafka, stamp


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True); p.add_argument("--end", required=True)
    p.add_argument("--replay-id", required=True); p.add_argument("--output-dir", default="data/replay")
    p.add_argument("--topics", nargs="+", default=["market.raw.trades", "market.raw.orderbook", "market.raw.derivatives"])
    p.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "none"])
    p.add_argument("--max-events", type=int, default=1000000)
    a = p.parse_args()
    print(export_kafka(os.environ["KAFKA_BOOTSTRAP_SERVERS"], a.output_dir, a.replay_id, stamp(a.start), stamp(a.end), a.topics, a.compression, a.max_events))


if __name__ == "__main__": main()
