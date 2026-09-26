"""Run the isolated in-process replay; no traffic is published to live Kafka."""
import argparse
import json
import os
from pathlib import Path
from integrity_engine.replay.archive import stamp
from integrity_engine.replay.runner import run_replay
from integrity_engine.replay.storage import ReplayStore


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True); p.add_argument("--speed", default="max")
    p.add_argument("--start"); p.add_argument("--end"); p.add_argument("--exchanges", nargs="+"); p.add_argument("--streams", nargs="+")
    p.add_argument("--configuration"); p.add_argument("--output-dir", default="data/replay/runs")
    p.add_argument("--run-id"); p.add_argument("--clickhouse", action="store_true")
    a = p.parse_args()
    if os.getenv("PROCESSING_MODE", "replay") != "replay": p.error("PROCESSING_MODE must be replay")
    config = json.loads(Path(a.configuration).read_text()) if a.configuration else None
    run, outputs, directory = run_replay(a.manifest, a.output_dir, a.speed, config,
        stamp(a.start) if a.start else None, stamp(a.end) if a.end else None, a.exchanges, a.streams, a.run_id)
    if a.clickhouse:
        from integrity_engine.storage.clickhouse import ClickHouseStore
        store = ClickHouseStore()
        try:
            replay = ReplayStore(store); replay.initialize()
            stats = replay.write(run, outputs, json.loads((directory/"catalog.json").read_text()))
            (directory/"storage.json").write_text(json.dumps(stats, indent=2))
        finally: store.close()
    print(run.model_dump_json(indent=2)); print(f"Output: {directory}")


if __name__ == "__main__": main()
