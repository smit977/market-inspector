"""Explicit opt-in ClickHouse evaluation storage, never live analytical tables."""
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from .archive import stamp
from .runner import canonical

TABLES = {"replay.metrics": "replay_metrics", "replay.metrics.cross_venue": "replay_cross_venue_metrics",
          "replay.metrics.derivatives": "replay_derivatives_metrics", "replay.anomalies": "replay_anomalies", "replay.integrity": "replay_integrity"}


class ReplayStore:
    def __init__(self, store): self.store = store

    def initialize(self):
        client = self.store.client
        client.database = self.store.database
        for table in [*TABLES.values(), "replay_runs", "historical_events"]:
            client.command(f"CREATE TABLE IF NOT EXISTS `{table}` (replay_id String, replay_run_id String, event_id String, original_event_time DateTime64(6, 'UTC'), replay_generated_at DateTime64(6, 'UTC'), is_replay UInt8, payload String) ENGINE = ReplacingMergeTree(replay_generated_at) ORDER BY (replay_id, replay_run_id, event_id)")

    def write(self, run, outputs, catalog):
        started = time.perf_counter(); now = datetime.now(timezone.utc); groups = defaultdict(list)
        def row(event_id, timestamp, payload):
            return [run.replay_id, run.replay_run_id, event_id, stamp(timestamp), now, 1, canonical(payload)]
        for output in outputs:
            if not output.get("is_replay") or output["replay_id"] != run.replay_id: raise ValueError("replay envelope mismatch")
            if output["topic"] in TABLES:
                groups[TABLES[output["topic"]]].append(row(output["event"]["event_id"], output["original_event_time"], output["event"]))
        groups["replay_runs"].append(row(run.replay_run_id, run.started_at, run.model_dump(mode="json")))
        for entry in catalog:
            groups["historical_events"].append(row(entry["event_id"], entry["event_end"], entry))
        columns = ["replay_id", "replay_run_id", "event_id", "original_event_time", "replay_generated_at", "is_replay", "payload"]
        for table, rows in groups.items():
            for start in range(0, len(rows), 500):
                self.store.client.insert(table, rows[start:start+500], column_names=columns, database=self.store.database)
        count = sum(len(rows) for rows in groups.values()); elapsed = time.perf_counter()-started
        return {"rows": count, "seconds": elapsed, "rows_per_second": count/elapsed, "tables": {k: len(v) for k, v in groups.items()}}
