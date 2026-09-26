"""Read-only replay/live isolation evidence, bounded to recent live rows."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition
from integrity_engine.storage.clickhouse import ClickHouseStore
from integrity_engine.mcp.models import TABLES as MCP_TABLES


def snapshot():
    store = ClickHouseStore(); result = {"captured_at": datetime.now(timezone.utc).isoformat()}
    try:
        tables = {"market_metrics": "timestamp", "cross_venue_metrics": "timestamp", "derivatives_metrics": "timestamp", "market_anomalies": "detected_at", "market_integrity": "calculated_at"}
        result["live_tables"] = {}
        for table, time in tables.items():
            row = store.query(f"SELECT count(), maxOrNull({time}), countIf(JSONExtractBool(original_event, 'is_replay')) FROM {table} WHERE {time} >= now() - INTERVAL 5 MINUTE").result_rows[0]
            result["live_tables"][table] = {"rows_last_5m": row[0], "latest": str(row[1]), "replay_marked_rows": row[2]}
        existing = {r[0] for r in store.query("SHOW TABLES").result_rows}
        pairs = {"market_metrics": "replay_metrics", "cross_venue_metrics": "replay_cross_venue_metrics", "derivatives_metrics": "replay_derivatives_metrics", "market_anomalies": "replay_anomalies", "market_integrity": "replay_integrity"}
        result["replay_event_id_collisions"] = {}
        for live, replay in pairs.items():
            if replay in existing:
                result["replay_event_id_collisions"][live] = store.query(f"SELECT count() FROM {live} WHERE {tables[live]} >= now() - INTERVAL 1 HOUR AND toString(event_id) IN (SELECT event_id FROM {replay})").result_rows[0][0]
        result["mcp_live_only"] = not any("replay" in t or t == "historical_events" for t in MCP_TABLES)
    finally: store.close()
    c = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "group.id": "replay-isolation-readonly", "enable.auto.commit": False})
    try:
        result["live_kafka_high_offsets"] = {}
        for topic in ("market.raw.trades", "market.raw.orderbook", "market.raw.derivatives", "market.integrity"):
            partitions = c.list_topics(topic, timeout=10).topics[topic].partitions
            result["live_kafka_high_offsets"][topic] = {str(p): c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
    finally: c.close()
    result["grafana_live_only"] = all("replay_" not in p.read_text() and "historical_events" not in p.read_text() for p in Path("grafana/dashboards").glob("*.json"))
    return result


if __name__ == "__main__":
    import argparse
    p=argparse.ArgumentParser(); p.add_argument("--output", required=True); a=p.parse_args()
    result=snapshot(); Path(a.output).write_text(json.dumps(result,indent=2)); print(json.dumps(result,indent=2))
