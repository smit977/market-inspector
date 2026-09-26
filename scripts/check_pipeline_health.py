"""Read-only pipeline readiness snapshot; run inside the app container."""
import json
import os

from confluent_kafka.admin import AdminClient
from integrity_engine.storage.clickhouse import ClickHouseStore


def main():
    live = os.getenv("PROCESSING_MODE", "live") == "live"
    suffix = "-live-" + os.getenv("PROCESSING_RUN_ID", "current") if live else ""
    groups = [name + suffix for name in (
        "trade-normalizer-v1", "orderbook-reconstructor-v1", "market-feature-engine-v1",
        "cross-venue-feature-engine-v1", "derivatives-normalizer-v1",
        "market-move-detector-v1", "market-integrity-engine-v1")]
    groups.append("clickhouse-writer-v1-live-v2" if live else "clickhouse-writer-v1")
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    result = {"groups": {}, "tables": {}}
    healthy = True
    for name, future in admin.describe_consumer_groups(groups, request_timeout=15).items():
        group = future.result()
        result["groups"][name] = {"state": str(group.state), "members": len(group.members)}
        healthy &= bool(group.members) and group.state.name == "STABLE"
    store = ClickHouseStore()
    try:
        for table, column in (("market_metrics", "timestamp"), ("cross_venue_metrics", "timestamp"),
                              ("derivatives_metrics", "timestamp"), ("market_anomalies", "detected_at"),
                              ("market_integrity", "calculated_at")):
            rows, latest, age = store.query(
                f"SELECT count(), max({column}), dateDiff('second', max({column}), now()) FROM {table}"
            ).result_rows[0]
            result["tables"][table] = {"rows": rows, "latest_utc": str(latest), "age_seconds": age}
            # Anomalies and integrity are event-triggered, not periodic heartbeats.
            if column == "timestamp":
                healthy &= rows > 0 and 0 <= age <= 30
    finally:
        store.close()
    result["pipeline_healthy"] = bool(healthy)
    result["scope"] = "Consumer membership and periodic output freshness; not full venue coverage or scoring accuracy."
    print(json.dumps(result, indent=2))
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
