"""Idempotently initialize ClickHouse analytical tables."""
import os
from integrity_engine.storage.clickhouse import ClickHouseStore

if __name__ == "__main__":
    if os.getenv("CLICKHOUSE_ENABLED", "true").lower() in {"1", "true", "yes"}:
        ClickHouseStore().initialize()
        print("ClickHouse schema initialized")
