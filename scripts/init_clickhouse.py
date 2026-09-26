"""Idempotently initialize ClickHouse analytical tables."""
from integrity_engine.storage.clickhouse import ClickHouseSettings, ClickHouseStore

if __name__ == "__main__":
    if ClickHouseSettings().enabled:
        store = ClickHouseStore()
        try:
            store.initialize()
            print("ClickHouse schema initialized")
        finally:
            store.close()
