"""Idempotently provision Grafana's read-only ClickHouse account."""
from integrity_engine.storage.grafana_user import main

if __name__ == "__main__":
    main()
