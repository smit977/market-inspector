"""Provision the least-privilege ClickHouse identity used by Grafana."""
import os
import re
import time

from .clickhouse import ClickHouseSettings, ClickHouseStore

TABLES = (
    "market_alerts",
    "operational_health",
    "market_metrics",
    "cross_venue_metrics",
    "derivatives_metrics",
    "market_anomalies",
    "market_integrity",
)


def provision_grafana_user(store: ClickHouseStore, database: str, username: str, password: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", username):
        raise ValueError("CLICKHOUSE_GRAFANA_USER must be a simple SQL identifier")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("CLICKHOUSE_DATABASE must be a simple SQL identifier")
    if not password:
        raise ValueError("CLICKHOUSE_GRAFANA_PASSWORD must be non-empty")

    # The profile keeps SELECT read-only while allowing the datasource driver to
    # set its mandatory per-query timeout. It does not grant data privileges.
    store.client.command(
        "CREATE SETTINGS PROFILE IF NOT EXISTS grafana_reader_profile "
        "SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY"
    )
    store.client.command(
        "ALTER SETTINGS PROFILE grafana_reader_profile "
        "SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY"
    )
    store.client.command(
        f"CREATE USER IF NOT EXISTS `{username}` IDENTIFIED WITH sha256_password BY {{password:String}}",
        parameters={"password": password},
    )
    # Reapply password/profile and remove any direct grants on rerun. This also
    # makes environment-driven password rotation effective without exposing it.
    store.client.command(
        f"ALTER USER `{username}` IDENTIFIED WITH sha256_password BY {{password:String}}",
        parameters={"password": password},
    )
    store.client.command(f"REVOKE ALL ON *.* FROM `{username}`")
    store.client.command(
        f"ALTER USER `{username}` SETTINGS PROFILE grafana_reader_profile"
    )
    for table in TABLES:
        store.client.command(
            f"GRANT SELECT ON `{database}`.`{table}` TO `{username}`"
        )


def main() -> None:
    settings = ClickHouseSettings()
    username = os.getenv("CLICKHOUSE_GRAFANA_USER", "grafana_reader")
    password = os.getenv("CLICKHOUSE_GRAFANA_PASSWORD", "")
    store = ClickHouseStore(settings=settings)
    try:
        tables = ", ".join(f"'{table}'" for table in TABLES)
        deadline = time.monotonic() + 120
        while True:
            result = store.query(
                "SELECT count() FROM system.tables "
                f"WHERE database = '{settings.database}' AND name IN ({tables})"
            )
            if result.result_rows and result.result_rows[0][0] == len(TABLES):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("ClickHouse analytical tables are not initialized")
            time.sleep(2)
        provision_grafana_user(store, settings.database, username, password)
    finally:
        store.close()


if __name__ == "__main__":
    main()
