"""Provision the narrowly scoped ClickHouse identity used by MCP."""
from __future__ import annotations

import os
import re
import time

from integrity_engine.storage.clickhouse import ClickHouseSettings, ClickHouseStore
from .models import TABLES


def provision_mcp_user(store: ClickHouseStore, database: str, username: str, password: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", username):
        raise ValueError("CLICKHOUSE_MCP_USER must be a simple SQL identifier")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("CLICKHOUSE_DATABASE must be a simple SQL identifier")
    if not password:
        raise ValueError("CLICKHOUSE_MCP_PASSWORD must be non-empty")
    store.client.command(
        "CREATE SETTINGS PROFILE IF NOT EXISTS mcp_reader_profile "
        "SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY, "
        "max_result_rows = 5000, result_overflow_mode = 'throw', "
        "max_threads = 1, max_block_size = 512, preferred_block_size_bytes = 262144"
    )
    store.client.command(
        "ALTER SETTINGS PROFILE mcp_reader_profile "
        "SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY, "
        "max_result_rows = 5000, result_overflow_mode = 'throw', "
        "max_threads = 1, max_block_size = 512, preferred_block_size_bytes = 262144"
    )
    store.client.command(
        f"CREATE USER IF NOT EXISTS `{username}` IDENTIFIED WITH sha256_password BY {{password:String}}",
        parameters={"password": password},
    )
    store.client.command(
        f"ALTER USER `{username}` IDENTIFIED WITH sha256_password BY {{password:String}}",
        parameters={"password": password},
    )
    store.client.command(f"REVOKE ALL ON *.* FROM `{username}`")
    store.client.command(f"ALTER USER `{username}` SETTINGS PROFILE mcp_reader_profile")
    for table in TABLES:
        store.client.command(f"GRANT SELECT ON `{database}`.`{table}` TO `{username}`")


def main() -> None:
    settings = ClickHouseSettings()
    username = os.getenv("CLICKHOUSE_MCP_USER", "mcp_reader")
    password = os.getenv("CLICKHOUSE_MCP_PASSWORD", "")
    if not password:
        raise ValueError("CLICKHOUSE_MCP_PASSWORD must be non-empty")
    store = ClickHouseStore(settings=settings)
    try:
        table_names = ", ".join(f"'{name}'" for name in TABLES)
        deadline = time.monotonic() + 120
        while True:
            count = store.query(
                "SELECT count() FROM system.tables "
                f"WHERE database = '{settings.database}' AND name IN ({table_names})"
            ).result_rows[0][0]
            if count == len(TABLES):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("ClickHouse analytical tables are not initialized")
            time.sleep(2)
        provision_mcp_user(store, settings.database, username, password)
    finally:
        store.close()


if __name__ == "__main__":
    main()
