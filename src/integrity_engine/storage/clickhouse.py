"""Small ClickHouse client wrapper with idempotent schema initialization."""

from __future__ import annotations

import os
from typing import Any

from .models import TABLE_COLUMNS, TABLE_DDL


class ClickHouseStore:
    def __init__(self, client: Any | None = None, **config: Any) -> None:
        self.host = config.get("host", os.getenv("CLICKHOUSE_HOST", "clickhouse"))
        self.port = int(config.get("port", os.getenv("CLICKHOUSE_PORT", "9000")))
        # clickhouse-connect speaks HTTP; CLICKHOUSE_PORT remains the native
        # port for operators and other clients, while this client uses 8123.
        self.http_port = int(config.get("http_port", os.getenv("CLICKHOUSE_HTTP_PORT", "8123")))
        self.database = config.get("database", os.getenv("CLICKHOUSE_DATABASE", "market_integrity"))
        self.user = config.get("user", os.getenv("CLICKHOUSE_USER", "default"))
        self.password = config.get("password", os.getenv("CLICKHOUSE_PASSWORD", ""))
        if client is None:
            import clickhouse_connect
            client = clickhouse_connect.get_client(host=self.host, port=self.http_port, username=self.user, password=self.password)
        self.client = client

    def initialize(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
        self.client.command(f"USE `{self.database}`")
        for statement in TABLE_DDL.values():
            self.client.command(statement)

    def insert_rows(self, table: str, rows: list[list[Any]]) -> None:
        if rows:
            self.client.insert(table, rows, column_names=TABLE_COLUMNS[table], database=self.database)

    def query(self, sql: str) -> Any:
        self.client.database = self.database
        return self.client.query(sql)

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()
