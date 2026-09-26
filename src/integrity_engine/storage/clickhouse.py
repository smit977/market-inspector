"""ClickHouse configuration, non-destructive schema migration, and batch client."""
from decimal import localcontext
from typing import Any

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import DECIMAL, HIGH_SCALE_DECIMAL, TABLE_COLUMNS, TABLE_DDL, TABLE_TYPES


class ClickHouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_", extra="ignore")
    enabled: bool = True
    host: str = "clickhouse"
    port: int = Field(default=9000, ge=1, le=65535)
    http_port: int = Field(default=8123, ge=1, le=65535)
    database: str = Field(default="market_integrity", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    user: str = "default"
    password: SecretStr = SecretStr("")
    batch_size: int = Field(default=500, ge=1, le=10000)
    flush_interval_ms: int = Field(default=1000, ge=1, le=60000)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_backoff_ms: int = Field(default=500, ge=0, le=60000)


class ClickHouseStore:
    def __init__(self, client: Any | None = None, settings: ClickHouseSettings | None = None, **config: Any):
        self.settings = settings or ClickHouseSettings(**config)
        self.host, self.port = self.settings.host, self.settings.port
        self.http_port, self.database = self.settings.http_port, self.settings.database
        if client is None:
            import clickhouse_connect
            # This driver uses HTTP, not the native operator port (9000).
            client = clickhouse_connect.get_client(
                host=self.host, port=self.http_port, username=self.settings.user,
                password=self.settings.password.get_secret_value(),
                connect_timeout=5, send_receive_timeout=10, query_retries=0)
        self.client = client

    def initialize(self):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
        self.client.database = self.database
        for table, statement in TABLE_DDL.items():
            self.client.command(statement)
            existing = {row[0]: row[1] for row in self.client.query(f"DESCRIBE TABLE `{table}`").result_rows}
            changes = []
            for name, typ in TABLE_TYPES[table].items():
                if name not in existing:
                    changes.append(f"ADD COLUMN IF NOT EXISTS `{name}` {typ}")
                elif existing[name].replace(" ", "") != typ.replace(" ", ""):
                    # Only widen the original numeric columns; never rewrite keys or drop data.
                    old = existing[name]
                    widening = (typ in {DECIMAL, HIGH_SCALE_DECIMAL} and "Decimal(" in old) or (
                        typ == "Nullable(UInt64)" and old in {"UInt16", "UInt64"})
                    if not widening:
                        raise ValueError(f"unsupported schema migration: {table}.{name}")
                    changes.append(f"MODIFY COLUMN `{name}` {typ}")
            if changes:
                self.client.command(f"ALTER TABLE `{table}` " + ", ".join(changes) + " SETTINGS mutations_sync=1")

    def insert_rows(self, table, rows):
        if rows:
            # Decimal256 serialization must not inherit the default 28-digit arithmetic context.
            with localcontext() as context:
                context.prec = 80
                self.client.insert(table, rows, column_names=TABLE_COLUMNS[table], database=self.database,
                                   settings={"async_insert": 0})

    def query(self, sql):
        self.client.database = self.database
        return self.client.query(sql)

    def close(self):
        close = getattr(self.client, "close", None)
        if close:
            close()
