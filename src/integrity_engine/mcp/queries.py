"""Bounded ClickHouse query runner; SQL text is always server-authored."""
from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from time import perf_counter
from typing import Any

import structlog

from integrity_engine.storage.clickhouse import ClickHouseSettings, ClickHouseStore
from .models import TABLE_COLUMNS, TABLES

log = structlog.get_logger(__name__)


@dataclass
class QueryResult:
    rows: list[dict[str, Any]]
    duration_ms: float


class MCPQueryRunner:
    """Serializes query use of the synchronous client and enforces row/time limits."""

    def __init__(self, settings: ClickHouseSettings | None = None, timeout_seconds: int = 5,
                 max_result_rows: int = 500, store_factory=ClickHouseStore):
        self.settings = settings or ClickHouseSettings()
        self.timeout_seconds = timeout_seconds
        self.max_result_rows = max_result_rows
        self.store_factory = store_factory
        self._store = None
        self._lock = RLock()

    def _client(self):
        if self._store is None:
            self._store = self.store_factory(settings=self.settings)
        self._store.client.database = self.settings.database
        return self._store.client

    def run(self, sql: str, parameters: dict[str, Any] | None = None, limit: int | None = None) -> QueryResult:
        cap = min(limit or self.max_result_rows, self.max_result_rows)
        bound_parameters = dict(parameters or {})
        for key in ("limit", "row_limit"):
            if key in bound_parameters:
                bound_parameters[key] = min(int(bound_parameters[key]), cap)
        start = perf_counter()
        with self._lock:
            try:
                result = self._client().query(
                    sql,
                    parameters=bound_parameters,
                    settings={
                        "max_execution_time": self.timeout_seconds,
                    },
                )
                names = list(result.column_names)
                rows = [dict(zip(names, row)) for row in result.result_rows]
                if len(rows) > cap:
                    raise RuntimeError("result row cap exceeded")
                duration = (perf_counter() - start) * 1000
                return QueryResult(rows=rows, duration_ms=round(duration, 3))
            except Exception as error:
                # Drop the client after transport failures; the next tool call creates a
                # fresh connection after ClickHouse recovers. Never log query values.
                if self._store is not None:
                    try:
                        self._store.close()
                    except Exception:
                        pass
                    self._store = None
                log.warning("mcp_clickhouse_query_failed", error_type=type(error).__name__)
                raise

    @staticmethod
    def select_sql(table: str, columns: list[str], lookback_minutes: int,
                   filters: dict[str, Any] | None = None, limit: int = 100) -> tuple[str, dict[str, Any]]:
        """Build a time-bounded SELECT from fixed table/column/filter allowlists."""
        if table not in TABLES or not columns or any(c not in TABLE_COLUMNS[table] for c in columns):
            raise ValueError("unsupported_table_or_column")
        time_col = TABLES[table]
        where = [f"`{time_col}` >= now() - INTERVAL {{lookback:UInt32}} MINUTE", f"`{time_col}` <= now()"]
        params: dict[str, Any] = {"lookback": lookback_minutes}
        allowed_filters = {"canonical_pair", "exchange", "symbol", "window", "event_type", "severity", "event_id"}
        for key, value in (filters or {}).items():
            if key not in allowed_filters or key not in TABLE_COLUMNS[table]:
                raise ValueError("unsupported_filter")
            if key == "symbol" and isinstance(value, list):
                where.append(f"`{key}` IN {{{key}:Array(String)}}")
                params[key] = value
            else:
                where.append(f"`{key}` = {{{key}:String}}")
                params[key] = str(value)
        sql = (f"SELECT {', '.join(f'`{c}`' for c in columns)} FROM `{table}` "
               f"WHERE {' AND '.join(where)} ORDER BY `{time_col}` DESC, `event_id` "
               f"LIMIT 1 BY `event_id` LIMIT {{row_limit:UInt32}}")
        params["row_limit"] = limit
        return sql, params

    def select(self, table: str, columns: list[str], lookback_minutes: int,
               filters: dict[str, Any] | None = None, limit: int = 100) -> QueryResult:
        bounded_limit = min(limit, self.max_result_rows)
        sql, params = self.select_sql(table, columns, lookback_minutes, filters, bounded_limit)
        return self.run(sql, params, bounded_limit)

    def close(self) -> None:
        with self._lock:
            if self._store is not None:
                self._store.close()
                self._store = None
