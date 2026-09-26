"""Read-only, evidence-first market-intelligence tools."""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from decimal import Decimal
from time import monotonic, perf_counter
from typing import Any

import structlog
from pydantic import ValidationError

from .config import MCPSettings
from .models import (
    EXCHANGES, TABLE_COLUMNS, TABLES, LatestMetricsInput, LookbackInput,
    QueryClickHouseInput, response_dict,
)
from .queries import MCPQueryRunner

log = structlog.get_logger(__name__)
SEVERITY_ORDER = {"low": 1, "moderate": 2, "medium": 2, "high": 3, "critical": 4, "severe": 4}
COMPONENTS = (
    "Cross-Venue Confirmation", "Spot Participation", "Liquidity Stability",
    "Volume Quality", "Derivatives Dependence", "Liquidation Dependence",
)
INTEGRITY_WINDOWS = ("10s", "30s", "1m", "5m")
SIMILARITY_WEIGHTS = {
    "trigger_type": 0.18, "classification": 0.14, "move_direction": 0.08,
    "trigger_severity": 0.10, "integrity_score": 0.20,
    "Cross-Venue Confirmation": 0.06, "Spot Participation": 0.06,
    "Liquidity Stability": 0.06, "Volume Quality": 0.06,
    "Derivatives Dependence": 0.03, "Liquidation Dependence": 0.03,
}
TABLES_LIST = list(TABLES)


def _safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if hasattr(value, "model_dump"):
        return _safe(value.model_dump(mode="python"))
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe(v) for v in value]
    return value


def _decode(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ("venue_prices", "venue_deviation_bps", "venue_returns", "venue_volume_shares",
                  "venue_quote_volumes", "follow_lag_ms", "metrics_snapshot", "evidence", "components"):
        value = result.get(field)
        if isinstance(value, str) and value:
            try:
                result[field] = json.loads(value)
            except (ValueError, TypeError):
                pass
    original = result.get("original_event")
    if isinstance(original, str):
        try:
            original = json.loads(original)
        except ValueError:
            original = None
    if isinstance(original, dict):
        for field in ("spread_change", "depth_change", "price_acceleration"):
            result.setdefault(field, original.get(field))
    return result


def _last_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
        except ValueError:
            return None
    if isinstance(value, dict):
        for key in ("timestamp", "detected_at", "calculated_at", "data_as_of", "latest_at"):
            if (parsed := _last_time(value.get(key))) is not None:
                return parsed
        for nested in value.values():
            if (parsed := _last_time(nested)) is not None:
                return parsed
    if isinstance(value, list):
        for nested in value:
            if (parsed := _last_time(nested)) is not None:
                return parsed
    return None


def _pair_symbols(pair: str) -> list[str]:
    base, _, quote = pair.upper().replace("/", "-").partition("-")
    if not base or not quote:
        return [pair]
    symbols = [f"{base}-{quote}", f"{base}/{quote}", f"{base}{quote}"]
    if quote in {"USD", "USDT", "USDC"}:
        symbols.extend((f"{base}-USDT", f"{base}USDT", f"{base}-USD", f"{base}/USD"))
    return list(dict.fromkeys(symbols))


def _naive_utc(stamp: Any) -> datetime:
    parsed = _last_time(stamp)
    if parsed is None:
        raise ValueError("invalid_argument")
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


class MCPTools:
    """Application service. All externally callable methods go through invoke()."""

    def __init__(self, runner: MCPQueryRunner | None = None, stale_after_seconds: int = 60,
                 settings: MCPSettings | None = None):
        settings = settings or MCPSettings()
        self.runner = runner or MCPQueryRunner(timeout_seconds=settings.query_timeout_seconds,
                                                max_result_rows=settings.max_result_rows)
        self.stale_after_seconds = stale_after_seconds
        self.started_at = monotonic()
        self._query_ms = 0.0
        from threading import RLock
        self._invoke_lock = RLock()

    def invoke(self, name: str, *args, **kwargs) -> dict[str, Any]:
        with self._invoke_lock:
            started = perf_counter()
            self._query_ms = 0.0
            try:
                result = getattr(self, name)(*args, **kwargs)
                return response_dict(
                    status=result.get("status", "ok"), generated_at=datetime.now(timezone.utc),
                    data_as_of=result.get("data_as_of"), source_tables=result.get("source_tables", []),
                    quality_flags=result.get("quality_flags", []),
                    query_duration_ms=round(max(self._query_ms, (perf_counter() - started) * 1000), 3),
                    evidence=result.get("evidence", {}), data=result.get("data"), error=result.get("error"),
                )
            except ValidationError as error:
                messages = [e["msg"] for e in error.errors(include_input=False)]
                code = "unsupported_exchange" if any("unsupported_exchange" in m for m in messages) else "invalid_argument"
                return self._error(code, "Input validation failed", started)
            except ValueError as error:
                code = str(error) if str(error) in {"unsupported_exchange", "unsupported_table", "unsupported_column", "unsupported_filter", "invalid_argument"} else "invalid_argument"
                return self._error(code, "Input validation failed", started)
            except Exception as error:
                message = (type(error).__name__ + " " + str(error)).lower()
                code = "query_timeout" if "timeout" in message else "database_unavailable"
                log.error("mcp_tool_failed", tool=name, error_type=type(error).__name__)
                return self._error(code, "The analytical data store could not complete this request", started)

    def _error(self, code: str, message: str, started: float) -> dict[str, Any]:
        return response_dict(
            status="error", generated_at=datetime.now(timezone.utc), source_tables=[],
            quality_flags=[], query_duration_ms=round((perf_counter() - started) * 1000, 3),
            evidence={}, error={"code": code, "message": message},
        )

    def _fetch(self, sql: str, params: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
        result = self.runner.run(sql, params, limit)
        self._query_ms += result.duration_ms
        return [_decode(row) for row in result.rows]

    def _select(self, table: str, columns: list[str], lookback: int, filters: dict[str, Any], limit: int):
        result = self.runner.select(table, columns, lookback, filters, limit)
        self._query_ms += result.duration_ms
        return [_decode(row) for row in result.rows]

    def _freshness(self, table: str, time_column: str, lookback_minutes: int = 5,
                   include_flags: bool = False) -> dict[str, Any] | None:
        flags = f", argMax(data_quality_flags, {time_column}) AS latest_flags" if include_flags else ""
        rows = self._fetch(
            f"SELECT max({time_column}) AS latest_timestamp{flags} FROM {table} "
            f"PREWHERE {time_column} >= now() - INTERVAL {{lookback:UInt32}} MINUTE "
            f"AND {time_column} <= now()",
            {"lookback": lookback_minutes}, 1)
        return rows[0] if rows else None

    @staticmethod
    def _result(data: Any, tables: list[str], group: str = "observed_fact", flags: list[str] | None = None,
                data_as_of: Any = None, status: str | None = None) -> dict[str, Any]:
        empty = data is None or data == []
        state = status or ("no_data" if empty else "ok")
        groups = {"observed_fact": None, "calculated_metric": None,
                  "statistical_inference": None, "ai_interpretation": None}
        if group in groups:
            groups[group] = data
        return {"status": state, "data": _safe(data), "source_tables": tables,
                "quality_flags": flags or [], "data_as_of": _safe(data_as_of), "evidence": groups}

    def _latest_market(self, pair: str, exchange: str | None, window: str, lookback: int, limit: int = 1):
        params = {"symbols": _pair_symbols(pair), "window": window, "lookback": lookback, "limit": limit}
        where = ["symbol IN {symbols:Array(String)}", "window = {window:String}",
                 "timestamp >= now() - INTERVAL {lookback:UInt32} MINUTE", "timestamp <= now()"]
        if exchange:
            where.append("exchange = {exchange:String}")
            params["exchange"] = exchange
        sql = ("SELECT event_id, timestamp, exchange, symbol, window, price_return, absolute_return, price_range, "
               "trade_count, base_volume, quote_volume, buy_volume, sell_volume, trade_imbalance, vwap, "
               "average_trade_size, largest_trade_size, best_bid, best_ask, mid_price, spread, spread_bps, "
               "bid_depth_20, ask_depth_20, orderbook_imbalance_20, depth_change, "
               "price_acceleration, data_complete, data_quality_flags FROM market_metrics WHERE " +
               " AND ".join(where) + " ORDER BY timestamp DESC, event_id "
               "LIMIT 1 BY event_id LIMIT {limit:UInt32}")
        return self._fetch(sql, params, limit)

    def _latest_cross(self, pair: str, window: str, lookback: int, limit: int = 1):
        return self._select("cross_venue_metrics", [
            "event_id", "timestamp", "canonical_pair", "window", "active_venue_count", "stale_venue_count",
            "median_price", "mean_price", "min_price", "max_price", "dispersion", "dispersion_bps",
            "confirmation_ratio", "confirmation_state", "median_return", "return_dispersion",
            "largest_venue_volume_share", "volume_hhi", "first_observed_venue", "venue_prices",
            "venue_deviation_bps", "venue_returns", "venue_volume_shares", "follow_lag_ms", "data_quality_flags",
        ], lookback, {"canonical_pair": pair, "window": window}, limit)

    def _latest_derivatives(self, pair: str, exchange: str | None, window: str, lookback: int, limit: int = 1):
        filters: dict[str, Any] = {"canonical_pair": pair, "window": window}
        if exchange:
            filters["exchange"] = exchange
        return self._select("derivatives_metrics", [
            "event_id", "timestamp", "exchange", "canonical_pair", "window", "perpetual_price",
            "perpetual_return", "perpetual_base_volume", "perpetual_quote_volume", "perpetual_trade_count",
            "perpetual_trade_imbalance", "open_interest", "open_interest_change", "open_interest_change_pct",
            "funding_rate", "funding_rate_change", "liquidation_count", "long_liquidation_volume",
            "short_liquidation_volume", "total_liquidation_volume", "spot_reference_price", "basis", "basis_bps",
            "perpetual_to_spot_volume_ratio", "spot_perpetual_return_difference", "data_complete",
            "data_quality_flags", "volume_unit", "open_interest_unit", "open_interest_comparable",
            "spot_reference_timestamp", "source_timestamp",
        ], lookback, filters, limit)

    def _latest_integrity(self, pair: str, event_id: str | None = None,
                          include_components: bool = False):
        columns = ("event_id, calculated_at, canonical_pair, trigger_event_id, trigger_type, trigger_severity, "
                   "move_direction, move_window, integrity_score, overall_confidence, evidence_coverage, "
                   "classification, classification_confidence, data_quality_flags")
        params: dict[str, Any] = {"lookback": 365, "limit": 1}
        if event_id:
            params["event_id"] = event_id
            rows = self._fetch(
                f"SELECT {columns} FROM market_integrity WHERE calculated_at >= now() - "
                "INTERVAL {lookback:UInt32} DAY AND calculated_at <= now() "
                "AND event_id = {event_id:String} ORDER BY calculated_at DESC LIMIT {limit:UInt32}", params, 1)
        else:
            params["pair"] = pair
            # move_window is the second primary-key field. Query one latest row
            # per supported window first, then sort at most four compact rows.
            branches = []
            for index, window in enumerate(INTEGRITY_WINDOWS):
                params[f"window_{index}"] = window
                branches.append(
                    f"(SELECT {columns} FROM market_integrity WHERE calculated_at >= now() - "
                    f"INTERVAL {{lookback:UInt32}} DAY AND calculated_at <= now() "
                    f"AND canonical_pair = {{pair:String}} AND move_window = {{window_{index}:String}} "
                    "ORDER BY calculated_at DESC, event_id LIMIT 1 BY event_id LIMIT 1)"
                )
            rows = self._fetch(
                f"SELECT {columns} FROM (" + " UNION ALL ".join(branches) +
                ") ORDER BY calculated_at DESC, event_id LIMIT {limit:UInt32}", params, 1)
        if include_components and rows and not rows[0].get("components"):
            row = rows[0]
            component_rows = self._fetch(
                "SELECT components FROM market_integrity WHERE canonical_pair = {pair:String} "
                "AND move_window = {window:String} AND calculated_at = {calculated_at:DateTime64(3)} "
                "AND event_id = {event_id:String} LIMIT 1",
                {"pair": row["canonical_pair"], "window": row["move_window"],
                 "calculated_at": _naive_utc(row["calculated_at"]), "event_id": row["event_id"]}, 1)
            row["components"] = component_rows[0].get("components") if component_rows else None
        return rows

    def _latest_anomaly(self, pair: str, lookback: int):
        return self._fetch(
            "SELECT event_id, detected_at, exchange, symbol, window, event_type, severity, direction, "
            "baseline_ready, detectors_triggered, metrics_snapshot, evidence, data_quality_flags "
            "FROM market_anomalies FINAL WHERE detected_at >= now() - INTERVAL {lookback:UInt32} MINUTE "
            "AND detected_at <= now() AND symbol IN {symbols:Array(String)} "
            "ORDER BY detected_at DESC LIMIT {limit:UInt32}",
            {"lookback": lookback, "symbols": _pair_symbols(pair), "limit": 1}, 1)

    @staticmethod
    def _max_time(*rows):
        times = [_last_time(row) for row in rows if row]
        times = [t for t in times if t]
        return max(times) if times else None

    @staticmethod
    def _stale(timestamp: Any, threshold: int = 60) -> bool:
        parsed = _last_time(timestamp)
        return parsed is None or (datetime.now(timezone.utc) - parsed).total_seconds() > threshold

    def get_market_summary(self, canonical_pair: str = "BTC-USD", lookback_minutes: int = 15) -> dict[str, Any]:
        args = LookbackInput(canonical_pair=canonical_pair, lookback_minutes=lookback_minutes)
        pair, lookback = args.canonical_pair, args.lookback_minutes
        market = self._latest_market(pair, None, "30s", lookback)
        cross = self._latest_cross(pair, "30s", lookback)
        derivatives = self._latest_derivatives(pair, None, "1m", lookback, 5)
        integrity = self._latest_integrity(pair)
        anomaly = self._latest_anomaly(pair, lookback)
        latest = market[0] if market else None
        cross_latest = cross[0] if cross else None
        integrity_latest = integrity[0] if integrity else None
        anomaly_latest = anomaly[0] if anomaly else None
        timestamps = {"market_metrics": latest.get("timestamp") if latest else None,
                      "cross_venue_metrics": cross_latest.get("timestamp") if cross_latest else None,
                      "derivatives_metrics": self._max_time(derivatives),
                      "integrity": integrity_latest.get("calculated_at") if integrity_latest else None,
                      "anomalies": anomaly_latest.get("detected_at") if anomaly_latest else None}
        stale = [table for table, timestamp in timestamps.items() if timestamp and self._stale(timestamp)]
        if not market: stale.append("market_metrics")
        if not cross: stale.append("cross_venue_metrics")
        metrics = {
            "latest_reference_price": cross_latest.get("median_price") if cross_latest else None,
            "price_change": latest.get("price_return") if latest else None,
            "latest_spot_volume": latest.get("quote_volume") if latest else None,
            "spread": latest.get("spread") if latest else None,
            "spread_bps": latest.get("spread_bps") if latest else None,
            "orderbook_imbalance": latest.get("orderbook_imbalance_20") if latest else None,
            "cross_venue_confirmation": cross_latest.get("confirmation_ratio") if cross_latest else None,
            "cross_venue_dispersion_bps": cross_latest.get("dispersion_bps") if cross_latest else None,
            "active_venues": cross_latest.get("active_venue_count") if cross_latest else None,
        }
        data = {"canonical_pair": pair, **metrics, "latest_derivatives_summary": derivatives,
                "latest_integrity_event": integrity_latest, "latest_anomaly": anomaly_latest,
                "data_freshness": timestamps}
        flags = list(stale)
        if integrity_latest is None:
            flags.append("no_recent_integrity_event")
        if not derivatives:
            flags.append("derivatives_unavailable_in_requested_window")
        status = "no_data" if not any((market, cross, derivatives, integrity, anomaly)) else "stale_data" if stale else "ok"
        return self._result(data, ["market_metrics", "cross_venue_metrics", "derivatives_metrics", "market_integrity", "market_anomalies"],
                            "calculated_metric", flags, self._max_time(latest, cross_latest, derivatives[0] if derivatives else None, integrity_latest, anomaly_latest), status)

    def get_latest_market_metrics(self, canonical_pair: str = "BTC-USD", exchange: str | None = None,
                                  window: str = "30s", lookback_minutes: int = 15) -> dict[str, Any]:
        args = LatestMetricsInput(canonical_pair=canonical_pair, exchange=exchange, window=window, lookback_minutes=lookback_minutes)
        rows = self._latest_market(args.canonical_pair, args.exchange, args.window, args.lookback_minutes, 100)
        flags = sorted({flag for row in rows for flag in (row.get("data_quality_flags") or [])})
        state = "stale_data" if rows and self._stale(rows[0].get("timestamp"), self.stale_after_seconds) else None
        if state:
            flags.append("stale_data")
        return self._result(rows, ["market_metrics"], "calculated_metric", flags,
                            rows[0].get("timestamp") if rows else None, state)

    def get_active_market_moves(self, canonical_pair: str = "BTC-USD", lookback_minutes: int = 60,
                                minimum_severity: str | None = None) -> dict[str, Any]:
        args = LookbackInput(canonical_pair=canonical_pair, lookback_minutes=lookback_minutes)
        rows = self._fetch(
            "SELECT event_id, detected_at, event_type, severity, direction, detectors_triggered, window, "
            "exchange, symbol, data_quality_flags FROM market_anomalies FINAL WHERE detected_at >= "
            "now() - INTERVAL {lookback:UInt32} MINUTE AND detected_at <= now() "
            "AND symbol IN {symbols:Array(String)} ORDER BY detected_at DESC LIMIT {limit:UInt32}",
            {"lookback": args.lookback_minutes, "symbols": _pair_symbols(args.canonical_pair), "limit": 500}, 500)
        threshold = None
        if minimum_severity:
            threshold = SEVERITY_ORDER.get(minimum_severity.lower())
            if threshold is None:
                raise ValueError("invalid_argument")
            rows = [r for r in rows if SEVERITY_ORDER.get(str(r.get("severity", "")).lower(), 0) >= threshold]
        flags = sorted({f for row in rows for f in (row.get("data_quality_flags") or [])})
        return self._result(rows, ["market_anomalies"], "statistical_inference", flags,
                            rows[0].get("detected_at") if rows else None)

    def get_integrity_score(self, canonical_pair: str = "BTC-USD", event_id: str | None = None) -> dict[str, Any]:
        pair = LookbackInput(canonical_pair=canonical_pair).canonical_pair
        rows = self._latest_integrity(pair, event_id)
        if not rows:
            return self._result({"state": "no_recent_integrity_event", "latest_integrity_event": None},
                                ["market_integrity"], flags=["no_recent_integrity_event"], status="no_data")
        row = rows[0]
        fields = {k: row.get(k) for k in ("event_id", "calculated_at", "canonical_pair", "integrity_score",
                  "overall_confidence", "evidence_coverage", "classification", "classification_confidence",
                  "trigger_event_id", "trigger_type", "trigger_severity", "move_direction", "move_window", "data_quality_flags")}
        return self._result(fields, ["market_integrity"], "calculated_metric", row.get("data_quality_flags", []), row.get("calculated_at"))

    def get_integrity_components(self, event_id: str | None = None,
                                 canonical_pair: str = "BTC-USD") -> dict[str, Any]:
        pair = LookbackInput(canonical_pair=canonical_pair).canonical_pair
        rows = self._latest_integrity(pair, event_id, include_components=True)
        if not rows:
            return self._result({"state": "no_recent_integrity_event", "components": []},
                                ["market_integrity"], flags=["no_recent_integrity_event"], status="no_data")
        row = rows[0]
        raw = row.get("components") or []
        if isinstance(raw, str):
            raw = json.loads(raw)
        mapping = {component.get("name"): component for component in raw if isinstance(component, dict)}
        components = []
        for name in COMPONENTS:
            value = mapping.get(name)
            if value is None:
                components.append({"name": name, "score": None, "availability": "unavailable",
                                   "confidence": None, "weight": None, "status": "unavailable",
                                   "evidence": [], "data_quality_flags": ["component_not_present"]})
            else:
                components.append({"name": name, "score": value.get("score"),
                    "availability": value.get("availability", "available"), "confidence": value.get("confidence"),
                    "weight": value.get("weight"), "status": value.get("status"),
                    "evidence": value.get("evidence", []), "data_quality_flags": value.get("data_quality_flags", [])})
        return self._result({"event_id": row.get("event_id"), "calculated_at": row.get("calculated_at"),
                             "components": components}, ["market_integrity"], "observed_fact",
                            row.get("data_quality_flags", []), row.get("calculated_at"))

    def compare_exchanges(self, canonical_pair: str = "BTC-USD", window: str = "30s",
                          lookback_minutes: int = 15) -> dict[str, Any]:
        args = LatestMetricsInput(canonical_pair=canonical_pair, window=window, lookback_minutes=lookback_minutes)
        rows = self._latest_cross(args.canonical_pair, args.window, args.lookback_minutes, 100)
        flags = sorted({f for row in rows for f in (row.get("data_quality_flags") or [])})
        stale = bool(rows and self._stale(rows[0].get("timestamp"), self.stale_after_seconds))
        if stale: flags.append("stale_data")
        return self._result(rows, ["cross_venue_metrics"], "calculated_metric", flags,
                            rows[0].get("timestamp") if rows else None, "stale_data" if stale else None)

    def get_cross_venue_confirmation(self, canonical_pair: str = "BTC-USD", window: str = "30s",
                                     lookback_minutes: int = 15) -> dict[str, Any]:
        return self.compare_exchanges(canonical_pair, window, lookback_minutes)

    def get_orderbook_analysis(self, canonical_pair: str = "BTC-USD", exchange: str = "binance",
                               window: str = "30s") -> dict[str, Any]:
        args = LatestMetricsInput(canonical_pair=canonical_pair, exchange=exchange, window=window, lookback_minutes=15)
        rows = self._latest_market(args.canonical_pair, args.exchange, args.window, 15, 50)
        if not rows:
            return self._result({"state": "no_recent_orderbook_metrics", "metrics": None}, ["market_metrics"],
                                "calculated_metric", ["no_recent_orderbook_metrics"], status="no_data")
        latest = rows[0]
        anomalies = self._fetch(
            "SELECT event_id, detected_at, event_type, evidence, data_quality_flags FROM market_anomalies FINAL "
            "WHERE detected_at >= now() - INTERVAL 15 MINUTE AND detected_at <= now() "
            "AND exchange = {exchange:String} AND symbol IN {symbols:Array(String)} "
            "ORDER BY detected_at DESC LIMIT {limit:UInt32}",
            {"exchange": args.exchange, "symbols": _pair_symbols(args.canonical_pair), "limit": 50}, 50)
        liquidity = [e for e in anomalies if any(k in str(e.get("event_type", "")).lower()
                    for k in ("liquidity", "orderbook", "depth", "spread"))]
        previous_spread = rows[1].get("spread") if len(rows) > 1 else None
        spread_change = (latest.get("spread") - previous_spread
                         if latest.get("spread") is not None and previous_spread is not None else None)
        metrics = {k: latest.get(k) for k in ("timestamp", "best_bid", "best_ask", "spread", "spread_bps",
                   "bid_depth_20", "ask_depth_20", "orderbook_imbalance_20", "depth_change")}
        metrics["spread_change"] = spread_change
        data = {"exchange": args.exchange, "window": args.window,
                "metrics": metrics,
                "liquidity_anomaly_evidence": liquidity}
        return self._result(data, ["market_metrics", "market_anomalies"], "calculated_metric",
                            latest.get("data_quality_flags", []), latest.get("timestamp"))

    def get_derivatives_analysis(self, canonical_pair: str = "BTC-USD", exchange: str | None = None,
                                window: str = "1m") -> dict[str, Any]:
        args = LatestMetricsInput(canonical_pair=canonical_pair, exchange=exchange, window=window, lookback_minutes=15)
        rows = self._latest_derivatives(args.canonical_pair, args.exchange, args.window, 15, 100)
        flags = sorted({f for row in rows for f in (row.get("data_quality_flags") or [])})
        if not rows:
            flags.append("derivatives_unavailable_in_requested_window")
        stale = bool(rows and self._stale(rows[0].get("timestamp"), self.stale_after_seconds))
        if stale: flags.append("stale_data")
        return self._result(rows, ["derivatives_metrics"], "calculated_metric", flags,
                            rows[0].get("timestamp") if rows else None, "stale_data" if stale else None)

    def get_liquidation_activity(self, canonical_pair: str = "BTC-USD", exchange: str | None = None,
                                 lookback_minutes: int = 60) -> dict[str, Any]:
        args = LookbackInput(canonical_pair=canonical_pair, lookback_minutes=lookback_minutes)
        filters = {"canonical_pair": args.canonical_pair}
        if exchange:
            if exchange.lower() not in EXCHANGES:
                raise ValueError("unsupported_exchange")
            filters["exchange"] = exchange.lower()
        filters["window"] = "1m"
        rows = self._select("derivatives_metrics", ["event_id", "timestamp", "exchange", "liquidation_count",
            "long_liquidation_volume", "short_liquidation_volume", "total_liquidation_volume", "data_quality_flags"],
            args.lookback_minutes, filters, 500)
        fields = ("liquidation_count", "long_liquidation_volume", "short_liquidation_volume", "total_liquidation_volume")
        present = {key: [r[key] for r in rows if r.get(key) is not None] for key in fields}
        sums = {key: (sum((Decimal(str(v)) for v in values), Decimal(0)) if values else None)
                for key, values in present.items()}
        flags = sorted({f for row in rows for f in (row.get("data_quality_flags") or [])})
        available = any(present.values())
        if not available:
            flags.append("liquidation_evidence_unavailable")
        data = {"exchange": exchange, "window": "1m", **sums, "observed_rows": len(rows),
                "timestamps": [r.get("timestamp") for r in rows if any(r.get(f) is not None for f in fields)],
                "data_availability": "available" if available else "unavailable"}
        return self._result(data, ["derivatives_metrics"], "calculated_metric", flags,
                            rows[0].get("timestamp") if rows else None, "ok" if available else "no_data")

    def get_anomalies(self, canonical_pair: str = "BTC-USD", lookback_minutes: int = 60,
                      event_type: str | None = None, severity: str | None = None,
                      limit: int = 100) -> dict[str, Any]:
        args = LookbackInput(canonical_pair=canonical_pair, lookback_minutes=lookback_minutes)
        if not 1 <= limit <= 5000:
            raise ValueError("invalid_argument")
        where = ["detected_at >= now() - INTERVAL {lookback:UInt32} MINUTE", "detected_at <= now()",
                 "symbol IN {symbols:Array(String)}"]
        params: dict[str, Any] = {"lookback": args.lookback_minutes, "symbols": _pair_symbols(args.canonical_pair), "limit": limit}
        if event_type:
            where.append("event_type = {event_type:String}"); params["event_type"] = event_type
        if severity:
            where.append("severity = {severity:String}"); params["severity"] = severity.lower()
        rows = self._fetch("SELECT event_id, detected_at, event_type, severity, direction, detectors_triggered, "
            "window, exchange, symbol, baseline_ready, metrics_snapshot, evidence, data_quality_flags "
            "FROM market_anomalies WHERE " + " AND ".join(where) +
            " ORDER BY detected_at DESC, event_id LIMIT 1 BY event_id LIMIT {limit:UInt32}", params, limit)
        flags = sorted({f for row in rows for f in (row.get("data_quality_flags") or [])})
        return self._result(rows, ["market_anomalies"], "statistical_inference", flags,
                            rows[0].get("detected_at") if rows else None)

    def _historical_similarity(self, anchor: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def components(row):
            raw = row.get("components") or []
            if isinstance(raw, str):
                try: raw = json.loads(raw)
                except ValueError: raw = []
            return {c.get("name"): float(c["score"]) for c in raw if isinstance(c, dict)
                    and c.get("score") is not None and _is_number(c.get("score"))}
        anchor_components = components(anchor)
        results = []
        for candidate in candidates:
            if candidate.get("event_id") == anchor.get("event_id"):
                continue
            candidate_components = components(candidate)
            values: list[tuple[str, float, float]] = []
            for field, weight in SIMILARITY_WEIGHTS.items():
                left = anchor_components.get(field) if field in COMPONENTS else anchor.get(field)
                right = candidate_components.get(field) if field in COMPONENTS else candidate.get(field)
                if left is None or right is None:
                    continue
                if field == "integrity_score" or field in COMPONENTS:
                    sim = max(0.0, 1.0 - abs(float(left) - float(right)) / 100.0)
                else:
                    sim = 1.0 if str(left).lower() == str(right).lower() else 0.0
                values.append((field, weight, sim))
            total = sum(weight for _, weight, _ in values)
            if total == 0:
                continue
            component_sims = {name: round(sim, 4) for name, _, sim in values if name in COMPONENTS}
            diffs = [name for name, _, sim in values if sim < 0.75]
            results.append({"event_id": candidate.get("event_id"), "timestamp": candidate.get("calculated_at"),
                "similarity_score": round(100 * sum(weight * sim for _, weight, sim in values) / total, 2),
                "classification": candidate.get("classification"), "integrity_score": candidate.get("integrity_score"),
                "component_similarities": component_sims, "key_differences": diffs,
                "similarity_method": "weighted deterministic feature similarity; not outcome prediction"})
        return sorted(results, key=lambda item: item["similarity_score"], reverse=True)

    def get_historical_similar_events(self, event_id: str, limit: int = 5) -> dict[str, Any]:
        if not event_id or len(event_id) > 80 or not 1 <= limit <= 50:
            raise ValueError("invalid_argument")
        anchor_rows = self._latest_integrity("BTC-USD", event_id, include_components=True)
        if not anchor_rows:
            return self._result({"state": "event_not_found", "similar_events": []}, ["market_integrity"],
                                flags=["event_not_found"], status="no_data")
        anchor = anchor_rows[0]
        pair = anchor.get("canonical_pair", "BTC-USD")
        candidate_columns = ("event_id, calculated_at, trigger_type, trigger_severity, move_direction, "
                             "integrity_score, classification, components")
        candidate_params: dict[str, Any] = {"pair": pair, "lookback": 365, "candidate_limit": 50,
                                            "limit": 200}
        branches = []
        for index, window in enumerate(INTEGRITY_WINDOWS):
            candidate_params[f"window_{index}"] = window
            branches.append(
                f"(SELECT {candidate_columns} FROM market_integrity WHERE calculated_at >= now() - "
                f"INTERVAL {{lookback:UInt32}} DAY AND calculated_at <= now() "
                f"AND canonical_pair = {{pair:String}} AND move_window = {{window_{index}:String}} "
                "ORDER BY calculated_at DESC, event_id LIMIT 1 BY event_id "
                "LIMIT {candidate_limit:UInt32})"
            )
        candidates = self._fetch(
            f"SELECT {candidate_columns} FROM (" + " UNION ALL ".join(branches) +
            ") ORDER BY calculated_at DESC, event_id LIMIT {limit:UInt32}",
            candidate_params, 200)
        similar = self._historical_similarity(anchor, candidates)[:limit]
        return self._result({"anchor_event_id": event_id, "similar_events": similar,
                             "method": "deterministic weighted feature similarity, not outcome prediction"},
                            ["market_integrity"], "statistical_inference", data_as_of=anchor.get("calculated_at"),
                            status="ok" if similar else "no_data")

    def get_data_quality(self, lookback_minutes: int = 60) -> dict[str, Any]:
        if not 1 <= lookback_minutes <= 60:
            raise ValueError("invalid_argument")
        freshness = {}
        table_flags = {}
        for table, time_col in TABLES.items():
            aggregate = self._freshness(table, time_col, lookback_minutes, include_flags=True)
            newest = aggregate.get("latest_timestamp") if aggregate else None
            freshness[table] = {"latest_timestamp": newest,
                                "age_seconds": round((datetime.now(timezone.utc) - _last_time(newest)).total_seconds(), 3)
                                if newest and _last_time(newest) else None,
                                "stale": self._stale(newest)}
            table_flags[table] = sorted(set((aggregate or {}).get("latest_flags") or []))
        missing_fields = ("funding_rate", "open_interest", "liquidation_count", "spot_reference_price", "basis_bps")
        missing_expr = ", ".join(f"countIf({field} IS NULL) AS {field}_missing" for field in missing_fields)
        derivative_counts = self._fetch(
            "SELECT count() AS row_count, " + missing_expr + " FROM derivatives_metrics "
            "PREWHERE timestamp >= now() - INTERVAL {lookback:UInt32} MINUTE AND timestamp <= now()",
            {"lookback": lookback_minutes}, 1)
        counts = derivative_counts[0] if derivative_counts else {"row_count": 0}
        missing = {field: (counts.get(field + "_missing") if counts.get("row_count") else None)
                   for field in missing_fields}
        data = {"latest_timestamps": freshness, "stale_feeds": [k for k, v in freshness.items() if v["stale"]],
                "quality_flags_by_table": table_flags, "missing_derivatives_fields": missing,
                "unavailable_sources": {"kafka_lag": "unavailable_not_persisted", "system_dlq": "unavailable_not_persisted",
                                         "exchange_source_health": "unavailable_not_persisted"}}
        return self._result(data, TABLES_LIST, "observed_fact",
                            sorted({f for flags in table_flags.values() for f in flags}),
                            self._max_time(*(v["latest_timestamp"] for v in freshness.values())),
                            "stale_data" if data["stale_feeds"] else "ok")

    def get_pipeline_health(self) -> dict[str, Any]:
        start = monotonic()
        rows = {}
        for table, time_col in TABLES.items():
            aggregate = self._freshness(table, time_col)
            rows[table] = aggregate.get("latest_timestamp") if aggregate else None
        required_tables = ("market_metrics", "cross_venue_metrics", "derivatives_metrics")
        all_fresh = all(rows[name] is not None and not self._stale(rows[name], 120)
                        for name in required_tables)
        data = {"clickhouse_reachable": True, "latest_row_timestamp_by_table": rows,
                "sparse_event_tables": ["market_anomalies", "market_integrity"],
                "kafka_lag": "unavailable", "grafana_datasource_required": False,
                "mcp_uptime_seconds": round(monotonic() - self.started_at, 2),
                "health_state": "healthy" if all_fresh else "stale_or_empty_data"}
        return self._result(data, TABLES_LIST, "observed_fact",
                            [] if all_fresh else ["one_or_more_tables_stale_or_empty"],
                            self._max_time(*rows.values()), "ok" if all_fresh else "stale_data")

    def query_clickhouse(self, table: str, columns: list[str], lookback_minutes: int = 15,
                         canonical_pair: str | None = None, exchange: str | None = None,
                         limit: int = 100) -> dict[str, Any]:
        request = QueryClickHouseInput(table=table, columns=columns, lookback_minutes=lookback_minutes,
                                       canonical_pair=canonical_pair, exchange=exchange, limit=limit)
        filters = {}
        for key in ("canonical_pair", "exchange"):
            value = getattr(request, key)
            if value:
                if key == "canonical_pair" and request.table in {"market_metrics", "market_anomalies"}:
                    filters["symbol"] = _pair_symbols(value)
                else:
                    filters[key] = value.lower() if key == "exchange" else value.upper()
        effective_limit = min(request.limit, getattr(self.runner, "max_result_rows", 500))
        rows = self._select(request.table, request.columns, request.lookback_minutes, filters, effective_limit)
        return self._result({"table": table, "rows": rows, "row_count": len(rows), "limit": effective_limit,
                             "lookback_minutes": request.lookback_minutes}, [table],
                            "observed_fact", data_as_of=self._max_time(*rows), status="ok" if rows else "no_data")

    def generate_incident_report(self, integrity_event_id: str | None = None,
                                 anomaly_event_id: str | None = None) -> dict[str, Any]:
        if bool(integrity_event_id) == bool(anomaly_event_id):
            raise ValueError("invalid_argument")
        if integrity_event_id:
            rows = self._latest_integrity("BTC-USD", integrity_event_id, include_components=True)
            if not rows:
                return self._result({"state": "event_not_found"}, ["market_integrity"], flags=["event_not_found"], status="no_data")
            event = rows[0]
            stamp = event.get("calculated_at")
            pair = event.get("canonical_pair") or "BTC-USD"
            exchange = None
            symbol_list = _pair_symbols(pair)
            event_time = _naive_utc(stamp)
            anomalies = self._fetch("SELECT event_id, detected_at, event_type, severity, direction, evidence, "
                "metrics_snapshot, data_quality_flags FROM market_anomalies FINAL WHERE detected_at BETWEEN "
                "{start:DateTime64(3)} - INTERVAL 15 MINUTE AND {end:DateTime64(3)} + INTERVAL 15 MINUTE "
                "AND symbol IN {symbols:Array(String)} "
                "ORDER BY detected_at DESC LIMIT {limit:UInt32}",
                {"start": event_time, "end": event_time,
                 "symbols": symbol_list, "limit": 100}, 100)
            metrics = self._fetch("SELECT timestamp, exchange, window, price_return, quote_volume, spread_bps, "
                "orderbook_imbalance_20, bid_depth_20, ask_depth_20, data_quality_flags FROM market_metrics FINAL "
                "WHERE timestamp BETWEEN {start:DateTime64(3)} - INTERVAL 15 MINUTE AND "
                "{end:DateTime64(3)} + INTERVAL 15 MINUTE AND symbol IN {symbols:Array(String)} "
                "ORDER BY timestamp DESC LIMIT {limit:UInt32}",
                {"start": event_time, "end": event_time,
                 "symbols": symbol_list, "limit": 200}, 200)
            cross = self._fetch("SELECT timestamp, window, active_venue_count, stale_venue_count, median_price, "
                "dispersion_bps, confirmation_ratio, confirmation_state, venue_prices, venue_deviation_bps, "
                "venue_returns, venue_volume_shares, follow_lag_ms, data_quality_flags FROM cross_venue_metrics FINAL "
                "WHERE timestamp BETWEEN {start:DateTime64(3)} - INTERVAL 15 MINUTE AND "
                "{end:DateTime64(3)} + INTERVAL 15 MINUTE AND canonical_pair = {pair:String} "
                "ORDER BY timestamp DESC LIMIT {limit:UInt32}",
                {"start": event_time, "end": event_time, "pair": pair, "limit": 100}, 100)
            deriv = self._fetch("SELECT timestamp, exchange, perpetual_price, spot_reference_price, basis_bps, "
                "perpetual_quote_volume, open_interest, funding_rate, liquidation_count, "
                "long_liquidation_volume, short_liquidation_volume, data_quality_flags FROM derivatives_metrics FINAL "
                "WHERE timestamp BETWEEN {start:DateTime64(3)} - INTERVAL 15 MINUTE AND "
                "{end:DateTime64(3)} + INTERVAL 15 MINUTE AND canonical_pair = {pair:String} "
                "ORDER BY timestamp DESC LIMIT {limit:UInt32}",
                {"start": event_time, "end": event_time,
                 "pair": pair, "limit": 100}, 100)
            data = {"event_type": "integrity", "event_metadata": event,
                    "trigger": {k: event.get(k) for k in ("trigger_event_id", "trigger_type", "trigger_severity", "move_direction", "move_window")},
                    "integrity_components": event.get("components"), "price_movement": metrics,
                    "cross_venue_evidence": cross, "spot_participation_and_liquidity": metrics,
                    "derivatives_and_liquidations": deriv, "related_anomalies": anomalies,
                    "timeline": sorted([x for x in ([event] + metrics + cross + deriv + anomalies)],
                                       key=lambda r: str(r.get("timestamp", r.get("calculated_at", r.get("detected_at", ""))))) }
            return self._result(data, ["market_integrity", "market_metrics", "cross_venue_metrics", "derivatives_metrics", "market_anomalies"],
                "observed_fact", event.get("data_quality_flags", []), event.get("calculated_at"))
        rows = self._fetch("SELECT event_id, detected_at, event_type, severity, direction, window, exchange, symbol, "
            "detectors_triggered, metrics_snapshot, evidence, data_quality_flags FROM market_anomalies FINAL "
            "WHERE detected_at >= now() - INTERVAL 365 DAY AND detected_at <= now() AND event_id = {event_id:String} "
            "ORDER BY detected_at DESC LIMIT {limit:UInt32}", {"event_id": anomaly_event_id, "limit": 1}, 1)
        if not rows:
            return self._result({"state": "event_not_found"}, ["market_anomalies"], flags=["event_not_found"], status="no_data")
        event = rows[0]
        event_time = _naive_utc(event.get("detected_at"))
        symbol = event.get("symbol") or "BTC-USD"
        flat_symbol = re.sub(r"[-/]", "", symbol.upper())
        base = flat_symbol
        for quote in ("USDT", "USDC", "USD"):
            if flat_symbol.endswith(quote) and len(flat_symbol) > len(quote):
                base = flat_symbol[:-len(quote)]
                break
        pair = f"{base}-USD"
        metrics = self._fetch("SELECT timestamp, exchange, window, price_return, quote_volume, spread_bps, "
            "orderbook_imbalance_20, bid_depth_20, ask_depth_20, data_quality_flags FROM market_metrics FINAL "
            "WHERE timestamp BETWEEN {start:DateTime64(3)} - INTERVAL 15 MINUTE AND "
            "{end:DateTime64(3)} + INTERVAL 15 MINUTE AND symbol = {symbol:String} "
            "ORDER BY timestamp DESC LIMIT {limit:UInt32}",
            {"start": event_time, "end": event_time, "symbol": symbol, "limit": 200}, 200)
        cross = self._fetch("SELECT timestamp, window, active_venue_count, stale_venue_count, median_price, "
            "dispersion_bps, confirmation_ratio, confirmation_state, venue_prices, venue_deviation_bps, "
            "venue_returns, venue_volume_shares, follow_lag_ms, data_quality_flags FROM cross_venue_metrics FINAL "
            "WHERE timestamp BETWEEN {start:DateTime64(3)} - INTERVAL 15 MINUTE AND "
            "{end:DateTime64(3)} + INTERVAL 15 MINUTE AND canonical_pair = {pair:String} "
            "ORDER BY timestamp DESC LIMIT {limit:UInt32}",
            {"start": event_time, "end": event_time, "pair": pair, "limit": 100}, 100)
        deriv = self._fetch("SELECT timestamp, exchange, perpetual_price, spot_reference_price, basis_bps, "
            "perpetual_quote_volume, open_interest, funding_rate, liquidation_count, long_liquidation_volume, "
            "short_liquidation_volume, data_quality_flags FROM derivatives_metrics FINAL WHERE timestamp BETWEEN "
            "{start:DateTime64(3)} - INTERVAL 15 MINUTE AND {end:DateTime64(3)} + INTERVAL 15 MINUTE "
            "AND canonical_pair = {pair:String} ORDER BY timestamp DESC LIMIT {limit:UInt32}",
            {"start": event_time, "end": event_time, "pair": pair, "limit": 100}, 100)
        return self._result({"event_type": "anomaly", "event_metadata": event,
            "trigger": {k: event.get(k) for k in ("event_type", "severity", "direction", "window", "detectors_triggered")},
            "evidence": event.get("evidence"), "metrics_snapshot": event.get("metrics_snapshot"),
            "spot_participation_and_liquidity": metrics, "cross_venue_evidence": cross,
            "derivatives_and_liquidations": deriv,
            "timeline": sorted([event] + metrics + cross + deriv,
                key=lambda r: str(r.get("timestamp", r.get("detected_at", ""))))},
            ["market_anomalies", "market_metrics", "cross_venue_metrics", "derivatives_metrics"],
            "observed_fact", event.get("data_quality_flags", []), event.get("detected_at"))


def _is_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False
