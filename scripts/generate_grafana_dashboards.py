"""Generate the checked-in Grafana dashboard JSON from bounded SQL definitions."""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"
DS = {"type": "grafana-clickhouse-datasource", "uid": "market-integrity-clickhouse"}
PAIR = "('${canonical_pair}' = 'all' OR canonical_pair = '${canonical_pair}')"
WINDOW = "('${window}' = 'all' OR window = '${window}')"
EXCHANGE = "('${exchange}' = 'all' OR exchange = '${exchange}')"
TIME_OPTS = {"from": "now-15m", "to": "now"}
REFRESH = "2s"
REFRESH_OPTIONS = ["1s", "2s", "5s", "10s", "30s", "1m"]


def target(sql, ref="A", series=False):
    return {"datasource": DS, "editorType": "sql", "format": 0 if series else 1,
            "queryType": "sql" if not series else "timeseries", "rawSql": sql, "refId": ref}


def panel(title, sql, kind="timeseries", unit="short", description="", w=12, h=8, series=None):
    is_series = kind == "timeseries" if series is None else series
    return {
        "datasource": DS,
        "fieldConfig": {"defaults": {"unit": unit, "decimals": 4,
            "color": {"mode": "palette-classic"}, "custom": {"drawStyle": "line", "lineWidth": 2,
            "fillOpacity": 10, "showPoints": "never"}}, "overrides": []},
        "gridPos": {"h": h, "w": w, "x": 0, "y": 0},
        "options": ({"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                     "tooltip": {"mode": "multi", "sort": "desc"}} if kind == "timeseries" else
                    {"showHeader": True} if kind == "table" else
                    {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "orientation": "auto", "textMode": "auto", "colorMode": "value", "graphMode": "area"}),
        "targets": [target(sql, series=is_series)], "title": title, "description": description, "type": kind,
    }


def variables(pair=True, exchange=True, window=True, classification=False, event=False):
    result = []
    def custom(name, label, values, default):
        result.append({"name": name, "label": label, "type": "custom", "query": values,
            "current": {"text": default, "value": default}, "options": [], "includeAll": False,
            "multi": False, "hide": 0, "refresh": 0, "skipUrlSync": False})
    def query(name, label, sql, default, multi=False, refresh=2):
        result.append({"name": name, "label": label, "type": "query", "datasource": DS,
            "definition": sql, "query": sql, "current": {"text": default, "value": default},
            "options": [], "includeAll": False, "multi": multi, "hide": 0,
            "refresh": refresh, "skipUrlSync": False,
            "regex": "", "sort": 1, "allowCustomValue": False,
            "queryValue": ""})
    if pair:
        query("canonical_pair", "Pair", "SELECT DISTINCT canonical_pair FROM cross_venue_metrics WHERE timestamp >= now() - INTERVAL 1 DAY ORDER BY canonical_pair LIMIT 20", "BTC-USD")
    if exchange:
        custom("exchange", "Venue", "all,binance,coinbase,kraken,okx,bybit", "all")
    if window:
        custom("window", "Window", "10s,30s,1m,5m", "30s")
    if classification:
        query("classification", "Classification", "SELECT DISTINCT classification FROM market_integrity WHERE calculated_at >= now() - INTERVAL 7 DAY ORDER BY classification LIMIT 20", "all")
        result[-1]["includeAll"] = True
        result[-1]["allValue"] = "all"
        result[-1]["current"] = {"text": "all", "value": "all"}
    if event:
        query("event_id", "Integrity event", "SELECT event_id FROM market_integrity FINAL WHERE calculated_at >= now() - INTERVAL 30 DAY ORDER BY calculated_at DESC LIMIT 500", "", multi=True)
        result[-1]["includeAll"] = False
    return result


def dashboard(uid, title, panels, vars=None, historical=False, description=""):
    # Lay panels out in a regular two-column grid; tables span the full width.
    y = 0
    x = 0
    row_h = 0
    for p in panels:
        w = p["gridPos"]["w"]
        h = p["gridPos"]["h"]
        if w == 24:
            if x:
                y += row_h
                x, row_h = 0, 0
            p["gridPos"]["x"], p["gridPos"]["y"] = 0, y
            y += h
        else:
            if x + w > 24:
                y += row_h
                x, row_h = 0, 0
            p["gridPos"]["x"], p["gridPos"]["y"] = x, y
            x += w
            row_h = max(row_h, h)
            if x == 24:
                y += row_h
                x, row_h = 0, 0
    return {"id": None, "uid": uid, "title": title, "description": description,
        "tags": ["market-move-integrity", "clickhouse"], "timezone": "browser",
        "schemaVersion": 41, "version": 1, "editable": False,
        "time": {"from": "now-30d" if historical else "now-15m", "to": "now"},
        "timepicker": {"refresh_intervals": REFRESH_OPTIONS},
        "refresh": "30s" if historical else REFRESH,
        "annotations": {"list": []}, "panels": panels,
        "templating": {"list": vars or []}, "links": [], "liveNow": False}


def write(filename, doc):
    (OUT / filename).write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")


def latest(table, time_col, field, filters):
    return f"SELECT argMax({field}, {time_col}) AS value FROM {table} FINAL WHERE $__timeFilter({time_col}) AND {filters}"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    sym_filter = "symbol IN ('${canonical_pair}', replaceOne('${canonical_pair}', '-', ''), replaceOne('${canonical_pair}', '-', 'USDT'), replaceOne('${canonical_pair}', '-', '/'))"
    spot = f"{sym_filter} AND {EXCHANGE} AND {WINDOW}"
    cross = f"{PAIR} AND {WINDOW}"
    deriv = f"{PAIR} AND {EXCHANGE} AND {WINDOW}"
    overview = [
        panel("Current BTC reference price", latest("cross_venue_metrics", "timestamp", "median_price", cross), "stat", "currencyUSD", "Latest cross-venue median in the selected dashboard range."),
        panel("Cross-venue median price", f"SELECT timestamp AS time, median_price AS value FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000", unit="currencyUSD"),
        panel("30s price return", latest("market_metrics", "timestamp", "price_return", f"{spot} AND window = '30s'"), "stat", "percentunit"),
        panel("Spot quote volume", f"SELECT timestamp AS time, exchange AS metric, quote_volume AS value FROM market_metrics FINAL WHERE $__timeFilter(timestamp) AND {spot} ORDER BY time, metric LIMIT 2000", unit="currencyUSD"),
        panel("Trade count", f"SELECT timestamp AS time, exchange AS metric, trade_count AS value FROM market_metrics FINAL WHERE $__timeFilter(timestamp) AND {spot} ORDER BY time, metric LIMIT 2000"),
        panel("VWAP and mid-price", f"SELECT timestamp AS time, exchange, vwap, mid_price FROM market_metrics FINAL WHERE $__timeFilter(timestamp) AND {spot} ORDER BY time LIMIT 2000", unit="currencyUSD"),
        panel("Spread (bps)", f"SELECT timestamp AS time, exchange AS metric, spread_bps AS value FROM market_metrics FINAL WHERE $__timeFilter(timestamp) AND {spot} ORDER BY time, metric LIMIT 2000", unit="bps"),
        panel("Order-book imbalance (20 levels)", f"SELECT timestamp AS time, exchange AS metric, orderbook_imbalance_20 AS value FROM market_metrics FINAL WHERE $__timeFilter(timestamp) AND {spot} ORDER BY time, metric LIMIT 2000", unit="percentunit"),
        panel("Bid depth vs ask depth", f"SELECT timestamp AS time, exchange, bid_depth_20, ask_depth_20 FROM market_metrics FINAL WHERE $__timeFilter(timestamp) AND {spot} ORDER BY time LIMIT 2000"),
        panel("Cross-venue confirmation ratio", latest("cross_venue_metrics", "timestamp", "confirmation_ratio", cross), "stat", "percentunit"),
        panel("Cross-venue dispersion (bps)", f"SELECT timestamp AS time, dispersion_bps AS value FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000", unit="bps"),
        panel("Active venues", latest("cross_venue_metrics", "timestamp", "active_venue_count", cross), "stat", "short"),
        panel("Perpetual / spot volume ratio", latest("derivatives_metrics", "timestamp", "perpetual_to_spot_volume_ratio", deriv), "stat", "percentunit"),
        panel("Basis (bps) by derivatives venue", f"SELECT timestamp AS time, exchange AS metric, basis_bps AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="bps"),
        panel("Latest integrity score", f"SELECT argMax(integrity_score, calculated_at) AS value FROM market_integrity FINAL WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}'", "stat", "short", "No recent triggered move returns N/A; the score is event-triggered, not continuous."),
        panel("Latest triggered move classification", f"SELECT if(count() = 0, 'No recent triggered move', argMax(classification, calculated_at)) AS classification FROM market_integrity FINAL WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}'", "stat", "none"),
        panel("Data freshness by persisted stream", "SELECT stream, latest_at, dateDiff('second', latest_at, now()) AS age_seconds FROM (SELECT 'market metrics' AS stream, max(timestamp) AS latest_at FROM market_metrics WHERE timestamp >= now() - INTERVAL 1 DAY UNION ALL SELECT 'cross-venue metrics', max(timestamp) FROM cross_venue_metrics WHERE timestamp >= now() - INTERVAL 1 DAY UNION ALL SELECT 'derivatives metrics', max(timestamp) FROM derivatives_metrics WHERE timestamp >= now() - INTERVAL 1 DAY UNION ALL SELECT 'integrity events', max(calculated_at) FROM market_integrity WHERE calculated_at >= now() - INTERVAL 1 DAY) ORDER BY stream LIMIT 10", "table", "s", "Freshness is computed from rows persisted in the last day; no event means NULL."),
    ]
    write("market-overview.json", dashboard("market-overview", "Market Overview", overview, variables()))

    integrity = [
        panel("Integrity score history", f"SELECT calculated_at AS time, integrity_score AS value FROM market_integrity FINAL WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}' AND ('${{classification}}' = 'all' OR classification = '${{classification}}') ORDER BY time LIMIT 1000", unit="short"),
        panel("Latest integrity score", latest("market_integrity", "calculated_at", "integrity_score", f"canonical_pair = '${{canonical_pair}}' AND ('${{classification}}' = 'all' OR classification = '${{classification}}')"), "stat", "short", "No recent trigger returns N/A."),
        panel("Overall confidence", latest("market_integrity", "calculated_at", "overall_confidence", f"canonical_pair = '${{canonical_pair}}' AND ('${{classification}}' = 'all' OR classification = '${{classification}}')"), "gauge", "percentunit"),
        panel("Evidence coverage", latest("market_integrity", "calculated_at", "evidence_coverage", f"canonical_pair = '${{canonical_pair}}' AND ('${{classification}}' = 'all' OR classification = '${{classification}}')"), "gauge", "percentunit"),
        panel("Classification history", f"SELECT calculated_at AS time, classification, trigger_severity, move_direction, move_window FROM market_integrity FINAL WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}' AND ('${{classification}}' = 'all' OR classification = '${{classification}}') ORDER BY time DESC LIMIT 500", "table", "none", w=24, h=10),
        panel("Integrity component scores (NULL scores omitted)", f"SELECT calculated_at AS time, JSONExtractString(component, 'name') AS metric, JSONExtract(component, 'score', 'Nullable(Float64)') AS value FROM market_integrity FINAL ARRAY JOIN JSONExtractArrayRaw(components) AS component WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}' AND JSONExtractRaw(component, 'score') != 'null' ORDER BY time LIMIT 2000", unit="short"),
        panel("Component evidence and confidence", f"SELECT calculated_at, JSONExtractString(component, 'name') AS component, if(JSONExtractRaw(component, 'score') = 'null', 'N/A', JSONExtractRaw(component, 'score')) AS score, JSONExtractRaw(component, 'confidence') AS confidence, JSONExtractString(component, 'status') AS status, JSONExtractString(component, 'availability') AS availability, JSONExtractRaw(component, 'evidence') AS evidence, JSONExtractRaw(component, 'data_quality_flags') AS data_quality_flags FROM market_integrity FINAL ARRAY JOIN JSONExtractArrayRaw(components) AS component WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}' ORDER BY calculated_at DESC LIMIT 100", "table", "none", w=24, h=10),
        panel("Trigger event information", f"SELECT calculated_at, trigger_event_id, trigger_type, trigger_severity, move_direction, move_window, integrity_score, overall_confidence, evidence_coverage FROM market_integrity FINAL WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}' ORDER BY calculated_at DESC LIMIT 100", "table", "none", w=24, h=9),
        panel("Data-quality flags", f"SELECT calculated_at, event_id, data_quality_flags FROM market_integrity FINAL WHERE $__timeFilter(calculated_at) AND canonical_pair = '${{canonical_pair}}' AND notEmpty(data_quality_flags) ORDER BY calculated_at DESC LIMIT 100", "table", "none", w=24, h=8),
    ]
    write("market-integrity.json", dashboard("market-integrity", "Market Move Integrity", integrity, variables(exchange=False, window=False, classification=True)))

    venue_names = "['binance','coinbase','kraken','okx','bybit']"
    cross_panels = [
        panel("Venue prices over time", f"SELECT timestamp AS time, venue AS metric, toFloat64OrNull(JSONExtractString(venue_prices, venue)) AS value FROM cross_venue_metrics FINAL ARRAY JOIN {venue_names} AS venue WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time, metric LIMIT 2000", unit="currencyUSD"),
        panel("Median reference price", f"SELECT timestamp AS time, median_price AS value FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000", unit="currencyUSD"),
        panel("Price dispersion (bps)", f"SELECT timestamp AS time, dispersion_bps AS value FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000", unit="bps"),
        panel("Confirmation ratio", f"SELECT timestamp AS time, confirmation_ratio AS value FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000", unit="percentunit"),
        panel("Confirmation state", f"SELECT timestamp, confirmation_state, active_venue_count, stale_venue_count FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY timestamp DESC LIMIT 100", "table", "none", w=24, h=8),
        panel("Active vs stale venue count", f"SELECT timestamp AS time, active_venue_count, stale_venue_count FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000"),
        panel("Per-venue deviation from median (bps)", f"SELECT timestamp AS time, venue AS metric, toFloat64OrNull(JSONExtractString(venue_deviation_bps, venue)) AS value FROM cross_venue_metrics FINAL ARRAY JOIN {venue_names} AS venue WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time, metric LIMIT 2000", unit="bps"),
        panel("Venue quote-volume shares", f"SELECT timestamp AS time, venue AS metric, toFloat64OrNull(JSONExtractString(venue_volume_shares, venue)) AS value FROM cross_venue_metrics FINAL ARRAY JOIN {venue_names} AS venue WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time, metric LIMIT 2000", unit="percentunit"),
        panel("Largest venue share and volume HHI", f"SELECT timestamp AS time, largest_venue_volume_share, volume_hhi FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time LIMIT 1000"),
        panel("Venue returns", f"SELECT timestamp AS time, venue AS metric, toFloat64OrNull(JSONExtractString(venue_returns, venue)) AS value FROM cross_venue_metrics FINAL ARRAY JOIN {venue_names} AS venue WHERE $__timeFilter(timestamp) AND {cross} ORDER BY time, metric LIMIT 2000", unit="percentunit"),
        panel("Lead / lag observations (only present values)", f"SELECT timestamp, first_observed_venue, venue, JSONExtractString(follow_lag_ms, venue) AS follow_lag_ms FROM cross_venue_metrics FINAL ARRAY JOIN {venue_names} AS venue WHERE $__timeFilter(timestamp) AND {cross} AND JSONExtractString(follow_lag_ms, venue) != '' ORDER BY timestamp DESC LIMIT 200", "table", "none", w=24, h=9),
    ]
    write("cross-venue.json", dashboard("cross-venue", "Cross-Venue Analysis", cross_panels, variables(exchange=False)))

    deriv_panels = [
        panel("Perpetual price by venue", f"SELECT timestamp AS time, exchange AS metric, perpetual_price AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="currencyUSD"),
        panel("Spot reference price by venue", f"SELECT timestamp AS time, exchange AS metric, spot_reference_price AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="currencyUSD"),
        panel("Perpetual vs spot price", f"SELECT timestamp AS time, exchange, perpetual_price, spot_reference_price FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time LIMIT 1000", unit="currencyUSD"),
        panel("Basis (bps)", f"SELECT timestamp AS time, exchange AS metric, basis_bps AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="bps"),
        panel("Perpetual quote volume", f"SELECT timestamp AS time, exchange AS metric, perpetual_quote_volume AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="currencyUSD"),
        panel("Perpetual / spot volume ratio", f"SELECT timestamp AS time, exchange AS metric, perpetual_to_spot_volume_ratio AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="percentunit"),
        panel("Open interest by venue and native unit (not summed)", f"SELECT timestamp AS time, concat(exchange, ' (', ifNull(open_interest_unit, 'unit unavailable'), ')') AS metric, if(open_interest_comparable, open_interest, CAST(NULL, 'Nullable(Decimal(76,38))')) AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", description="Open interest is shown per venue only; incompatible units are not aggregated."),
        panel("Open-interest change", f"SELECT timestamp AS time, exchange AS metric, open_interest_change_pct AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="percentunit"),
        panel("Funding rate", f"SELECT timestamp AS time, exchange AS metric, funding_rate AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="percentunit"),
        panel("Liquidation count", f"SELECT timestamp AS time, exchange AS metric, liquidation_count AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000"),
        panel("Long vs short liquidation volume", f"SELECT timestamp AS time, exchange, long_liquidation_volume, short_liquidation_volume FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time LIMIT 1000", unit="currencyUSD"),
        panel("Total liquidation volume", f"SELECT timestamp AS time, exchange AS metric, total_liquidation_volume AS value FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY time, metric LIMIT 1000", unit="currencyUSD"),
        panel("Derivatives data-quality flags", f"SELECT timestamp, exchange, event_id, data_quality_flags, volume_unit, open_interest_unit, open_interest_comparable, spot_reference_timestamp FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) AND {deriv} ORDER BY timestamp DESC LIMIT 100", "table", "none", w=24, h=9),
        panel("Binance derivatives availability", "", "text", "none", "Binance derivatives source-health is not persisted in ClickHouse. The current verified source is unavailable/restricted. Missing rows are not interpreted as zero; see ingestion source-health logs."),
    ]
    deriv_panels[-1]["options"] = {"mode": "markdown", "content": "**Binance derivatives:** unavailable/restricted in the current verified environment. Source-health is not persisted in ClickHouse; missing rows are not zero."}
    deriv_panels[-1]["targets"] = []
    deriv_panels[-1].pop("datasource", None)
    write("derivatives.json", dashboard("derivatives", "Derivatives & Liquidations", deriv_panels, variables(window=True)))

    anomalies = [
        panel("Recent anomalies", f"SELECT detected_at, event_id, event_type, severity, direction, window, exchange, symbol, detectors_triggered, baseline_ready, evidence FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) AND {EXCHANGE} ORDER BY detected_at DESC LIMIT 200", "table", "none", w=24, h=12),
        panel("Anomalies over time", f"SELECT toStartOfInterval(detected_at, INTERVAL 10 SECOND) AS time, count() AS value FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) AND {EXCHANGE} GROUP BY time ORDER BY time LIMIT 1000"),
        panel("Anomaly count by type", f"SELECT event_type, count() AS events FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) AND {EXCHANGE} GROUP BY event_type ORDER BY events DESC LIMIT 20", "table", "short", w=12, h=8),
        panel("Severity distribution", f"SELECT severity, count() AS events FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) AND {EXCHANGE} GROUP BY severity ORDER BY events DESC LIMIT 10", "barchart", "short", w=12, h=8),
        panel("Detector triggers", f"SELECT detector, count() AS events FROM (SELECT arrayJoin(detectors_triggered) AS detector FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) AND {EXCHANGE}) GROUP BY detector ORDER BY events DESC LIMIT 20", "table", "short", w=12, h=8),
        panel("Selected anomaly evidence", f"SELECT detected_at, event_id, metrics_snapshot, evidence, data_quality_flags FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) AND {EXCHANGE} ORDER BY detected_at DESC LIMIT 50", "table", "none", w=24, h=10),
    ]
    write("anomaly-center.json", dashboard("anomaly-center", "Anomaly Center", anomalies, variables(exchange=True, pair=False, window=False)))

    historical = [
        panel("Historical integrity events", f"SELECT calculated_at, event_id, canonical_pair, trigger_type, trigger_severity, integrity_score, overall_confidence, evidence_coverage, classification, move_direction, move_window FROM market_integrity FINAL WHERE calculated_at >= now() - INTERVAL 30 DAY AND calculated_at <= now() AND canonical_pair = '${{canonical_pair}}' AND event_id IN (${{event_id:singlequote}}) ORDER BY calculated_at DESC LIMIT 500", "table", "none", w=24, h=12),
        panel("Selected-event component comparison", f"SELECT event_id, calculated_at, JSONExtractString(component, 'name') AS component, if(JSONExtractRaw(component, 'score') = 'null', 'N/A', JSONExtractRaw(component, 'score')) AS score, JSONExtractRaw(component, 'confidence') AS confidence, JSONExtractString(component, 'availability') AS availability FROM market_integrity FINAL ARRAY JOIN JSONExtractArrayRaw(components) AS component WHERE calculated_at >= now() - INTERVAL 30 DAY AND calculated_at <= now() AND canonical_pair = '${{canonical_pair}}' AND event_id IN (${{event_id:singlequote}}) ORDER BY calculated_at DESC LIMIT 1000", "table", "none", w=24, h=12),
        panel("Comparison scope", "", "text", "none", "Historical comparison is a record-level comparison of selected triggered integrity events. This view does not perform similarity scoring or imply machine-learning matches."),
    ]
    historical[-1]["options"] = {"mode": "markdown", "content": "Select one or more event IDs above. Historical comparisons are record-level; similarity search is not implemented."}
    historical[-1]["targets"] = []
    historical[-1].pop("datasource", None)
    hvars = variables(exchange=False, window=False, classification=False, event=True)
    # Historical selector is intentionally initialized without a stale hardcoded ID.
    hvars[1]["current"] = {"text": "", "value": ""}
    write("historical-events.json", dashboard("historical-events", "Historical Event Comparison", historical, hvars, historical=True))

    freshness = "SELECT stream, latest_at, dateDiff('second', latest_at, now()) AS age_seconds FROM (SELECT 'market metrics' AS stream, max(timestamp) AS latest_at FROM market_metrics WHERE timestamp >= now() - INTERVAL 1 DAY UNION ALL SELECT 'cross-venue metrics', max(timestamp) FROM cross_venue_metrics WHERE timestamp >= now() - INTERVAL 1 DAY UNION ALL SELECT 'derivatives metrics', max(timestamp) FROM derivatives_metrics WHERE timestamp >= now() - INTERVAL 1 DAY UNION ALL SELECT 'anomalies', max(detected_at) FROM market_anomalies WHERE detected_at >= now() - INTERVAL 1 DAY UNION ALL SELECT 'integrity events', max(calculated_at) FROM market_integrity WHERE calculated_at >= now() - INTERVAL 1 DAY) ORDER BY stream LIMIT 10"
    rates = "SELECT bucket AS time, stream AS metric, events / 10.0 AS value FROM (SELECT toStartOfInterval(ts, INTERVAL 10 SECOND) AS bucket, stream, count() AS events FROM (SELECT timestamp AS ts, 'market metrics' AS stream FROM market_metrics FINAL WHERE $__timeFilter(timestamp) UNION ALL SELECT timestamp, 'cross-venue metrics' FROM cross_venue_metrics FINAL WHERE $__timeFilter(timestamp) UNION ALL SELECT timestamp, 'derivatives metrics' FROM derivatives_metrics FINAL WHERE $__timeFilter(timestamp) UNION ALL SELECT detected_at, 'anomalies' FROM market_anomalies FINAL WHERE $__timeFilter(detected_at) UNION ALL SELECT calculated_at, 'integrity events' FROM market_integrity FINAL WHERE $__timeFilter(calculated_at)) GROUP BY bucket, stream) ORDER BY time, metric LIMIT 2000"
    infra = [
        panel("Persisted-data freshness", freshness, "table", "s", "Freshness is derived from persisted event timestamps, bounded to the last day.", w=24, h=10),
        panel("Persisted rows per second (10s buckets)", rates, unit="ops"),
        panel("Latest timestamp by table", freshness.replace(", dateDiff('second', latest_at, now()) AS age_seconds", ""), "table", "dateTimeAsIso", w=24, h=10),
        panel("Operational metrics not persisted", "", "text", "none", "Kafka consumer lag, producer errors, worker health, DLQ rate, and WebSocket status are not stored in ClickHouse. They are intentionally not shown as zero or inferred here."),
    ]
    infra[-1]["options"] = {"mode": "markdown", "content": "**Not persisted yet:** Kafka consumer lag, producer errors, worker health, DLQ rate, WebSocket status. No zero-valued placeholder panels are used."}
    infra[-1]["targets"] = []
    infra[-1].pop("datasource", None)
    write("infrastructure-health.json", dashboard("infrastructure-health", "Streaming Infrastructure Health", infra, variables(pair=False, exchange=False, window=False)))


if __name__ == "__main__":
    main()
