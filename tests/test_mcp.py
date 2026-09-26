from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from integrity_engine.mcp.models import MCPResponse, QueryClickHouseInput, TABLES
from integrity_engine.mcp.queries import MCPQueryRunner, QueryResult
from integrity_engine.mcp.tools import MCPTools, SIMILARITY_WEIGHTS, _decode


class EmptyRunner:
    def __init__(self):
        self.calls = []

    def run(self, sql, parameters=None, limit=None):
        self.calls.append((sql, parameters, limit))
        return QueryResult([], 1.25)

    def select(self, table, columns, lookback, filters=None, limit=100):
        self.calls.append((table, columns, lookback, filters, limit))
        return QueryResult([], 1.25)

    def close(self):
        pass


def test_mcp_response_has_evidence_categories_and_json_safe_metadata():
    result = MCPResponse(status="ok", data={"price": Decimal("1.2300")},
                         data_as_of=datetime.now(timezone.utc), source_tables=["market_metrics"])
    encoded = result.model_dump(mode="json")
    assert encoded["data"]["price"] == "1.2300"
    assert set(encoded["evidence"]) == {
        "observed_fact", "calculated_metric", "statistical_inference", "ai_interpretation"
    }
    assert encoded["evidence"]["ai_interpretation"] is None


@pytest.mark.parametrize("name,args", [
    ("get_market_summary", ()), ("get_latest_market_metrics", ()),
    ("get_active_market_moves", ()), ("get_integrity_score", ()),
    ("get_integrity_components", ()), ("compare_exchanges", ()),
    ("get_cross_venue_confirmation", ()), ("get_orderbook_analysis", ()),
    ("get_derivatives_analysis", ()), ("get_liquidation_activity", ()),
    ("get_anomalies", ()), ("get_historical_similar_events", ("known-event",)),
    ("get_data_quality", ()), ("get_pipeline_health", ()),
    ("query_clickhouse", ("market_metrics", ["timestamp", "event_id"])),
    ("generate_incident_report", ("known-event", None)),
])
def test_every_tool_returns_structured_no_data_or_explicit_not_found(name, args):
    service = MCPTools(runner=EmptyRunner())
    response = service.invoke(name, *args)
    assert response["status"] in {"no_data", "stale_data", "ok"}
    assert "generated_at" in response
    assert "query_duration_ms" in response
    assert "source_tables" in response
    assert set(response["evidence"]) == {
        "observed_fact", "calculated_metric", "statistical_inference", "ai_interpretation"
    }


def test_query_runner_sql_is_bounded_and_only_allowlisted_identifiers_are_accepted():
    sql, params = MCPQueryRunner.select_sql(
        "market_metrics", ["timestamp", "event_id", "spread_bps"], 15,
        {"exchange": "binance"}, 500,
    )
    assert "FROM `market_metrics`" in sql and "FINAL" not in sql
    assert "timestamp` >= now() - INTERVAL {lookback:UInt32} MINUTE" in sql
    assert "ORDER BY `timestamp` DESC, `event_id` LIMIT 1 BY `event_id` LIMIT {row_limit:UInt32}" in sql
    assert "SELECT *" not in sql.upper()
    assert params["lookback"] == 15 and params["row_limit"] == 500
    with pytest.raises(ValueError):
        MCPQueryRunner.select_sql("market_metrics; DROP TABLE x", ["timestamp"], 15)
    with pytest.raises(ValueError):
        MCPQueryRunner.select_sql("market_metrics", ["original_event; DROP"], 15)


def test_query_clickhouse_rejects_unknown_table_column_and_bounds_limit():
    with pytest.raises(ValidationError):
        QueryClickHouseInput(table="system.tables", columns=["name"])
    with pytest.raises(ValidationError):
        QueryClickHouseInput(table="market_metrics", columns=["password"])
    with pytest.raises(ValidationError):
        QueryClickHouseInput(table="market_metrics", columns=["timestamp"], limit=5001)
    assert set(TABLES) == {"market_metrics", "cross_venue_metrics", "derivatives_metrics", "market_anomalies", "market_integrity"}


def test_market_metric_query_clickhouse_aliases_pair_to_bounded_symbol_filter():
    runner = EmptyRunner()
    result = MCPTools(runner=runner).invoke(
        "query_clickhouse", "market_metrics", ["timestamp", "symbol"], 10, "BTC-USD", None, 20
    )
    table, columns, lookback, filters, limit = runner.calls[0]
    assert table == "market_metrics"
    assert set(filters["symbol"]) == {"BTC-USD", "BTC/USD", "BTCUSD", "BTC-USDT", "BTCUSDT"}
    assert lookback == 10 and limit == 20
    assert result["status"] == "no_data"


def test_latest_market_projection_avoids_large_original_event_payload():
    runner = EmptyRunner()
    MCPTools(runner=runner)._latest_market("BTC-USD", None, "30s", 15, 100)
    sql, params, limit = runner.calls[0]
    assert "original_event" not in sql
    assert "timestamp >= now() - INTERVAL {lookback:UInt32} MINUTE" in sql
    assert "LIMIT 1 BY event_id LIMIT {limit:UInt32}" in sql
    assert params["lookback"] == 15 and params["limit"] == 100 and limit == 100


def test_integrity_score_projection_omits_component_evidence_unless_requested():
    runner = EmptyRunner()
    service = MCPTools(runner=runner)
    service._latest_integrity("BTC-USD")
    pair_sql = runner.calls[-1][0]
    assert "components" not in pair_sql
    assert "UNION ALL" in pair_sql
    assert "move_window = {window_" in pair_sql
    assert "LIMIT 1 BY event_id LIMIT 1" in pair_sql
    service._latest_integrity("BTC-USD", include_components=True)
    assert "components" not in runner.calls[-1][0]
    assert "calculated_at >= now() - INTERVAL {lookback:UInt32} DAY" in runner.calls[-1][0]


def test_component_evidence_is_loaded_by_exact_event_key_after_metadata_lookup():
    class EventRunner(EmptyRunner):
        def run(self, sql, parameters=None, limit=None):
            self.calls.append((sql, parameters, limit))
            if sql.startswith("SELECT components FROM"):
                return QueryResult([{"components": "[]"}], 1.0)
            return QueryResult([{"event_id": "event-1", "calculated_at": datetime.now(timezone.utc),
                "canonical_pair": "BTC-USD", "move_window": "30s", "data_quality_flags": []}], 1.0)

    runner = EventRunner()
    rows = MCPTools(runner=runner)._latest_integrity("BTC-USD", include_components=True)
    assert rows[0]["components"] == []
    sql, params, _ = runner.calls[-1]
    assert "canonical_pair = {pair:String}" in sql
    assert "move_window = {window:String}" in sql
    assert "calculated_at = {calculated_at:DateTime64(3)}" in sql
    assert params["event_id"] == "event-1"


def test_pipeline_health_uses_bounded_timestamp_aggregates():
    runner = EmptyRunner()
    result = MCPTools(runner=runner).invoke("get_pipeline_health")
    assert result["status"] == "stale_data"
    assert len(runner.calls) == len(TABLES)
    for sql, params, limit in runner.calls:
        assert "maxOrNull(" in sql
        assert "now() - INTERVAL {lookback:UInt32} MINUTE" in sql
        assert "ORDER BY" not in sql
        assert params["lookback"] == 5 and limit == 1


def test_anomaly_listing_deduplicates_by_event_id_without_final():
    runner = EmptyRunner()
    MCPTools(runner=runner).invoke("get_anomalies")
    sql, params, limit = runner.calls[0]
    assert "FROM market_anomalies WHERE" in sql and " FINAL " not in sql
    assert "detected_at >= now() - INTERVAL {lookback:UInt32} MINUTE" in sql
    assert "LIMIT 1 BY event_id LIMIT {limit:UInt32}" in sql
    assert params["limit"] == limit == 100


def test_timeout_and_database_errors_are_structured_without_leaking_secret_text():
    class BrokenRunner(EmptyRunner):
        def run(self, *args, **kwargs):
            raise TimeoutError("database password=top-secret")

    response = MCPTools(runner=BrokenRunner()).invoke("get_integrity_score")
    assert response["status"] == "error"
    assert response["error"]["code"] == "query_timeout"
    assert "top-secret" not in str(response)
    assert "password" not in response["error"]["message"].lower()


def test_unavailable_financial_evidence_is_not_synthesized_as_zero():
    service = MCPTools(runner=EmptyRunner())
    result = service.invoke("get_liquidation_activity")
    assert result["data"]["data_availability"] == "unavailable"
    assert result["data"]["total_liquidation_volume"] is None
    assert result["status"] == "no_data"


def test_stale_market_rows_are_labeled_stale():
    class StaleRunner(EmptyRunner):
        def run(self, sql, parameters=None, limit=None):
            return QueryResult([{"timestamp": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                 "data_quality_flags": []}], 1.0)

    result = MCPTools(runner=StaleRunner()).invoke("get_latest_market_metrics")
    assert result["status"] == "stale_data"
    assert "stale_data" in result["quality_flags"]


def test_unsupported_exchange_is_a_structured_argument_error():
    result = MCPTools(runner=EmptyRunner()).invoke("get_derivatives_analysis", exchange="not-an-exchange")
    assert result["status"] == "error"
    assert result["error"]["code"] == "unsupported_exchange"


def test_historical_similarity_is_deterministic_and_returns_feature_differences():
    service = MCPTools(runner=EmptyRunner())
    anchor = {"event_id": "a", "trigger_type": "price_move", "classification": "broad",
              "move_direction": "up", "trigger_severity": "high", "integrity_score": Decimal("80"),
              "components": '[{"name":"Spot Participation","score":75}]'}
    candidate = {"event_id": "b", "trigger_type": "price_move", "classification": "broad",
                 "move_direction": "up", "trigger_severity": "high", "integrity_score": Decimal("82"),
                 "calculated_at": datetime.now(timezone.utc),
                 "components": '[{"name":"Spot Participation","score":70}]'}
    first = service._historical_similarity(anchor, [candidate])
    second = service._historical_similarity(anchor, [candidate])
    assert first == second
    assert first[0]["similarity_score"] > 90
    assert first[0]["component_similarities"]["Spot Participation"] == 0.95
    assert "not outcome prediction" in first[0]["similarity_method"]
    assert SIMILARITY_WEIGHTS["integrity_score"] > 0


def test_component_scores_preserve_missing_values_as_none():
    class IntegrityRunner(EmptyRunner):
        def run(self, sql, parameters=None, limit=None):
            return QueryResult([{"event_id": "e", "calculated_at": datetime.now(timezone.utc),
                "components": '[{"name":"Cross-Venue Confirmation","score":null,"availability":"unavailable","confidence":"0","weight":"0.2","status":"insufficient_data","evidence":[],"data_quality_flags":["missing"]}]',
                "data_quality_flags": []}], 1.0)

    result = MCPTools(runner=IntegrityRunner()).invoke("get_integrity_components", event_id="e")
    component = result["data"]["components"][0]
    assert component["score"] is None
    assert component["availability"] == "unavailable"
    assert component["data_quality_flags"] == ["missing"]


def test_query_runner_sets_server_execution_and_row_caps_and_recreates_after_failure():
    client = MagicMock()
    client.query.return_value.column_names = ["timestamp"]
    client.query.return_value.result_rows = []
    store = MagicMock(client=client)
    runner = MCPQueryRunner(timeout_seconds=4, max_result_rows=500, store_factory=lambda **_: store)
    runner.run("SELECT timestamp FROM market_metrics WHERE timestamp >= now() - INTERVAL 1 HOUR LIMIT {limit:UInt32}",
               parameters={"limit": 100}, limit=10)
    kwargs = client.query.call_args.kwargs
    assert kwargs["settings"]["max_execution_time"] == 4
    assert "max_result_rows" not in kwargs["settings"]
    assert kwargs["parameters"]["limit"] == 10
    client.query.side_effect = TimeoutError("timeout")
    with pytest.raises(TimeoutError):
        runner.run("SELECT 1")
    assert store.close.called


@pytest.mark.skipif(__import__("os").getenv("MCP_CLICKHOUSE_INTEGRATION") != "1",
                    reason="run from the MCP container with MCP_CLICKHOUSE_INTEGRATION=1")
def test_tools_against_local_clickhouse():
    from integrity_engine.mcp.tools import MCPTools

    import os
    from integrity_engine.storage.clickhouse import ClickHouseSettings
    # Exercise the deployed read-only profile when integration credentials exist;
    # the replay-storage test retains the separate writer identity.
    settings = ClickHouseSettings()
    if os.getenv("CLICKHOUSE_MCP_USER") and os.getenv("CLICKHOUSE_MCP_PASSWORD"):
        settings = ClickHouseSettings(user=os.environ["CLICKHOUSE_MCP_USER"], password=os.environ["CLICKHOUSE_MCP_PASSWORD"])
    service = MCPTools(runner=MCPQueryRunner(settings=settings))
    try:
        calls = [
            ("get_market_summary", ()), ("get_latest_market_metrics", ()),
            ("get_integrity_score", ()), ("get_integrity_components", ()),
            ("compare_exchanges", ()), ("get_cross_venue_confirmation", ()),
            ("get_orderbook_analysis", ()), ("get_derivatives_analysis", ()),
            ("get_liquidation_activity", ()), ("get_anomalies", ()),
            ("get_data_quality", ()), ("get_pipeline_health", ()),
            ("query_clickhouse", ("market_metrics", ["timestamp", "event_id"])),
        ]
        for name, args in calls:
            response = service.invoke(name, *args)
            assert response["status"] != "error", (name, response.get("error"))
            assert response["source_tables"]
            assert response["query_duration_ms"] is not None
        event = service.runner.run(
            "SELECT event_id FROM market_integrity FINAL WHERE calculated_at >= now() - INTERVAL 365 DAY "
            "AND calculated_at <= now() ORDER BY calculated_at DESC LIMIT 1", limit=1,
        ).rows
        if event:
            event_id = event[0]["event_id"]
            for name, args in [("get_historical_similar_events", (event_id,)),
                               ("generate_incident_report", (event_id, None))]:
                response = service.invoke(name, *args)
                assert response["status"] != "error"
    finally:
        service.runner.close()



@pytest.mark.parametrize("state,ratio,returns,expected,signal", [
    ("fragmented", "0", {"a": "0.00001", "b": "-0.00001", "c": "0"}, "neutral", False),
    ("fragmented", "0.4", {"a": "0.01", "b": "-0.01"}, "fragmented", True),
    ("broad", "1", {"a": "0.01", "b": "0.02"}, "broad", True),
    ("broad", "1", {"a": "-0.01", "b": "-0.02"}, "broad", True),
    ("partial", "0.6", {"a": "0.01", "b": "-0.02"}, "partial", True),
    ("insufficient_data", "0", {"a": "0"}, "insufficient_data", None),
    ("fragmented", None, {"a": None}, "insufficient_data", None),
])
def test_mcp_persisted_confirmation_semantics(state, ratio, returns, expected, signal):
    row = _decode({"confirmation_state": state, "confirmation_ratio": ratio, "venue_returns": returns})
    assert row["confirmation_state"] == expected
    assert row["confirmation_ratio"] == ratio
    assert row["directional_signal_present"] is signal


def summary_service(event_age=120, derivatives_age=2, threshold=60):
    now = datetime.now(timezone.utc)
    service = MCPTools(runner=EmptyRunner(), stale_after_seconds=threshold)
    service._latest_market = lambda *a: [{"timestamp": now, "exchange": "binance", "symbol": "BTC-USDT", "price_return": Decimal("0"), "data_quality_flags": []}]
    service._latest_cross = lambda *a: [_decode({"timestamp": now, "confirmation_ratio": "0", "confirmation_state": "fragmented", "venue_returns": {"a": "0", "b": "0", "c": "0"}, "data_quality_flags": []})]
    service._latest_derivatives = lambda *a: [{"timestamp": now - timedelta(seconds=derivatives_age), "data_quality_flags": ["missing_funding"]}]
    service._latest_integrity = lambda *a: [] if event_age is None else [{"calculated_at": now - timedelta(seconds=event_age), "integrity_score": Decimal("70"), "classification": "Liquidity Shock", "data_quality_flags": []}]
    service._latest_anomaly = lambda *a: []
    return service


@pytest.mark.parametrize("event_age,derivatives_age,status,event_status", [
    (120, 2, "ok", "stale"),
    (None, 2, "ok", "no_data"),
    (None, 120, "stale_data", "no_data"),
    (2, 2, "ok", "fresh"),
])
def test_summary_separates_live_freshness_and_triggered_events(event_age, derivatives_age, status, event_status):
    response = summary_service(event_age, derivatives_age).invoke("get_market_summary")
    data = response["data"]
    assert response["status"] == status
    assert data["market_data_status"] == data["cross_venue_status"] == "fresh"
    assert data["derivatives_status"] == ("stale" if derivatives_age > 60 else "fresh")
    assert data["latest_integrity_event_status"] == event_status
    assert data["latest_anomaly_status"] == "no_data"
    assert data["triggered_move_state"] == ("recent_triggered_move" if event_status == "fresh" else "no_recent_triggered_move")
    assert "integrity_score" not in data["current_market_state"]
    assert data["current_market_state"]["spot_metrics_context"]["window"] == "30s"
    assert data["current_market_state"]["cross_venue_confirmation_state"] == "neutral"
    assert "missing_funding" in data["data_limitations"]["derivatives_metrics"]
    event = data["latest_triggered_integrity_event"]
    assert event == data["latest_integrity_event"]
    if event:
        assert event["event_age_seconds"] >= event_age
        assert event["describes_current_market"] is False
    else:
        assert event is None
    assert response["evidence"]["statistical_inference"]["latest_integrity_assessment"] == event


def test_event_freshness_respects_configured_threshold():
    service = summary_service(event_age=120, threshold=180)
    assert service.invoke("get_market_summary")["data"]["latest_integrity_event_status"] == "fresh"
    result = service.invoke("get_integrity_score")
    assert result["data"]["event_status"] == "fresh"
    service.stale_after_seconds = 30
    result = service.invoke("get_integrity_score")
    assert result["status"] == "stale_data"
    assert result["data"]["state"] == "no_recent_triggered_move"


def test_empty_integrity_has_no_synthetic_score():
    result = summary_service(event_age=None).invoke("get_integrity_score")
    assert result["data"]["state"] == "no_recent_triggered_move"
    assert "integrity_score" not in result["data"]


def test_epoch_aggregate_is_missing_not_decades_stale():
    service = MCPTools(runner=EmptyRunner())
    service._freshness = lambda *a, **kw: {"latest_timestamp": datetime(1970, 1, 1, tzinfo=timezone.utc)}
    result = service.invoke("get_data_quality")
    for row in result["data"]["latest_timestamps"].values():
        assert row["status"] == "no_data"
        assert row["latest_timestamp"] is None
        assert row["age_seconds"] is None


def test_partial_cross_projection_does_not_invent_insufficient_data():
    assert _decode({"confirmation_state": "broad"}) == {"confirmation_state": "broad"}


def test_data_quality_fresh_metrics_with_old_events_is_not_globally_stale():
    service = MCPTools(runner=EmptyRunner(), stale_after_seconds=30)
    now = datetime.now(timezone.utc)
    service._freshness = lambda table, *a, **kw: {"latest_timestamp": now - timedelta(
        seconds=120 if table in {"market_integrity", "market_anomalies"} else 2)}
    response = service.invoke("get_data_quality")
    assert response["status"] == "ok"
    assert response["data"]["latest_timestamps"]["market_integrity"]["status"] == "stale"
    assert response["data"]["latest_timestamps"]["market_anomalies"]["state"] == "no_recent_triggered_move"


def test_historical_limitations_are_scoped_away_from_live_conditions():
    service = summary_service()
    service._latest_integrity = lambda *a: [{
        "calculated_at": datetime.now(timezone.utc) - timedelta(seconds=120),
        "data_quality_flags": ["missing_cross_venue_metrics"]}]
    data = service.invoke("get_market_summary")["data"]
    assert data["data_limitations"]["latest_integrity_event"] == ["missing_cross_venue_metrics"]
    assert data["data_limitations"]["cross_venue_metrics"] == []
    assert data["current_market_state"]["freshness"]["cross_venue_metrics"]["status"] == "fresh"


@pytest.mark.parametrize("name", ["get_active_market_moves", "get_anomalies"])
def test_no_anomaly_list_explicitly_reports_no_recent_trigger(name):
    response = MCPTools(runner=EmptyRunner()).invoke(name)
    assert response["status"] == "no_data"
    assert "no_recent_triggered_move" in response["quality_flags"]


def test_mcp_resource_limits_are_fixed_in_readonly_profile():
    from integrity_engine.mcp.user import provision_mcp_user
    store = MagicMock()
    provision_mcp_user(store, "market_integrity", "test_reader", "test-only-password")
    profile_sql = [c.args[0] for c in store.client.command.call_args_list if "SETTINGS PROFILE" in c.args[0] and "readonly = 1" in c.args[0]]
    assert len(profile_sql) == 2
    assert all("max_threads = 1" in sql and "max_block_size = 512" in sql for sql in profile_sql)
    assert all("preferred_block_size_bytes = 262144" in sql for sql in profile_sql)
