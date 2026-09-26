from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from integrity_engine.mcp.models import MCPResponse, QueryClickHouseInput, TABLES
from integrity_engine.mcp.queries import MCPQueryRunner, QueryResult
from integrity_engine.mcp.tools import MCPTools, SIMILARITY_WEIGHTS


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
        assert "max(" in sql
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

    service = MCPTools()
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
