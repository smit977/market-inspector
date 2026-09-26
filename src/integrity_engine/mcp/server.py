"""MCP registration and transport setup."""
from __future__ import annotations

import asyncio
import sys

import structlog
from mcp.server import MCPServer

from .config import MCPSettings
from .models import MCPResponse
from .tools import MCPTools

structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
log = structlog.get_logger(__name__)
settings = MCPSettings()
tools = MCPTools(settings=settings)
server = MCPServer(
    name="Market Move Integrity Engine",
    title="Market Move Integrity Engine",
    version="0.1.0",
    instructions=("Read-only market intelligence sourced from bounded ClickHouse queries. "
                  "Responses distinguish persisted observations, calculated metrics, and stored statistical "
                  "events. No tool provides trading execution or buy/sell advice."),
)


@server.tool(description="Summarize current persisted spot, cross-venue, derivatives, integrity and anomaly evidence.")
def get_market_summary(canonical_pair: str = "BTC-USD", lookback_minutes: int = 15) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_market_summary", canonical_pair, lookback_minutes))


@server.tool(description="Return the latest persisted spot metrics for a pair, venue and window.")
def get_latest_market_metrics(canonical_pair: str = "BTC-USD", exchange: str | None = None,
                              window: str = "30s", lookback_minutes: int = 15) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_latest_market_metrics", canonical_pair, exchange, window, lookback_minutes))


@server.tool(description="List recent anomaly-triggered market moves. This is not a trading recommendation.")
def get_active_market_moves(canonical_pair: str = "BTC-USD", lookback_minutes: int = 60,
                            minimum_severity: str | None = None) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_active_market_moves", canonical_pair, lookback_minutes, minimum_severity))


@server.tool(description="Return a stored integrity event or an explicit no-event state.")
def get_integrity_score(canonical_pair: str = "BTC-USD", event_id: str | None = None) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_integrity_score", canonical_pair, event_id))


@server.tool(description="Return stored integrity component scores, availability, confidence, weights and evidence.")
def get_integrity_components(event_id: str | None = None, canonical_pair: str = "BTC-USD") -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_integrity_components", event_id, canonical_pair))


@server.tool(description="Compare venue observations without ranking or trust labels.")
def compare_exchanges(canonical_pair: str = "BTC-USD", window: str = "30s",
                      lookback_minutes: int = 15) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("compare_exchanges", canonical_pair, window, lookback_minutes))


@server.tool(description="Return latest and recent cross-venue confirmation, freshness and dispersion evidence.")
def get_cross_venue_confirmation(canonical_pair: str = "BTC-USD", window: str = "30s",
                                 lookback_minutes: int = 15) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_cross_venue_confirmation", canonical_pair, window, lookback_minutes))


@server.tool(description="Return bounded order-book metrics and neutral liquidity-stress evidence.")
def get_orderbook_analysis(canonical_pair: str = "BTC-USD", exchange: str = "binance",
                           window: str = "30s") -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_orderbook_analysis", canonical_pair, exchange, window))


@server.tool(description="Return persisted derivatives metrics; unavailable financial values remain null.")
def get_derivatives_analysis(canonical_pair: str = "BTC-USD", exchange: str | None = None,
                             window: str = "1m") -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_derivatives_analysis", canonical_pair, exchange, window))


@server.tool(description="Summarize stored liquidation evidence; missing feed evidence remains unavailable, not zero.")
def get_liquidation_activity(canonical_pair: str = "BTC-USD", exchange: str | None = None,
                             lookback_minutes: int = 60) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_liquidation_activity", canonical_pair, exchange, lookback_minutes))


@server.tool(description="List recent anomalies with stored evidence and data-quality flags.")
def get_anomalies(canonical_pair: str = "BTC-USD", lookback_minutes: int = 60,
                  event_type: str | None = None, severity: str | None = None,
                  limit: int = 100) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_anomalies", canonical_pair, lookback_minutes, event_type, severity, limit))


@server.tool(description="Find explainably similar historical integrity events using deterministic feature similarity, not prediction.")
def get_historical_similar_events(event_id: str, limit: int = 5) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_historical_similar_events", event_id, limit))


@server.tool(description="Report persisted table freshness, quality flags, null derivatives fields and unavailable operational feeds.")
def get_data_quality(lookback_minutes: int = 60) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_data_quality", lookback_minutes))


@server.tool(description="Return ClickHouse reachability, table freshness and MCP uptime; Kafka lag is explicitly unavailable.")
def get_pipeline_health() -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("get_pipeline_health"))


@server.tool(description="Read selected allowlisted columns from one curated table within a required bounded lookback; arbitrary SQL is not accepted.")
def query_clickhouse(table: str, columns: list[str], lookback_minutes: int = 15,
                     canonical_pair: str | None = None, exchange: str | None = None,
                     limit: int = 500) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("query_clickhouse", table, columns, lookback_minutes, canonical_pair, exchange, limit))


@server.tool(description="Assemble report-ready event evidence and timeline without unsupported causal claims.")
def generate_incident_report(integrity_event_id: str | None = None,
                             anomaly_event_id: str | None = None) -> MCPResponse:
    return MCPResponse.model_validate(tools.invoke("generate_incident_report", integrity_event_id, anomaly_event_id))


async def run() -> None:
    try:
        if settings.transport == "stdio":
            log.info("mcp_server_starting", transport="stdio")
            await server.run_stdio_async()
        else:
            log.info("mcp_server_starting", transport="streamable-http", host=settings.host,
                     port=settings.port, endpoint="/mcp")
            await server.run_streamable_http_async(
                host=settings.host, port=settings.port, streamable_http_path="/mcp",
                stateless_http=True, json_response=True,
            )
    finally:
        tools.runner.close()


def main() -> None:
    asyncio.run(run())
