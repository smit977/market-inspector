import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from integrity_engine.storage.grafana_user import TABLES, provision_grafana_user

ROOT = Path(__file__).resolve().parents[1]
GRAFANA = ROOT / "grafana"


def test_provisioning_uses_docker_dns_native_protocol_and_secure_password():
    source = (GRAFANA / "provisioning/datasources/clickhouse.yaml").read_text()
    assert "name: Market Integrity ClickHouse" in source
    assert "type: grafana-clickhouse-datasource" in source
    assert "host: clickhouse" in source
    assert "localhost" not in source
    assert "port: 9000" in source and "protocol: native" in source
    assert "defaultDatabase: market_integrity" in source
    assert "password: ${CLICKHOUSE_GRAFANA_PASSWORD}" in source
    assert "CLICKHOUSE_GRAFANA_USER" in source
    assert "secureJsonData:" in source


def test_compose_provisions_local_grafana_and_named_runtime_volume():
    compose = (ROOT / "docker-compose.yml").read_text()
    assert "grafana/grafana:12.2.1" in compose
    assert "GF_PLUGINS_PREINSTALL_SYNC: grafana-clickhouse-datasource@4.21.3" in compose
    assert "GF_DASHBOARDS_MIN_REFRESH_INTERVAL: 2s" in compose
    assert '"127.0.0.1:${GRAFANA_PORT:-3000}:3000"' in compose
    assert "- grafana_data:/var/lib/grafana" in compose
    assert "grafana_data:" in compose
    assert "service_completed_successfully" in compose
    assert "${GRAFANA_ADMIN_PASSWORD:?Set GRAFANA_ADMIN_PASSWORD in .env}" in compose
    assert "${CLICKHOUSE_GRAFANA_PASSWORD:?Set CLICKHOUSE_GRAFANA_PASSWORD in .env}" in compose


def test_all_expected_dashboards_are_valid_bounded_and_versioned():
    expected = {
        "market-overview.json": ("market-overview", "Market Overview"),
        "market-integrity.json": ("market-integrity", "Market Move Integrity"),
        "cross-venue.json": ("cross-venue", "Cross-Venue Analysis"),
        "derivatives.json": ("derivatives", "Derivatives & Liquidations"),
        "anomaly-center.json": ("anomaly-center", "Anomaly Center"),
        "historical-events.json": ("historical-events", "Historical Event Comparison"),
        "infrastructure-health.json": ("infrastructure-health", "Streaming Infrastructure Health"),
    }
    files = {path.name for path in (GRAFANA / "dashboards").glob("*.json")}
    assert files == set(expected)
    for filename, (uid, title) in expected.items():
        dashboard = json.loads((GRAFANA / "dashboards" / filename).read_text())
        assert dashboard["uid"] == uid
        assert dashboard["title"] == title
        assert dashboard["panels"]
        if uid != "historical-events":
            assert dashboard["time"]["from"] == "now-15m"
            assert dashboard["refresh"] == "2s"
        for panel in dashboard["panels"]:
            for query in panel.get("targets", []):
                sql = query["rawSql"]
                assert "SELECT *" not in sql.upper()
                assert "$__timeFilter(" in sql or "now() - INTERVAL" in sql
                assert query["datasource"]["uid"] == "market-integrity-clickhouse"


def test_dashboard_folder_and_refresh_options_are_provisioned():
    provider = (GRAFANA / "provisioning/dashboards/dashboards.yaml").read_text()
    assert "folder: Market Move Integrity" in provider
    assert "folderUid: market-move-integrity" in provider
    assert "allowUiUpdates: false" in provider
    dashboard = json.loads((GRAFANA / "dashboards/market-overview.json").read_text())
    assert dashboard["timepicker"]["refresh_intervals"] == ["1s", "2s", "5s", "10s", "30s", "1m"]
    names = {v["name"] for v in dashboard["templating"]["list"]}
    assert {"canonical_pair", "exchange", "window"}.issubset(names)


def test_read_only_clickhouse_account_is_limited_to_five_tables_and_secret_is_parameterized():
    client = MagicMock()
    provision_grafana_user(MagicMock(client=client), "market_integrity", "grafana_reader", "test-password")
    commands = [call.args[0] for call in client.command.call_args_list]
    assert any("readonly = 1" in query and "CHANGEABLE_IN_READONLY" in query for query in commands)
    grants = [query for query in commands if query.startswith("GRANT SELECT")]
    assert len(grants) == len(TABLES)
    assert all(any(f"market_integrity`.`{table}" in grant for grant in grants) for table in TABLES)
    assert all("*.*" not in grant for grant in grants)
    password_calls = [call for call in client.command.call_args_list if "{password:String}" in call.args[0]]
    assert len(password_calls) == 2
    assert all(call.kwargs["parameters"] == {"password": "test-password"} for call in password_calls)
    assert all("test-password" not in call.args[0] for call in password_calls)


def test_invalid_grafana_user_identifier_is_rejected_before_sql():
    client = MagicMock()
    with pytest.raises(ValueError):
        provision_grafana_user(MagicMock(client=client), "market_integrity", "grafana; DROP USER default", "")
    client.command.assert_not_called()


def test_empty_grafana_password_is_rejected_before_sql():
    client = MagicMock()
    with pytest.raises(ValueError, match="must be non-empty"):
        provision_grafana_user(MagicMock(client=client), "market_integrity", "grafana_reader", "")
    client.command.assert_not_called()
