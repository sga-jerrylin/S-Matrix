from dc_runtime import mcp_server


def test_mcp_tools_list_contains_runtime_surface():
    response = mcp_server.handle_jsonrpc_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    )

    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert {
        "dc_health",
        "dc_doctor",
        "dc_query",
        "dc_list_tables",
        "dc_query_catalog",
        "dc_table_schema",
        "dc_query_history",
        "dc_insight_table",
        "dc_insight_replay",
        "dc_report_list",
        "dc_report_detail",
        "dc_report_summary",
        "dc_report_latest",
        "dc_forecast",
    }.issubset(tool_names)


def test_mcp_forecast_tool_schema_exposes_current_contract_fields():
    response = mcp_server.handle_jsonrpc_request(
        {"jsonrpc": "2.0", "id": 10, "method": "tools/list"}
    )

    forecast_tool = next(tool for tool in response["result"]["tools"] if tool["name"] == "dc_forecast")
    properties = forecast_tool["inputSchema"]["properties"]
    assert {
        "metric_key",
        "granularity",
        "horizon_steps",
        "horizon_unit",
        "start_at",
        "end_at",
        "lookback_points",
        "filters",
        "resource_name",
        "external_signals",
    }.issubset(set(properties))


def test_mcp_query_tool_uses_shared_runtime_client(monkeypatch):
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def query_natural(self, payload):
            recorded["payload"] = payload
            return {"success": True, "sql": "select 1"}

    monkeypatch.setattr(mcp_server.RuntimeSettings, "load", staticmethod(lambda: object()))
    monkeypatch.setattr(mcp_server, "RuntimeClient", FakeClient)

    result = mcp_server.call_tool(
        "dc_query",
        {
            "question": "how many orders",
            "table_names": ["orders"],
            "resource_name": "warehouse_ai",
        },
    )

    assert recorded["payload"] == {
        "query": "how many orders",
        "table_names": ["orders"],
        "resource_name": "warehouse_ai",
    }
    assert result["success"] is True


def test_mcp_unknown_tool_returns_jsonrpc_error():
    response = mcp_server.handle_jsonrpc_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "dc_missing", "arguments": {}},
        }
    )

    assert response["error"]["data"]["code"] == "unknown_tool"


def test_mcp_report_detail_dispatches_to_runtime_client(monkeypatch):
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def report_detail(self, report_id, *, include_reasoning=False):
            recorded["report_id"] = report_id
            recorded["include_reasoning"] = include_reasoning
            return {"success": True, "id": report_id}

    monkeypatch.setattr(mcp_server.RuntimeSettings, "load", staticmethod(lambda: object()))
    monkeypatch.setattr(mcp_server, "RuntimeClient", FakeClient)

    result = mcp_server.call_tool(
        "dc_report_detail",
        {"report_id": "report-1", "include_reasoning": True},
    )

    assert recorded == {
        "report_id": "report-1",
        "include_reasoning": True,
    }
    assert result["id"] == "report-1"


def test_mcp_report_summary_dispatches_to_runtime_client(monkeypatch):
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def report_summary(self, report_id):
            recorded["report_id"] = report_id
            return {"success": True, "id": report_id, "summary": "ok"}

    monkeypatch.setattr(mcp_server.RuntimeSettings, "load", staticmethod(lambda: object()))
    monkeypatch.setattr(mcp_server, "RuntimeClient", FakeClient)

    result = mcp_server.call_tool(
        "dc_report_summary",
        {"report_id": "report-1"},
    )

    assert recorded == {"report_id": "report-1"}
    assert result["summary"] == "ok"


def test_mcp_forecast_dispatches_to_runtime_client(monkeypatch):
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def forecast(self, payload):
            recorded["payload"] = payload
            return {"success": True, "status": "contract_only"}

    monkeypatch.setattr(mcp_server.RuntimeSettings, "load", staticmethod(lambda: object()))
    monkeypatch.setattr(mcp_server, "RuntimeClient", FakeClient)

    result = mcp_server.call_tool(
        "dc_forecast",
        {
            "metric_key": "inventory.turnover",
            "table_names": ["warehouse_stock_in_items"],
            "granularity": "day",
            "horizon_steps": 14,
            "horizon_unit": "day",
            "start_at": "2026-01-01T00:00:00",
            "end_at": "2026-04-19T00:00:00",
            "lookback_points": 30,
            "filters": {"region": "east"},
        },
    )

    assert recorded["payload"] == {
        "metric_key": "inventory.turnover",
        "table_names": ["warehouse_stock_in_items"],
        "granularity": "day",
        "horizon_steps": 14,
        "horizon_unit": "day",
        "start_at": "2026-01-01T00:00:00",
        "end_at": "2026-04-19T00:00:00",
        "lookback_points": 30,
        "filters": {"region": "east"},
    }
    assert result["status"] == "contract_only"


def test_mcp_forecast_horizon_unit_sets_matching_granularity(monkeypatch):
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def forecast(self, payload):
            recorded["payload"] = payload
            return {"success": True, "status": "contract_only"}

    monkeypatch.setattr(mcp_server.RuntimeSettings, "load", staticmethod(lambda: object()))
    monkeypatch.setattr(mcp_server, "RuntimeClient", FakeClient)

    result = mcp_server.call_tool(
        "dc_forecast",
        {
            "metric_key": "gmv_total",
            "horizon_unit": "week",
        },
    )

    assert recorded["payload"] == {
        "metric_key": "gmv_total",
        "horizon_unit": "week",
        "granularity": "week",
    }
    assert result["status"] == "contract_only"
