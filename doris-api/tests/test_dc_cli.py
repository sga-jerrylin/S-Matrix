import io
import json
from types import SimpleNamespace

from dc_runtime import cli


def test_dc_query_forwards_payload(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def query_natural(self, payload):
            recorded["payload"] = payload
            return {"success": True, "query": payload["query"], "count": 0}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(
        [
            "query",
            "--question",
            "show sales",
            "--table",
            "orders",
            "--resource-name",
            "warehouse_ai",
        ]
    )

    assert exit_code == 0
    assert recorded["payload"] == {
        "query": "show sales",
        "table_names": ["orders"],
        "resource_name": "warehouse_ai",
    }

    output = json.loads(capsys.readouterr().out)
    assert output["success"] is True
    assert output["query"] == "show sales"


def test_dc_insight_routes_to_table_endpoint(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def analyze_table(self, table_name, payload):
            recorded["table_name"] = table_name
            recorded["payload"] = payload
            return {"analysis_id": "rpt-1", "table_name": table_name}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(["insight", "--table-name", "orders", "--depth", "deep"])

    assert exit_code == 0
    assert recorded == {
        "table_name": "orders",
        "payload": {"depth": "deep"},
    }

    output = json.loads(capsys.readouterr().out)
    assert output["analysis_id"] == "rpt-1"
    assert output["table_name"] == "orders"


def test_dc_report_list_routes_to_report_contract(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def report_list(self, *, table_names=None, limit=20, offset=0):
            recorded["table_names"] = table_names
            recorded["limit"] = limit
            recorded["offset"] = offset
            return {"success": True, "count": 1, "reports": [{"id": "report-1"}]}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(["report", "list", "--table-name", "sales", "--limit", "5", "--offset", "2"])

    assert exit_code == 0
    assert recorded == {
        "table_names": "sales",
        "limit": 5,
        "offset": 2,
    }
    output = json.loads(capsys.readouterr().out)
    assert output["success"] is True
    assert output["count"] == 1


def test_dc_report_summary_routes_to_report_contract(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def report_summary(self, report_id):
            recorded["report_id"] = report_id
            return {"success": True, "id": report_id, "summary": "ok"}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(["report", "summary", "--report-id", "report-1"])

    assert exit_code == 0
    assert recorded == {"report_id": "report-1"}
    output = json.loads(capsys.readouterr().out)
    assert output["summary"] == "ok"


def test_dc_forecast_routes_payload_to_boundary(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def forecast(self, payload):
            recorded["payload"] = payload
            return {"success": True, "status": "contract_only", "forecast": {"metric_key": payload["metric_key"]}}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(
        [
            "forecast",
            "--metric-key",
            "inventory.turnover",
            "--table-name",
            "warehouse_stock_in_items",
            "--horizon-steps",
            "14",
            "--horizon-unit",
            "day",
            "--external-signal",
            "SGA-EastFactory:sentiment.index",
        ]
    )

    assert exit_code == 0
    assert recorded["payload"] == {
        "metric_key": "inventory.turnover",
        "table_names": ["warehouse_stock_in_items"],
        "granularity": "day",
        "horizon_steps": 14,
        "horizon_unit": "day",
        "external_signals": [
            {"source": "SGA-EastFactory", "signal_key": "sentiment.index"},
        ],
    }
    output = json.loads(capsys.readouterr().out)
    assert output["success"] is True
    assert output["status"] == "contract_only"


def test_dc_forecast_horizon_unit_sets_matching_granularity(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def forecast(self, payload):
            recorded["payload"] = payload
            return {"success": True, "status": "contract_only"}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(["forecast", "--metric-key", "m1", "--horizon-unit", "week"])

    assert exit_code == 0
    assert recorded["payload"]["metric_key"] == "m1"
    assert recorded["payload"]["horizon_unit"] == "week"
    assert recorded["payload"]["granularity"] == "week"
    output = json.loads(capsys.readouterr().out)
    assert output["success"] is True


def test_dc_forecast_stdin_json_forwards_full_contract_fields(monkeypatch, capsys):
    settings = SimpleNamespace()
    recorded = {}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def forecast(self, payload):
            recorded["payload"] = payload
            return {"success": True, "status": "contract_only"}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)
    monkeypatch.setattr(
        cli.sys,
        "stdin",
        io.StringIO(
            json.dumps(
                {
                    "metric_key": "gmv_total",
                    "granularity": "week",
                    "horizon_unit": "week",
                    "horizon_steps": 6,
                    "start_at": "2026-01-01T00:00:00",
                    "end_at": "2026-04-01T00:00:00",
                    "lookback_points": 40,
                    "filters": {"region": "east"},
                    "resource_name": "forecast_ai",
                    "external_signals": [{"source": "SGA-EastFactory", "signal_key": "weather.index"}],
                }
            )
        ),
    )

    exit_code = cli.main(["forecast", "--stdin"])

    assert exit_code == 0
    assert recorded["payload"] == {
        "metric_key": "gmv_total",
        "granularity": "week",
        "horizon_unit": "week",
        "horizon_steps": 6,
        "start_at": "2026-01-01T00:00:00",
        "end_at": "2026-04-01T00:00:00",
        "lookback_points": 40,
        "filters": {"region": "east"},
        "resource_name": "forecast_ai",
        "external_signals": [{"source": "SGA-EastFactory", "signal_key": "weather.index"}],
    }
    output = json.loads(capsys.readouterr().out)
    assert output["success"] is True


def test_dc_forecast_rejects_mismatched_horizon_and_granularity(monkeypatch, capsys):
    settings = SimpleNamespace()
    called = {"forecast": False}

    class FakeClient:
        def __init__(self, _settings):
            pass

        def forecast(self, payload):
            called["forecast"] = True
            return {"success": True, "status": "contract_only"}

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(
        [
            "forecast",
            "--metric-key",
            "m1",
            "--horizon-unit",
            "week",
            "--granularity",
            "month",
        ]
    )

    assert exit_code == 1
    assert called["forecast"] is False
    output = json.loads(capsys.readouterr().out)
    assert output["success"] is False
    assert output["error"]["code"] == "input_error"
    assert "horizon_unit must equal granularity" in output["error"]["message"]


def test_dc_context_remains_placeholder(monkeypatch, capsys):
    settings = SimpleNamespace()

    class FakeClient:
        def __init__(self, _settings):
            pass

    monkeypatch.setattr(cli.RuntimeSettings, "load", staticmethod(lambda: settings))
    monkeypatch.setattr(cli, "RuntimeClient", FakeClient)

    exit_code = cli.main(["context"])

    assert exit_code == 2
    output = json.loads(capsys.readouterr().out)
    assert output["success"] is False
    assert output["error"]["code"] == "capability_not_ready"
