import asyncio

import pytest
from fastapi.testclient import TestClient

from conftest import reload_main


STABLE_REPORT_FIELDS = {
    "summary",
    "insights",
    "top_insights",
    "anomalies",
    "recommendations",
    "action_items",
    "insight_count",
    "anomaly_count",
}


def assert_stable_report_fields(payload):
    missing = STABLE_REPORT_FIELDS.difference(payload.keys())
    assert not missing
    assert isinstance(payload["summary"], str)
    assert isinstance(payload["insights"], list)
    assert isinstance(payload["top_insights"], list)
    assert isinstance(payload["anomalies"], list)
    assert isinstance(payload["recommendations"], list)
    assert isinstance(payload["action_items"], list)
    assert payload["insight_count"] == len(payload["insights"])
    assert payload["anomaly_count"] == len(payload["anomalies"])
    assert payload["top_insights"] == payload["insights"][: len(payload["top_insights"])]


class FakeAnalystAgent:
    def __init__(self):
        self.calls = []

    def _build_report_payload(
        self,
        *,
        report_id,
        table_name,
        trigger_type,
        depth="quick",
        history_id=None,
        note=None,
    ):
        insights = [
            {"title": "Stable volume", "detail": f"{table_name} row volume remains stable."},
            {"title": "Top contributor", "detail": "Top segment contributes expected share."},
        ]
        anomalies = []
        recommendations = [
            "Keep daily monitoring on growth and conversion.",
            "Review week-over-week variance in key metrics.",
        ]
        action_items = [
            {"title": "Action item 1", "detail": "Set weekly threshold alerts for major KPIs."},
            {"title": "Action item 2", "detail": "Validate source freshness before business review."},
        ]
        payload = {
            "success": True,
            "id": report_id,
            "table_names": table_name,
            "trigger_type": trigger_type,
            "depth": depth,
            "summary": f"Insight report for {table_name} is ready.",
            "insights": insights,
            "top_insights": insights[:1],
            "anomalies": anomalies,
            "recommendations": recommendations,
            "action_items": action_items,
            "insight_count": len(insights),
            "anomaly_count": len(anomalies),
        }
        if history_id:
            payload["history_id"] = history_id
        if note:
            payload["note"] = note
        return payload

    def analyze_table(self, table_name, depth="standard", resource_name=None):
        self.calls.append(("analyze_table", table_name, depth, resource_name))
        return self._build_report_payload(
            report_id="report-1",
            table_name=table_name,
            trigger_type="table_analysis",
            depth=depth,
        )

    def replay_from_history(self, history_id, resource_name=None):
        self.calls.append(("replay_from_history", history_id, resource_name))
        return self._build_report_payload(
            report_id="report-2",
            table_name="sales",
            trigger_type="history_replay",
            depth="quick",
            history_id=history_id,
            note="Replayed against current data.",
        )

    def list_reports(self, table_name=None, limit=20, offset=0):
        self.calls.append(("list_reports", table_name, limit, offset))
        payload = self._build_report_payload(
            report_id="report-1",
            table_name=table_name or "sales",
            trigger_type="table_analysis",
            depth="quick",
        )
        return {
            "success": True,
            "reports": [
                {
                    "id": payload["id"],
                    "table_names": payload["table_names"],
                    "trigger_type": payload["trigger_type"],
                    "depth": payload["depth"],
                    "summary": payload["summary"],
                    "insight_count": payload["insight_count"],
                    "anomaly_count": payload["anomaly_count"],
                }
            ],
            "count": 1,
            "limit": limit,
            "offset": offset,
        }

    def get_report(self, report_id, include_reasoning=False):
        self.calls.append(("get_report", report_id, include_reasoning))
        if report_id == "report-2":
            payload = self._build_report_payload(
                report_id=report_id,
                table_name="sales",
                trigger_type="history_replay",
                depth="quick",
                history_id="history-1",
                note="Replayed against current data.",
            )
        else:
            payload = self._build_report_payload(
                report_id=report_id,
                table_name="sales",
                trigger_type="table_analysis",
                depth="quick",
            )
        if include_reasoning:
            payload["reasoning_traces"] = [{"round": 1, "trace": "details"}]
        return payload

    def delete_report(self, report_id):
        self.calls.append(("delete_report", report_id))
        return {"success": True, "deleted": True, "id": report_id}

    def get_latest_report(self, table_name, include_reasoning=False):
        self.calls.append(("get_latest_report", table_name, include_reasoning))
        payload = self._build_report_payload(
            report_id="latest-1",
            table_name=table_name,
            trigger_type="table_analysis",
            depth="quick",
        )
        if include_reasoning:
            payload["reasoning_traces"] = [{"round": 1, "trace": "details"}]
        return payload

    def get_report_summary(self, report_id):
        self.calls.append(("get_report_summary", report_id))
        payload = self._build_report_payload(
            report_id=report_id,
            table_name="sales",
            trigger_type="table_analysis",
            depth="quick",
        )
        return {
            "success": True,
            "contract_version": "insight.report.summary.v1",
            "id": payload["id"],
            "table_names": payload["table_names"],
            "summary": payload["summary"],
            "insights": payload["insights"],
            "top_insights": payload["top_insights"],
            "anomalies": payload["anomalies"],
            "recommendations": payload["recommendations"],
            "action_items": payload["action_items"],
            "insight_count": payload["insight_count"],
            "anomaly_count": payload["anomaly_count"],
        }

    def get_report_with_reasoning(self, report_id, include_reasoning=False):
        self.calls.append(("get_report_with_reasoning", report_id, include_reasoning))
        payload = self.get_report(report_id, include_reasoning=include_reasoning)
        if include_reasoning:
            payload["reasoning_traces"] = [{"round": 1, "trace": "details"}]
        return payload

    def get_latest_report_with_reasoning(self, table_name, include_reasoning=False):
        self.calls.append(("get_latest_report_with_reasoning", table_name, include_reasoning))
        payload = self.get_latest_report(table_name, include_reasoning=include_reasoning)
        if include_reasoning:
            payload["reasoning_traces"] = [{"round": 1, "trace": "details"}]
        return payload

    def forecast_metric(
        self,
        metric_key,
        *,
        granularity="day",
        horizon_steps=7,
        start_at=None,
        end_at=None,
        filters=None,
        lookback_points=180,
        metric_provider=None,
    ):
        self.calls.append(
            (
                "forecast_metric",
                metric_key,
                granularity,
                horizon_steps,
                start_at,
                end_at,
                filters or {},
                lookback_points,
                metric_provider,
            )
        )
        if "invalid" in str(metric_key):
            return {
                "success": False,
                "status": "failed",
                "contract_version": "insight.forecast.result.v1",
                "forecast_id": "forecast-failed-1",
                "metric_key": metric_key,
                "horizon": {
                    "steps": horizon_steps,
                    "unit": granularity,
                    "granularity": granularity,
                    "start_at": None,
                    "end_at": None,
                    "history_window": {"start_at": start_at, "end_at": end_at},
                },
                "points": [],
                "assumptions": [],
                "backtest_summary": {
                    "status": "unavailable",
                    "holdout_points": 0,
                    "train_points": 0,
                    "mae": None,
                    "rmse": None,
                    "mape": None,
                    "residual_std": None,
                },
                "model_info": {
                    "name": "baseline_internal",
                    "version": "baseline.internal.v1",
                    "status": "failed",
                    "granularity": granularity,
                    "aggregation": None,
                    "table_name": None,
                    "time_column": None,
                    "value_column": None,
                    "training_points": 0,
                    "history_points": 0,
                },
                "error": {"code": "invalid_input", "message": "bad metric key", "details": {}},
            }
        return {
            "success": True,
            "status": "completed",
            "contract_version": "insight.forecast.result.v1",
            "forecast_id": "forecast-1",
            "metric_key": metric_key,
            "horizon": {
                "steps": horizon_steps,
                "unit": granularity,
                "granularity": granularity,
                "start_at": "2026-04-20",
                "end_at": "2026-04-26",
                "history_window": {"start_at": start_at, "end_at": end_at},
            },
            "points": [
                {
                    "ts": "2026-04-20",
                    "value": 10.0,
                    "lower": 8.5,
                    "upper": 11.5,
                    "confidence": 0.8,
                }
            ],
            "assumptions": ["Baseline model uses internal history only."],
            "backtest_summary": {
                "status": "ok",
                "holdout_points": 3,
                "train_points": 12,
                "mae": 1.2,
                "rmse": 1.5,
                "mape": 5.2,
                "residual_std": 0.7,
            },
            "model_info": {
                "name": "seasonal_naive_p7",
                "version": "baseline.internal.v1",
                "status": "ready",
                "granularity": granularity,
                "aggregation": "sum",
                "table_name": "orders",
                "time_column": "order_date",
                "value_column": "amount",
                "training_points": 15,
                "history_points": 15,
            },
        }


class FakeAnalysisScheduler:
    def __init__(self):
        self.calls = []

    def create_schedule(self, config):
        self.calls.append(("create_schedule", config))
        return {"success": True, "schedule": {"id": "schedule-1", **config}}

    def list_schedules(self):
        self.calls.append(("list_schedules",))
        return {"success": True, "count": 1, "schedules": [{"id": "schedule-1", "name": "Daily sales"}]}

    def update_schedule(self, schedule_id, config):
        self.calls.append(("update_schedule", schedule_id, config))
        return {"success": True, "schedule": {"id": schedule_id, **config}}

    def delete_schedule(self, schedule_id):
        self.calls.append(("delete_schedule", schedule_id))
        return {"success": True, "deleted": True, "id": schedule_id}

    def run_now(self, schedule_id):
        self.calls.append(("run_now", schedule_id))
        return {"success": True, "count": 2, "reports": [{"id": "report-1"}, {"id": "report-2"}]}

    def toggle_schedule(self, schedule_id):
        self.calls.append(("toggle_schedule", schedule_id))
        return {"success": True, "schedule": {"id": schedule_id, "enabled": False}}


def test_build_api_config_with_env_key(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("DEEPSEEK_API_KEY", "env-key")
    monkeypatch.setenv("DEEPSEEK_MODEL", "env-model")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://env.example.com")
    main.resolve_llm_resource_config = lambda resource_name=None: {
        "resource_name": resource_name,
        "provider": "DEEPSEEK",
        "model": "resource-model",
        "base_url": "https://resource.example.com",
        "endpoint": "https://resource.example.com/chat/completions",
    }

    config = main.build_api_config("Deepseek")

    assert config["api_key"] == "env-key"
    assert config["model"] == "resource-model"
    assert config["base_url"] == "https://resource.example.com"
    assert config["resource_name"] == "Deepseek"


def test_build_api_config_without_key_returns_none(monkeypatch):
    main = reload_main()
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    main.resolve_llm_resource_config = lambda resource_name=None: None

    config = main.build_api_config()

    assert config["api_key"] is None
    assert config["model"] == "deepseek-chat"
    assert config["base_url"] == "https://api.deepseek.com"


def test_app_uses_lifespan_context_manager():
    main = reload_main()

    assert main.app.router.lifespan_context is main.lifespan


def test_llm_config_request_uses_configdict_for_protected_namespaces():
    main = reload_main()

    assert main.LLMConfigRequest.model_config["protected_namespaces"] == ()


def test_analysis_table_endpoint_round_trip(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/table/sales",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "quick", "resource_name": "Deepseek"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert_stable_report_fields(payload)
    assert main.analyst_agent.calls[0] == ("analyze_table", "sales", "quick", "Deepseek")


def test_analysis_replay_endpoint_documents_current_data(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/replay/history-1",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"resource_name": "Deepseek"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert_stable_report_fields(payload)
    assert payload["history_id"] == "history-1"
    assert "current data" in payload["note"].lower()


def test_analysis_reports_list_supports_filters(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports?table_names=sales&limit=10&offset=5",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("list_reports", "sales", 10, 5)
    payload = response.json()
    assert payload["count"] == 1
    report = payload["reports"][0]
    assert {"summary", "insight_count", "anomaly_count"}.issubset(report.keys())


def test_analysis_report_detail_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/report-1",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_report", "report-1", False)
    payload = response.json()
    assert payload["id"] == "report-1"
    assert_stable_report_fields(payload)


def test_analysis_report_detail_include_reasoning(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    main.analyst_agent = FakeAnalystAgent()
    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/report-1?include_reasoning=true",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_report", "report-1", True)
    payload = response.json()
    assert payload["reasoning_traces"][0]["trace"] == "details"
    assert_stable_report_fields(payload)


def test_analysis_report_delete_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.delete(
        "/api/analysis/reports/report-1",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("delete_report", "report-1")
    assert response.json()["deleted"] is True


def test_analysis_latest_report_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/latest/sales",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_latest_report", "sales", False)
    payload = response.json()
    assert payload["id"] == "latest-1"
    assert_stable_report_fields(payload)


def test_analysis_latest_report_include_reasoning(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    main.analyst_agent = FakeAnalystAgent()
    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/latest/sales?include_reasoning=true",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_latest_report", "sales", True)
    payload = response.json()
    assert payload["reasoning_traces"][0]["trace"] == "details"
    assert_stable_report_fields(payload)


def test_analysis_report_summary_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/report-1/summary",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_report_summary", "report-1")
    payload = response.json()
    assert payload["contract_version"] == "insight.report.summary.v1"
    assert_stable_report_fields(payload)


def test_analysis_contract_endpoint_exposes_boundaries(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    client = TestClient(main.app)

    response = client.get(
        "/api/analysis/contracts",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["module"] == "insight"
    assert payload["contracts"]["contract_version"] == "insight.report.read.v1"
    assert payload["forecast_boundary"]["contract_version"] == "insight.forecast.boundary.v1"
    assert payload["collaboration_boundary"]["contract_version"] == "insight.collaboration.boundary.v1"


def test_analysis_forecast_endpoint_returns_mvp_payload(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()
    client = TestClient(main.app)

    response = client.post(
        "/api/analysis/forecast",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "metric_key": "gmv_total",
            "horizon_steps": 14,
            "granularity": "day",
            "start_at": "2026-01-01T00:00:00",
            "end_at": "2026-04-19T00:00:00",
            "filters": {"region": "east"},
            "external_signals": [{"source": "SGA-EastFactory", "signal_key": "sentiment.index"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    recorded_call = main.analyst_agent.calls[0]
    assert recorded_call[:8] == (
        "forecast_metric",
        "gmv_total",
        "day",
        14,
        "2026-01-01T00:00:00",
        "2026-04-19T00:00:00",
        {"region": "east"},
        180,
    )
    assert recorded_call[8] is main.datasource_handler
    assert payload["success"] is True
    assert payload["status"] == "completed"
    assert payload["contract"]["contract_version"] == "insight.forecast.boundary.v1"
    assert payload["contract_version"] == "insight.forecast.result.v1"
    assert payload["metric_key"] == "gmv_total"
    assert payload["horizon"]["steps"] == 14
    assert payload["horizon"]["unit"] == "day"
    assert payload["points"][0]["confidence"] == 0.8
    assert payload["backtest_summary"]["status"] == "ok"
    assert payload["model_info"]["status"] == "ready"


def test_analysis_forecast_endpoint_returns_stable_failure_payload(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()
    client = TestClient(main.app)

    response = client.post(
        "/api/analysis/forecast",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "metric_key": "invalid.metric",
            "horizon_steps": 7,
            "granularity": "day",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is False
    assert payload["status"] == "failed"
    assert payload["contract_version"] == "insight.forecast.result.v1"
    assert payload["points"] == []
    assert payload["error"]["code"] == "invalid_input"
    assert payload["backtest_summary"]["status"] == "unavailable"
    assert payload["model_info"]["status"] == "failed"


def test_analysis_collaboration_contract_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    client = TestClient(main.app)

    response = client.get(
        "/api/analysis/collaboration/contracts",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["module"] == "insight"
    boundaries = payload["collaboration_boundary"]["boundaries"]
    assert set(boundaries.keys()) == {"SGA-Web", "SGA-EastFactory", "Evole"}


def test_analysis_contract_consistent_across_replay_list_detail_and_latest(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    class ContractConsistentAgent:
        def __init__(self):
            self.calls = []
            self._report = {
                "success": True,
                "id": "report-replay-1",
                "table_names": "sales",
                "trigger_type": "history_replay",
                "depth": "quick",
                "history_id": "history-1",
                "summary": "Replay contract report",
                "insights": [{"title": "Replay insight", "detail": "Replay compares against current data."}],
                "top_insights": [{"title": "Replay insight", "detail": "Replay compares against current data."}],
                "anomalies": [],
                "recommendations": ["Track drift for replayed metric."],
                "action_items": [{"title": "Action item 1", "detail": "Pin replay baseline weekly."}],
                "insight_count": 1,
                "anomaly_count": 0,
                "note": "Replayed against current data.",
            }

        def replay_from_history(self, history_id, resource_name=None):
            self.calls.append(("replay_from_history", history_id, resource_name))
            payload = dict(self._report)
            payload["history_id"] = history_id
            return payload

        def list_reports(self, table_name=None, limit=20, offset=0):
            self.calls.append(("list_reports", table_name, limit, offset))
            return {
                "success": True,
                "reports": [
                    {
                        "id": self._report["id"],
                        "table_names": self._report["table_names"],
                        "trigger_type": self._report["trigger_type"],
                        "depth": self._report["depth"],
                        "summary": self._report["summary"],
                        "insight_count": self._report["insight_count"],
                        "anomaly_count": self._report["anomaly_count"],
                    }
                ],
                "count": 1,
                "limit": limit,
                "offset": offset,
            }

        def get_report(self, report_id, include_reasoning=False):
            self.calls.append(("get_report", report_id, include_reasoning))
            payload = dict(self._report)
            payload["id"] = report_id
            return payload

        def get_latest_report(self, table_name, include_reasoning=False):
            self.calls.append(("get_latest_report", table_name, include_reasoning))
            payload = dict(self._report)
            payload["table_names"] = table_name
            return payload

    main.analyst_agent = ContractConsistentAgent()
    client = TestClient(main.app)

    replay_payload = client.post(
        "/api/analysis/replay/history-1",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"resource_name": "Deepseek"},
    ).json()
    list_payload = client.get(
        "/api/analysis/reports?table_names=sales",
        headers={"X-API-Key": "secret-key"},
    ).json()
    detail_payload = client.get(
        f"/api/analysis/reports/{replay_payload['id']}",
        headers={"X-API-Key": "secret-key"},
    ).json()
    latest_payload = client.get(
        "/api/analysis/reports/latest/sales",
        headers={"X-API-Key": "secret-key"},
    ).json()

    assert_stable_report_fields(replay_payload)
    assert_stable_report_fields(detail_payload)
    assert_stable_report_fields(latest_payload)
    for field in STABLE_REPORT_FIELDS:
        assert replay_payload[field] == detail_payload[field] == latest_payload[field]

    list_row = list_payload["reports"][0]
    assert list_row["id"] == replay_payload["id"]
    assert list_row["summary"] == replay_payload["summary"]
    assert list_row["insight_count"] == replay_payload["insight_count"]
    assert list_row["anomaly_count"] == replay_payload["anomaly_count"]


def test_analysis_schedule_create_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/schedules",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "name": "Daily sales",
            "tables": ["sales", "orders"],
            "depth": "deep",
            "resource_name": "Deepseek",
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
            "delivery": {"channels": [{"type": "webhook", "webhook_url": "***configured***"}]},
        },
    )

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0][0] == "create_schedule"
    assert response.json()["schedule"]["tables"] == ["sales", "orders"]


def test_analysis_schedule_list_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.get("/api/analysis/schedules", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("list_schedules",)
    assert response.json()["count"] == 1


def test_analysis_schedule_update_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.put(
        "/api/analysis/schedules/schedule-1",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"name": "Updated sales", "tables": ["sales"], "schedule_type": "weekly", "schedule_day_of_week": 1},
    )

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0][0] == "update_schedule"
    assert response.json()["schedule"]["name"] == "Updated sales"


def test_analysis_schedule_update_endpoint_accepts_expert_depth(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.put(
        "/api/analysis/schedules/schedule-1",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "expert"},
    )

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("update_schedule", "schedule-1", {"depth": "expert"})


def test_analysis_schedule_delete_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.delete("/api/analysis/schedules/schedule-1", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("delete_schedule", "schedule-1")
    assert response.json()["deleted"] is True


def test_analysis_schedule_run_now_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.post("/api/analysis/schedules/schedule-1/run", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("run_now", "schedule-1")
    assert response.json()["count"] == 2


def test_analysis_schedule_toggle_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.post("/api/analysis/schedules/schedule-1/toggle", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("toggle_schedule", "schedule-1")
    assert response.json()["schedule"]["enabled"] is False


def test_analysis_requires_configured_api_key():
    main = reload_main()
    client = TestClient(main.app)

    response = client.get("/api/analysis/reports")

    assert response.status_code == 503


def test_analysis_requires_auth_header(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get("/api/analysis/reports")

    assert response.status_code == 401


def test_analysis_endpoint_returns_503_when_agent_not_ready(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = None

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/table/sales",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "quick"},
    )

    assert response.status_code == 503


def test_analysis_invalid_identifier_returns_400(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    class InvalidIdentifierAgent(FakeAnalystAgent):
        def analyze_table(self, table_name, depth="standard", resource_name=None):
            raise ValueError(f"Invalid identifier: {table_name}")

    main.analyst_agent = InvalidIdentifierAgent()
    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/table/bad!name",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "quick"},
    )

    assert response.status_code == 400


def test_analysis_schedule_requires_ready_scheduler(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = None

    client = TestClient(main.app)
    response = client.get("/api/analysis/schedules", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 503


def test_analysis_websocket_rejects_without_key(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    from analysis_dispatcher import AnalysisDispatcher

    main.analysis_dispatcher = AnalysisDispatcher()
    client = TestClient(main.app)

    with client.websocket_connect("/ws/analysis") as websocket:
        with pytest.raises(Exception):
            websocket.receive_text()


def test_analysis_websocket_rejects_when_gateway_key_is_not_configured(monkeypatch):
    main = reload_main()
    monkeypatch.delenv("SMATRIX_API_KEY", raising=False)
    from analysis_dispatcher import AnalysisDispatcher

    main.analysis_dispatcher = AnalysisDispatcher()
    client = TestClient(main.app)

    with client.websocket_connect("/ws/analysis?api_key=secret-key") as websocket:
        with pytest.raises(Exception):
            websocket.receive_text()


def test_analysis_websocket_push_reaches_client(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    from analysis_dispatcher import AnalysisDispatcher

    main.analysis_dispatcher = AnalysisDispatcher()
    client = TestClient(main.app)

    with client.websocket_connect("/ws/analysis?api_key=secret-key") as websocket:
        asyncio.run(main.analysis_dispatcher._push_ws({"id": "report-1", "summary": "ready"}))
        payload = websocket.receive_json()

    assert payload["id"] == "report-1"
