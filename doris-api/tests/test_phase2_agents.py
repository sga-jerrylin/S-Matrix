import sys
import types
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
import requests

from conftest import reload_main
from llm_executor import LLMExecutionError
from metadata_analyzer import MetadataAnalyzer
from vanna_doris import VannaDoris


class FakeAgentDb:
    def __init__(self):
        self.queries = []
        self.updates = []

    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        if "_sys_table_agents" in sql:
            return [
                {
                    "table_name": "institutions",
                    "agent_config": '{"fields":{"所在市":{"semantic":"geographic-city","match":"fuzzy"}}}',
                }
            ]
        if "SELECT * FROM `institutions` LIMIT 10" in sql:
            return [{"所在市": "广州市", "机构类型": "基金会", "年份": 2022}]
        return []

    def execute_update(self, sql, params=None):
        self.updates.append((sql, params))
        return 1

    def validate_identifier(self, identifier):
        return f"`{identifier}`"

    def get_table_schema(self, table_name):
        return [
            {"Field": "所在市", "Type": "VARCHAR"},
            {"Field": "机构类型", "Type": "VARCHAR"},
            {"Field": "年份", "Type": "INT"},
        ]


class FakeResourceDb(FakeAgentDb):
    def __init__(self, resource_rows=None):
        super().__init__()
        self.resource_rows = resource_rows or []

    def execute_query(self, sql, params=None):
        normalized_sql = " ".join(str(sql).split())
        if "SHOW RESOURCES" in normalized_sql:
            return list(self.resource_rows)
        return super().execute_query(sql, params)


class SamplePhase2Vanna(VannaDoris):
    def system_message(self, message: str):
        return {"role": "system", "content": message}

    def user_message(self, message: str):
        return {"role": "user", "content": message}

    def assistant_message(self, message: str):
        return {"role": "assistant", "content": message}


def test_agent_endpoint_returns_table_agent(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.metadata_analyzer.get_agent_config = lambda table_name: {
        "table_name": table_name,
        "agent_config": {"fields": {"所在市": {"semantic": "geographic-city", "match": "fuzzy"}}},
    }

    client = TestClient(main.app)
    response = client.get("/api/agents/institutions", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["agent"]["table_name"] == "institutions"


def test_feedback_endpoint_updates_quality_gate(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.update_query_feedback_async = AsyncMock(
        return_value={"success": True, "id": 1, "quality_gate": 2}
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/query/history/1/feedback",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"quality_gate": 2},
    )

    assert response.status_code == 200
    assert response.json()["quality_gate"] == 2


def test_get_related_documentation_includes_agent_config():
    vanna = SamplePhase2Vanna(doris_client=FakeAgentDb())

    docs = vanna.get_related_documentation("广州的机构")

    assert any("geographic-city" in doc for doc in docs)


def test_refresh_agent_assets_persists_agent_config_and_field_catalog(monkeypatch):
    analyzer = MetadataAnalyzer()
    analyzer.db = FakeAgentDb()
    analyzer.get_metadata = lambda table_name: {
        "table_name": table_name,
        "description": "机构表",
        "columns_info": {"所在市": "城市", "机构类型": "类型", "年份": "年份"},
    }
    analyzer._call_llm = lambda prompt: {
        "table_description": "机构表",
        "fields": {
            "所在市": {"semantic": "geographic-city", "match": "fuzzy", "values": ["广州市"]},
            "机构类型": {"semantic": "categorical", "match": "exact", "values": ["基金会"]},
            "年份": {"semantic": "temporal-year", "match": "range"},
        },
        "cot_template": "识别字段后生成 WHERE 条件",
    }

    result = analyzer.refresh_agent_assets("institutions", "excel")

    assert result["success"] is True
    assert len(analyzer.db.updates) >= 2


def test_call_llm_uses_http_request_and_parses_markdown_json(monkeypatch):
    analyzer = MetadataAnalyzer()
    analyzer.api_key = "test-key"
    analyzer.model = "deepseek-chat"
    analyzer.base_url = "https://example.test"

    fake_openai = types.SimpleNamespace(
        OpenAI=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("OpenAI SDK should not be used"))
    )
    monkeypatch.setitem(sys.modules, "openai", fake_openai)

    called = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": "```json\n{\"description\":\"机构表\",\"columns\":{\"所在市\":\"城市\"}}\n```"
                        }
                    }
                ]
            }

    def fake_post(url, headers=None, json=None, timeout=None):
        called["url"] = url
        called["headers"] = headers
        called["json"] = json
        called["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(requests, "post", fake_post)

    result = analyzer._call_llm("请分析这张表")

    assert result["description"] == "机构表"
    assert result["columns"]["所在市"] == "城市"
    assert called["url"] == "https://example.test/chat/completions"
    assert called["headers"]["Authorization"] == "Bearer test-key"
    assert called["json"]["model"] == "deepseek-chat"


def test_metadata_runtime_prefers_configured_resource_over_env_key(monkeypatch):
    analyzer = MetadataAnalyzer()
    analyzer.db = FakeResourceDb(
        resource_rows=[
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.provider_type", "Value": "openai"},
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.endpoint", "Value": "https://api.openrouter.ai/v1/chat/completions"},
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.model_name", "Value": "deepseek/deepseek-chat-v3-0324:free"},
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.api_key", "Value": "******"},
        ]
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "env-direct-key")

    runtime = analyzer._build_runtime_api_config("openrutor")

    assert runtime["success"] is True
    config = runtime["api_config"]
    assert config["resource_name"] == "openrutor"
    assert config["llm_execution_mode"] == "doris_resource"
    assert config["resource_timeout_seconds"] >= 1
    assert config["resource_query_timeout_seconds"] >= 1


def test_metadata_runtime_falls_back_to_direct_api_when_resource_key_unavailable(monkeypatch):
    analyzer = MetadataAnalyzer()
    analyzer.db = FakeResourceDb(
        resource_rows=[
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.provider_type", "Value": "openai"},
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.endpoint", "Value": "https://api.openrouter.ai/v1/chat/completions"},
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.model_name", "Value": "deepseek/deepseek-chat-v3-0324:free"},
            {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.api_key", "Value": ""},
        ]
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "env-direct-key")

    runtime = analyzer._build_runtime_api_config("openrutor")

    assert runtime["success"] is True
    config = runtime["api_config"]
    assert config["resource_name"] == "openrutor"
    assert config["llm_execution_mode"] == "direct_api"
    assert config["api_key"] == "env-direct-key"


def test_compact_analysis_prompt_limits_schema_and_sample_payload():
    analyzer = MetadataAnalyzer()
    columns = [f"col_{idx}" for idx in range(80)]
    sample_data = [{name: ("value-" + "x" * 240) for name in columns}]

    prompt = analyzer._build_compact_analysis_prompt("orders", columns, sample_data)

    assert "columns_total: 80" in prompt
    assert "... (+32 more columns)" in prompt
    assert "...(truncated)" in prompt
    assert len(prompt) <= 12080


def test_refresh_agent_assets_uses_fallback_without_second_resource_call():
    class MiniDb:
        def __init__(self):
            self.updates = []

        def validate_identifier(self, identifier):
            return f"`{identifier}`"

        def execute_query(self, sql, params=None):
            if "SELECT * FROM `orders` LIMIT 10" in sql:
                return [{"member_id": 1, "paid_amount": 12800, "created_at": "2026-05-01 00:00:00"}]
            return []

        def execute_update(self, sql, params=None):
            self.updates.append((sql, params))
            return 1

    analyzer = MetadataAnalyzer()
    analyzer.db = MiniDb()
    analyzer.get_metadata = lambda table_name: {
        "table_name": table_name,
        "description": "订单事实表",
        "columns_info": {
            "member_id": "会员ID",
            "paid_amount": "实收金额",
            "created_at": "创建时间",
        },
    }
    analyzer._call_llm_with_runtime = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("should not invoke second LLM resource call")
    )

    result = analyzer.refresh_agent_assets(
        "orders",
        "database_sync",
        runtime_api_config={"llm_execution_mode": "doris_resource", "resource_name": "openrutor"},
    )

    assert result["success"] is True
    fields = result["agent_config"]["fields"]
    assert fields["paid_amount"]["semantic"] == "financial-income"
    assert fields["created_at"]["semantic"] == "temporal-year"
    assert fields["member_id"]["semantic"] == "id"


def test_analyze_table_falls_back_to_local_semantics_on_resource_timeout():
    class FallbackDb:
        def __init__(self):
            self.updates = []

        def validate_identifier(self, identifier):
            return f"`{identifier}`"

        def get_table_schema(self, table_name):
            return [
                {"Field": "member_id", "Type": "BIGINT"},
                {"Field": "paid_amount", "Type": "DECIMAL(18,2)"},
                {"Field": "created_at", "Type": "DATETIME"},
            ]

        def execute_query(self, sql, params=None):
            normalized = " ".join(str(sql).split())
            if "SHOW RESOURCES" in normalized:
                return [
                    {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.provider_type", "Value": "openai"},
                    {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.endpoint", "Value": "https://api.openrouter.ai/v1/chat/completions"},
                    {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.model_name", "Value": "deepseek/deepseek-chat-v3-0324:free"},
                    {"Name": "openrutor", "ResourceType": "ai", "Item": "ai.api_key", "Value": "******"},
                ]
            if "SELECT * FROM `orders` LIMIT 10" in normalized:
                return [{"member_id": 101, "paid_amount": 256.8, "created_at": "2026-05-01 10:00:00"}]
            return []

        def execute_update(self, sql, params=None):
            self.updates.append((sql, params))
            return 1

    analyzer = MetadataAnalyzer()
    analyzer.db = FallbackDb()

    def fake_llm(*args, **kwargs):
        raise LLMExecutionError(
            "resource timeout",
            llm_execution_mode="doris_resource",
            resource_name="openrutor",
            error_code="resource_timeout",
        )

    analyzer._call_llm_with_runtime = fake_llm

    result = analyzer.analyze_table("orders", "manual", resource_name="openrutor")

    assert result["success"] is True
    assert result["fallback_used"] is True
    assert result["fallback_reason"] == "resource_timeout"
    assert result["llm_execution_mode"] == "doris_resource"
    assert result["resource_name"] == "openrutor"
    assert result["analysis"]["columns"]["paid_amount"].startswith("金额字段")


def test_analysis_column_coverage_fills_missing_schema_columns():
    analyzer = MetadataAnalyzer()
    merged = analyzer._ensure_analysis_column_coverage(
        "orders",
        {
            "display_name": "订单表",
            "description": "订单信息",
            "columns": {"order_id": "订单ID，整数"},
        },
        ["order_id", "paid_amount", "created_at"],
        [{"order_id": 1, "paid_amount": 99.5, "created_at": "2026-05-01 00:00:00"}],
    )

    assert merged["columns"]["order_id"] == "订单ID，整数"
    assert merged["columns"]["paid_amount"].startswith("金额字段")
    assert merged["columns"]["created_at"].startswith("时间字段")
