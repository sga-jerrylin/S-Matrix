import asyncio
import json

from vanna_native_runtime import (
    DCToolRuntimeAdapter,
    build_native_runtime_backbone,
    register_dc_query_tools,
)


class FakeDatasourceHandler:
    async def list_table_registry(self, table_names=None):
        rows = [
            {"table_name": "orders", "display_name": "Orders"},
            {"table_name": "member", "display_name": "Members"},
        ]
        if table_names:
            table_names = set(table_names)
            rows = [row for row in rows if row["table_name"] in table_names]
        return rows

    async def list_foundation_tables(self, table_names=None):
        return [{"table_name": "orders", "field_count": 12}]

    async def list_query_catalog(self):
        return [{"table_name": "orders", "fields": [{"field_name": "paid_amount"}]}]


class FakeVanna:
    def __init__(self, fail_memory=False):
        self.fail_memory = fail_memory

    def get_similar_question_sql_with_trace(self, question, **kwargs):
        if self.fail_memory:
            raise RuntimeError("memory backend unavailable")
        return {
            "examples": [{"question": question, "sql": "SELECT COUNT(*) FROM `orders`"}],
            "trace": {
                "memory_hit": True,
                "fallback_used": False,
                "selected_source": "query_history.vector",
                "source_labels": ["query_history.vector"],
                "sources_attempted": ["query_history.vector"],
                "candidate_count": 1,
                "limit": int(kwargs.get("limit", 5)),
                "errors": [],
            },
        }

    def get_related_ddl_with_trace(self, question, **kwargs):
        return {
            "items": ["CREATE TABLE `orders` (`id` BIGINT);"],
            "trace": {"count": 1, "source_labels": ["information_schema.columns"], "cache_hit": True},
        }

    def get_related_documentation_with_trace(self, question, **kwargs):
        return {
            "items": ["Table: orders\nDescription: order facts"],
            "trace": {"count": 1, "source_labels": ["_sys_table_registry"]},
        }

    # Methods consumed by local LegacyVannaAdapter fallback path
    def get_related_ddl(self, question, **kwargs):
        return ["CREATE TABLE `orders` (`id` BIGINT);"]

    def get_related_documentation(self, question, **kwargs):
        return ["Table: orders\nDescription: order facts"]

    def get_similar_question_sql(self, question, **kwargs):
        return [{"question": question, "sql": "SELECT COUNT(*) FROM `orders`"}]


class FakeTableAdminAgent:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail

    def generate_sql_for_subtask_with_trace(self, subtask, question, api_config=None):
        if self.should_fail:
            raise RuntimeError("generation failed")
        return {
            "sql": "SELECT COUNT(*) AS total FROM `orders`",
            "trace": {"strategy": "single_table_prompt", "table_name": subtask.get("table")},
        }


class FakeRepairAgent:
    def repair_sql_with_trace(self, question, failed_sql, error_message, ddl_list=None, api_config=None):
        return {
            "sql": "SELECT COUNT(*) AS total FROM `orders`",
            "trace": {"model": "fake-model", "ddl_count": len(ddl_list or [])},
        }


class FakeDorisClient:
    async def execute_query_async(self, sql, params=None):
        if "boom" in (sql or "").lower():
            raise RuntimeError("execution failed")
        return [{"total": 1}]


def _build_tool_context(backbone, groups):
    imports = backbone.imports
    return imports.ToolContext(
        user=imports.User(id="tool-user", group_memberships=list(groups)),
        conversation_id="conv-1",
        request_id="req-1",
        agent_memory=backbone.agent_memory,
    )


def test_register_dc_query_tools_enforces_access_groups():
    backbone = build_native_runtime_backbone(default_user_groups=["user"])
    adapter = DCToolRuntimeAdapter(
        datasource_handler=FakeDatasourceHandler(),
        doris_client=FakeDorisClient(),
        table_admin_factory=lambda: FakeTableAdminAgent(),
        repair_agent_factory=lambda api_config: FakeRepairAgent(),
        vanna_factory=lambda api_config: FakeVanna(),
    )
    registration = register_dc_query_tools(backbone, adapter=adapter)
    assert "dc_sql_repair" in registration["tool_names"]

    imports = backbone.imports
    user_ctx = _build_tool_context(backbone, ["user"])
    repair_call = imports.ToolCall(
        id="call-1",
        name="dc_sql_repair",
        arguments={
            "question": "query",
            "failed_sql": "SELECT 1",
            "error_message": "bad column",
            "ddl_list": [],
            "api_config": {},
        },
    )

    result = asyncio.run(backbone.tool_registry.execute(repair_call, user_ctx))
    assert result.success is False
    assert "Insufficient group access" in str(result.error)


def test_dc_catalog_tool_returns_structured_payload_and_audit():
    backbone = build_native_runtime_backbone(default_user_groups=["user"])
    adapter = DCToolRuntimeAdapter(
        datasource_handler=FakeDatasourceHandler(),
        doris_client=FakeDorisClient(),
        table_admin_factory=lambda: FakeTableAdminAgent(),
        repair_agent_factory=lambda api_config: FakeRepairAgent(),
        vanna_factory=lambda api_config: FakeVanna(),
    )
    register_dc_query_tools(backbone, adapter=adapter)

    imports = backbone.imports
    user_ctx = _build_tool_context(backbone, ["user"])
    call = imports.ToolCall(
        id="call-2",
        name="dc_catalog_retrieval",
        arguments={
            "table_names": ["orders"],
            "include_table_registry": True,
            "include_foundation_tables": True,
            "include_query_catalog": True,
        },
    )
    result = asyncio.run(backbone.tool_registry.execute(call, user_ctx))
    assert result.success is True
    payload = json.loads(result.result_for_llm)
    assert payload["tool_name"] == "dc_catalog_retrieval"
    assert payload["success"] is True
    assert payload["data"]["trace"]["table_registry_count"] == 1
    assert payload["data"]["trace"]["query_catalog_count"] == 1

    events = backbone.audit_bridge.snapshot()
    assert any(event["phase"] == "tool_invocation" for event in events)
    assert any(
        event["phase"] == "tool_result"
        and (event.get("payload") or {}).get("tool_name") == "dc_catalog_retrieval"
        for event in events
    )


def test_dc_memory_retrieval_degrades_when_backend_fails():
    backbone = build_native_runtime_backbone(default_user_groups=["user"])
    adapter = DCToolRuntimeAdapter(
        datasource_handler=FakeDatasourceHandler(),
        doris_client=FakeDorisClient(),
        table_admin_factory=lambda: FakeTableAdminAgent(),
        repair_agent_factory=lambda api_config: FakeRepairAgent(),
        vanna_factory=lambda api_config: FakeVanna(fail_memory=True),
    )
    register_dc_query_tools(backbone, adapter=adapter)

    imports = backbone.imports
    user_ctx = _build_tool_context(backbone, ["user"])
    call = imports.ToolCall(
        id="call-3",
        name="dc_memory_retrieval",
        arguments={"question": "订单总数", "limit": 3, "api_config": {}},
    )
    result = asyncio.run(backbone.tool_registry.execute(call, user_ctx))
    assert result.success is True

    payload = json.loads(result.result_for_llm)
    assert payload["success"] is True
    assert payload["data"]["examples"] == []
    assert payload["data"]["trace"]["memory_hit"] is False
    assert payload["data"]["trace"]["fallback_used"] is True
    assert payload["error"]["code"] == "memory_retrieval_failed"


def test_dc_sql_generation_and_execution_return_structured_errors():
    backbone = build_native_runtime_backbone(default_user_groups=["user"])
    adapter = DCToolRuntimeAdapter(
        datasource_handler=FakeDatasourceHandler(),
        doris_client=FakeDorisClient(),
        table_admin_factory=lambda: FakeTableAdminAgent(should_fail=True),
        repair_agent_factory=lambda api_config: FakeRepairAgent(),
        vanna_factory=lambda api_config: FakeVanna(),
    )
    register_dc_query_tools(backbone, adapter=adapter)
    imports = backbone.imports
    user_ctx = _build_tool_context(backbone, ["user"])

    gen_call = imports.ToolCall(
        id="call-4",
        name="dc_sql_generation",
        arguments={
            "question": "订单总数",
            "table_name": "orders",
            "subtask": {"table": "orders"},
            "api_config": {},
        },
    )
    gen_result = asyncio.run(backbone.tool_registry.execute(gen_call, user_ctx))
    assert gen_result.success is False
    gen_payload = json.loads(gen_result.result_for_llm)
    assert gen_payload["error"]["code"] == "sql_generation_failed"

    exec_call = imports.ToolCall(
        id="call-5",
        name="dc_sql_execution",
        arguments={"sql": "SELECT boom", "params": []},
    )
    exec_result = asyncio.run(backbone.tool_registry.execute(exec_call, user_ctx))
    assert exec_result.success is False
    exec_payload = json.loads(exec_result.result_for_llm)
    assert exec_payload["error"]["code"] == "sql_execution_failed"
