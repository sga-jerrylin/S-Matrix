import asyncio
import json
from types import SimpleNamespace

import vanna_native_runtime.query_kernel as query_kernel
from vanna_native_runtime.memory_backend import DCAgentMemoryAdapter


class _HistoryQueryClient:
    def __init__(self, rows_by_source=None, fail_sources=None):
        self.rows_by_source = dict(rows_by_source or {})
        self.fail_sources = set(fail_sources or [])
        self.config = {"database": "doris_db"}

    def execute_query(self, sql, params=None):
        sql_text = str(sql or "")
        if "_sys_query_history" in sql_text:
            if "MATCH_ANY" in sql_text:
                source = "query_history.match_any"
            elif "LIKE" in sql_text:
                source = "query_history.like_keyword"
            else:
                source = "query_history.recent"
            if source in self.fail_sources:
                raise RuntimeError(f"{source} unavailable")
            return list(self.rows_by_source.get(source, []))

        if "FROM `_sys_table_registry`" in sql_text:
            return list(self.rows_by_source.get("doc_registry", []))

        if "FROM information_schema.COLUMNS" in sql_text:
            return list(self.rows_by_source.get("ddl_columns", []))

        return []


def test_dc_agent_memory_adapter_reads_query_history_as_native_memory():
    row = {
        "id": "history-1",
        "question": "How many orders?",
        "sql": "SELECT COUNT(*) AS order_count FROM `orders`",
        "table_names": "orders",
        "question_hash": "hash-1",
        "quality_gate": 1,
        "is_empty_result": 0,
        "row_count": 1,
        "created_at": "2026-05-04 12:00:00",
    }
    client = _HistoryQueryClient(rows_by_source={"query_history.match_any": [row]})
    adapter = DCAgentMemoryAdapter(doris_client=client)

    result = asyncio.run(
        adapter.retrieve_similar_sql_candidates(
            question="orders total count",
            context=SimpleNamespace(request_id="req-memory-1"),
            limit=3,
        )
    )

    assert result["trace"]["selected_source"] == "query_history.match_any"
    assert result["trace"]["candidate_count"] == 1
    memory = result["results"][0].memory
    assert memory.tool_name == "dc_sql_generation"
    assert memory.args["table_names"] == ["orders"]
    assert "orders" in memory.args["sql"]


def test_dc_agent_memory_adapter_builds_doc_and_ddl_text_memories():
    client = _HistoryQueryClient(
        rows_by_source={
            "doc_registry": [
                {
                    "table_name": "orders",
                    "display_name": "Orders",
                    "description": "order facts",
                    "auto_description": "",
                    "columns_info": "{\"order_id\":\"pk\"}",
                }
            ],
            "ddl_columns": [
                {"TABLE_NAME": "orders", "COLUMN_NAME": "order_id", "DATA_TYPE": "BIGINT"},
                {"TABLE_NAME": "orders", "COLUMN_NAME": "paid_amount", "DATA_TYPE": "DECIMAL(18,2)"},
            ],
        }
    )
    adapter = DCAgentMemoryAdapter(doris_client=client)
    results = asyncio.run(
        adapter.search_text_memories(
            query="orders paid amount",
            context=SimpleNamespace(request_id="req-memory-2"),
            limit=10,
            similarity_threshold=0.0,
        )
    )

    memory_ids = [str(item.memory.memory_id) for item in results]
    assert any(memory_id == "doc:orders" for memory_id in memory_ids)
    assert any(memory_id == "ddl:orders" for memory_id in memory_ids)


def test_dc_agent_memory_adapter_degrades_when_history_queries_fail():
    client = _HistoryQueryClient(
        rows_by_source={},
        fail_sources={
            "query_history.match_any",
            "query_history.like_keyword",
            "query_history.recent",
        },
    )
    adapter = DCAgentMemoryAdapter(doris_client=client)

    result = asyncio.run(
        adapter.retrieve_similar_sql_candidates(
            question="orders total count",
            context=SimpleNamespace(request_id="req-memory-3"),
            limit=3,
        )
    )

    assert result["results"] == []
    assert result["trace"]["degraded"] is True
    assert result["trace"]["candidate_count"] == 0
    assert "query_history.match_any" in result["trace"]["sources_attempted"]
    assert "query_history.like_keyword" in result["trace"]["sources_attempted"]
    assert "query_history.recent" in result["trace"]["sources_attempted"]
    assert len(result["trace"]["errors"]) >= 1


class _KernelFakeResult:
    def __init__(self, payload):
        self.success = bool(payload.get("success", True))
        self.result_for_llm = json.dumps(payload, ensure_ascii=False)
        self.metadata = dict(payload)
        self.error = ((payload.get("error") or {}).get("message") if payload.get("error") else None)


class _KernelFakeToolRegistry:
    def __init__(self):
        self.calls = []
        self.generated_sql = "SELECT COUNT(*) AS order_count FROM `orders`"
        self.rows = [{"order_count": 1}]

    async def execute(self, call, _context):
        self.calls.append((call.name, dict(call.arguments or {})))
        if call.name == "dc_catalog_retrieval":
            payload = {
                "tool_name": call.name,
                "success": True,
                "data": {
                    "table_registry": [{"table_name": "orders", "display_name": "Orders"}],
                    "trace": {"source_labels": ["table_registry"]},
                },
                "error": None,
            }
            return _KernelFakeResult(payload)

        if call.name == "dc_ddl_doc_retrieval":
            payload = {
                "tool_name": call.name,
                "success": True,
                "data": {
                    "ddl": ["CREATE TABLE `orders` (`id` BIGINT);"],
                    "documentation": ["Table: orders"],
                    "trace": {"source_labels": ["information_schema.columns", "_sys_table_registry"]},
                },
                "error": None,
            }
            return _KernelFakeResult(payload)

        if call.name == "dc_sql_generation":
            payload = {
                "tool_name": call.name,
                "success": True,
                "data": {
                    "sql": self.generated_sql,
                    "trace": {
                        "table_name": "orders",
                        "strategy": "native_tool_sql_generation",
                        "prompt_attempts": 1,
                        "metadata_available": True,
                        "schema_column_count": 3,
                        "example_count": 0,
                        "ddl_count": 1,
                        "documentation_count": 1,
                        "retrieval_source_labels": [],
                        "phases": ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"],
                        "target_only": True,
                        "referenced_tables": ["orders"],
                    },
                },
                "error": None,
            }
            return _KernelFakeResult(payload)

        if call.name == "dc_sql_validation":
            payload = {
                "tool_name": call.name,
                "success": True,
                "data": {"validation": {"is_valid": True, "errors": [], "warnings": []}},
                "error": None,
            }
            return _KernelFakeResult(payload)

        if call.name == "dc_sql_execution":
            payload = {
                "tool_name": call.name,
                "success": True,
                "data": {"rows": list(self.rows), "row_count": len(self.rows)},
                "error": None,
            }
            return _KernelFakeResult(payload)

        if call.name == "dc_sql_repair":
            payload = {
                "tool_name": call.name,
                "success": False,
                "data": {"sql": "", "trace": {}},
                "error": {"code": "sql_repair_failed", "message": "not expected in this test"},
            }
            return _KernelFakeResult(payload)

        payload = {
            "tool_name": call.name,
            "success": False,
            "data": {},
            "error": {"code": "unknown_tool", "message": call.name},
        }
        return _KernelFakeResult(payload)


class _KernelFakeAuditBridge:
    def __init__(self):
        self.events = []

    def snapshot(self):
        return list(self.events)


class _KernelFakeBackbone:
    def __init__(self, *, agent_memory, tool_registry):
        self.agent_memory = agent_memory
        self.tool_registry = tool_registry
        self.audit_bridge = _KernelFakeAuditBridge()
        self.imports = SimpleNamespace(
            User=_KernelUser,
            ToolContext=_KernelToolContext,
            ToolCall=_KernelToolCall,
        )


class _KernelUser:
    def __init__(self, id, username=None, group_memberships=None, metadata=None, email=None):
        self.id = id
        self.username = username
        self.group_memberships = list(group_memberships or [])
        self.metadata = dict(metadata or {})
        self.email = email


class _KernelToolContext:
    def __init__(self, user, conversation_id, request_id, agent_memory, metadata=None):
        self.user = user
        self.conversation_id = conversation_id
        self.request_id = request_id
        self.agent_memory = agent_memory
        self.metadata = dict(metadata or {})


class _KernelToolCall:
    def __init__(self, id, name, arguments):
        self.id = id
        self.name = name
        self.arguments = dict(arguments or {})


class _KernelFakeMemoryBackend:
    def __init__(self, scenarios):
        self.scenarios = list(scenarios)
        self.calls = []

    async def retrieve_similar_sql_candidates(self, *, question, context, limit=5):
        self.calls.append({"question": question, "request_id": getattr(context, "request_id", ""), "limit": limit})
        scenario = self.scenarios.pop(0) if self.scenarios else {"results": [], "trace": {}}
        if "raise_error" in scenario:
            raise RuntimeError(str(scenario["raise_error"]))
        return scenario


def _kernel_memory_result(*, question, sql, table_names, confidence, source):
    return _kernel_memory_results(
        [
            {
                "question": question,
                "sql": sql,
                "table_names": table_names,
                "confidence": confidence,
                "source": source,
            }
        ]
    )


def _kernel_memory_results(candidates):
    results = []
    selected_source = ""
    for candidate in list(candidates or []):
        source = str(candidate.get("source") or "")
        if source and not selected_source:
            selected_source = source
        memory = SimpleNamespace(
            question=str(candidate.get("question") or ""),
            args={
                "sql": str(candidate.get("sql") or ""),
                "table_names": list(candidate.get("table_names") or []),
            },
            metadata={"source": source},
        )
        results.append(
            SimpleNamespace(
                memory=memory,
                similarity_score=float(candidate.get("confidence", 0.0)),
            )
        )

    if not selected_source:
        selected_source = "query_history.match_any"

    return {
        "results": results,
        "trace": {
            "selected_source": selected_source,
            "sources_attempted": ["query_history.match_any"],
            "errors": [],
            "degraded": False,
            "candidate_count": len(results),
        },
    }


def _prepare_kernel(monkeypatch, *, memory_scenarios):
    query_kernel._NATIVE_RUNTIME_CACHE.clear()
    tool_registry = _KernelFakeToolRegistry()
    memory_backend = _KernelFakeMemoryBackend(memory_scenarios)

    monkeypatch.setattr(query_kernel, "DCAgentMemoryAdapter", lambda doris_client: memory_backend)
    monkeypatch.setattr(
        query_kernel,
        "build_native_runtime_backbone",
        lambda **kwargs: _KernelFakeBackbone(agent_memory=kwargs["agent_memory"], tool_registry=tool_registry),
    )
    monkeypatch.setattr(query_kernel, "register_dc_query_tools", lambda backbone: {"tool_names": []})

    class _FakePlannerAgent:
        def __init__(self, tables_context=None):
            self.tables_context = tables_context or []

        def plan(self, question):
            return {
                "intent": "count",
                "tables": ["orders"],
                "subtasks": [{"table": "orders", "question": question}],
                "needs_join": False,
                "normalized_question": question,
                "fallback_used": False,
                "routing_reason": "single table",
                "candidates": [],
            }

    class _FakeCoordinatorAgent:
        def coordinate_with_trace(self, plan, sql_map, relationships=None):
            return {
                "sql": list(sql_map.values())[0],
                "trace": {
                    "strategy": "passthrough",
                    "input_tables": list(sql_map.keys()),
                    "candidate_relationship_count": len(relationships or []),
                    "selected_relationship": None,
                },
            }

    monkeypatch.setattr(query_kernel, "PlannerAgent", _FakePlannerAgent)
    monkeypatch.setattr(query_kernel, "CoordinatorAgent", _FakeCoordinatorAgent)
    monkeypatch.setattr(
        "vanna_doris.VannaDorisOpenAI",
        type(
            "FakeHistoryVanna",
            (),
            {
                "__init__": lambda self, *args, **kwargs: None,
                "add_question_sql": lambda self, **kwargs: {"status": "stored", "id": "history-native-memory"},
            },
        ),
    )

    return tool_registry, memory_backend


def test_native_kernel_reuses_high_confidence_same_table_memory(monkeypatch):
    scenario = _kernel_memory_result(
        question="orders total count",
        sql="SELECT COUNT(*) AS order_count FROM `orders`",
        table_names=["orders"],
        confidence=0.93,
        source="query_history.match_any",
    )
    tool_registry, _memory_backend = _prepare_kernel(monkeypatch, memory_scenarios=[scenario])

    result = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="orders total count",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=SimpleNamespace(),
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-kernel-1",
            session_id="sess-kernel-1",
            user_id="u-1",
            max_repair_attempts=0,
        )
    )

    called_tools = [name for name, _args in tool_registry.calls]
    assert "dc_sql_generation" not in called_tools
    assert result.sql == "SELECT COUNT(*) AS order_count FROM `orders`"
    assert result.native_trace["memory"]["vanna_memory_hit"] is True
    assert result.native_trace["memory"]["used_as"] == "sql_reuse"
    assert result.native_trace["memory"]["candidate_count"] == 1
    assert result.native_trace["memory"]["confidence"] >= 0.9
    assert result.native_trace["memory"]["chosen_candidate"]["table_names"] == ["orders"]
    assert result.native_trace["memory"]["chosen_candidate"]["source"] == "query_history.match_any"
    assert result.native_trace["memory"]["query_intent"] == "aggregate_count"
    assert result.native_trace["memory"]["candidate_intent"] == "aggregate_count"
    assert result.native_trace["memory"]["intent_matched"] is True
    assert result.native_trace["memory"]["reuse_gate_reason"] == "reuse_allowed"
    assert len(result.native_trace["memory"]["candidate_examples"]) == 1


def test_native_kernel_rejects_cross_table_memory_without_reuse(monkeypatch):
    scenario = _kernel_memory_result(
        question="member total count",
        sql="SELECT COUNT(*) AS member_count FROM `member`",
        table_names=["member"],
        confidence=0.94,
        source="query_history.match_any",
    )
    tool_registry, _memory_backend = _prepare_kernel(monkeypatch, memory_scenarios=[scenario])

    result = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="orders total count",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=SimpleNamespace(),
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-kernel-2",
            session_id="sess-kernel-2",
            user_id="u-2",
            max_repair_attempts=0,
        )
    )

    called_tools = [name for name, _args in tool_registry.calls]
    assert "dc_sql_generation" in called_tools
    assert result.native_trace["memory"]["vanna_memory_hit"] is False
    assert result.native_trace["memory"]["used_as"] == "rejected"
    assert result.native_trace["memory"]["rejected_reason"] == "cross_table_memory_mismatch"
    assert result.native_trace["memory"]["reuse_gate_reason"] == "reuse_blocked_cross_table"
    assert result.native_trace["memory"]["chosen_candidate"] == {}
    assert len(result.native_trace["memory"]["rejected_candidates"]) >= 1
    assert result.subtask_traces[0]["memory_hit"] is False
    assert result.subtask_traces[0]["candidate_memory_hit"] is True


def test_native_kernel_prefers_same_table_candidate_over_higher_cross_table_candidate(monkeypatch):
    scenario = _kernel_memory_results(
        [
            {
                "question": "member total count",
                "sql": "SELECT COUNT(*) AS member_count FROM `member`",
                "table_names": ["member"],
                "confidence": 0.99,
                "source": "query_history.match_any",
            },
            {
                "question": "orders total count",
                "sql": "SELECT COUNT(*) AS order_count FROM `orders`",
                "table_names": ["orders"],
                "confidence": 0.88,
                "source": "query_history.match_any",
            },
        ]
    )
    tool_registry, _memory_backend = _prepare_kernel(monkeypatch, memory_scenarios=[scenario])

    result = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="orders total count",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=SimpleNamespace(),
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-kernel-2b",
            session_id="sess-kernel-2b",
            user_id="u-2b",
            max_repair_attempts=0,
        )
    )

    called_tools = [name for name, _args in tool_registry.calls]
    assert "dc_sql_generation" not in called_tools
    assert result.native_trace["memory"]["used_as"] == "sql_reuse"
    assert result.native_trace["memory"]["vanna_memory_hit"] is True
    chosen = result.native_trace["memory"]["chosen_candidate"]
    assert chosen["table_names"] == ["orders"]
    assert chosen["confidence"] == 0.88
    assert result.native_trace["memory"]["rejected_reason"] == ""
    assert result.native_trace["memory"]["reuse_gate_reason"] == "reuse_allowed"
    assert len(result.native_trace["memory"]["rejected_candidates"]) >= 1
    assert result.subtask_traces[0]["memory_hit"] is True
    assert result.subtask_traces[0]["candidate_memory_hit"] is True


def test_native_kernel_blocks_sql_reuse_when_intent_mismatch(monkeypatch):
    scenario = _kernel_memory_result(
        question="orders total count",
        sql="SELECT COUNT(*) AS order_count FROM `orders`",
        table_names=["orders"],
        confidence=0.97,
        source="query_history.match_any",
    )
    tool_registry, _memory_backend = _prepare_kernel(monkeypatch, memory_scenarios=[scenario])

    result = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="查询订单表前10条",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=SimpleNamespace(),
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-kernel-2c",
            session_id="sess-kernel-2c",
            user_id="u-2c",
            max_repair_attempts=0,
        )
    )

    called_tools = [name for name, _args in tool_registry.calls]
    assert "dc_sql_generation" in called_tools
    assert result.native_trace["memory"]["used_as"] == "prompt_context"
    assert result.native_trace["memory"]["vanna_memory_hit"] is True
    assert result.native_trace["memory"]["query_intent"] == "detail_listing"
    assert result.native_trace["memory"]["candidate_intent"] == "aggregate_count"
    assert result.native_trace["memory"]["intent_matched"] is False
    assert result.native_trace["memory"]["reuse_gate_reason"] == "reuse_blocked_intent_mismatch"


def test_native_kernel_memory_backend_error_is_degraded_but_not_500(monkeypatch):
    tool_registry, _memory_backend = _prepare_kernel(
        monkeypatch,
        memory_scenarios=[{"raise_error": "history table missing"}],
    )

    result = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="orders total count",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=SimpleNamespace(),
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-kernel-3",
            session_id="sess-kernel-3",
            user_id="u-3",
            max_repair_attempts=0,
        )
    )

    called_tools = [name for name, _args in tool_registry.calls]
    assert "dc_sql_generation" in called_tools
    assert result.sql.startswith("SELECT COUNT(*)")
    assert result.native_trace["memory"]["used_as"] == "degraded"
    assert result.native_trace["memory"]["vanna_memory_hit"] is False
    assert result.native_trace["memory"]["candidate_count"] == 0
    assert result.native_trace["memory"]["rejected_reason"] == "history table missing"
    assert result.native_trace["memory"]["reuse_gate_reason"] == "reuse_blocked_memory_backend_degraded"


def test_native_kernel_runtime_backbone_is_reused_between_requests(monkeypatch):
    scenario_a = _kernel_memory_result(
        question="orders total count",
        sql="SELECT COUNT(*) AS order_count FROM `orders`",
        table_names=["orders"],
        confidence=0.93,
        source="query_history.match_any",
    )
    scenario_b = _kernel_memory_result(
        question="orders total count",
        sql="SELECT COUNT(*) AS order_count FROM `orders`",
        table_names=["orders"],
        confidence=0.94,
        source="query_history.match_any",
    )
    _tool_registry, _memory_backend = _prepare_kernel(
        monkeypatch,
        memory_scenarios=[scenario_a, scenario_b],
    )

    shared_doris_client = SimpleNamespace()

    first = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="orders total count",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=shared_doris_client,
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-runtime-a",
            session_id="sess-runtime-a",
            user_id="u-runtime",
            max_repair_attempts=0,
        )
    )
    second = asyncio.run(
        query_kernel.run_native_query_kernel(
            query="orders total count",
            requested_tables=[],
            api_config={"api_key": "x", "model": "m", "base_url": "https://example.com"},
            doris_client=shared_doris_client,
            datasource_handler=SimpleNamespace(list_relationships_async=lambda tables: []),
            request_id="req-runtime-b",
            session_id="sess-runtime-b",
            user_id="u-runtime",
            max_repair_attempts=0,
        )
    )

    assert first.native_trace["runtime_reused"] is False
    assert second.native_trace["runtime_reused"] is True
