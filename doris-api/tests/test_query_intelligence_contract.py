import json
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main


def test_natural_query_returns_fixed_schema_and_trace(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[{"table_name": "institutions"}])
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])

    main.PlannerAgent = type(
        "FakePlannerAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "plan": lambda self, question: {
                "intent": "count",
                "tables": ["institutions"],
                "subtasks": [{"table": "institutions", "question": question}],
                "needs_join": False,
                "normalized_question": question,
                "fallback_used": False,
                "routing_reason": "selected single table",
                "candidates": [
                    {
                        "table_name": "institutions",
                        "score": 8,
                        "matched_terms": ["count"],
                        "selected": True,
                        "rank": 1,
                    }
                ],
            },
        },
    )
    main.TableAdminAgent = type(
        "FakeTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask_with_trace": lambda self, subtask, question, api_config=None: {
                "sql": "SELECT COUNT(*) AS total FROM `institutions`",
                "trace": {
                    "table_name": "institutions",
                    "question": question,
                    "strategy": "single_table_prompt",
                    "prompt_attempts": 1,
                    "metadata_available": True,
                    "schema_column_count": 3,
                    "example_count": 1,
                    "ddl_count": 2,
                    "documentation_count": 1,
                    "retrieval_source_labels": [
                        "query_history.vector",
                        "information_schema.columns",
                        "_sys_table_registry",
                    ],
                    "memory_hit": True,
                    "candidate_memory_hit": True,
                    "memory_fallback_used": False,
                    "memory_source": "query_history.vector",
                    "phases": ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"],
                    "target_only": True,
                    "referenced_tables": ["institutions"],
                },
            },
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "coordinate_with_trace": lambda self, plan, sql_map, relationships=None: {
                "sql": "SELECT COUNT(*) AS total FROM `institutions`",
                "trace": {
                    "strategy": "passthrough",
                    "input_tables": ["institutions"],
                    "candidate_relationship_count": 0,
                    "selected_relationship": None,
                },
            },
        },
    )
    main.VannaDorisOpenAI = type(
        "FakeHistoryVanna",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "add_question_sql": lambda self, *args, **kwargs: {"status": "stored", "id": "history-1"},
        },
    )
    main.RepairAgent = type("FakeRepairAgent", (), {"__init__": lambda self, *args, **kwargs: None})
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 42}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "query": "How many institutions are there?",
            "kernel": "legacy",
            "request_id": "req-123",
            "session_id": "sess-123",
            "context": {
                "user_id": "user-a",
                "source": "api",
                "labels": ["contract-test"],
                "attributes": {"tenant": "acme"},
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "nlq.v1"
    assert payload["intent"] == "count"
    assert payload["table_names"] == ["institutions"]
    assert payload["history_id"] == "history-1"
    assert payload["warnings"] == []
    assert payload["trace"]["planner"]["selected_tables"] == ["institutions"]
    assert payload["trace"]["subtasks"][0]["strategy"] == "single_table_prompt"
    assert payload["trace"]["subtasks"][0]["ddl_count"] == 2
    assert payload["trace"]["subtasks"][0]["documentation_count"] == 1
    assert payload["trace"]["subtasks"][0]["memory_hit"] is True
    assert payload["trace"]["subtasks"][0]["memory_source"] == "query_history.vector"
    assert payload["trace"]["orchestration"]["strategy"] == "passthrough"
    assert payload["trace"]["repair"]["attempted"] is False
    assert payload["trace"]["execution"]["history_id"] == "history-1"
    assert payload["trace"]["execution"]["llm_execution_mode"] == "direct_api"
    assert payload["trace"]["retrieval"]["example_count"] == 1
    assert payload["trace"]["retrieval"]["ddl_count"] == 2
    assert payload["trace"]["retrieval"]["documentation_count"] == 1
    assert payload["trace"]["retrieval"]["memory_hit"] is True
    assert "query_history.vector" in payload["trace"]["retrieval"]["source_labels"]
    assert payload["trace"]["context"]["request_id"] == "req-123"
    assert payload["trace"]["context"]["session_id"] == "sess-123"
    assert payload["trace"]["context"]["user_id"] == "user-a"
    assert any(item["phase"] == "llm_resolution" for item in payload["trace"]["phases"])
    assert any(item["phase"] == "planner" for item in payload["trace"]["phases"])
    assert any(item["phase"] == "memory_retrieval" for item in payload["trace"]["phases"])


def test_natural_query_accepts_legacy_question_and_table_name_fields(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    main.datasource_handler.list_table_registry = AsyncMock(
        return_value=[
            {"table_name": "institutions"},
            {"table_name": "activities"},
        ]
    )
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    seen = {}

    class FakePlannerAgent:
        def __init__(self, tables_context=None):
            seen["tables_context"] = tables_context or []

        def plan(self, question):
            seen["query"] = question
            return {
                "intent": "count",
                "tables": ["institutions"],
                "subtasks": [{"table": "institutions", "question": question}],
                "needs_join": False,
                "normalized_question": question,
                "fallback_used": False,
                "routing_reason": "selected single table",
                "candidates": [],
            }

    main.PlannerAgent = FakePlannerAgent
    main.TableAdminAgent = type(
        "FakeTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask": lambda self, subtask, question, api_config=None: "SELECT COUNT(*) AS total FROM `institutions`",
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "coordinate": lambda self, plan, sql_map, relationships=None: "SELECT COUNT(*) AS total FROM `institutions`",
        },
    )
    main.VannaDorisOpenAI = type(
        "FakeHistoryVanna",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "add_question_sql": lambda self, *args, **kwargs: {"status": "stored", "id": "history-legacy"},
        },
    )
    main.RepairAgent = type("FakeRepairAgent", (), {"__init__": lambda self, *args, **kwargs: None})
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 42}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"question": "How many institutions are there?", "table_name": "institutions"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["table_names"] == ["institutions"]
    assert payload["trace"]["planner"]["requested_tables"] == ["institutions"]
    assert payload["trace"]["context"]["request_id"]
    assert payload["trace"]["context"]["session_id"].startswith("session-")
    assert payload["trace"]["retrieval"]["example_count"] == 0
    assert payload["trace"]["retrieval"]["ddl_count"] == 0
    assert payload["trace"]["retrieval"]["documentation_count"] == 0
    assert payload["trace"]["retrieval"]["memory_hit"] is False
    assert payload["trace"]["execution"]["llm_execution_mode"] == "direct_api"
    assert any(item["phase"] == "validation_repair" for item in payload["trace"]["phases"])
    assert seen["query"] == "How many institutions are there?"
    assert [row["table_name"] for row in seen["tables_context"]] == ["institutions"]


def test_natural_query_resource_only_uses_doris_resource_mode(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    main.resolve_llm_resource_config = lambda resource_name=None: {
        "resource_name": resource_name,
        "provider": "DEEPSEEK",
        "model": "deepseek-v4-flash",
        "base_url": "https://api.deepseek.com",
        "endpoint": "https://api.deepseek.com/chat/completions",
        "api_key_configured": True,
    }
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[{"table_name": "orders"}])
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    captured = {}

    main.PlannerAgent = type(
        "FakePlannerAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "plan": lambda self, question: {
                "intent": "count",
                "tables": ["orders"],
                "subtasks": [{"table": "orders", "question": question}],
                "needs_join": False,
                "normalized_question": question,
                "fallback_used": False,
                "routing_reason": "single table",
                "candidates": [],
            },
        },
    )
    main.TableAdminAgent = type(
        "FakeTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask_with_trace": lambda self, subtask, question, api_config=None: (
                captured.setdefault("api_config", dict(api_config or {})),
                {
                    "sql": "SELECT COUNT(*) AS total FROM `orders`",
                    "trace": {
                        "table_name": "orders",
                        "question": question,
                        "strategy": "single_table_prompt",
                        "prompt_attempts": 1,
                        "metadata_available": True,
                        "schema_column_count": 3,
                        "example_count": 0,
                        "ddl_count": 0,
                        "documentation_count": 0,
                        "retrieval_source_labels": [],
                        "candidate_retrieval_source_labels": [],
                        "memory_hit": False,
                        "candidate_memory_hit": False,
                        "memory_fallback_used": False,
                        "memory_source": "",
                        "phases": ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"],
                        "target_only": True,
                        "referenced_tables": ["orders"],
                    },
                },
            )[1],
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "coordinate_with_trace": lambda self, plan, sql_map, relationships=None: {
                "sql": "SELECT COUNT(*) AS total FROM `orders`",
                "trace": {
                    "strategy": "passthrough",
                    "input_tables": ["orders"],
                    "candidate_relationship_count": 0,
                    "selected_relationship": None,
                },
            },
        },
    )
    main.VannaDorisOpenAI = type(
        "FakeHistoryVanna",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "add_question_sql": lambda self, *args, **kwargs: {"status": "stored", "id": "history-r1"}},
    )
    main.RepairAgent = type("FakeRepairAgent", (), {"__init__": lambda self, *args, **kwargs: None})
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 10}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询订单总数", "resource_name": "ds", "kernel": "legacy"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert captured["api_config"]["llm_execution_mode"] == "doris_resource"
    assert captured["api_config"]["resource_name"] == "ds"
    assert captured["api_config"]["api_key"] is None
    assert payload["trace"]["execution"]["llm_execution_mode"] == "doris_resource"
    assert payload["trace"]["execution"]["resource_name"] == "ds"


def test_natural_query_returns_structured_error_when_resource_missing(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    main.resolve_llm_resource_config = lambda resource_name=None: None

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询会员总数", "resource_name": "ds", "kernel": "legacy"},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["success"] is False
    assert detail["error_code"] == "llm_resource_not_found"
    assert detail["llm_execution_mode"] == "direct_api"
    assert detail["resource_name"] == "ds"


def test_natural_query_kernel_native_success_returns_native_trace(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")

    class FakeNativeResult:
        intent = "count"
        plan = {
            "intent": "count",
            "tables": ["orders"],
            "needs_join": False,
            "normalized_question": "查询订单总数",
            "fallback_used": False,
            "routing_reason": "native",
            "candidates": [
                {
                    "table_name": "orders",
                    "score": 10,
                    "matched_terms": ["订单"],
                    "selected": True,
                    "rank": 1,
                }
            ],
        }
        subtask_traces = [
            {
                "table_name": "orders",
                "question": "查询订单总数",
                "strategy": "native_tool_sql_generation",
                "sql": "SELECT COUNT(*) AS total FROM `orders`",
                "prompt_attempts": 1,
                "metadata_available": True,
                "schema_column_count": 5,
                "example_count": 0,
                "ddl_count": 0,
                "documentation_count": 0,
                "retrieval_source_labels": [],
                "candidate_retrieval_source_labels": [],
                "memory_hit": False,
                "candidate_memory_hit": False,
                "memory_fallback_used": False,
                "memory_source": "",
                "phases": ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"],
                "target_only": True,
                "referenced_tables": ["orders"],
            }
        ]
        retrieval_summary = {
            "example_count": 0,
            "ddl_count": 0,
            "documentation_count": 0,
            "source_labels": [],
            "memory_hit": False,
            "memory_fallback_used": False,
        }
        orchestration_trace = {
            "strategy": "passthrough",
            "input_tables": ["orders"],
            "candidate_relationship_count": 0,
            "selected_relationship": None,
        }
        repair_trace = {"attempted": False, "max_attempts": 2, "attempts": []}
        sql = "SELECT COUNT(*) AS total FROM `orders`"
        data = [{"total": 12}]
        history_id = "native-history-1"
        history_status = "stored"
        warnings = []
        phase_traces = [
            {
                "phase": "planner",
                "status": "ok",
                "details": {"intent": "count", "selected_table_count": 1, "needs_join": False},
                "source_labels": ["planner.tables_context"],
            },
            {
                "phase": "execution",
                "status": "ok",
                "details": {"row_count": 1, "history_status": "stored"},
                "source_labels": ["dc_sql_execution"],
            },
        ]
        native_trace = {
            "kernel": "native",
            "runtime_reused": True,
            "runtime_cache_key": "doris_client:1|groups:admin,user",
            "tools_called": [{"tool_name": "dc_sql_generation", "success": True, "error": {}}],
            "memory": {
                "example_count": 1,
                "memory_hit": True,
                "sources_attempted": ["query_history.match_any"],
                "vanna_memory_hit": True,
                "vanna_memory_source": "query_history.match_any",
                "confidence": 0.91,
                "used_as": "sql_reuse",
                "rejected_reason": "",
                "candidate_count": 1,
                "candidate_examples": [
                    {
                        "question": "query order count",
                        "table_names": ["orders"],
                        "confidence": 0.91,
                        "source": "query_history.match_any",
                        "sql": "SELECT COUNT(*) AS order_count FROM `orders`",
                    }
                ],
                "chosen_candidate": {
                    "question": "query order count",
                    "table_names": ["orders"],
                    "confidence": 0.91,
                    "source": "query_history.match_any",
                    "sql": "SELECT COUNT(*) AS order_count FROM `orders`",
                },
                "chosen_candidates": [
                    {
                        "table_name": "orders",
                        "question": "query order count",
                        "table_names": ["orders"],
                        "confidence": 0.91,
                        "source": "query_history.match_any",
                        "sql": "SELECT COUNT(*) AS order_count FROM `orders`",
                        "used_as": "sql_reuse",
                    }
                ],
                "rejected_candidates": [],
                "query_intent": "aggregate_count",
                "candidate_intent": "aggregate_count",
                "intent_matched": True,
                "reuse_gate_reason": "reuse_allowed",
                "subtasks": [],
            },
            "audit_events": [{"phase": "tool_result", "payload": {"tool_name": "dc_sql_generation"}}],
            "fallback_reason": "",
        }

        @property
        def table_names(self):
            return ["orders"]

    async def fake_native_runner(**kwargs):
        return FakeNativeResult()

    main.run_native_query_kernel = fake_native_runner

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询订单总数", "kernel": "native"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "nlq.v1"
    assert payload["table_names"] == ["orders"]
    assert payload["trace"]["native"]["kernel"] == "native"
    assert payload["trace"]["native"]["runtime_reused"] is True
    assert payload["trace"]["native"]["fallback_reason"] == ""
    assert payload["trace"]["native"]["tools_called"][0]["tool_name"] == "dc_sql_generation"
    assert payload["trace"]["native"]["memory"]["vanna_memory_hit"] is True
    assert payload["trace"]["native"]["memory"]["vanna_memory_source"] == "query_history.match_any"
    assert payload["trace"]["native"]["memory"]["used_as"] == "sql_reuse"
    assert payload["trace"]["native"]["memory"]["reuse_gate_reason"] == "reuse_allowed"
    assert payload["trace"]["native"]["memory"]["candidate_count"] == 1
    assert payload["trace"]["native"]["memory"]["chosen_candidate"]["table_names"] == ["orders"]
    assert len(payload["trace"]["native"]["memory"]["candidate_examples"]) == 1


def test_natural_query_kernel_auto_fallback_to_legacy(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")

    async def fake_native_runner(**kwargs):
        raise main.NativeKernelExecutionError(
            "native_probe_failed",
            "native failed",
            details={"tool_name": "dc_sql_generation"},
        )

    main.run_native_query_kernel = fake_native_runner
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[{"table_name": "orders"}])
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    main.PlannerAgent = type(
        "FallbackPlannerAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "plan": lambda self, question: {
                "intent": "count",
                "tables": ["orders"],
                "subtasks": [{"table": "orders", "question": question}],
                "needs_join": False,
                "normalized_question": question,
                "fallback_used": False,
                "routing_reason": "single table",
                "candidates": [],
            },
        },
    )
    main.TableAdminAgent = type(
        "FallbackTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask_with_trace": lambda self, subtask, question, api_config=None: {
                "sql": "SELECT COUNT(*) AS total FROM `orders`",
                "trace": {
                    "table_name": "orders",
                    "question": question,
                    "strategy": "single_table_prompt",
                    "prompt_attempts": 1,
                    "metadata_available": True,
                    "schema_column_count": 3,
                    "example_count": 0,
                    "ddl_count": 0,
                    "documentation_count": 0,
                    "retrieval_source_labels": [],
                    "candidate_retrieval_source_labels": [],
                    "memory_hit": False,
                    "candidate_memory_hit": False,
                    "memory_fallback_used": False,
                    "memory_source": "",
                    "phases": ["sql_generation"],
                    "target_only": True,
                    "referenced_tables": ["orders"],
                },
            },
        },
    )
    main.CoordinatorAgent = type(
        "FallbackCoordinatorAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "coordinate_with_trace": lambda self, plan, sql_map, relationships=None: {
                "sql": "SELECT COUNT(*) AS total FROM `orders`",
                "trace": {
                    "strategy": "passthrough",
                    "input_tables": ["orders"],
                    "candidate_relationship_count": 0,
                    "selected_relationship": None,
                },
            },
        },
    )
    main.VannaDorisOpenAI = type(
        "FallbackHistoryVanna",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "add_question_sql": lambda self, *args, **kwargs: {"status": "stored", "id": "history-fallback"}},
    )
    main.RepairAgent = type("FallbackRepairAgent", (), {"__init__": lambda self, *args, **kwargs: None})
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 7}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询订单总数", "kernel": "auto"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["trace"]["native"]["kernel"] == "legacy"
    assert "native_probe_failed" in payload["trace"]["native"]["fallback_reason"]
    assert payload["trace"]["native"]["tools_called"][0]["tool_name"] == "dc_sql_generation"
    assert payload["trace"]["native"]["memory"]["used_as"] == "degraded"
    assert payload["trace"]["native"]["memory"]["reuse_gate_reason"] == "reuse_blocked_native_fallback"
    assert payload["trace"]["native"]["memory"]["candidate_count"] == 0
    assert any("native kernel fallback to legacy" in warning for warning in payload["warnings"])


def test_natural_query_uses_env_default_kernel_when_request_kernel_missing(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setenv("DC_NLQ_DEFAULT_KERNEL", "auto")

    class FakeNativeResult:
        intent = "count"
        plan = {"tables": ["orders"], "intent": "count", "needs_join": False, "fallback_used": False}
        subtask_traces = []
        retrieval_summary = {
            "example_count": 0,
            "ddl_count": 0,
            "documentation_count": 0,
            "source_labels": [],
            "memory_hit": False,
            "memory_fallback_used": False,
        }
        orchestration_trace = {"strategy": "passthrough", "input_tables": ["orders"], "candidate_relationship_count": 0}
        repair_trace = {"attempted": False, "max_attempts": 2, "attempts": []}
        sql = "SELECT COUNT(*) AS total FROM `orders`"
        data = [{"total": 1}]
        history_id = "native-history-default-kernel"
        history_status = "stored"
        warnings = []
        phase_traces = []
        native_trace = {
            "kernel": "native",
            "runtime_reused": True,
            "runtime_cache_key": "cache-key",
            "tools_called": [{"tool_name": "dc_sql_generation", "success": True, "error": {}}],
            "memory": {
                "example_count": 0,
                "memory_hit": False,
                "sources_attempted": [],
                "vanna_memory_hit": False,
                "vanna_memory_source": "",
                "confidence": 0.0,
                "used_as": "none",
                "rejected_reason": "",
                "candidate_count": 0,
                "candidate_examples": [],
                "chosen_candidate": {},
                "chosen_candidates": [],
                "rejected_candidates": [],
                "query_intent": "aggregate_count",
                "candidate_intent": "unknown",
                "intent_matched": False,
                "reuse_gate_reason": "reuse_blocked_no_candidate",
                "subtasks": [],
            },
            "audit_events": [],
            "fallback_reason": "",
        }

        @property
        def table_names(self):
            return ["orders"]

    async def fake_native_runner(**kwargs):
        return FakeNativeResult()

    main.run_native_query_kernel = fake_native_runner

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询订单总数"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["trace"]["native"]["kernel"] == "native"
    llm_phase = next(item for item in payload["trace"]["phases"] if item["phase"] == "llm_resolution")
    assert llm_phase["details"]["kernel_default"] == "auto"
    assert llm_phase["details"]["kernel_effective"] == "auto"
    assert llm_phase["details"]["kernel_requested"] == "(default)"


def test_natural_query_default_kernel_is_auto_when_env_missing(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.delenv("DC_NLQ_DEFAULT_KERNEL", raising=False)

    class FakeNativeResult:
        intent = "count"
        plan = {"tables": ["orders"], "intent": "count", "needs_join": False, "fallback_used": False}
        subtask_traces = []
        retrieval_summary = {
            "example_count": 0,
            "ddl_count": 0,
            "documentation_count": 0,
            "source_labels": [],
            "memory_hit": False,
            "memory_fallback_used": False,
        }
        orchestration_trace = {"strategy": "passthrough", "input_tables": ["orders"], "candidate_relationship_count": 0}
        repair_trace = {"attempted": False, "max_attempts": 2, "attempts": []}
        sql = "SELECT COUNT(*) AS total FROM `orders`"
        data = [{"total": 1}]
        history_id = "native-history-default-auto"
        history_status = "stored"
        warnings = []
        phase_traces = []
        native_trace = {
            "kernel": "native",
            "runtime_reused": True,
            "runtime_cache_key": "cache-key",
            "tools_called": [],
            "memory": {
                "example_count": 0,
                "memory_hit": False,
                "sources_attempted": [],
                "vanna_memory_hit": False,
                "vanna_memory_source": "",
                "confidence": 0.0,
                "used_as": "none",
                "rejected_reason": "",
                "candidate_count": 0,
                "candidate_examples": [],
                "chosen_candidate": {},
                "chosen_candidates": [],
                "rejected_candidates": [],
                "query_intent": "aggregate_count",
                "candidate_intent": "unknown",
                "intent_matched": False,
                "reuse_gate_reason": "reuse_blocked_no_candidate",
                "subtasks": [],
            },
            "audit_events": [],
            "fallback_reason": "",
        }

        @property
        def table_names(self):
            return ["orders"]

    async def fake_native_runner(**kwargs):
        return FakeNativeResult()

    main.run_native_query_kernel = fake_native_runner
    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询订单总数"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["trace"]["native"]["kernel"] == "native"
    llm_phase = next(item for item in payload["trace"]["phases"] if item["phase"] == "llm_resolution")
    assert llm_phase["details"]["kernel_default"] == "auto"
    assert llm_phase["details"]["kernel_effective"] == "auto"
    assert llm_phase["details"]["kernel_requested"] == "(default)"


def test_natural_query_kernel_native_failure_returns_error(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")

    async def fake_native_runner(**kwargs):
        raise main.NativeKernelExecutionError("native_failed", "native hard failure")

    main.run_native_query_kernel = fake_native_runner

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "查询订单总数", "kernel": "native"},
    )

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["error_code"] == "native_failed"
    assert detail["kernel_requested"] == "native"


def _parse_sse_events(raw_text: str):
    events = []
    current_event = None
    current_data_lines = []

    def flush():
        nonlocal current_event, current_data_lines
        if not current_event:
            return
        data_text = "\n".join(current_data_lines).strip()
        payload = json.loads(data_text) if data_text else {}
        events.append({"event": current_event, "data": payload})
        current_event = None
        current_data_lines = []

    for line in (raw_text or "").splitlines():
        if line.startswith("event: "):
            flush()
            current_event = line[len("event: "):].strip()
            current_data_lines = []
            continue
        if line.startswith("data: "):
            current_data_lines.append(line[len("data: "):])
            continue
        if not line.strip():
            flush()

    flush()
    return events


def test_native_query_sse_requires_auth(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")

    client = TestClient(main.app)
    response = client.post(
        "/api/query/native-chat-sse",
        headers={"Content-Type": "application/json"},
        json={"query": "鏌ヨ璁㈠崟鎬绘暟", "kernel": "native"},
    )

    assert response.status_code == 401


def test_native_query_sse_streams_events_and_done(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")

    class FakeNativeResult:
        intent = "count"
        plan = {"tables": ["orders"]}
        subtask_traces = [{"table_name": "orders", "strategy": "native_tool_sql_generation"}]
        sql = "SELECT COUNT(*) AS order_count FROM `orders`"
        data = [{"order_count": 10}]
        history_id = "native-h-1"
        history_status = "stored"
        warnings = []
        native_trace = {
            "kernel": "native",
            "runtime_reused": True,
            "runtime_cache_key": "cache-key",
            "tools_called": [{"tool_name": "dc_sql_generation", "success": True, "error": {}}],
            "memory": {
                "example_count": 1,
                "memory_hit": True,
                "sources_attempted": ["query_history.match_any"],
                "vanna_memory_hit": True,
                "vanna_memory_source": "query_history.match_any",
                "confidence": 0.91,
                "used_as": "sql_reuse",
                "rejected_reason": "",
                "candidate_count": 1,
                "query_intent": "aggregate_count",
                "candidate_intent": "aggregate_count",
                "intent_matched": True,
                "reuse_gate_reason": "reuse_allowed",
                "subtasks": [],
            },
            "audit_events": [
                {"phase": "tool_invocation", "payload": {"tool_name": "dc_sql_generation"}},
                {"phase": "tool_result", "payload": {"tool_name": "dc_sql_generation", "success": True, "error": None}},
            ],
            "fallback_reason": "",
        }

        @property
        def table_names(self):
            return ["orders"]

    async def fake_native_runner(**kwargs):
        return FakeNativeResult()

    main.run_native_query_kernel = fake_native_runner

    client = TestClient(main.app)
    response = client.post(
        "/api/query/native-chat-sse",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "鏌ヨ璁㈠崟鎬绘暟", "kernel": "native"},
    )

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("text/event-stream")

    events = _parse_sse_events(response.text)
    event_names = [item["event"] for item in events]
    assert "request_start" in event_names
    assert "tool_invocation" in event_names
    assert "tool_result" in event_names
    assert "sql_generated" in event_names
    assert "execution_result" in event_names
    assert "done" in event_names
    assert "error" not in event_names

    sql_event = next(item for item in events if item["event"] == "sql_generated")
    assert "COUNT(*)" in sql_event["data"]["sql"]
    execution_event = next(item for item in events if item["event"] == "execution_result")
    assert execution_event["data"]["row_count"] == 1
    done_event = next(item for item in events if item["event"] == "done")
    assert done_event["data"]["success"] is True
    assert done_event["data"]["memory"]["used_as"] == "sql_reuse"
    assert done_event["data"]["memory"]["reuse_gate_reason"] == "reuse_allowed"
    assert done_event["data"]["memory"] == FakeNativeResult.native_trace["memory"]


def test_native_query_sse_returns_error_event_without_hanging(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")

    async def fake_native_runner(**kwargs):
        raise main.NativeKernelExecutionError(
            "native_kernel_failed",
            "native stream failed",
            details={"tool_name": "dc_sql_generation"},
        )

    main.run_native_query_kernel = fake_native_runner

    client = TestClient(main.app)
    response = client.post(
        "/api/query/native-chat-sse",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "鏌ヨ璁㈠崟鎬绘暟", "kernel": "native"},
    )

    assert response.status_code == 200
    events = _parse_sse_events(response.text)
    event_names = [item["event"] for item in events]
    assert "request_start" in event_names
    assert "error" in event_names
    assert "done" in event_names

    error_event = next(item for item in events if item["event"] == "error")
    assert error_event["data"]["error_code"] == "native_kernel_failed"
    done_event = next(item for item in events if item["event"] == "done")
    assert done_event["data"]["success"] is False
