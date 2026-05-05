import golden_runner


def test_evaluate_case_passes_with_strict_result_shape():
    case = {
        "question": "广州有多少机构？",
        "expected_status": 200,
        "expected_sql_pattern": "COUNT",
        "forbidden_sql_pattern": "SELECT\\s+\\*",
        "expected_min_rows": 1,
        "allow_empty_result": False,
        "expected_columns_all": ["total"],
        "expected_result_contains": ["42"],
        "expected_result_not_contains": ["error"],
    }
    payload = {
        "success": True,
        "sql": "SELECT COUNT(*) AS total FROM `institutions`",
        "data": [{"total": 42}],
        "count": 1,
    }

    result = golden_runner.evaluate_case(case, 200, payload)

    assert result["passed"] is True
    assert result["errors"] == []


def test_evaluate_case_collects_multiple_failure_reasons():
    case = {
        "question": "坏用例",
        "expected_status": 200,
        "expected_sql_pattern": "COUNT",
        "forbidden_sql_pattern": "SELECT\\s+\\*",
        "expected_min_rows": 1,
        "allow_empty_result": False,
        "expected_columns_all": ["total"],
        "expected_result_not_contains": ["广州"],
    }
    payload = {
        "success": True,
        "sql": "SELECT * FROM `institutions`",
        "data": [],
        "count": 0,
    }

    result = golden_runner.evaluate_case(case, 200, payload)

    assert result["passed"] is False
    assert any("SQL pattern mismatch" in error for error in result["errors"])
    assert any("forbidden SQL pattern" in error for error in result["errors"])
    assert any("below expected minimum" in error for error in result["errors"])
    assert any("empty result" in error for error in result["errors"])
    assert any("missing expected columns" in error for error in result["errors"])


def test_summarize_results_enforces_acceptance_thresholds():
    results = [
        {"question": "q1", "passed": True, "errors": []},
        {"question": "q2", "passed": True, "errors": []},
        {"question": "q3", "passed": False, "errors": ["boom"]},
    ]

    summary = golden_runner.summarize_results(results, min_pass_rate=0.8, min_passed=3)

    assert summary["success"] is False
    assert summary["passed"] == 2
    assert summary["failed"] == 1
    assert summary["pass_rate"] == 2 / 3
    assert summary["thresholds"]["min_pass_rate"] == 0.8
    assert summary["thresholds"]["min_passed"] == 3


def test_evaluate_case_requires_expected_sql_contains_fragments():
    case = {
        "question": "广州有多少机构？",
        "expected_status": 200,
        "expected_sql_contains": ["广州", "COUNT"],
    }
    payload = {
        "success": True,
        "sql": "SELECT COUNT(*) FROM `institutions` WHERE `city` = '深圳'",
        "data": [{"COUNT(*)": 1}],
        "count": 1,
    }

    result = golden_runner.evaluate_case(case, 200, payload)

    assert result["passed"] is False
    assert any("missing expected SQL fragments" in error for error in result["errors"])


def test_evaluate_case_validates_trace_schema_when_required():
    case = {
        "question": "广州有多少机构？",
        "expected_status": 200,
        "expected_schema_version": "nlq.v1",
        "require_trace_schema": True,
        "expected_table_names": ["institutions"],
        "expected_trace_needs_join": False,
        "expected_repair_attempted": False,
    }
    payload = {
        "success": True,
        "schema_version": "nlq.v1",
        "intent": "count",
        "table_names": ["institutions"],
        "sql": "SELECT COUNT(*) AS total FROM `institutions`",
        "data": [{"total": 42}],
        "count": 1,
        "trace": {
            "trace_id": "trace-1",
            "planner": {"needs_join": False},
            "subtasks": [],
            "orchestration": {"strategy": "passthrough"},
            "repair": {"attempted": False},
            "execution": {"row_count": 1},
        },
    }

    result = golden_runner.evaluate_case(case, 200, payload)

    assert result["passed"] is True
    assert result["errors"] == []


def test_evaluate_case_reports_missing_trace_fields():
    case = {
        "question": "广州有多少机构？",
        "expected_status": 200,
        "expected_schema_version": "nlq.v1",
        "require_trace_schema": True,
    }
    payload = {
        "success": True,
        "schema_version": "nlq.v1",
        "intent": "count",
        "table_names": ["institutions"],
        "sql": "SELECT COUNT(*) AS total FROM `institutions`",
        "data": [{"total": 42}],
        "count": 1,
        "trace": {"planner": {}},
    }

    result = golden_runner.evaluate_case(case, 200, payload)

    assert result["passed"] is False
    assert any("missing trace fields" in error for error in result["errors"])


def test_evaluate_case_validates_selected_tables_fallback_and_latency():
    case = {
        "question": "查询订单总数",
        "expected_status": 200,
        "require_trace_schema": True,
        "expected_selected_tables": ["orders"],
        "expected_fallback_used": False,
        "max_latency_seconds": 10,
        "require_sql_executable": True,
        "require_execution_llm_fields": True,
        "expected_llm_execution_mode": "doris_resource",
        "expected_resource_name": "openrutor",
    }
    payload = {
        "success": True,
        "schema_version": "nlq.v1",
        "intent": "count",
        "table_names": ["orders"],
        "sql": "SELECT COUNT(*) AS order_count FROM `orders`",
        "data": [{"order_count": 10}],
        "count": 1,
        "trace": {
            "trace_id": "trace-1",
            "planner": {
                "needs_join": False,
                "fallback_used": False,
                "selected_tables": ["orders"],
            },
            "subtasks": [],
            "orchestration": {"strategy": "passthrough"},
            "repair": {"attempted": False},
            "execution": {
                "row_count": 1,
                "llm_execution_mode": "doris_resource",
                "resource_name": "openrutor",
            },
        },
    }

    result = golden_runner.evaluate_case(case, 200, payload, elapsed_seconds=1.2)

    assert result["passed"] is True
    assert result["errors"] == []


def test_evaluate_case_rejects_latency_and_forbidden_selected_table():
    case = {
        "question": "查询订单总数",
        "expected_status": 200,
        "require_trace_schema": True,
        "forbidden_selected_tables": ["warehouse_stock_out_items"],
        "max_latency_seconds": 2,
    }
    payload = {
        "success": True,
        "schema_version": "nlq.v1",
        "intent": "count",
        "table_names": ["warehouse_stock_out_items"],
        "sql": "SELECT COUNT(*) FROM `warehouse_stock_out_items`",
        "data": [{"count": 1}],
        "count": 1,
        "trace": {
            "trace_id": "trace-1",
            "planner": {
                "needs_join": False,
                "fallback_used": True,
                "selected_tables": ["warehouse_stock_out_items"],
            },
            "subtasks": [],
            "orchestration": {"strategy": "passthrough"},
            "repair": {"attempted": False},
            "execution": {"row_count": 1},
        },
    }

    result = golden_runner.evaluate_case(case, 200, payload, elapsed_seconds=3.0)

    assert result["passed"] is False
    assert any("forbidden tables" in error for error in result["errors"])
    assert any("latency" in error for error in result["errors"])


def test_run_case_forwards_kernel_to_query_api(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "success": True,
                "schema_version": "nlq.v1",
                "intent": "count",
                "table_names": ["orders"],
                "sql": "SELECT COUNT(*) AS total FROM `orders`",
                "data": [{"total": 1}],
                "count": 1,
                "trace": {
                    "trace_id": "trace-1",
                    "planner": {"selected_tables": ["orders"], "needs_join": False, "fallback_used": False},
                    "subtasks": [],
                    "orchestration": {"strategy": "passthrough"},
                    "repair": {"attempted": False},
                    "execution": {"row_count": 1},
                },
            }

        text = ""

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = dict(headers or {})
        captured["json"] = dict(json or {})
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(golden_runner.requests, "post", fake_post)

    case = {
        "question": "查询订单总数",
        "kernel": "native",
        "expected_status": 200,
        "require_trace_schema": True,
    }
    result = golden_runner.run_case(case, base_url="http://localhost:38018", headers={"X-API-Key": "x"}, timeout=30)

    assert result["passed"] is True
    assert captured["json"]["query"] == "查询订单总数"
    assert captured["json"]["kernel"] == "native"


def test_evaluate_case_validates_sql_semantic_type():
    case = {
        "question": "查询订单总数",
        "expected_status": 200,
        "expected_sql_semantic": "count",
    }
    payload = {
        "success": True,
        "sql": "SELECT * FROM `orders` ORDER BY `created_at` DESC LIMIT 10",
        "data": [{"id": 1}],
        "count": 1,
    }

    result = golden_runner.evaluate_case(case, 200, payload)

    assert result["passed"] is False
    assert any("SQL semantic" in error for error in result["errors"])


def test_validate_trace_schema_supports_native_memory_expectation_by_kernel():
    case = {
        "question": "查询订单总数",
        "expected_status": 200,
        "require_trace_schema": True,
        "kernel": "native",
        "expected_native_memory_used_as_by_kernel": {
            "legacy": "none",
            "native": "sql_reuse",
            "auto": "sql_reuse",
        },
        "expected_reuse_gate_reason_by_kernel": {
            "native": "reuse_allowed",
        },
        "require_native_memory_gate_fields": True,
    }
    payload = {
        "success": True,
        "schema_version": "nlq.v1",
        "intent": "count",
        "table_names": ["orders"],
        "sql": "SELECT COUNT(*) AS total FROM `orders`",
        "data": [{"total": 1}],
        "count": 1,
        "trace": {
            "trace_id": "trace-1",
            "planner": {"selected_tables": ["orders"], "needs_join": False, "fallback_used": False},
            "subtasks": [],
            "orchestration": {"strategy": "passthrough"},
            "repair": {"attempted": False},
            "execution": {"row_count": 1},
            "native": {
                "kernel": "native",
                "runtime_reused": True,
                "tools_called": [],
                "memory": {
                    "used_as": "sql_reuse",
                    "reuse_gate_reason": "reuse_allowed",
                    "query_intent": "aggregate_count",
                    "candidate_intent": "aggregate_count",
                    "intent_matched": True,
                },
                "audit_events": [],
                "fallback_reason": "",
            },
        },
    }

    result = golden_runner.evaluate_case(case, 200, payload)
    assert result["passed"] is True
