"""
Utilities for evaluating golden natural-language query cases.
"""
from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Iterable, List, Optional

import requests


def build_headers(api_key: str = "") -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["X-API-Key"] = api_key
    return headers


def extract_columns(rows: Iterable[Any]) -> List[str]:
    columns = set()
    for row in rows or []:
        if isinstance(row, dict):
            columns.update(row.keys())
    return sorted(columns)


def _resolve_case_expectation(case: Dict[str, Any], key: str) -> Any:
    kernel = str(case.get("kernel") or "").strip().lower()
    by_kernel = case.get(f"{key}_by_kernel")
    if isinstance(by_kernel, dict):
        if kernel and kernel in by_kernel:
            return by_kernel.get(kernel)
        if "default" in by_kernel:
            return by_kernel.get("default")
    return case.get(key)


def _normalize_expected_values(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _detect_sql_semantic(sql: str) -> str:
    normalized = " ".join(str(sql or "").lower().split())
    if not normalized:
        return "unknown"
    has_date_expr = bool(
        re.search(r"\b(date|date_trunc|to_date|day|week|month|date_format)\s*\(", normalized)
    ) or "created_at" in normalized
    if " group by " in normalized and has_date_expr:
        return "trend"
    if "count(" in normalized and " group by " not in normalized:
        return "count"
    if "sum(" in normalized and " group by " not in normalized:
        return "amount_sum"
    if " order by " in normalized and " limit " in normalized:
        return "listing"
    if " limit " in normalized:
        return "listing"
    return "unknown"


def validate_trace_schema(case: Dict[str, Any], payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    require_trace_schema = bool(case.get("require_trace_schema", False))
    expected_schema_version = case.get("expected_schema_version")

    if expected_schema_version is not None:
        actual_schema_version = payload.get("schema_version")
        if actual_schema_version != expected_schema_version:
            errors.append(
                f"schema_version {actual_schema_version!r} did not match expected {expected_schema_version!r}"
            )

    if not require_trace_schema:
        return errors

    required_top_level_fields = ["schema_version", "intent", "table_names", "trace"]
    missing_top_level_fields = [field for field in required_top_level_fields if field not in payload]
    if missing_top_level_fields:
        errors.append(f"missing top-level fields: {missing_top_level_fields}")
        return errors

    trace = payload.get("trace")
    if not isinstance(trace, dict):
        errors.append("trace must be an object when require_trace_schema=true")
        return errors

    required_trace_sections = ["trace_id", "planner", "subtasks", "orchestration", "repair", "execution"]
    missing_trace_sections = [field for field in required_trace_sections if field not in trace]
    if missing_trace_sections:
        errors.append(f"missing trace fields: {missing_trace_sections}")

    expected_table_names = case.get("expected_table_names")
    if expected_table_names is not None:
        actual_table_names = payload.get("table_names") or []
        if list(actual_table_names) != list(expected_table_names):
            errors.append(f"table_names {actual_table_names} did not match expected {expected_table_names}")

    expected_selected_tables = case.get("expected_selected_tables")
    if expected_selected_tables is not None:
        planner_selected_tables = ((trace.get("planner") or {}).get("selected_tables") or [])
        if list(planner_selected_tables) != list(expected_selected_tables):
            errors.append(
                f"trace planner selected_tables {planner_selected_tables} did not match expected {expected_selected_tables}"
            )

    forbidden_selected_tables = list(case.get("forbidden_selected_tables") or [])
    if forbidden_selected_tables:
        planner_selected_tables = set(((trace.get("planner") or {}).get("selected_tables") or []))
        forbidden_hits = sorted(table for table in forbidden_selected_tables if table in planner_selected_tables)
        if forbidden_hits:
            errors.append(f"trace planner selected forbidden tables: {forbidden_hits}")

    planner = trace.get("planner") if isinstance(trace, dict) else {}
    if case.get("expected_fallback_used") is not None:
        actual_fallback_used = bool((planner or {}).get("fallback_used"))
        expected_fallback_used = bool(case.get("expected_fallback_used"))
        if actual_fallback_used != expected_fallback_used:
            errors.append(
                f"trace planner fallback_used {actual_fallback_used} did not match expected {expected_fallback_used}"
            )

    if case.get("expected_trace_needs_join") is not None:
        actual_needs_join = bool((planner or {}).get("needs_join"))
        if actual_needs_join != bool(case.get("expected_trace_needs_join")):
            errors.append(
                f"trace planner needs_join {actual_needs_join} did not match expected {bool(case.get('expected_trace_needs_join'))}"
            )

    if case.get("expected_repair_attempted") is not None:
        repair = trace.get("repair") if isinstance(trace, dict) else {}
        actual_repair_attempted = bool((repair or {}).get("attempted"))
        if actual_repair_attempted != bool(case.get("expected_repair_attempted")):
            errors.append(
                f"trace repair attempted {actual_repair_attempted} did not match expected {bool(case.get('expected_repair_attempted'))}"
            )

    execution_trace = trace.get("execution") if isinstance(trace, dict) else {}
    if bool(case.get("require_execution_llm_fields", False)):
        missing_execution_fields = [
            field
            for field in ("llm_execution_mode", "resource_name")
            if field not in (execution_trace or {})
        ]
        if missing_execution_fields:
            errors.append(f"trace execution missing fields: {missing_execution_fields}")

    expected_llm_execution_mode = case.get("expected_llm_execution_mode")
    if expected_llm_execution_mode is not None:
        actual_llm_execution_mode = (execution_trace or {}).get("llm_execution_mode")
        if actual_llm_execution_mode != expected_llm_execution_mode:
            errors.append(
                f"trace execution llm_execution_mode {actual_llm_execution_mode!r} did not match expected {expected_llm_execution_mode!r}"
            )

    expected_resource_name = case.get("expected_resource_name")
    if expected_resource_name is not None:
        actual_resource_name = (execution_trace or {}).get("resource_name")
        if actual_resource_name != expected_resource_name:
            errors.append(
                f"trace execution resource_name {actual_resource_name!r} did not match expected {expected_resource_name!r}"
            )

    native_trace = trace.get("native") if isinstance(trace, dict) else {}
    native_memory = (native_trace or {}).get("memory") if isinstance(native_trace, dict) else {}
    if not isinstance(native_memory, dict):
        native_memory = {}

    if bool(_resolve_case_expectation(case, "require_native_memory_gate_fields")):
        required_gate_fields = [
            "used_as",
            "reuse_gate_reason",
            "query_intent",
            "candidate_intent",
            "intent_matched",
        ]
        missing_gate_fields = [field for field in required_gate_fields if field not in native_memory]
        if missing_gate_fields:
            errors.append(f"trace native memory missing gate fields: {missing_gate_fields}")

    expected_native_memory_used_as = _resolve_case_expectation(case, "expected_native_memory_used_as")
    if expected_native_memory_used_as is not None:
        allowed_used_as = [str(item) for item in _normalize_expected_values(expected_native_memory_used_as)]
        actual_used_as = str(native_memory.get("used_as") or "")
        if actual_used_as not in allowed_used_as:
            errors.append(
                f"trace native memory used_as {actual_used_as!r} did not match expected {allowed_used_as!r}"
            )

    expected_reuse_gate_reason = _resolve_case_expectation(case, "expected_reuse_gate_reason")
    if expected_reuse_gate_reason is not None:
        allowed_reasons = [str(item) for item in _normalize_expected_values(expected_reuse_gate_reason)]
        actual_reason = str(native_memory.get("reuse_gate_reason") or "")
        if actual_reason not in allowed_reasons:
            errors.append(
                f"trace native memory reuse_gate_reason {actual_reason!r} did not match expected {allowed_reasons!r}"
            )

    expected_runtime_reused = _resolve_case_expectation(case, "expected_runtime_reused")
    if expected_runtime_reused is not None:
        actual_runtime_reused = bool((native_trace or {}).get("runtime_reused", False))
        if actual_runtime_reused != bool(expected_runtime_reused):
            errors.append(
                f"trace native runtime_reused {actual_runtime_reused} did not match expected {bool(expected_runtime_reused)}"
            )

    return errors


def evaluate_case(
    case: Dict[str, Any],
    status_code: int,
    payload: Dict[str, Any],
    *,
    elapsed_seconds: float = 0.0,
) -> Dict[str, Any]:
    errors: List[str] = []
    expected_status = int(case.get("expected_status", 200))
    question = case["question"]

    response_excerpt = ""
    if isinstance(payload, dict):
        response_excerpt = str(payload.get("detail") or payload.get("raw_text") or "")[:500]

    if status_code != expected_status:
        if response_excerpt:
            errors.append(
                f"HTTP {status_code} did not match expected status {expected_status}: {response_excerpt}"
            )
        else:
            errors.append(f"HTTP {status_code} did not match expected status {expected_status}")

    sql = payload.get("sql", "") if isinstance(payload, dict) else ""
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    serialized_rows = json.dumps(rows, ensure_ascii=False, sort_keys=True)
    row_count = len(rows) if isinstance(rows, list) else 0
    if isinstance(payload, dict):
        payload_count = payload.get("count")
        if isinstance(payload_count, int):
            row_count = payload_count
    columns = extract_columns(rows if isinstance(rows, list) else [])

    expected_success = case.get("expected_success")
    if expected_success is not None:
        actual_success = bool(payload.get("success")) if isinstance(payload, dict) else False
        if actual_success != bool(expected_success):
            errors.append(f"success flag {actual_success} did not match expected {bool(expected_success)}")

    expected_sql_pattern = case.get("expected_sql_pattern")
    if expected_sql_pattern and not re.search(expected_sql_pattern, sql):
        errors.append(f"SQL pattern mismatch: {sql}")

    missing_sql_fragments = [fragment for fragment in case.get("expected_sql_contains", []) if fragment not in sql]
    if missing_sql_fragments:
        errors.append(f"missing expected SQL fragments: {missing_sql_fragments}")

    forbidden_sql_pattern = case.get("forbidden_sql_pattern")
    if forbidden_sql_pattern and re.search(forbidden_sql_pattern, sql):
        errors.append(f"matched forbidden SQL pattern: {sql}")

    expected_min_rows = int(case.get("expected_min_rows", 0))
    if row_count < expected_min_rows:
        errors.append(f"row count {row_count} below expected minimum {expected_min_rows}")

    expected_max_rows = case.get("expected_max_rows")
    if expected_max_rows is not None and row_count > int(expected_max_rows):
        errors.append(f"row count {row_count} above expected maximum {int(expected_max_rows)}")

    expected_row_count = case.get("expected_row_count")
    if expected_row_count is not None and row_count != int(expected_row_count):
        errors.append(f"row count {row_count} did not match expected {int(expected_row_count)}")

    if not bool(case.get("allow_empty_result", True)) and row_count == 0:
        errors.append("empty result is not allowed for this case")

    expected_columns_all = case.get("expected_columns_all", [])
    missing_columns = [column for column in expected_columns_all if column not in columns]
    if missing_columns:
        errors.append(f"missing expected columns: {missing_columns}")

    expected_columns_any = case.get("expected_columns_any", [])
    if expected_columns_any and not any(column in columns for column in expected_columns_any):
        errors.append(f"expected at least one column from: {expected_columns_any}")

    missing_values = [value for value in case.get("expected_result_contains", []) if value not in serialized_rows]
    if missing_values:
        errors.append(f"missing expected values: {missing_values}")

    unexpected_values = [value for value in case.get("expected_result_not_contains", []) if value in serialized_rows]
    if unexpected_values:
        errors.append(f"found forbidden values: {unexpected_values}")

    max_latency_seconds = case.get("max_latency_seconds")
    if max_latency_seconds is not None and elapsed_seconds > float(max_latency_seconds):
        errors.append(
            f"latency {elapsed_seconds:.2f}s exceeded max_latency_seconds {float(max_latency_seconds):.2f}s"
        )

    if bool(case.get("require_sql_executable", False)) and status_code == expected_status:
        if not sql or not isinstance(sql, str):
            errors.append("expected executable SQL but response sql is empty")

    expected_sql_semantic = _resolve_case_expectation(case, "expected_sql_semantic")
    if expected_sql_semantic:
        actual_semantic = _detect_sql_semantic(sql)
        allowed_semantics = [str(item) for item in _normalize_expected_values(expected_sql_semantic)]
        if actual_semantic not in allowed_semantics:
            errors.append(
                f"SQL semantic {actual_semantic!r} did not match expected {allowed_semantics!r}: {sql}"
            )

    if isinstance(payload, dict):
        errors.extend(validate_trace_schema(case, payload))

    return {
        "question": question,
        "passed": not errors,
        "errors": errors,
        "status_code": status_code,
        "sql": sql,
        "row_count": row_count,
        "columns": columns,
        "elapsed_seconds": elapsed_seconds,
        "response_excerpt": response_excerpt,
    }


def run_case(case: Dict[str, Any], base_url: str, headers: Dict[str, str], timeout: int = 120) -> Dict[str, Any]:
    request_payload = {"query": case["question"]}
    if "resource_name" in case:
        request_payload["resource_name"] = case.get("resource_name")
    if "kernel" in case:
        request_payload["kernel"] = case.get("kernel")
    if "table_names" in case:
        request_payload["table_names"] = case.get("table_names")
    if "include_trace" in case:
        request_payload["include_trace"] = bool(case.get("include_trace"))

    started = time.perf_counter()
    try:
        response = requests.post(
            f"{base_url.rstrip('/')}/api/query/natural",
            headers=headers,
            json=request_payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        elapsed = time.perf_counter() - started
        return {
            "question": case["question"],
            "passed": False,
            "errors": [f"request failed: {exc}"],
            "status_code": 0,
            "sql": "",
            "row_count": 0,
            "columns": [],
            "elapsed_seconds": elapsed,
            "response_excerpt": str(exc)[:500],
        }
    elapsed = time.perf_counter() - started

    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}

    return evaluate_case(case, response.status_code, payload, elapsed_seconds=elapsed)


def run_cases(
    cases: Iterable[Dict[str, Any]],
    base_url: str,
    headers: Dict[str, str],
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    return [run_case(case, base_url=base_url, headers=headers, timeout=timeout) for case in cases]


def summarize_results(
    results: Iterable[Dict[str, Any]],
    min_pass_rate: float = 1.0,
    min_passed: Optional[int] = None,
) -> Dict[str, Any]:
    results = list(results)
    total = len(results)
    passed = sum(1 for result in results if result.get("passed"))
    failed = total - passed
    pass_rate = (passed / total) if total else 0.0
    failures = [
        {
            "question": result.get("question"),
            "errors": result.get("errors", []),
            "status_code": result.get("status_code"),
            "row_count": result.get("row_count"),
            "elapsed_seconds": result.get("elapsed_seconds"),
            "sql": result.get("sql", ""),
            "response_excerpt": result.get("response_excerpt", ""),
        }
        for result in results
        if not result.get("passed")
    ]
    success = total > 0 and pass_rate >= float(min_pass_rate)
    if min_passed is not None:
        success = success and passed >= int(min_passed)

    return {
        "success": success,
        "total": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": pass_rate,
        "thresholds": {
            "min_pass_rate": float(min_pass_rate),
            "min_passed": None if min_passed is None else int(min_passed),
        },
        "failures": failures,
        "results": results,
    }
