"""
Utilities for evaluating golden natural-language query cases.
"""
from __future__ import annotations

import json
import re
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


def evaluate_case(case: Dict[str, Any], status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
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

    return {
        "question": question,
        "passed": not errors,
        "errors": errors,
        "status_code": status_code,
        "sql": sql,
        "row_count": row_count,
        "columns": columns,
        "response_excerpt": response_excerpt,
    }


def run_case(case: Dict[str, Any], base_url: str, headers: Dict[str, str], timeout: int = 120) -> Dict[str, Any]:
    try:
        response = requests.post(
            f"{base_url.rstrip('/')}/api/query/natural",
            headers=headers,
            json={"query": case["question"]},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        return {
            "question": case["question"],
            "passed": False,
            "errors": [f"request failed: {exc}"],
            "status_code": 0,
            "sql": "",
            "row_count": 0,
            "columns": [],
            "response_excerpt": str(exc)[:500],
        }

    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}

    return evaluate_case(case, response.status_code, payload)


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
