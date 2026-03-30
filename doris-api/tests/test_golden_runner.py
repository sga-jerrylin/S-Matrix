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
