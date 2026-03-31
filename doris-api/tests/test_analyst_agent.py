import pytest

from analyst_agent import AnalystAgent
import analyst_agent as analyst_agent_module


class RecordingDB:
    def __init__(self):
        self.queries = []
        self.updates = []

    def validate_identifier(self, identifier):
        if "!" in identifier:
            raise ValueError(f"Invalid identifier: {identifier}")
        return f"`{identifier}`"

    def get_table_schema(self, table_name):
        return []

    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        return []

    def execute_update(self, sql, params=None):
        self.updates.append((sql, params))
        return 1


def test_init_tables_creates_reports_table():
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {})

    assert agent.init_tables() is True

    ddl = db.updates[0][0]
    assert "_sys_analysis_reports" in ddl
    assert "UNIQUE KEY(`id`)" in ddl
    assert 'PROPERTIES ("replication_num" = "1")' in ddl


def test_profile_large_table_uses_sampling():
    class ProfilingDB(RecordingDB):
        def get_table_schema(self, table_name):
            return [
                {"Field": "amount", "Type": "INT"},
                {"Field": "city", "Type": "VARCHAR(50)"},
            ]

        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "COUNT(*) AS row_count" in sql:
                return [{"row_count": 250000}]
            if "MIN(`amount`)" in sql:
                return [{"min_value": 1, "max_value": 99, "avg_value": 40.5, "stddev_value": 12.3, "null_count": 5}]
            if "COUNT(DISTINCT `city`)" in sql:
                return [{"unique_count": 3, "null_count": 0}]
            if "GROUP BY `city`" in sql:
                return [{"value": "Shanghai", "count": 10}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    agent = AnalystAgent(ProfilingDB(), lambda **kwargs: {"api_key": "env-key"})

    profile = agent._profile_table("sales")

    assert profile["sampled"] is True
    assert profile["columns"]["amount"]["max"] == 99
    assert profile["columns"]["city"]["top_values"][0]["value"] == "Shanghai"
    assert any("TABLESAMPLE" in sql for sql, _ in agent.db.queries)


def test_profile_small_table_skips_sampling():
    class SmallTableDB(RecordingDB):
        def get_table_schema(self, table_name):
            return [{"Field": "amount", "Type": "INT"}]

        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "COUNT(*) AS row_count" in sql:
                return [{"row_count": 12}]
            if "MIN(`amount`)" in sql:
                return [{"min_value": 1, "max_value": 12, "avg_value": 6.0, "stddev_value": 3.4, "null_count": 0}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    agent = AnalystAgent(SmallTableDB(), lambda **kwargs: {"api_key": "env-key"})

    profile = agent._profile_table("sales")

    assert profile["sampled"] is False
    assert profile["sample_size"] == 12
    assert all("TABLESAMPLE" not in sql and "ORDER BY RAND()" not in sql for sql, _ in agent.db.queries)


def test_profile_large_table_falls_back_when_tablesample_is_unsupported():
    class FallbackDB(RecordingDB):
        def get_table_schema(self, table_name):
            return [{"Field": "amount", "Type": "INT"}]

        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "COUNT(*) AS row_count" in sql:
                return [{"row_count": 250000}]
            if "TABLESAMPLE" in sql:
                raise Exception("syntax error near TABLESAMPLE")
            if "ORDER BY RAND()" in sql and "MIN(`amount`)" in sql:
                return [{"min_value": 1, "max_value": 99, "avg_value": 40.5, "stddev_value": 12.3, "null_count": 5}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    agent = AnalystAgent(FallbackDB(), lambda **kwargs: {"api_key": "env-key"})

    profile = agent._profile_table("sales")

    assert profile["sampled"] is True
    assert any("ORDER BY RAND()" in sql for sql, _ in agent.db.queries)


def test_execute_with_repair_fixes_bad_sql(monkeypatch):
    class RepairingDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if len(self.queries) == 1:
                raise Exception("syntax error")
            return [{"total": 3}]

    class FakeRepairAgent:
        def __init__(self, *args, **kwargs):
            pass

        def repair_sql(self, question, failed_sql, error_message, ddl_list, api_config=None):
            assert question == "How many rows?"
            assert failed_sql == "SELECT BROKEN"
            assert "syntax error" in error_message
            return "SELECT 3 AS total"

    monkeypatch.setattr(analyst_agent_module, "RepairAgent", FakeRepairAgent)
    agent = AnalystAgent(RepairingDB(), lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})

    data, final_sql, success, error_message = agent._execute_with_repair(
        "SELECT BROKEN",
        "How many rows?",
        {"api_key": "key", "model": "model", "base_url": "https://example.com"},
    )

    assert success is True
    assert error_message is None
    assert final_sql == "SELECT 3 AS total"
    assert data == [{"total": 3}]


def test_analyze_table_rejects_invalid_identifier():
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})

    with pytest.raises(ValueError):
        agent.analyze_table("bad!name")


def test_analyze_table_passes_safe_name_to_profile(monkeypatch):
    class CountingDB(RecordingDB):
        def __init__(self):
            super().__init__()
            self.validate_calls = []

        def validate_identifier(self, identifier):
            self.validate_calls.append(identifier)
            return super().validate_identifier(identifier)

    db = CountingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})
    captured = {}

    def fake_profile(table_name, safe_table_name=None):
        captured["table_name"] = table_name
        captured["safe_table_name"] = safe_table_name
        return {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}}

    monkeypatch.setattr(agent, "_profile_table", fake_profile)
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name, "description": "sales facts"})
    monkeypatch.setattr(
        agent,
        "_plan_analysis",
        lambda profile, metadata, depth, api_config: [{"title": "Volume", "question": "How many rows?", "sql": "SELECT COUNT(*) AS total FROM `sales`"}],
    )
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 10}], sql, True, None),
    )
    monkeypatch.setattr(
        agent,
        "_generate_insights",
        lambda step_results, profile, api_config: {"summary": "ok", "insights": [], "anomalies": [], "recommendations": []},
    )

    agent.analyze_table("sales", depth="quick", resource_name="Deepseek")

    assert db.validate_calls == ["sales"]
    assert captured == {"table_name": "sales", "safe_table_name": "`sales`"}


def test_analyze_table_produces_report(monkeypatch):
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})

    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name, "description": "sales facts"})
    monkeypatch.setattr(
        agent,
        "_plan_analysis",
        lambda profile, metadata, depth, api_config: [
            {"title": "Volume", "question": "How many rows?", "sql": "SELECT COUNT(*) AS total FROM `sales`"}
        ],
    )
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 10}], sql, True, None),
    )
    monkeypatch.setattr(
        agent,
        "_generate_insights",
        lambda step_results, profile, api_config: {
            "summary": "Sales volume is stable.",
            "insights": [{"title": "Stable volume", "detail": "10 rows returned"}],
            "anomalies": [],
            "recommendations": ["Monitor weekly growth."],
        },
    )

    report = agent.analyze_table("sales", depth="quick", resource_name="Deepseek")

    assert report["success"] is True
    assert report["table_names"] == "sales"
    assert report["insight_count"] == 1
    assert report["failed_step_count"] == 0
    assert any("_sys_analysis_reports" in sql for sql, _ in db.updates)


def test_generate_insights_returns_fallback_on_llm_failure(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})
    monkeypatch.setattr(
        agent,
        "_call_json_completion",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("LLM unavailable")),
    )

    insights = agent._generate_insights(
        step_results=[{"title": "Volume", "success": True, "data": [{"total": 10}]}],
        profile={"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
        api_config={"api_key": "key", "model": "model", "base_url": "https://example.com"},
    )

    assert insights["summary"] == "Analysis completed with limited automated insights."
    assert insights["insights"] == []
    assert insights["anomalies"] == []


def test_replay_from_history_reuses_saved_query(monkeypatch):
    class ReplayDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "FROM `_sys_query_history`" in sql:
                return [{
                    "id": "history-1",
                    "question": "How many sales?",
                    "sql": "SELECT COUNT(*) AS total FROM `sales`",
                    "table_names": "sales",
                }]
            return []

    agent = AnalystAgent(ReplayDB(), lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})
    monkeypatch.setattr(agent, "_profile_table", lambda table_name: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}})
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 10}], sql, True, None),
    )
    monkeypatch.setattr(
        agent,
        "_generate_insights",
        lambda step_results, profile, api_config: {
            "summary": "Replay succeeded.",
            "insights": [{"title": "Current total", "detail": "10 rows"}],
            "anomalies": [],
            "recommendations": [],
        },
    )

    report = agent.replay_from_history("history-1", resource_name="Deepseek")

    assert report["success"] is True
    assert report["history_id"] == "history-1"
    assert report["trigger_type"] == "history_replay"
    assert "current data" in report["note"].lower()


def test_build_strategist_config_uses_default_provider_without_resource(monkeypatch):
    def build_api_config(**kwargs):
        raise AssertionError("_build_strategist_config must not depend on build_api_config")

    agent = AnalystAgent(RecordingDB(), build_api_config)
    monkeypatch.delenv("ANALYST_STRATEGIST_MODEL", raising=False)
    monkeypatch.delenv("ANALYST_STRATEGIST_BASE_URL", raising=False)
    monkeypatch.delenv("ANALYST_STRATEGIST_API_KEY", raising=False)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "default-key")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://default.example.com")

    strategist = agent._build_strategist_config()

    assert strategist == {
        "api_key": "default-key",
        "model": "deepseek-reasoner",
        "base_url": "https://default.example.com",
    }


def test_build_strategist_config_warns_on_partial_override(monkeypatch, caplog):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    monkeypatch.setenv("ANALYST_STRATEGIST_MODEL", "custom-reasoner")
    monkeypatch.delenv("ANALYST_STRATEGIST_BASE_URL", raising=False)
    monkeypatch.delenv("ANALYST_STRATEGIST_API_KEY", raising=False)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fallback-key")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://fallback.example.com")

    strategist = agent._build_strategist_config()

    assert "Partial ANALYST_STRATEGIST_* config" in caplog.text
    assert strategist == {
        "api_key": "fallback-key",
        "model": "deepseek-reasoner",
        "base_url": "https://fallback.example.com",
    }


def test_build_round_prompt_keeps_base_context_after_round_one():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    profile = {"row_count": 12, "sampled": False, "columns": {"amount": {"type": "DOUBLE"}}}
    stats = [{"type": "growth", "column": "amount", "growth_pct": 12.5}]
    metadata = {"table_name": "sales", "description": "sales facts"}
    compressed_history = [{"round": 1, "strategist_output": {"hypotheses": [{"id": "H1"}]}, "results": []}]

    round_two_prompt = agent._build_round_prompt(2, profile, stats, metadata, compressed_history)
    round_three_prompt = agent._build_round_prompt(3, profile, stats, metadata, compressed_history)

    for prompt in (round_two_prompt, round_three_prompt):
        assert "Metadata:" in prompt
        assert "Profile:" in prompt
        assert "Statistics:" in prompt
        assert "sales facts" in prompt
        assert '"growth_pct": 12.5' in prompt


def test_call_strategist_reasoner_uses_user_only_message_and_captures_reasoning(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": '{"summary":"ok","continue":false}',
                            "reasoning_content": "step by step",
                        }
                    }
                ]
            }

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(analyst_agent_module.requests, "post", fake_post)

    payload = agent._call_strategist(
        "Investigate the data",
        {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": "secret"},
    )

    assert captured["url"] == "https://api.example.com/chat/completions"
    assert captured["json"]["messages"] == [{"role": "user", "content": "Investigate the data"}]
    assert captured["json"]["max_tokens"] == 8000
    assert "temperature" not in captured["json"]
    assert captured["timeout"] == 120
    assert payload["reasoning"] == "step by step"
    assert payload["response"]["summary"] == "ok"


def test_call_strategist_non_reasoner_avoids_generic_system_prompt(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": '{"summary":"ok","continue":false}'}}]}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(analyst_agent_module.requests, "post", fake_post)

    payload = agent._call_strategist(
        "Investigate the data",
        {"model": "deepseek-chat", "base_url": "https://api.example.com", "api_key": "secret"},
    )

    assert captured["json"]["messages"] == [{"role": "user", "content": "Investigate the data"}]
    assert captured["json"]["temperature"] == 0.1
    assert captured["timeout"] == 60
    assert payload["response"]["summary"] == "ok"


def test_compute_statistical_facts_queries_growth_outlier_and_correlation():
    class StatsDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            compact_sql = " ".join(sql.split())
            if "DATE_FORMAT(`order_date`, '%Y-%m')" in compact_sql:
                return [
                    {"period": "2026-03", "metric_value": 120.0},
                    {"period": "2026-02", "metric_value": 100.0},
                ]
            if "NTILE(10) OVER (ORDER BY `revenue` DESC)" in compact_sql:
                return [{"top_share_pct": 37.5}]
            if "COUNT(*) AS outlier_count" in compact_sql and "`revenue`" in compact_sql:
                assert params == (30.0, 70.0)
                return [{"outlier_count": 4}]
            if "CORR(`revenue`, `profit`)" in compact_sql:
                return [{"correlation_value": 0.82}]
            return []

    agent = AnalystAgent(StatsDB(), lambda **kwargs: {"api_key": "key"})
    facts = agent._compute_statistical_facts(
        "`sales`",
        {
            "row_count": 200,
            "sampled": False,
            "sample_size": 200,
            "columns": {
                "order_date": {"type": "DATETIME"},
                "revenue": {"type": "DOUBLE", "avg": 50.0, "stddev": 10.0, "min": 10.0, "max": 100.0},
                "profit": {"type": "DOUBLE", "avg": 5.0, "stddev": 2.0, "min": 0.0, "max": 15.0},
            },
        },
    )

    assert any(fact.get("type") == "growth" and fact.get("column") == "revenue" for fact in facts)
    assert any(fact.get("type") == "concentration" and fact.get("column") == "revenue" for fact in facts)
    assert any(fact.get("type") == "outlier" and fact.get("column") == "revenue" for fact in facts)
    assert any(fact.get("type") == "correlation" and fact.get("columns") == ["revenue", "profit"] for fact in facts)


def test_analyze_table_expert_falls_back_to_deep_on_strategist_failure(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile: [])
    monkeypatch.setattr(agent, "_call_strategist", lambda prompt, strategist_config: (_ for _ in ()).throw(RuntimeError("R1 unavailable")))

    captured = {}

    def fake_standard_analyze(self, table_name, depth="standard", resource_name=None, **kwargs):
        captured["table_name"] = table_name
        captured["depth"] = depth
        captured["resource_name"] = resource_name
        captured["kwargs"] = kwargs
        return {"success": True, "depth": depth, "summary": "fallback"}

    monkeypatch.setattr(AnalystAgent, "analyze_table", fake_standard_analyze)

    report = agent.analyze_table_expert("sales", resource_name="OpenAI", trigger_type="scheduled_analysis", schedule_id="schedule-1")

    assert report["depth"] == "deep"
    assert captured["table_name"] == "sales"
    assert captured["depth"] == "deep"
    assert captured["resource_name"] == "OpenAI"
    assert captured["kwargs"]["trigger_type"] == "scheduled_analysis"
    assert captured["kwargs"]["schedule_id"] == "schedule-1"


def test_analyze_table_expert_validates_strategist_api_key_before_call(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "executor-key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile: [])
    monkeypatch.setattr(agent, "_build_strategist_config", lambda: {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": None})

    called = {"strategist": False}

    def fake_call_strategist(prompt, strategist_config):
        called["strategist"] = True
        raise AssertionError("strategist should not be called without api_key")

    monkeypatch.setattr(agent, "_call_strategist", fake_call_strategist)

    captured = {}

    def fake_standard_analyze(self, table_name, depth="standard", resource_name=None, **kwargs):
        captured["table_name"] = table_name
        captured["depth"] = depth
        return {"success": True, "depth": depth}

    monkeypatch.setattr(AnalystAgent, "analyze_table", fake_standard_analyze)

    report = agent.analyze_table_expert("sales", resource_name="OpenAI")

    assert called["strategist"] is False
    assert report["depth"] == "deep"
    assert captured["table_name"] == "sales"


def test_analyze_table_expert_runs_multiple_rounds_and_truncates_reasoning(monkeypatch):
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setenv("ANALYST_MAX_REASONING_CHARS", "10")
    monkeypatch.setenv("ANALYST_MAX_ROUNDS", "3")

    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name, "description": "sales facts"})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile: [{"title": "rows", "value": 10}])
    monkeypatch.setattr(agent, "_build_strategist_config", lambda: {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": "strategist-key"})

    prompts = []

    def fake_build_round_prompt(round_num, profile, stats, metadata, compressed_history):
        prompts.append((round_num, compressed_history))
        return f"round {round_num}"

    monkeypatch.setattr(agent, "_build_round_prompt", fake_build_round_prompt)

    strategist_outputs = iter(
        [
            {
                "reasoning": "1234567890ABCDEFGHIJ",
                "response": {
                    "hypotheses": [{"id": "H1", "title": "Volume", "query_description": "Count rows"}],
                    "continue": True,
                },
            },
            {
                "reasoning": "second-round-reasoning",
                "response": {
                    "summary": "done",
                    "findings": [],
                    "recommendations": [],
                    "limitations": [],
                    "continue": False,
                },
            },
        ]
    )
    monkeypatch.setattr(agent, "_call_strategist", lambda prompt, strategist_config: next(strategist_outputs))
    monkeypatch.setattr(
        agent,
        "_extract_queries_from_strategist",
        lambda strategist_output, round_num: strategist_output["response"].get("hypotheses", []),
    )
    monkeypatch.setattr(agent, "_executor_translate_to_sql", lambda query, metadata, api_config: "SELECT 1 AS total")
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 1}] * 150, sql, True, None),
    )

    captured = {}

    def fake_build_expert_report(**kwargs):
        captured.update(kwargs)
        return {
            "success": True,
            "id": "expert-1",
            "table_names": "sales",
            "depth": "expert",
            "summary": "done",
            "insight_count": 1,
            "anomaly_count": 0,
            "failed_step_count": 0,
            "status": "completed",
            "reasoning_traces": kwargs["reasoning_traces"],
            "conversation_chain": kwargs["compressed_history"],
            "steps": kwargs["all_step_results"],
        }

    monkeypatch.setattr(agent, "_build_expert_report", fake_build_expert_report)

    report = agent.analyze_table("sales", depth="expert", resource_name="OpenAI")

    assert report["depth"] == "expert"
    assert prompts[0] == (1, [])
    assert len(prompts[1][1]) == 1
    assert captured["all_step_results"][0]["row_count"] == 100
    assert len(captured["all_step_results"][0]["data"]) == 100
    assert captured["reasoning_traces"][0]["trace"].startswith("1234567890")
    assert "truncated" in captured["reasoning_traces"][0]["trace"]


def test_analyze_table_expert_supports_three_round_synthesis_without_followup_queries(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setenv("ANALYST_MAX_ROUNDS", "3")
    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile: [])
    monkeypatch.setattr(agent, "_build_strategist_config", lambda: {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": "strategist-key"})

    strategist_outputs = iter(
        [
            {
                "reasoning": "",
                "response": {
                    "hypotheses": [{"id": "H1", "title": "Volume", "query_description": "Count rows"}],
                    "continue": True,
                },
            },
            {
                "reasoning": "",
                "response": {
                    "assessments": [{"hypothesis_id": "H1", "verdict": "confirmed"}],
                    "follow_ups": [],
                    "continue": True,
                },
            },
            {
                "reasoning": "",
                "response": {
                    "summary": "final synthesis",
                    "findings": [{"title": "Volume stable", "hypothesis_id": "H1"}],
                    "anomalies": [],
                    "recommendations": [],
                    "limitations": [],
                    "continue": False,
                },
            },
        ]
    )
    monkeypatch.setattr(agent, "_call_strategist", lambda prompt, strategist_config: next(strategist_outputs))
    monkeypatch.setattr(
        agent,
        "_extract_queries_from_strategist",
        lambda strategist_output, round_num: strategist_output["response"].get("hypotheses", []),
    )
    monkeypatch.setattr(agent, "_executor_translate_to_sql", lambda query, metadata, api_config: "SELECT 1 AS total")
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 1}], sql, True, None),
    )

    report = agent.analyze_table_expert("sales")

    assert report["summary"] == "final synthesis"
    assert len(report["conversation_chain"]) == 3
    assert report["steps"][0]["round"] == 1


def test_build_evidence_chains_scopes_history_per_finding():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    chains = agent._build_evidence_chains(
        [
            {"title": "Revenue decline", "hypothesis_id": "H1"},
            {"title": "Profit stable", "hypothesis_id": "H2"},
        ],
        [
            {
                "round": 1,
                "strategist_output": {
                    "hypotheses": [
                        {"id": "H1", "title": "Revenue"},
                        {"id": "H2", "title": "Profit"},
                    ]
                },
                "results": [],
            },
            {
                "round": 2,
                "strategist_output": {
                    "assessments": [
                        {"hypothesis_id": "H1", "verdict": "confirmed"},
                        {"hypothesis_id": "H2", "verdict": "refuted"},
                    ],
                    "follow_ups": [
                        {"id": "F1", "hypothesis_id": "H1", "reason": "Verify revenue"},
                        {"id": "F2", "hypothesis_id": "H2", "reason": "Verify profit"},
                    ],
                },
                "results": [],
            },
        ],
    )

    assert chains[0]["hypotheses"] == [{"id": "H1", "title": "Revenue"}]
    assert chains[0]["assessments"] == [{"hypothesis_id": "H1", "verdict": "confirmed"}]
    assert chains[0]["follow_ups"] == [{"id": "F1", "hypothesis_id": "H1", "reason": "Verify revenue"}]
    assert chains[1]["hypotheses"] == [{"id": "H2", "title": "Profit"}]
    assert chains[1]["assessments"] == [{"hypothesis_id": "H2", "verdict": "refuted"}]
    assert chains[1]["follow_ups"] == [{"id": "F2", "hypothesis_id": "H2", "reason": "Verify profit"}]


def test_get_report_and_latest_report_hide_reasoning_by_default():
    class ReportDB(RecordingDB):
        def execute_query(self, sql, params=None):
            payload = {
                "success": True,
                "id": "report-1",
                "table_names": "sales",
                "reasoning_traces": [{"round": 1, "trace": "secret"}],
            }
            return [{"report_json": payload}]

    agent = AnalystAgent(ReportDB(), lambda **kwargs: {"api_key": "key"})

    detail = agent.get_report("report-1")
    with_reasoning = agent.get_report("report-1", include_reasoning=True)
    latest = agent.get_latest_report("sales")

    assert "reasoning_traces" not in detail
    assert with_reasoning["reasoning_traces"][0]["trace"] == "secret"
    assert "reasoning_traces" not in latest
