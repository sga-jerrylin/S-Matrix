from table_admin_agent import TableAdminAgent
from llm_executor import LLMExecutor, escape_sql_string


class FakeTableAdminDb:
    def get_table_schema(self, table_name):
        return [
            {"Field": "institution_name", "Type": "VARCHAR"},
            {"Field": "city", "Type": "VARCHAR"},
            {"Field": "year", "Type": "INT"},
        ]

    def execute_query(self, sql, params=None):
        if "LIMIT 3" in sql:
            return [{"institution_name": "绿色未来", "city": "广州", "year": 2022}]
        return []


def test_generate_sql_for_subtask_retries_with_single_table_prompt(monkeypatch):
    prompts = []

    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def generate_sql(self, question):
            return "SELECT COUNT(*) FROM `中国环保公益组织现状调研数据2022_2024`"

        def extract_table_names(self, sql):
            if "中国环保公益组织现状调研数据2022_2024" in sql:
                return ["中国环保公益组织现状调研数据2022_2024"]
            return ["institutions"]

        def submit_prompt(self, prompt):
            prompts.append(prompt)
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    sql = agent.generate_sql_for_subtask(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    assert sql == "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"
    assert prompts
    assert "ONLY use table `institutions`" in prompts[0]


def test_generate_sql_for_subtask_uses_single_table_prompt_first(monkeypatch):
    prompts = []

    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def generate_sql(self, question):
            raise AssertionError("generic multi-table prompt should not be used for targeted subtasks")

        def extract_table_names(self, sql):
            return ["institutions"] if "institutions" in sql else []

        def submit_prompt(self, prompt):
            prompts.append(prompt)
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

        def get_similar_question_sql(self, question, **kwargs):
            return []

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    sql = agent.generate_sql_for_subtask(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    assert sql == "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"
    assert len(prompts) == 1
    assert "ONLY use table `institutions`" in prompts[0]


def test_generate_sql_for_subtask_with_trace_returns_retrieval_contract(monkeypatch):
    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def extract_table_names(self, sql):
            return ["institutions"] if "institutions" in sql else []

        def submit_prompt(self, prompt):
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

        def get_similar_question_sql_with_trace(self, question, **kwargs):
            return {
                "examples": [
                    {"question": "广州有多少机构？", "sql": "SELECT COUNT(*) FROM `institutions`"},
                ],
                "trace": {
                    "memory_hit": True,
                    "fallback_used": False,
                    "selected_source": "query_history.vector",
                    "source_labels": ["query_history.vector"],
                },
            }

        def get_related_ddl_with_trace(self, question, **kwargs):
            return {
                "items": ["CREATE TABLE `institutions` (`city` VARCHAR(32));"],
                "trace": {
                    "count": 1,
                    "source_labels": ["information_schema.columns"],
                    "cache_hit": True,
                },
            }

        def get_related_documentation_with_trace(self, question, **kwargs):
            return {
                "items": ["Table: institutions\nDescription: organization table"],
                "trace": {
                    "count": 1,
                    "source_labels": ["_sys_table_registry"],
                },
            }

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    result = agent.generate_sql_for_subtask_with_trace(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    assert result["sql"] == "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"
    trace = result["trace"]
    assert trace["example_count"] == 1
    assert trace["ddl_count"] == 1
    assert trace["documentation_count"] == 1
    assert trace["memory_hit"] is True
    assert trace["candidate_memory_hit"] is True
    assert trace["memory_fallback_used"] is False
    assert trace["memory_source"] == "query_history.vector"
    assert "query_history.vector" in trace["retrieval_source_labels"]
    assert "query_history.vector" in trace["candidate_retrieval_source_labels"]
    assert "information_schema.columns" in trace["retrieval_source_labels"]
    assert "_sys_table_registry" in trace["retrieval_source_labels"]
    assert trace["phases"] == ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"]


def test_generate_sql_for_subtask_with_trace_downgrades_when_history_retrieval_fails(monkeypatch):
    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def extract_table_names(self, sql):
            return ["institutions"] if "institutions" in sql else []

        def submit_prompt(self, prompt):
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

        def get_similar_question_sql_with_trace(self, question, **kwargs):
            raise RuntimeError("history table missing")

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    result = agent.generate_sql_for_subtask_with_trace(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    assert result["sql"] == "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"
    trace = result["trace"]
    assert trace["example_count"] == 0
    assert trace["memory_hit"] is False
    assert trace["candidate_memory_hit"] is False
    assert trace["memory_source"] == ""
    assert trace["retrieval_source_labels"] == []


def test_generate_sql_for_subtask_with_trace_distinguishes_candidate_and_used_memory(monkeypatch):
    class FakeVanna:
        def __init__(self, *args, **kwargs):
            pass

        def extract_table_names(self, sql):
            if "activities" in sql:
                return ["activities"]
            if "institutions" in sql:
                return ["institutions"]
            return []

        def submit_prompt(self, prompt):
            return "SELECT COUNT(*) FROM `institutions` WHERE `city` LIKE '%广州%'"

        def auto_fuzzy_match_locations(self, sql):
            return sql

        def get_similar_question_sql_with_trace(self, question, **kwargs):
            return {
                "examples": [
                    {"question": "广州有多少活动？", "sql": "SELECT COUNT(*) FROM `activities`"},
                ],
                "trace": {
                    "memory_hit": True,
                    "fallback_used": False,
                    "selected_source": "query_history.vector",
                    "source_labels": ["query_history.vector"],
                },
            }

        def get_related_ddl_with_trace(self, question, **kwargs):
            return {"items": [], "trace": {"count": 0, "source_labels": []}}

        def get_related_documentation_with_trace(self, question, **kwargs):
            return {"items": [], "trace": {"count": 0, "source_labels": []}}

    monkeypatch.setattr("table_admin_agent.VannaDorisOpenAI", FakeVanna)

    agent = TableAdminAgent(doris_client_override=FakeTableAdminDb())
    result = agent.generate_sql_for_subtask_with_trace(
        {"table": "institutions", "question": "广州有多少机构？"},
        "广州有多少机构？",
        {"api_key": "test-key", "model": "deepseek-chat", "base_url": "https://api.deepseek.com"},
    )

    trace = result["trace"]
    assert trace["example_count"] == 0
    assert trace["memory_hit"] is False
    assert trace["candidate_memory_hit"] is True
    assert trace["memory_source"] == ""
    assert "query_history.vector" not in trace["retrieval_source_labels"]
    assert "query_history.vector" in trace["candidate_retrieval_source_labels"]


class _FakeLLMDb:
    def __init__(self):
        self.sql_history = []

    def execute_query(self, sql, params=None):
        self.sql_history.append(sql)
        return [{"llm_response": "SELECT 1"}]


def test_escape_sql_string_escapes_quotes_and_backslashes():
    assert escape_sql_string("a'b\\c") == "a''b\\\\c"


def test_doris_resource_mode_escapes_prompt_in_ai_generate_sql():
    fake_db = _FakeLLMDb()
    executor = LLMExecutor(
        doris_client=fake_db,
        api_config={
            "llm_execution_mode": "doris_resource",
            "resource_name": "ds",
            "model": "deepseek-v4-flash",
        },
    )

    result = executor.call(
        prompt="it's a test \\ prompt",
        system_prompt="sys'prompt",
        temperature=0.1,
        max_tokens=100,
    )

    assert result == "SELECT 1"
    assert fake_db.sql_history
    sql = fake_db.sql_history[0]
    assert "AI_GENERATE('ds'," in sql
    assert "sys''prompt" in sql
    assert "it''s a test \\\\ prompt" in sql


def test_doris_resource_timeout_guard_sets_session_timeout(monkeypatch):
    captured = {}
    executed_sql = []

    class FakeCursor:
        def execute(self, sql, params=None):
            executed_sql.append((sql, params))

        def fetchall(self):
            return [{"llm_response": "SELECT 1"}]

        def close(self):
            return None

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self, *args, **kwargs):
            captured["cursor_args"] = args
            captured["cursor_kwargs"] = kwargs
            return self.cursor_obj

        def close(self):
            captured["closed"] = True

    def fake_connect(**kwargs):
        captured["connect_kwargs"] = kwargs
        return FakeConnection()

    class FakeConfiguredDb:
        def __init__(self):
            self.config = {
                "host": "127.0.0.1",
                "port": 9030,
                "user": "root",
                "password": "",
                "database": "doris_db",
                "charset": "utf8mb4",
            }

        def execute_query(self, sql, params=None):
            raise AssertionError("timeout-guarded path should not use shared execute_query")

    monkeypatch.setattr("llm_executor.pymysql.connect", fake_connect)

    executor = LLMExecutor(
        doris_client=FakeConfiguredDb(),
        api_config={
            "llm_execution_mode": "doris_resource",
            "resource_name": "ds",
            "resource_timeout_seconds": 5,
            "resource_query_timeout_seconds": 7,
        },
    )
    result = executor.call(prompt="ping", system_prompt="sys")

    assert result == "SELECT 1"
    assert captured["connect_kwargs"]["read_timeout"] == 5
    assert captured["connect_kwargs"]["write_timeout"] == 5
    assert captured["connect_kwargs"]["connect_timeout"] == 5
    assert any("SET query_timeout = 7" in sql for sql, _ in executed_sql)
    assert captured.get("closed") is True


def test_doris_resource_timeout_is_mapped_to_resource_timeout_error():
    executor = LLMExecutor(
        doris_client=_FakeLLMDb(),
        api_config={
            "llm_execution_mode": "doris_resource",
            "resource_name": "ds",
            "resource_timeout_seconds": 3,
        },
    )
    executor._execute_doris_resource_query_with_timeouts = lambda *args, **kwargs: (_ for _ in ()).throw(
        TimeoutError("read timed out")
    )

    try:
        executor.call(prompt="ping", system_prompt="sys")
        assert False, "expected timeout-mapped LLMExecutionError"
    except Exception as exc:
        assert exc.__class__.__name__ == "LLMExecutionError"
        assert getattr(exc, "error_code", "") == "resource_timeout"


def test_timeout_guard_cancels_connection_on_timeout(monkeypatch):
    class TimeoutCursor:
        def execute(self, sql, params=None):
            if str(sql).startswith("SET query_timeout"):
                return None
            raise TimeoutError("read timed out")

        def fetchall(self):
            return []

        def close(self):
            return None

    class TimeoutConnection:
        def __init__(self):
            self.cursor_obj = TimeoutCursor()

        def thread_id(self):
            return 86

        def cursor(self, *args, **kwargs):
            return self.cursor_obj

        def close(self):
            return None

    class FakeConfiguredDb:
        def __init__(self):
            self.config = {
                "host": "127.0.0.1",
                "port": 9030,
                "user": "root",
                "password": "",
                "database": "doris_db",
                "charset": "utf8mb4",
            }

        def execute_query(self, sql, params=None):
            raise AssertionError("shared execute_query should not run in timeout-guarded path")

    monkeypatch.setattr("llm_executor.pymysql.connect", lambda **kwargs: TimeoutConnection())

    executor = LLMExecutor(
        doris_client=FakeConfiguredDb(),
        api_config={
            "llm_execution_mode": "doris_resource",
            "resource_name": "ds",
            "resource_timeout_seconds": 5,
            "resource_query_timeout_seconds": 5,
        },
    )
    cancelled = {}
    executor._cancel_doris_connection = lambda config, connection_id: cancelled.setdefault("connection_id", connection_id)

    try:
        executor.call(prompt="ping", system_prompt="sys")
        assert False, "expected timeout error"
    except Exception as exc:
        assert exc.__class__.__name__ == "LLMExecutionError"
        assert getattr(exc, "error_code", "") == "resource_timeout"
        assert cancelled.get("connection_id") == 86


def test_cancel_doris_connection_skips_kill_when_target_not_active(monkeypatch):
    class FakeConfiguredDb:
        def __init__(self):
            self.config = {
                "host": "127.0.0.1",
                "port": 9030,
                "user": "root",
                "password": "",
                "database": "doris_db",
                "charset": "utf8mb4",
            }

    executor = LLMExecutor(
        doris_client=FakeConfiguredDb(),
        api_config={},
    )
    executor._is_doris_connection_active = lambda config, connection_id: False
    executor._open_doris_connection = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("inactive connection should not trigger KILL")
    )

    executor._cancel_doris_connection(executor.doris_client.config, 86)


def test_cancel_doris_connection_retries_until_target_disappears(monkeypatch, caplog):
    class FakeConfiguredDb:
        def __init__(self):
            self.config = {
                "host": "127.0.0.1",
                "port": 9030,
                "user": "root",
                "password": "",
                "database": "doris_db",
                "charset": "utf8mb4",
            }

    class FakeCursor:
        def __init__(self, kill_sql_log):
            self.kill_sql_log = kill_sql_log

        def execute(self, sql, params=None):
            self.kill_sql_log.append(str(sql))
            if len(self.kill_sql_log) == 1:
                raise RuntimeError("(0, '')")
            return None

        def close(self):
            return None

    class FakeConnection:
        def __init__(self, kill_sql_log):
            self.cursor_obj = FakeCursor(kill_sql_log)

        def cursor(self, *args, **kwargs):
            return self.cursor_obj

        def close(self):
            return None

    executor = LLMExecutor(
        doris_client=FakeConfiguredDb(),
        api_config={},
    )
    active_states = [True, True, False]
    kill_sql = []
    executor._is_doris_connection_active = lambda config, connection_id: active_states.pop(0)
    executor._open_doris_connection = lambda *args, **kwargs: FakeConnection(kill_sql)
    monkeypatch.setattr("llm_executor.time.sleep", lambda *_args, **_kwargs: None)

    with caplog.at_level("WARNING"):
        executor._cancel_doris_connection(executor.doris_client.config, 86)

    assert kill_sql == ["KILL 86", "KILL 86"]
    assert "Failed to cancel Doris connection" not in caplog.text


def test_direct_api_mode_still_uses_openai_compatible_path(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "SELECT COUNT(*) FROM `institutions`"}}]}

    captured = {}

    def fake_post(url, headers=None, json=None, timeout=0):
        captured["url"] = url
        captured["headers"] = headers or {}
        captured["payload"] = json or {}
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("llm_executor.requests.post", fake_post)
    executor = LLMExecutor(
        doris_client=_FakeLLMDb(),
        api_config={
            "llm_execution_mode": "direct_api",
            "api_key": "test-key",
            "model": "deepseek-chat",
            "base_url": "https://api.deepseek.com",
        },
    )

    sql = executor.call(
        prompt="Generate SQL",
        system_prompt="You generate SQL",
        temperature=0.2,
        max_tokens=512,
    )

    assert sql == "SELECT COUNT(*) FROM `institutions`"
    assert captured["url"] == "https://api.deepseek.com/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer test-key"
    assert captured["payload"]["model"] == "deepseek-chat"
    assert captured["payload"]["messages"][0]["role"] == "system"
    assert captured["payload"]["messages"][1]["role"] == "user"
