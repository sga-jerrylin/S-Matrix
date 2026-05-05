from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main


def _auth_headers():
    return {"X-API-Key": "secret-key", "Content-Type": "application/json"}


def test_list_llm_configs_returns_normalized_resource_fields(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.doris_client.execute_query = lambda sql, params=None: [
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.provider_type", "Value": "DEEPSEEK"},
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.model_name", "Value": "deepseek-chat"},
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.endpoint", "Value": "https://api.deepseek.com/chat/completions"},
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.api_key", "Value": "******"},
    ]

    client = TestClient(main.app)
    response = client.get("/api/llm/config", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["resources"][0]["name"] == "Deepseek"
    assert payload["resources"][0]["provider"] == "DEEPSEEK"
    assert payload["resources"][0]["model"] == "deepseek-chat"
    assert payload["resources"][0]["endpoint"] == "https://api.deepseek.com/chat/completions"
    assert payload["resources"][0]["api_key_configured"] is True


def test_create_llm_config_does_not_return_or_log_api_key(monkeypatch, caplog):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    captured = {}
    main.doris_client.execute_update = lambda sql, params=None: captured.setdefault("sql", sql)

    client = TestClient(main.app)
    with caplog.at_level("INFO", logger=main.__name__):
        response = client.post(
            "/api/llm/config",
            headers=_auth_headers(),
            json={
                "resource_name": "Deepseek",
                "provider_type": "deepseek",
                "endpoint": "https://api.deepseek.com/chat/completions",
                "model_name": "deepseek-chat",
                "api_key": "sk-live-secret",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert "sql" not in payload
    assert "sk-live-secret" not in caplog.text
    assert "CREATE RESOURCE" not in caplog.text
    assert "sk-live-secret" in captured["sql"]
    assert "'ai.dimensions' = 1536" in captured["sql"]


def test_create_llm_config_normalizes_deepseek_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    captured = {}
    main.doris_client.execute_update = lambda sql, params=None: captured.setdefault("sql", sql)

    client = TestClient(main.app)
    response = client.post(
        "/api/llm/config",
        headers=_auth_headers(),
        json={
            "resource_name": "ds",
            "provider_type": "deepseek",
            "endpoint": "https://api.deepseek.com/v1",
            "model_name": "deepseek-v4-pro",
        },
    )

    assert response.status_code == 200
    assert "'ai.endpoint' = 'https://api.deepseek.com/chat/completions'" in captured["sql"]


def test_update_llm_config_uses_alter_resource_and_preserves_key_when_blank(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    captured = {}
    main.doris_client.execute_update = lambda sql, params=None: captured.setdefault("sql", sql)

    client = TestClient(main.app)
    response = client.put(
        "/api/llm/config/Deepseek",
        headers=_auth_headers(),
        json={
            "resource_name": "Deepseek",
            "provider_type": "deepseek",
            "endpoint": "https://api.deepseek.com/chat/completions",
            "model_name": "deepseek-chat",
            "api_key": "",
            "temperature": 0.2,
            "max_tokens": 1024,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["resource_name"] == "Deepseek"
    assert "ALTER RESOURCE 'Deepseek'" in captured["sql"]
    assert "CREATE RESOURCE" not in captured["sql"]
    assert "DROP RESOURCE" not in captured["sql"]
    assert "'type' = 'ai'" not in captured["sql"]
    assert "'ai.dimensions' = 1536" in captured["sql"]
    assert "ai.api_key" not in captured["sql"]
    assert "'ai.temperature' = 0.2" in captured["sql"]
    assert "'ai.max_tokens' = 1024" in captured["sql"]


def test_update_llm_config_filters_invalid_temperature_and_normalizes_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    captured = {}
    main.doris_client.execute_update = lambda sql, params=None: captured.setdefault("sql", sql)

    client = TestClient(main.app)
    response = client.put(
        "/api/llm/config/ds",
        headers=_auth_headers(),
        json={
            "resource_name": "ds",
            "provider_type": "deepseek",
            "endpoint": "https://api.deepseek.com",
            "model_name": "deepseek-v4-flash",
            "temperature": -1,
            "api_key": "",
        },
    )

    assert response.status_code == 200
    assert "'ai.endpoint' = 'https://api.deepseek.com/chat/completions'" in captured["sql"]
    assert "'ai.dimensions' = 1536" in captured["sql"]
    assert "ai.temperature" not in captured["sql"]
    assert "ai.api_key" not in captured["sql"]


def test_test_llm_config_failure_returns_structured_payload(monkeypatch, caplog):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    def fail_query(sql, params=None):
        raise RuntimeError("upstream rejected api_key sk-live-secret")

    main.doris_client.execute_query = fail_query

    client = TestClient(main.app)
    with caplog.at_level("WARNING", logger=main.__name__):
        response = client.post("/api/llm/config/Deepseek/test", headers=_auth_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "success": False,
        "error": "upstream rejected api_key [REDACTED]",
        "message": "LLM resource connection test failed",
        "resource_name": "Deepseek",
    }
    assert "sk-live-secret" not in caplog.text


def test_natural_query_route_uses_selected_resource_config(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "env-deepseek-key")
    captured = {}

    main.resolve_llm_resource_config = lambda resource_name=None: {
        "resource_name": resource_name,
        "provider": "DEEPSEEK",
        "model": "deepseek-chat",
        "base_url": "https://api.deepseek.com",
        "endpoint": "https://api.deepseek.com/chat/completions",
        "api_key_configured": True,
    }
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[{"table_name": "institutions"}])
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    main.PlannerAgent = type(
        "FakePlannerAgent",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "plan": lambda self, question: {
            "intent": "count",
            "tables": ["institutions"],
            "subtasks": [{"table": "institutions", "question": question}],
            "needs_join": False,
        }},
    )
    main.TableAdminAgent = type(
        "FakeTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask": lambda self, subtask, question, api_config=None: (
                captured.setdefault("api_config", dict(api_config or {})),
                "SELECT COUNT(*) AS total FROM `institutions`",
            )[1],
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "coordinate": lambda self, plan, sql_map, relationships=None: next(iter(sql_map.values()))},
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
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 1}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？", "resource_name": "Deepseek"},
    )

    assert response.status_code == 200
    assert captured["api_config"]["resource_name"] == "Deepseek"
    assert captured["api_config"]["model"] == "deepseek-chat"
    assert captured["api_config"]["base_url"] == "https://api.deepseek.com"
    assert captured["api_config"]["api_key"] == "env-deepseek-key"
