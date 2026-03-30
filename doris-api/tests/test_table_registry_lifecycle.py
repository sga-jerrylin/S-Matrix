from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main
from datasource_handler import DataSourceHandler
from db import DorisClient


class RecordingRegistryDb:
    def __init__(self):
        self.validator = DorisClient()
        self.executed_updates = []
        self.registry_deleted = False
        self.table_deleted = False

    def validate_identifier(self, identifier):
        return self.validator.validate_identifier(identifier)

    def table_exists(self, table_name):
        return not self.table_deleted

    def execute_update(self, sql, params=None):
        self.executed_updates.append((sql, params))
        if "DELETE FROM `_sys_table_registry`" in sql:
            self.registry_deleted = True
        if "DROP TABLE" in sql:
            self.table_deleted = True
        return 1

    def execute_query(self, sql, params=None):
        if "SHOW TABLES LIKE" in sql:
            return [] if self.table_deleted else [{"Tables_in_doris_db": params[0]}]
        if "_sys_table_registry" in sql:
            return [] if self.registry_deleted else [{"table_name": params[0]}]
        return []


def test_delete_registered_table_drops_physical_table_and_cleans_assets():
    handler = DataSourceHandler()
    handler.db = RecordingRegistryDb()

    result = handler.delete_registered_table("institutions")

    assert result["success"] is True
    executed_sql = [sql for sql, _ in handler.db.executed_updates]
    assert any("DROP TABLE `institutions`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_table_metadata`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_table_agents`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_field_catalog`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_table_relationships`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_table_registry`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_query_history`" in sql for sql in executed_sql)


def test_delete_table_registry_endpoint_calls_handler(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.delete_registered_table_async = AsyncMock(
        return_value={"success": True, "table_name": "institutions"}
    )

    client = TestClient(main.app)
    response = client.delete(
        "/api/table-registry/institutions",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["table_name"] == "institutions"
