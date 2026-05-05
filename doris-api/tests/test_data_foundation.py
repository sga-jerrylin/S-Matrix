from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main
from datasource_handler import DataSourceHandler
from db import DorisClient


class RecordingFoundationDb:
    def __init__(self):
        self.validator = DorisClient()
        self.executed_updates = []
        self.registry_exists = False
        self.source_exists = False

    def validate_identifier(self, identifier):
        return self.validator.validate_identifier(identifier)

    def execute_query(self, sql, params=None):
        if "_sys_table_registry" in sql:
            return [{"table_name": params[0]}] if self.registry_exists else []
        if "_sys_table_sources" in sql:
            return [{"table_name": params[0]}] if self.source_exists else []
        return []

    def execute_update(self, sql, params=None):
        self.executed_updates.append((sql, params))
        if "INSERT INTO `_sys_table_registry`" in sql:
            self.registry_exists = True
        if "INSERT INTO `_sys_table_sources`" in sql:
            self.source_exists = True
        return 1


def test_finalize_table_ingestion_records_source_and_resets_assets():
    handler = DataSourceHandler()
    handler.db = RecordingFoundationDb()

    result = handler.finalize_table_ingestion(
        "institutions",
        "excel",
        replace_existing=True,
        origin_kind="upload",
        origin_label="环保组织.xlsx",
        origin_path="环保组织.xlsx",
        ingest_mode="replace",
        last_rows=12,
    )

    assert result["success"] is True
    executed_sql = [sql for sql, _ in handler.db.executed_updates]
    assert any("DELETE FROM `_sys_table_metadata`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_table_agents`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_field_catalog`" in sql for sql in executed_sql)
    assert any("DELETE FROM `_sys_table_relationships`" in sql for sql in executed_sql)
    assert any("INSERT INTO `_sys_table_registry`" in sql for sql in executed_sql)
    assert any("INSERT INTO `_sys_table_sources`" in sql for sql in executed_sql)


def test_get_table_profile_aggregates_registry_metadata_and_relationships():
    handler = DataSourceHandler()
    handler.db = SimpleNamespace(
        get_table_schema=lambda table_name: [{"Field": "机构名称", "Type": "VARCHAR"}]
    )
    handler._list_table_registry_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "display_name": "机构基础表",
            "description": "",
            "source_type": "excel",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:00:00",
            "auto_description": "机构主表",
            "analyzed_at": "2026-04-19 00:10:00",
            "columns_info": {"机构名称": "机构正式名称"},
            "sample_queries": ["有哪些机构"],
            "agent_config": {},
            "source": {
                "table_name": "institutions",
                "source_type": "excel",
                "origin_kind": "upload",
                "origin_label": "环保组织.xlsx",
                "analysis_status": "ready",
                "last_ingested_at": "2026-04-19 00:00:00",
                "last_analyzed_at": "2026-04-19 00:10:00",
            },
            "metadata_status": "ready",
            "metadata_ready": True,
            "relationship_count": 1,
        }
    ]
    handler._get_table_source_rows_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "source_type": "excel",
            "origin_kind": "upload",
            "origin_id": "",
            "origin_label": "环保组织.xlsx",
            "origin_path": "环保组织.xlsx",
            "origin_table": "",
            "sync_task_id": "",
            "ingest_mode": "replace",
            "last_rows": 12,
            "analysis_status": "ready",
            "last_ingested_at": "2026-04-19 00:00:00",
            "last_analyzed_at": "2026-04-19 00:10:00",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:10:00",
        }
    ]
    handler._get_metadata_rows_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "description": "机构主表",
            "columns_info": '{"机构名称":"机构正式名称"}',
            "sample_queries": '["有哪些机构"]',
            "analyzed_at": "2026-04-19 00:10:00",
            "source_type": "excel",
        }
    ]
    handler._get_field_catalog_rows_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "field_name": "机构名称",
            "field_type": "text",
            "enum_values": "[]",
            "value_range": None,
        }
    ]
    handler.list_relationship_models = lambda tables=None: [
        {
            "id": "rel-1",
            "table_a": "institutions",
            "column_a": "机构名称",
            "table_b": "activities",
            "column_b": "机构名称",
            "relation_type": "logical",
            "relation_type_label": "逻辑关联",
            "relation_label": "机构基础表.机构名称 -> 活动参与表.机构名称",
        }
    ]

    profile = handler.get_table_profile("institutions")

    assert profile["table_name"] == "institutions"
    assert profile["registry"]["display_name"] == "机构基础表"
    assert profile["metadata"]["status"] == "ready"
    assert profile["metadata"]["fields"][0]["field_name"] == "机构名称"
    assert profile["source"]["origin_kind"] == "upload"
    assert profile["stats"]["relationship_count"] == 1


def test_foundation_tables_endpoint_returns_profiles(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.list_foundation_tables = AsyncMock(
        return_value=[
            {
                "table_name": "institutions",
                "registry": {"display_name": "机构基础表"},
                "metadata": {"status": "ready"},
                "relationships": [],
                "stats": {"field_count": 1},
            }
        ]
    )

    client = TestClient(main.app)
    response = client.get("/api/foundation/tables", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["tables"][0]["table_name"] == "institutions"


def test_relationships_endpoint_returns_stable_models(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.list_relationship_models_async = AsyncMock(
        return_value=[
            {
                "id": "rel-1",
                "table_a": "institutions",
                "table_b": "activities",
                "relation_type": "logical",
                "table_b_display_name": "活动参与表",
                "column_b_display_name": "活动机构ID",
                "relation_label": "机构基础表.机构ID -> 活动参与表.活动机构ID",
            }
        ]
    )

    client = TestClient(main.app)
    response = client.get("/api/relationships?tables=institutions", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["relationships"][0]["id"] == "rel-1"
    assert payload["relationships"][0]["table_b_display_name"] == "活动参与表"
    assert payload["relationships"][0]["column_b_display_name"] == "活动机构ID"


def test_list_relationship_models_fills_counterparty_display_context_on_single_table_filter():
    handler = DataSourceHandler()
    handler.db = SimpleNamespace(
        get_table_schema=lambda table_name: (
            [{"Field": "id", "Type": "BIGINT"}]
            if table_name == "institutions"
            else [{"Field": "org_id", "Type": "BIGINT"}]
        )
    )

    registry_map = {
        "institutions": {
            "table_name": "institutions",
            "display_name": "机构基础表",
            "description": "",
            "source_type": "excel",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:00:00",
            "auto_description": "",
            "analyzed_at": "2026-04-19 00:10:00",
            "columns_info": {"id": "机构ID"},
            "sample_queries": [],
            "agent_config": {},
            "source": None,
            "metadata_status": "ready",
            "metadata_ready": True,
            "relationship_count": 1,
        },
        "activities": {
            "table_name": "activities",
            "display_name": "活动参与表",
            "description": "",
            "source_type": "database_sync",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:00:00",
            "auto_description": "",
            "analyzed_at": "2026-04-19 00:10:00",
            "columns_info": {"org_id": "活动机构ID"},
            "sample_queries": [],
            "agent_config": {},
            "source": None,
            "metadata_status": "ready",
            "metadata_ready": True,
            "relationship_count": 1,
        },
    }

    handler._list_table_registry_sync = lambda table_names=None: [
        registry_map[name]
        for name in (table_names or list(registry_map.keys()))
        if name in registry_map
    ]
    handler.list_relationships = lambda tables=None: [
        {
            "id": "rel-1",
            "table_a": "institutions",
            "column_a": "id",
            "table_b": "activities",
            "column_b": "org_id",
            "rel_type": "logical",
            "confidence": 1.0,
            "is_manual": 1,
            "created_at": "2026-04-19 00:10:00",
        }
    ]
    handler._get_metadata_rows_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "columns_info": '{"id":"机构ID"}',
        },
        {
            "table_name": "activities",
            "columns_info": '{"org_id":"活动机构ID"}',
        },
    ]
    handler._get_field_catalog_rows_sync = lambda table_names=None: []

    relationships = handler.list_relationship_models(["institutions"])

    assert len(relationships) == 1
    rel = relationships[0]
    assert rel["table_b_display_name"] == "活动参与表"
    assert rel["column_b_display_name"] == "活动机构ID"
    assert rel["relation_label"] == "机构基础表.机构ID -> 活动参与表.活动机构ID"


def test_get_table_profile_relationships_keep_counterparty_human_labels():
    handler = DataSourceHandler()
    handler.db = SimpleNamespace(
        get_table_schema=lambda table_name: (
            [{"Field": "id", "Type": "BIGINT"}]
            if table_name == "institutions"
            else [{"Field": "org_id", "Type": "BIGINT"}]
        )
    )

    registry_map = {
        "institutions": {
            "table_name": "institutions",
            "display_name": "机构基础表",
            "description": "",
            "source_type": "excel",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:00:00",
            "auto_description": "机构主表",
            "analyzed_at": "2026-04-19 00:10:00",
            "columns_info": {"id": "机构ID"},
            "sample_queries": [],
            "agent_config": {},
            "source": None,
            "metadata_status": "ready",
            "metadata_ready": True,
            "relationship_count": 1,
        },
        "activities": {
            "table_name": "activities",
            "display_name": "活动参与表",
            "description": "",
            "source_type": "database_sync",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:00:00",
            "auto_description": "",
            "analyzed_at": "2026-04-19 00:10:00",
            "columns_info": {"org_id": "活动机构ID"},
            "sample_queries": [],
            "agent_config": {},
            "source": None,
            "metadata_status": "ready",
            "metadata_ready": True,
            "relationship_count": 1,
        },
    }

    handler._list_table_registry_sync = lambda table_names=None: [
        registry_map[name]
        for name in (table_names or list(registry_map.keys()))
        if name in registry_map
    ]
    handler._get_table_source_rows_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "source_type": "excel",
            "origin_kind": "upload",
            "origin_id": "",
            "origin_label": "机构.xlsx",
            "origin_path": "机构.xlsx",
            "origin_table": "",
            "sync_task_id": "",
            "ingest_mode": "replace",
            "last_rows": 5,
            "analysis_status": "ready",
            "last_ingested_at": "2026-04-19 00:00:00",
            "last_analyzed_at": "2026-04-19 00:10:00",
            "created_at": "2026-04-19 00:00:00",
            "updated_at": "2026-04-19 00:10:00",
        }
    ]
    handler._get_metadata_rows_sync = lambda table_names=None: [
        {
            "table_name": "institutions",
            "description": "机构主表",
            "columns_info": '{"id":"机构ID"}',
            "sample_queries": "[]",
            "analyzed_at": "2026-04-19 00:10:00",
            "source_type": "excel",
        },
        {
            "table_name": "activities",
            "description": "活动表",
            "columns_info": '{"org_id":"活动机构ID"}',
            "sample_queries": "[]",
            "analyzed_at": "2026-04-19 00:10:00",
            "source_type": "database_sync",
        },
    ]
    handler._get_field_catalog_rows_sync = lambda table_names=None: []
    handler.list_relationships = lambda tables=None: [
        {
            "id": "rel-1",
            "table_a": "institutions",
            "column_a": "id",
            "table_b": "activities",
            "column_b": "org_id",
            "rel_type": "logical",
            "confidence": 1.0,
            "is_manual": 1,
            "created_at": "2026-04-19 00:10:00",
        }
    ]

    profile = handler.get_table_profile("institutions")

    assert profile is not None
    assert profile["relationships"][0]["table_b_display_name"] == "活动参与表"
    assert profile["relationships"][0]["column_b_display_name"] == "活动机构ID"
    assert profile["relationships"][0]["relation_label"] == "机构基础表.机构ID -> 活动参与表.活动机构ID"


def test_sync_endpoint_schedules_metadata_analysis_with_resolved_target(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    main.datasource_handler.sync_table = AsyncMock(
        return_value={
            "success": True,
            "source_table": "orders",
            "target_table": "orders",
            "rows_synced": 0,
            "sync_capability": {
                "requested_strategy": "full",
                "effective_strategy": "full",
                "fallback_to_full": False,
                "fallback_reason": None,
                "explanation": "Requested full sync.",
                "window": {"start": None, "end": None},
            },
        }
    )
    main._analyze_table_async = AsyncMock(return_value={"success": True})

    scheduled = []

    def fake_create_task(coro):
        scheduled.append(coro)
        coro.close()
        return SimpleNamespace(done=lambda: False)

    monkeypatch.setattr(main.asyncio, "create_task", fake_create_task)

    client = TestClient(main.app)
    response = client.post(
        "/api/datasource/ds-1/sync",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"source_table": "orders"},
    )

    assert response.status_code == 200
    main.datasource_handler.sync_table.assert_awaited_once_with(
        ds_id="ds-1",
        source_table="orders",
        target_table=None,
        sync_strategy="full",
        incremental_time_field=None,
        incremental_start=None,
        incremental_end=None,
    )
    main._analyze_table_async.assert_called_once_with("orders", "database_sync")
    assert len(scheduled) == 1


def test_table_analyze_endpoint_accepts_resource_name(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.metadata_analyzer.analyze_table_async = AsyncMock(
        return_value={
            "success": True,
            "table_name": "orders",
            "analysis": {"display_name": "订单主表", "description": "订单核心事实表"},
            "llm_execution_mode": "doris_resource",
            "resource_name": "openrutor",
        }
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/tables/orders/analyze?resource_name=openrutor",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["resource_name"] == "openrutor"
    main.metadata_analyzer.analyze_table_async.assert_awaited_once_with(
        "orders",
        "manual",
        resource_name="openrutor",
    )


def test_table_analyze_endpoint_returns_structured_failure(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.metadata_analyzer.analyze_table_async = AsyncMock(
        return_value={
            "success": False,
            "status_code": 400,
            "error": {
                "code": "resource_not_found",
                "message": "LLM resource 'openrutor' not found",
                "details": {"resource_name": "openrutor"},
            },
        }
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/tables/orders/analyze?resource_name=openrutor",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"]["success"] is False
    assert payload["detail"]["error"]["code"] == "resource_not_found"
