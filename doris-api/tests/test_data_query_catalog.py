from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main
from datasource_handler import DataSourceHandler
from handlers import ActionHandler


class RecordingQueryDb:
    def __init__(self):
        self.sql = None

    def validate_identifier(self, identifier):
        return f"`{identifier}`"

    def execute_query(self, sql, params=None):
        self.sql = sql
        return [{"机构基础表_机构名称": "绿色江南"}]


def test_query_catalog_endpoint_returns_business_semantic_tables(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.list_query_catalog = AsyncMock(
        return_value=[
            {
                "table_name": "institutions",
                "display_name": "机构基础表",
                "description": "机构主表",
                "fields": [
                    {
                        "field_name": "机构名称",
                        "display_name": "机构名称",
                        "description": "机构正式名称",
                        "field_type": "VARCHAR",
                        "semantic": "text",
                        "semantic_label": "文本字段",
                    }
                ],
                "relationships": [
                    {
                        "related_table_name": "activities",
                        "related_display_name": "活动参与表",
                        "relation_description": "通过“机构ID = 机构ID”关联到“活动参与表”",
                    }
                ],
            }
        ]
    )

    client = TestClient(main.app)
    response = client.get("/api/query/catalog", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["tables"][0]["display_name"] == "机构基础表"
    assert payload["tables"][0]["relationships"][0]["related_display_name"] == "活动参与表"


def test_handle_query_supports_semantic_join_selection():
    handler = ActionHandler()
    handler.db = RecordingQueryDb()

    result = handler.handle_query(
        {
            "table": "institutions",
            "join_table": "activities",
            "join_left_column": "institution_id",
            "join_right_column": "institution_id",
            "selected_fields": [
                {
                    "table_name": "institutions",
                    "field_name": "机构名称",
                    "label": "机构基础表 机构名称",
                },
                {
                    "table_name": "activities",
                    "field_name": "活动名称",
                    "label": "活动参与表 活动名称",
                },
            ],
            "limit": 20,
        }
    )

    assert result["success"] is True
    assert "LEFT JOIN `activities` AS rel" in handler.db.sql
    assert "base.`institution_id` = rel.`institution_id`" in handler.db.sql
    assert "base.`机构名称` AS `机构基础表_机构名称`" in handler.db.sql
    assert "rel.`活动名称` AS `活动参与表_活动名称`" in handler.db.sql


def test_short_field_display_name_strips_data_type_suffix():
    assert (
        DataSourceHandler._short_field_display_name(
            "枢纽组织",
            "是否为枢纽型组织，数据类型：字符串（可能为空）",
        )
        == "是否为枢纽型组织"
    )
