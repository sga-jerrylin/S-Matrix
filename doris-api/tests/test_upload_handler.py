from datetime import datetime
from io import BytesIO

import pandas as pd

from metadata_analyzer import MetadataAnalyzer
from upload_handler import ExcelUploadHandler
from db import DorisClient


class RecordingUploadDb:
    def __init__(self):
        self.validator = DorisClient()
        self.executed_updates = []

    def validate_identifier(self, identifier):
        return self.validator.validate_identifier(identifier)

    def table_exists(self, table_name):
        return False

    def execute_update(self, sql, params=None):
        self.executed_updates.append((sql, params))
        return 1

    def get_table_schema(self, table_name):
        return []


class ExistingTableUploadDb(RecordingUploadDb):
    def __init__(self, schema_fields):
        super().__init__()
        self.schema_fields = schema_fields

    def table_exists(self, table_name):
        return True

    def get_table_schema(self, table_name):
        return [{"Field": field} for field in self.schema_fields]


def test_import_excel_sanitizes_complex_table_and_column_names(monkeypatch):
    handler = ExcelUploadHandler()
    handler.db = RecordingUploadDb()
    monkeypatch.setattr(handler, "stream_load", lambda df, table_name: {"Status": "Success", "table_name": table_name})

    frame = pd.DataFrame(
        [
            {
                "机构名称（中文）": "样例机构",
                "2022/收入(万元)": 12.5,
                "城市-省份": "广州/广东",
            }
        ]
    )
    buffer = BytesIO()
    frame.to_excel(buffer, index=False)

    result = handler.import_excel(
        file_content=buffer.getvalue(),
        table_name="中国环保公益组织现状调研数据（2022）",
        create_table_if_not_exists=True,
    )

    assert result["success"] is True
    assert result["table"] == "中国环保公益组织现状调研数据_2022"
    create_table_sql = handler.db.executed_updates[0][0]
    assert "`机构名称_中文`" in create_table_sql
    assert "`col_2022_收入_万元`" in create_table_sql
    assert "`城市_省份`" in create_table_sql


def test_import_excel_replace_mode_recreates_existing_table(monkeypatch):
    handler = ExcelUploadHandler()
    handler.db = ExistingTableUploadDb(["机构名称", "收入"])
    monkeypatch.setattr(handler, "stream_load", lambda df, table_name: {"Status": "Success", "table_name": table_name})

    frame = pd.DataFrame(
        [
            {
                "机构名称": "样例机构",
                "收入": 12.5,
            }
        ]
    )
    buffer = BytesIO()
    frame.to_excel(buffer, index=False)

    result = handler.import_excel(
        file_content=buffer.getvalue(),
        table_name="机构收入表",
        create_table_if_not_exists=True,
        import_mode="replace",
    )

    assert result["success"] is True
    assert result["table_existed"] is True
    assert result["table_replaced"] is True
    executed_sql = [sql for sql, _ in handler.db.executed_updates]
    assert any("DROP TABLE `机构收入表`" in sql for sql in executed_sql)
    assert any("CREATE TABLE IF NOT EXISTS `机构收入表`" in sql for sql in executed_sql)


def test_import_excel_append_mode_keeps_existing_table(monkeypatch):
    handler = ExcelUploadHandler()
    handler.db = ExistingTableUploadDb(["机构名称", "收入"])
    monkeypatch.setattr(handler, "stream_load", lambda df, table_name: {"Status": "Success", "table_name": table_name})

    frame = pd.DataFrame(
        [
            {
                "机构名称": "样例机构",
                "收入": 12.5,
            }
        ]
    )
    buffer = BytesIO()
    frame.to_excel(buffer, index=False)

    result = handler.import_excel(
        file_content=buffer.getvalue(),
        table_name="机构收入表",
        create_table_if_not_exists=True,
        import_mode="append",
    )

    assert result["success"] is True
    assert result["table_existed"] is True
    assert result["table_replaced"] is False
    assert handler.db.executed_updates == []


def test_build_agent_prompt_serializes_datetime_values():
    analyzer = MetadataAnalyzer()

    prompt = analyzer._build_agent_prompt(
        "events",
        {"description": "事件表", "columns_info": {"event_time": "事件时间"}},
        [{"event_time": datetime(2026, 3, 29, 12, 34, 56)}],
    )

    assert "2026-03-29 12:34:56" in prompt
