from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
import pytest

from conftest import reload_main
import datasource_handler as datasource_handler_module
from datasource_handler import DataSourceHandler
from db import DorisClient


class MetricFoundationDb:
    def __init__(self):
        self.validator = DorisClient()
        self.metrics = {}
        self.last_series_query = None
        self.parseability_by_field = {
            "event_time": {"non_null_count": 4, "unparseable_count": 2},
            "order_date": {"non_null_count": 10, "unparseable_count": 0},
            "region": {"non_null_count": 10, "unparseable_count": 10},
        }

    def validate_identifier(self, identifier):
        return self.validator.validate_identifier(identifier)

    def get_table_schema(self, table_name):
        if table_name == "orders":
            return [
                {"Field": "order_date", "Type": "DATETIME"},
                {"Field": "amount", "Type": "DECIMAL(18,2)"},
                {"Field": "region", "Type": "VARCHAR(32)"},
                {"Field": "channel", "Type": "VARCHAR(32)"},
                {"Field": "customer_id", "Type": "BIGINT"},
            ]
        if table_name == "orders_text_time":
            return [
                {"Field": "event_time", "Type": "VARCHAR(64)"},
                {"Field": "amount", "Type": "DECIMAL(18,2)"},
                {"Field": "region", "Type": "VARCHAR(32)"},
            ]
        return []

    def execute_query(self, sql, params=None):
        normalized_sql = " ".join(str(sql).split())
        params = params or ()

        if "SELECT metric_key FROM `_sys_metric_definitions` WHERE metric_key = %s LIMIT 1" in normalized_sql:
            metric_key = params[0]
            if metric_key in self.metrics:
                return [{"metric_key": metric_key}]
            return []

        if "SELECT * FROM `_sys_metric_definitions` WHERE metric_key = %s LIMIT 1" in normalized_sql:
            metric_key = params[0]
            row = self.metrics.get(metric_key)
            return [dict(row)] if row else []

        if "TRY_CAST(sampled_time AS DATETIME)" in normalized_sql and "AS sampled_time" in normalized_sql:
            for field_name, field_result in self.parseability_by_field.items():
                if f"`{field_name}` AS sampled_time" in normalized_sql:
                    return [dict(field_result)]
            return [{"non_null_count": 0, "unparseable_count": 0}]

        if "FROM `_sys_metric_definitions` ORDER BY updated_at DESC, metric_key ASC" in normalized_sql:
            rows = [dict(row) for _, row in sorted(self.metrics.items(), key=lambda item: item[0])]
            rows.reverse()
            return rows

        if "GROUP BY ts" in normalized_sql and "FROM `orders`" in normalized_sql:
            self.last_series_query = (sql, params)
            return [
                {"ts": "2026-04-01", "value": 100.0},
                {"ts": "2026-04-08", "value": 130.0},
                {"ts": "2026-04-15", "value": 90.0},
            ]

        return []

    def execute_update(self, sql, params=None):
        normalized_sql = " ".join(str(sql).split())
        params = params or ()

        if "INSERT INTO `_sys_metric_definitions`" in normalized_sql:
            metric_key = params[0]
            self.metrics[metric_key] = {
                "metric_key": params[0],
                "display_name": params[1],
                "description": params[2],
                "table_name": params[3],
                "time_field": params[4],
                "value_field": params[5],
                "aggregation_expression": params[6],
                "aggregation": params[7],
                "default_grain": params[8],
                "dimensions": params[9],
                "created_at": params[10],
                "updated_at": params[11],
            }
            return 1

        if "UPDATE `_sys_metric_definitions`" in normalized_sql:
            metric_key = params[10]
            existing = self.metrics.get(metric_key, {})
            self.metrics[metric_key] = {
                "metric_key": metric_key,
                "display_name": params[0],
                "description": params[1],
                "table_name": params[2],
                "time_field": params[3],
                "value_field": params[4],
                "aggregation_expression": params[5],
                "aggregation": params[6],
                "default_grain": params[7],
                "dimensions": params[8],
                "created_at": existing.get("created_at", params[9]),
                "updated_at": params[9],
            }
            return 1

        if "DELETE FROM `_sys_metric_definitions` WHERE metric_key = %s" in normalized_sql:
            metric_key = params[0]
            self.metrics.pop(metric_key, None)
            return 1

        return 1


class SyncFoundationDb:
    def __init__(
        self,
        *,
        events,
        target_exists=True,
        target_schema=None,
        anchor_value=None,
        anchor_error=None,
    ):
        self.validator = DorisClient()
        self.events = events
        self.target_exists = bool(target_exists)
        self.target_schema = target_schema or {}
        self.anchor_value = anchor_value
        self.anchor_error = anchor_error

    def validate_identifier(self, identifier):
        return self.validator.validate_identifier(identifier)

    def table_exists(self, table_name):
        return self.target_exists

    def get_table_schema(self, table_name):
        field_types = self.target_schema.get(table_name) or {}
        return [{"Field": field_name, "Type": field_type} for field_name, field_type in field_types.items()]

    def execute_update(self, sql, params=None):
        normalized_sql = " ".join(str(sql).split())
        if normalized_sql.startswith("TRUNCATE TABLE") or normalized_sql.startswith("DELETE FROM"):
            self.events.append(("clear_target", normalized_sql))
        return 1

    def execute_query(self, sql, params=None):
        normalized_sql = " ".join(str(sql).split())
        if "AS incremental_anchor" in normalized_sql:
            if self.anchor_error:
                raise Exception(self.anchor_error)
            return [{"incremental_anchor": self.anchor_value}]
        return []


class EmptySourceCursor:
    def __init__(self, events):
        self.events = events
        self.description = [("id",), ("updated_at",)]

    def execute(self, sql, params=None):
        self.events.append(("execute_source_sql", " ".join(str(sql).split()), params))

    def fetchmany(self, size):
        self.events.append(("fetchmany", size))
        return []


class RemoteConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        return None


def _build_metric_payload(
    metric_key="orders.revenue",
    table_name="orders",
    time_field="order_date",
    value_field="amount",
    aggregation="sum",
    dimensions=None,
):
    return {
        "metric_key": metric_key,
        "display_name": "Orders Revenue",
        "description": "Revenue from orders table",
        "table_name": table_name,
        "time_field": time_field,
        "value_field": value_field,
        "aggregation": aggregation,
        "default_grain": "day",
        "dimensions": dimensions if dimensions is not None else ["region", "channel"],
    }


def test_metric_definition_lifecycle_and_forecast_boundary():
    handler = DataSourceHandler()
    handler.db = MetricFoundationDb()

    ready_result = handler.upsert_metric_definition(_build_metric_payload())
    assert ready_result["success"] is True
    assert ready_result["action"] == "created"
    assert ready_result["metric"]["availability"]["forecast_ready"] is True

    blocked_result = handler.upsert_metric_definition(
        _build_metric_payload(metric_key="orders.bad_time", time_field="region")
    )
    assert blocked_result["metric"]["availability"]["forecast_ready"] is False
    blocking_codes = {
        item.get("code")
        for item in blocked_result["metric"]["availability"].get("blocking_reasons") or []
    }
    assert "time_field_unparseable_values" in blocking_codes

    metric = handler.get_metric_definition("orders.revenue")
    assert metric is not None
    assert metric["display_name"] == "Orders Revenue"
    assert metric["availability"]["forecast_ready"] is True

    ready_metrics = handler.list_metric_definitions(only_forecast_ready=True)
    assert len(ready_metrics) == 1
    assert ready_metrics[0]["metric_key"] == "orders.revenue"

    deleted = handler.delete_metric_definition("orders.bad_time")
    assert deleted["success"] is True
    assert handler.get_metric_definition("orders.bad_time") is None


def test_get_metric_series_returns_sorted_points_and_dimension_filter():
    handler = DataSourceHandler()
    metric_db = MetricFoundationDb()
    handler.db = metric_db
    handler.upsert_metric_definition(_build_metric_payload())

    series = handler.get_metric_series(
        "orders.revenue",
        start_time="2026-04-01",
        end_time="2026-04-30",
        grain="week",
        filters={"region": "east"},
        limit=20,
    )

    assert series["success"] is True
    assert series["metric_key"] == "orders.revenue"
    assert series["grain"] == "week"
    assert series["count"] == 3
    assert series["time_normalization"]["strategy"] == "cast_datetime"
    assert series["time_normalization"]["normalized_value_type"] == "DATETIME"
    assert [point["ts"] for point in series["points"]] == ["2026-04-01", "2026-04-08", "2026-04-15"]

    assert metric_db.last_series_query is not None
    query_sql, query_params = metric_db.last_series_query
    assert "CAST(`order_date` AS DATETIME) AS _metric_time_value" in query_sql
    assert "_metric_time_value >= CAST(%s AS DATETIME)" in query_sql
    assert "_metric_time_value <= CAST(%s AS DATETIME)" in query_sql
    assert query_params == ("2026-04-01", "2026-04-30", "east", 20)


@pytest.mark.parametrize(
    "grain,expected_format_sql,expected_format_after_bind",
    [
        ("day", "DATE_FORMAT(DATE(_metric_time_value), '%%Y-%%m-%%d')", "DATE_FORMAT(DATE(_metric_time_value), '%Y-%m-%d')"),
        (
            "week",
            "DATE_FORMAT(DATE_SUB(DATE(_metric_time_value), INTERVAL WEEKDAY(DATE(_metric_time_value)) DAY), '%%Y-%%m-%%d')",
            "DATE_FORMAT(DATE_SUB(DATE(_metric_time_value), INTERVAL WEEKDAY(DATE(_metric_time_value)) DAY), '%Y-%m-%d')",
        ),
        ("month", "DATE_FORMAT(DATE(_metric_time_value), '%%Y-%%m-01')", "DATE_FORMAT(DATE(_metric_time_value), '%Y-%m-01')"),
    ],
)
def test_metric_series_sql_keeps_limit_parameterized_and_escapes_date_format(
    grain,
    expected_format_sql,
    expected_format_after_bind,
):
    handler = DataSourceHandler()
    metric_db = MetricFoundationDb()
    handler.db = metric_db
    handler.upsert_metric_definition(_build_metric_payload())

    series = handler.get_metric_series(
        "orders.revenue",
        grain=grain,
        filters={},
        limit=10,
    )
    assert series["success"] is True
    assert series["grain"] == grain

    assert metric_db.last_series_query is not None
    query_sql, query_params = metric_db.last_series_query

    assert expected_format_sql in query_sql
    assert "LIMIT %s" in query_sql
    assert query_params == (10,)

    # Simulate PyMySQL %-format bind stage: %% escapes must collapse to valid DATE_FORMAT patterns.
    bound_sql = query_sql % query_params
    assert expected_format_after_bind in bound_sql


def test_metric_availability_blocks_non_numeric_value_field_for_numeric_aggregations():
    handler = DataSourceHandler()
    handler.db = MetricFoundationDb()

    sum_varchar_result = handler.upsert_metric_definition(
        _build_metric_payload(
            metric_key="orders.sum_region",
            value_field="region",
            aggregation="sum",
        )
    )
    assert sum_varchar_result["metric"]["availability"]["forecast_ready"] is False
    sum_blocking_codes = {
        item.get("code")
        for item in sum_varchar_result["metric"]["availability"].get("blocking_reasons") or []
    }
    assert "value_field_not_numeric" in sum_blocking_codes

    avg_varchar_result = handler.upsert_metric_definition(
        _build_metric_payload(
            metric_key="orders.avg_channel",
            value_field="channel",
            aggregation="avg",
        )
    )
    assert avg_varchar_result["metric"]["availability"]["forecast_ready"] is False
    avg_blocking_codes = {
        item.get("code")
        for item in avg_varchar_result["metric"]["availability"].get("blocking_reasons") or []
    }
    assert "value_field_not_numeric" in avg_blocking_codes

    sum_numeric_result = handler.upsert_metric_definition(
        _build_metric_payload(
            metric_key="orders.sum_amount",
            value_field="amount",
            aggregation="sum",
        )
    )
    assert sum_numeric_result["metric"]["availability"]["forecast_ready"] is True


def test_metric_availability_blocks_unparseable_time_field_values():
    handler = DataSourceHandler()
    metric_db = MetricFoundationDb()
    handler.db = metric_db

    blocked_result = handler.upsert_metric_definition(
        _build_metric_payload(
            metric_key="orders_text_time.revenue",
            table_name="orders_text_time",
            time_field="event_time",
            value_field="amount",
            aggregation="sum",
            dimensions=["region"],
        )
    )
    assert blocked_result["metric"]["availability"]["forecast_ready"] is False
    blocking_codes = {
        item.get("code")
        for item in blocked_result["metric"]["availability"].get("blocking_reasons") or []
    }
    assert "time_field_unparseable_values" in blocking_codes

    metric_db.parseability_by_field["event_time"] = {"non_null_count": 4, "unparseable_count": 0}
    ready_result = handler.upsert_metric_definition(
        _build_metric_payload(
            metric_key="orders_text_time.revenue_ready",
            table_name="orders_text_time",
            time_field="event_time",
            value_field="amount",
            aggregation="sum",
            dimensions=["region"],
        )
    )
    assert ready_result["metric"]["availability"]["forecast_ready"] is True


def test_incremental_sync_plan_fallback_and_acceptance():
    handler = DataSourceHandler()
    handler.db = MetricFoundationDb()

    fallback_plan = handler._resolve_sync_execution_plan(
        requested_strategy="incremental",
        incremental_time_field="",
        source_columns={"updated_at": "datetime"},
        target_table_exists=True,
        incremental_start=None,
        incremental_end=None,
    )
    assert fallback_plan["effective_strategy"] == "full"
    assert fallback_plan["fallback_reason"] == "missing_incremental_time_field"

    incremental_plan = handler._resolve_sync_execution_plan(
        requested_strategy="incremental",
        incremental_time_field="updated_at",
        source_columns={"updated_at": "datetime"},
        target_table_exists=True,
        incremental_start=None,
        incremental_end=None,
    )
    assert incremental_plan["effective_strategy"] == "incremental"
    assert incremental_plan["fallback_reason"] is None


def test_full_sync_clears_existing_target_before_fetch_when_source_empty(monkeypatch):
    events = []
    handler = DataSourceHandler()
    handler.db = SyncFoundationDb(
        events=events,
        target_exists=True,
        target_schema={"orders_target": {"updated_at": "DATETIME"}},
    )
    handler._get_datasource_sync = lambda ds_id: {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "pwd",
        "database_name": "demo",
        "name": "demo_source",
    }
    handler._get_remote_source_columns_sync = lambda conn, database_name, source_table: {
        "id": "bigint",
        "updated_at": "datetime",
    }
    handler.finalize_table_ingestion = lambda *args, **kwargs: {"success": True}

    remote_connection = RemoteConnection(EmptySourceCursor(events))
    monkeypatch.setattr(datasource_handler_module.pymysql, "connect", lambda **kwargs: remote_connection)
    monkeypatch.setattr(
        datasource_handler_module.excel_handler,
        "stream_load",
        lambda df, target_table: (_ for _ in ()).throw(AssertionError("stream_load should not run for empty source")),
    )

    result = handler._sync_table_sync_v2(
        ds_id="ds1",
        source_table="orders_source",
        target_table="orders_target",
        sync_strategy="full",
    )

    assert result["success"] is True
    assert result["rows_synced"] == 0
    assert result["table_replaced"] is True
    assert result["sync_capability"]["effective_strategy"] == "full"
    assert result["sync_capability"]["fallback_to_full"] is False

    clear_indices = [idx for idx, event in enumerate(events) if event[0] == "clear_target"]
    fetch_indices = [idx for idx, event in enumerate(events) if event[0] == "fetchmany"]
    assert clear_indices, "existing target table should be cleared in full sync"
    assert fetch_indices, "source cursor should still be consumed"
    assert clear_indices[0] < fetch_indices[0], "full sync should clear target before fetch loop"


def test_incremental_anchor_field_unavailable_falls_back_to_full(monkeypatch):
    events = []
    handler = DataSourceHandler()
    handler.db = SyncFoundationDb(
        events=events,
        target_exists=True,
        target_schema={"orders_target": {"id": "BIGINT"}},
    )
    handler._get_datasource_sync = lambda ds_id: {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "pwd",
        "database_name": "demo",
        "name": "demo_source",
    }
    handler._get_remote_source_columns_sync = lambda conn, database_name, source_table: {
        "id": "bigint",
        "updated_at": "datetime",
    }
    handler.finalize_table_ingestion = lambda *args, **kwargs: {"success": True}

    remote_connection = RemoteConnection(EmptySourceCursor(events))
    monkeypatch.setattr(datasource_handler_module.pymysql, "connect", lambda **kwargs: remote_connection)
    monkeypatch.setattr(
        datasource_handler_module.excel_handler,
        "stream_load",
        lambda df, target_table: (_ for _ in ()).throw(AssertionError("stream_load should not run for empty source")),
    )

    result = handler._sync_table_sync_v2(
        ds_id="ds1",
        source_table="orders_source",
        target_table="orders_target",
        sync_strategy="incremental",
        incremental_time_field="updated_at",
    )

    assert result["success"] is True
    capability = result["sync_capability"]
    assert capability["effective_strategy"] == "full"
    assert capability["fallback_to_full"] is True
    assert capability["fallback_reason"] == "incremental_anchor_field_unavailable"


def test_incremental_anchor_query_failure_falls_back_to_full(monkeypatch):
    events = []
    handler = DataSourceHandler()
    handler.db = SyncFoundationDb(
        events=events,
        target_exists=True,
        target_schema={"orders_target": {"updated_at": "DATETIME"}},
        anchor_error="cast failed",
    )
    handler._get_datasource_sync = lambda ds_id: {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "pwd",
        "database_name": "demo",
        "name": "demo_source",
    }
    handler._get_remote_source_columns_sync = lambda conn, database_name, source_table: {
        "id": "bigint",
        "updated_at": "datetime",
    }
    handler.finalize_table_ingestion = lambda *args, **kwargs: {"success": True}

    remote_connection = RemoteConnection(EmptySourceCursor(events))
    monkeypatch.setattr(datasource_handler_module.pymysql, "connect", lambda **kwargs: remote_connection)
    monkeypatch.setattr(
        datasource_handler_module.excel_handler,
        "stream_load",
        lambda df, target_table: (_ for _ in ()).throw(AssertionError("stream_load should not run for empty source")),
    )

    result = handler._sync_table_sync_v2(
        ds_id="ds1",
        source_table="orders_source",
        target_table="orders_target",
        sync_strategy="incremental",
        incremental_time_field="updated_at",
    )

    assert result["success"] is True
    capability = result["sync_capability"]
    assert capability["effective_strategy"] == "full"
    assert capability["fallback_to_full"] is True
    assert capability["fallback_reason"] == "incremental_anchor_unavailable"


def test_internal_metric_read_surface_for_agent_c(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.list_metric_definitions_async = AsyncMock(
        return_value=[
            {
                "metric_key": "orders.revenue",
                "display_name": "Orders Revenue",
                "availability": {"forecast_ready": True, "blocking_reasons": [], "warnings": []},
            }
        ]
    )
    main.datasource_handler.get_metric_series_async = AsyncMock(
        return_value={
            "success": True,
            "metric_key": "orders.revenue",
            "display_name": "Orders Revenue",
            "table_name": "orders",
            "time_field": "order_date",
            "value_field": "amount",
            "aggregation": "sum",
            "default_grain": "day",
            "grain": "day",
            "filters": {"region": "east"},
            "time_range": {"start_time": "2026-04-01", "end_time": "2026-04-30"},
            "availability": {"forecast_ready": True, "blocking_reasons": [], "warnings": []},
            "points": [{"ts": "2026-04-01", "value": 100.0}],
            "count": 1,
        }
    )

    client = TestClient(main.app)

    contract_response = client.get(
        "/api/internal/metrics/contracts",
        headers={"X-API-Key": "secret-key"},
    )
    assert contract_response.status_code == 200
    contract_payload = contract_response.json()
    assert contract_payload["contracts"]["contract_version"] == "foundation.metric.read.v1"

    list_response = client.get(
        "/api/internal/metrics?only_forecast_ready=true",
        headers={"X-API-Key": "secret-key"},
    )
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["success"] is True
    assert list_payload["surface"] == "internal_metric_read"
    assert list_payload["count"] == 1
    main.datasource_handler.list_metric_definitions_async.assert_called_once_with(
        only_forecast_ready=True
    )

    series_response = client.post(
        "/api/internal/metrics/series",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "metric_key": "orders.revenue",
            "start_time": "2026-04-01",
            "end_time": "2026-04-30",
            "grain": "day",
            "filters": {"region": "east"},
            "limit": 100,
        },
    )
    assert series_response.status_code == 200
    series_payload = series_response.json()
    assert series_payload["success"] is True
    assert series_payload["surface"] == "internal_metric_read"
    assert series_payload["metric_key"] == "orders.revenue"
    main.datasource_handler.get_metric_series_async.assert_called_once_with(
        "orders.revenue",
        start_time="2026-04-01",
        end_time="2026-04-30",
        grain="day",
        filters={"region": "east"},
        limit=100,
    )


def test_foundation_metric_crud_endpoints(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.upsert_metric_definition_async = AsyncMock(
        return_value={
            "success": True,
            "action": "created",
            "metric": {
                "metric_key": "orders.revenue",
                "display_name": "Orders Revenue",
                "availability": {"forecast_ready": True, "blocking_reasons": [], "warnings": []},
            },
        }
    )
    main.datasource_handler.list_metric_definitions_async = AsyncMock(
        return_value=[
            {
                "metric_key": "orders.revenue",
                "display_name": "Orders Revenue",
                "availability": {"forecast_ready": True, "blocking_reasons": [], "warnings": []},
            }
        ]
    )
    main.datasource_handler.get_metric_definition_async = AsyncMock(
        return_value={
            "metric_key": "orders.revenue",
            "display_name": "Orders Revenue",
            "availability": {"forecast_ready": True, "blocking_reasons": [], "warnings": []},
        }
    )
    main.datasource_handler.delete_metric_definition_async = AsyncMock(
        return_value={"success": True, "metric_key": "orders.revenue"}
    )

    client = TestClient(main.app)

    create_response = client.post(
        "/api/foundation/metrics",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "metric_key": "orders.revenue",
            "display_name": "Orders Revenue",
            "description": "Revenue from orders",
            "table_name": "orders",
            "time_field": "order_date",
            "value_field": "amount",
            "aggregation": "sum",
            "default_grain": "day",
            "dimensions": ["region"],
        },
    )
    assert create_response.status_code == 200
    assert create_response.json()["metric"]["metric_key"] == "orders.revenue"

    list_response = client.get(
        "/api/foundation/metrics?only_forecast_ready=false",
        headers={"X-API-Key": "secret-key"},
    )
    assert list_response.status_code == 200
    assert list_response.json()["count"] == 1
    main.datasource_handler.list_metric_definitions_async.assert_called_once_with(
        only_forecast_ready=False
    )

    detail_response = client.get(
        "/api/foundation/metrics/orders.revenue",
        headers={"X-API-Key": "secret-key"},
    )
    assert detail_response.status_code == 200
    assert detail_response.json()["metric"]["metric_key"] == "orders.revenue"

    delete_response = client.delete(
        "/api/foundation/metrics/orders.revenue",
        headers={"X-API-Key": "secret-key"},
    )
    assert delete_response.status_code == 200
    assert delete_response.json()["success"] is True
