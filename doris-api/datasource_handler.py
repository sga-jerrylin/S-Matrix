"""
澶栭儴鏁版嵁婧愬悓姝ュ鐞嗗櫒
"""
import pymysql
import pandas as pd
import json
import os
import hashlib
import asyncio
import time
import uuid
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from cryptography.fernet import Fernet
from config import DB_CONNECT_TIMEOUT, DB_READ_TIMEOUT, DB_WRITE_TIMEOUT
from db import doris_client
from data_foundation import (
    build_field_payload,
    build_metadata_payload,
    build_registry_payload,
    build_relationship_payload,
    build_source_payload,
    build_table_profile_payload,
    relation_type_label,
    safe_json_loads,
    semantic_label,
    short_field_display_name,
)
from upload_handler import excel_handler


class DataSourceHandler:
    """澶栭儴鏁版嵁婧愮鐞嗗拰鍚屾澶勭悊鍣?"""

    _METRIC_KEY_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.-]{0,127}$")
    _SUPPORTED_METRIC_AGGREGATIONS = {"sum", "avg", "min", "max", "count", "count_distinct"}
    _SUPPORTED_TIME_GRAINS = {"day", "week", "month"}
    _SUPPORTED_SYNC_STRATEGIES = {"full", "incremental"}
    
    def __init__(self):
        self.db = doris_client
        # 鍔犲瘑瀵嗛挜 - 蹇呴』閫氳繃鐜鍙橀噺 ENCRYPTION_KEY 鎻愪緵
        # 鐢熸垚鏂瑰紡锛歱ython -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
        key = os.getenv('ENCRYPTION_KEY')
        if key:
            self.cipher = Fernet(key.encode() if isinstance(key, str) else key)
        else:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "ENCRYPTION_KEY is not set: datasource passwords will be encrypted with a temporary key."
                "Set ENCRYPTION_KEY in .env for persistent encryption across restarts."
                "鐢熸垚鍛戒护锛歱ython -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )
            self.cipher = Fernet(Fernet.generate_key())
        self._tables_initialized = False

    def init_tables(self):
        """鍒濆鍖栫郴缁熻〃锛堝湪鏁版嵁搴撳氨缁悗璋冪敤锛?"""
        if self._tables_initialized:
            return True

        initialized = self._ensure_system_tables()
        self._tables_initialized = initialized
        return initialized

    def _ensure_system_tables(self):
        """纭繚绯荤粺琛ㄥ瓨鍦?"""
        # 鏁版嵁婧愰厤缃〃 - 浣跨敤 UNIQUE KEY 浠ユ敮鎸?UPDATE/DELETE
        sql_datasources = """
        CREATE TABLE IF NOT EXISTS `_sys_datasources` (
            `id` VARCHAR(64),
            `name` VARCHAR(200),
            `host` VARCHAR(200),
            `port` INT,
            `user` VARCHAR(100),
            `password_encrypted` VARCHAR(500),
            `database_name` VARCHAR(200),
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        # 鍚屾浠诲姟琛?- 浣跨敤 UNIQUE KEY 浠ユ敮鎸?UPDATE/DELETE
        sql_sync_tasks = """
        CREATE TABLE IF NOT EXISTS `_sys_sync_tasks` (
            `id` VARCHAR(64),
            `datasource_id` VARCHAR(64),
            `source_table` VARCHAR(200),
            `target_table` VARCHAR(200),
            `schedule_type` VARCHAR(50),
            `schedule_minute` INT DEFAULT "0",
            `schedule_hour` INT DEFAULT "0",
            `schedule_day_of_week` INT DEFAULT "1",
            `schedule_day_of_month` INT DEFAULT "1",
            `schedule_value` VARCHAR(100),
            `last_sync_at` DATETIME,
            `next_sync_at` DATETIME,
            `status` VARCHAR(50),
            `enabled_for_ai` TINYINT DEFAULT "1",
            `created_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        # 琛ㄥ厓鏁版嵁琛?- 浣跨敤 UNIQUE KEY 浠ユ敮鎸?UPDATE/DELETE
        sql_metadata = """
        CREATE TABLE IF NOT EXISTS `_sys_table_metadata` (
            `table_name` VARCHAR(200),
            `description` TEXT,
            `columns_info` TEXT,
            `sample_queries` TEXT,
            `analyzed_at` DATETIME,
            `source_type` VARCHAR(50)
        )
        UNIQUE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_table_registry = """
        CREATE TABLE IF NOT EXISTS `_sys_table_registry` (
            `table_name` VARCHAR(200),
            `display_name` VARCHAR(200),
            `description` TEXT,
            `source_type` VARCHAR(50),
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        UNIQUE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_table_sources = """
        CREATE TABLE IF NOT EXISTS `_sys_table_sources` (
            `table_name` VARCHAR(200),
            `source_type` VARCHAR(50),
            `origin_kind` VARCHAR(50),
            `origin_id` VARCHAR(100),
            `origin_label` VARCHAR(255),
            `origin_path` VARCHAR(500),
            `origin_table` VARCHAR(255),
            `sync_task_id` VARCHAR(64),
            `ingest_mode` VARCHAR(50),
            `last_rows` BIGINT,
            `analysis_status` VARCHAR(50),
            `last_ingested_at` DATETIME,
            `last_analyzed_at` DATETIME,
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        UNIQUE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_metric_definitions = """
        CREATE TABLE IF NOT EXISTS `_sys_metric_definitions` (
            `metric_key` VARCHAR(128),
            `display_name` VARCHAR(255),
            `description` TEXT,
            `table_name` VARCHAR(200),
            `time_field` VARCHAR(255),
            `value_field` VARCHAR(255),
            `aggregation_expression` TEXT,
            `aggregation` VARCHAR(50),
            `default_grain` VARCHAR(20),
            `dimensions` TEXT,
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        UNIQUE KEY(`metric_key`)
        DISTRIBUTED BY HASH(`metric_key`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_query_history = """
        CREATE TABLE IF NOT EXISTS `_sys_query_history` (
            `id` VARCHAR(36),
            `question` TEXT,
            `sql` TEXT,
            `table_names` VARCHAR(1000),
            `question_hash` VARCHAR(64),
            `quality_gate` TINYINT DEFAULT "1",
            `is_empty_result` TINYINT DEFAULT "0",
            `row_count` INT,
            `created_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_table_agents = """
        CREATE TABLE IF NOT EXISTS `_sys_table_agents` (
            `table_name` VARCHAR(255),
            `agent_config` TEXT,
            `source_hash` VARCHAR(64),
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        UNIQUE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_field_catalog = """
        CREATE TABLE IF NOT EXISTS `_sys_field_catalog` (
            `table_name` VARCHAR(255),
            `field_name` VARCHAR(255),
            `field_type` VARCHAR(50),
            `enum_values` TEXT,
            `value_range` VARCHAR(200),
            `updated_at` DATETIME
        )
        UNIQUE KEY(`table_name`, `field_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_relationships = """
        CREATE TABLE IF NOT EXISTS `_sys_table_relationships` (
            `id` VARCHAR(36),
            `table_a` VARCHAR(255),
            `column_a` VARCHAR(255),
            `table_b` VARCHAR(255),
            `column_b` VARCHAR(255),
            `rel_type` VARCHAR(50),
            `confidence` FLOAT,
            `is_manual` TINYINT DEFAULT "0",
            `created_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        import time
        max_retries = 10
        retryable_markers = (
            "available backend num is 0",
            "Failed to find enough backend",
            "failed to find enough backend",
            "hdd disks count={}",
            "ssd disk count={}",
            "storage medium: HDD",
        )
        for attempt in range(max_retries):
            try:
                self.db.execute_update(sql_datasources)
                self.db.execute_update(sql_sync_tasks)
                self.db.execute_update(sql_metadata)
                self.db.execute_update(sql_table_registry)
                self.db.execute_update(sql_table_sources)
                self.db.execute_update(sql_metric_definitions)
                self.db.execute_update(sql_query_history)
                self.db.execute_update(sql_table_agents)
                self.db.execute_update(sql_field_catalog)
                self.db.execute_update(sql_relationships)

                for index_sql in (
                    "CREATE INDEX IF NOT EXISTS idx_query_history_hash ON `_sys_query_history` (`question_hash`) USING INVERTED",
                    "CREATE INDEX IF NOT EXISTS idx_query_history_question ON `_sys_query_history` (`question`) USING INVERTED PROPERTIES(\"parser\"=\"chinese\")",
                ):
                    try:
                        self.db.execute_update(index_sql)
                    except Exception:
                        pass

                try:
                    self.ensure_query_history_vector_support()
                except Exception:
                    pass

                print("System tables created")
                return True
            except Exception as e:
                error_msg = str(e)
                if any(marker in error_msg for marker in retryable_markers) and attempt < max_retries - 1:
                    print(f"鈴?BE 灏氭湭灏辩华锛岀瓑寰呴噸璇?.. ({attempt + 1}/{max_retries})")
                    time.sleep(5)
                else:
                    print(f"Warning: Could not create system tables: {e}")
                    return False

        return False

    def ensure_table_registry(self, table_name: str, source_type: str,
                              display_name: Optional[str] = None,
                              description: Optional[str] = None) -> Dict[str, Any]:
        """纭繚琛ㄦ敞鍐屽瓨鍦?(鍚屾)"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exists_sql = "SELECT table_name FROM `_sys_table_registry` WHERE table_name = %s LIMIT 1"
        exists = self.db.execute_query(exists_sql, (table_name,))

        if exists:
            update_sql = """
            UPDATE `_sys_table_registry`
            SET source_type = COALESCE(%s, source_type),
                display_name = COALESCE(%s, display_name),
                description = COALESCE(%s, description),
                updated_at = %s
            WHERE table_name = %s
            """
            self.db.execute_update(update_sql, (source_type, display_name, description, now, table_name))
            return {'success': True, 'message': '琛ㄦ敞鍐屽凡鏇存柊', 'table_name': table_name}

        insert_sql = """
        INSERT INTO `_sys_table_registry`
        (`table_name`, `display_name`, `description`, `source_type`, `created_at`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.db.execute_update(insert_sql, (
            table_name,
            display_name if display_name is not None else '',
            description if description is not None else '',
            source_type,
            now,
            now
        ))
        return {'success': True, 'message': '琛ㄦ敞鍐屽凡鍒涘缓', 'table_name': table_name}

    def upsert_table_source(
        self,
        table_name: str,
        source_type: str,
        *,
        origin_kind: Optional[str] = None,
        origin_id: Optional[str] = None,
        origin_label: Optional[str] = None,
        origin_path: Optional[str] = None,
        origin_table: Optional[str] = None,
        sync_task_id: Optional[str] = None,
        ingest_mode: Optional[str] = None,
        last_rows: Optional[int] = None,
        analysis_status: str = "pending",
    ) -> Dict[str, Any]:
        """鍐欏叆鎴栨洿鏂拌〃鏉ユ簮鐘舵€併€?"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exists_sql = "SELECT table_name FROM `_sys_table_sources` WHERE table_name = %s LIMIT 1"
        exists = self.db.execute_query(exists_sql, (table_name,))

        if exists:
            update_sql = """
            UPDATE `_sys_table_sources`
            SET source_type = COALESCE(%s, source_type),
                origin_kind = COALESCE(%s, origin_kind),
                origin_id = COALESCE(%s, origin_id),
                origin_label = COALESCE(%s, origin_label),
                origin_path = COALESCE(%s, origin_path),
                origin_table = COALESCE(%s, origin_table),
                sync_task_id = COALESCE(%s, sync_task_id),
                ingest_mode = COALESCE(%s, ingest_mode),
                last_rows = COALESCE(%s, last_rows),
                analysis_status = %s,
                last_ingested_at = %s,
                updated_at = %s
            WHERE table_name = %s
            """
            self.db.execute_update(
                update_sql,
                (
                    source_type,
                    origin_kind,
                    origin_id,
                    origin_label,
                    origin_path,
                    origin_table,
                    sync_task_id,
                    ingest_mode,
                    last_rows,
                    analysis_status,
                    now,
                    now,
                    table_name,
                ),
            )
        else:
            insert_sql = """
            INSERT INTO `_sys_table_sources`
            (`table_name`, `source_type`, `origin_kind`, `origin_id`, `origin_label`,
             `origin_path`, `origin_table`, `sync_task_id`, `ingest_mode`, `last_rows`,
             `analysis_status`, `last_ingested_at`, `last_analyzed_at`, `created_at`, `updated_at`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s)
            """
            self.db.execute_update(
                insert_sql,
                (
                    table_name,
                    source_type,
                    origin_kind or "",
                    origin_id or "",
                    origin_label or "",
                    origin_path or "",
                    origin_table or "",
                    sync_task_id or "",
                    ingest_mode or "",
                    last_rows,
                    analysis_status,
                    now,
                    now,
                    now,
                ),
            )

        return {
            "success": True,
            "table_name": table_name,
            "analysis_status": analysis_status,
        }

    def mark_table_analysis_status(
        self,
        table_name: str,
        status: str,
        *,
        analyzed_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """鏇存柊琛ㄧ殑鍒嗘瀽鐘舵€併€?"""
        safe_table_name = (table_name or "").strip()
        if not safe_table_name:
            return {"success": False, "error": "table_name is required"}

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exists = self.db.execute_query(
            "SELECT table_name FROM `_sys_table_sources` WHERE table_name = %s LIMIT 1",
            (safe_table_name,),
        )
        if not exists:
            return {"success": True, "table_name": safe_table_name, "analysis_status": status, "skipped": True}

        if analyzed_at:
            sql = """
            UPDATE `_sys_table_sources`
            SET analysis_status = %s,
                last_analyzed_at = %s,
                updated_at = %s
            WHERE table_name = %s
            """
            params = (status, analyzed_at, now, safe_table_name)
        else:
            sql = """
            UPDATE `_sys_table_sources`
            SET analysis_status = %s,
                updated_at = %s
            WHERE table_name = %s
            """
            params = (status, now, safe_table_name)

        self.db.execute_update(sql, params)
        return {"success": True, "table_name": safe_table_name, "analysis_status": status}

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    def _normalize_metric_key(self, metric_key: str) -> str:
        normalized = str(metric_key or "").strip()
        if not normalized:
            raise ValueError("metric_key is required")
        if not self._METRIC_KEY_RE.match(normalized):
            raise ValueError(
                "metric_key must start with letter and only contain letters, numbers, '_', '-', '.'"
            )
        return normalized

    def _normalize_grain(self, grain: str, field_name: str = "default_grain") -> str:
        normalized = str(grain or "").strip().lower()
        if normalized not in self._SUPPORTED_TIME_GRAINS:
            supported = ", ".join(sorted(self._SUPPORTED_TIME_GRAINS))
            raise ValueError(f"{field_name} must be one of: {supported}")
        return normalized

    def _normalize_aggregation(self, aggregation: str) -> str:
        normalized = str(aggregation or "").strip().lower()
        if normalized not in self._SUPPORTED_METRIC_AGGREGATIONS:
            supported = ", ".join(sorted(self._SUPPORTED_METRIC_AGGREGATIONS))
            raise ValueError(f"aggregation must be one of: {supported}")
        return normalized

    def _normalize_dimensions(self, dimensions: Any) -> List[str]:
        if dimensions is None:
            return []
        if isinstance(dimensions, str):
            dimensions = [item.strip() for item in dimensions.split(",")]
        if not isinstance(dimensions, list):
            raise ValueError("dimensions must be a list or comma-separated string")
        normalized: List[str] = []
        seen = set()
        for item in dimensions:
            candidate = str(item or "").strip()
            if not candidate or candidate in seen:
                continue
            normalized.append(candidate)
            seen.add(candidate)
        return normalized

    def _parse_aggregation_expression(self, expression: str) -> Optional[Dict[str, Any]]:
        raw = str(expression or "").strip()
        if not raw:
            return None

        count_match = re.match(r"(?i)^\s*count\s*\(\s*\*\s*\)\s*$", raw)
        if count_match:
            return {
                "aggregation": "count",
                "value_field": None,
                "normalized_expression": "COUNT(*)",
            }

        distinct_match = re.match(
            r"(?i)^\s*count\s*\(\s*distinct\s+([a-zA-Z0-9_\-\u4e00-\u9fa5]+)\s*\)\s*$",
            raw,
        )
        if distinct_match:
            field_name = distinct_match.group(1)
            return {
                "aggregation": "count_distinct",
                "value_field": field_name,
                "normalized_expression": f"COUNT(DISTINCT {field_name})",
            }

        metric_match = re.match(
            r"(?i)^\s*(sum|avg|min|max)\s*\(\s*([a-zA-Z0-9_\-\u4e00-\u9fa5]+)\s*\)\s*$",
            raw,
        )
        if metric_match:
            aggregation = metric_match.group(1).lower()
            field_name = metric_match.group(2)
            return {
                "aggregation": aggregation,
                "value_field": field_name,
                "normalized_expression": f"{aggregation.upper()}({field_name})",
            }

        raise ValueError(
            "aggregation_expression only supports COUNT(*), COUNT(DISTINCT <field>), SUM/AVG/MIN/MAX(<field>)"
        )

    def _normalize_metric_definition_input(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metric_key = self._normalize_metric_key(payload.get("metric_key"))
        display_name = str(payload.get("display_name") or metric_key).strip()
        if not display_name:
            raise ValueError("display_name is required")

        table_name = str(payload.get("table_name") or "").strip()
        time_field = str(payload.get("time_field") or "").strip()
        if not table_name:
            raise ValueError("table_name is required")
        if not time_field:
            raise ValueError("time_field is required")

        default_grain = self._normalize_grain(payload.get("default_grain") or "day")
        description = str(payload.get("description") or "").strip()
        dimensions = self._normalize_dimensions(payload.get("dimensions"))

        aggregation_expression_raw = str(payload.get("aggregation_expression") or "").strip()
        parsed_expression = self._parse_aggregation_expression(aggregation_expression_raw) if aggregation_expression_raw else None
        if parsed_expression:
            aggregation = parsed_expression["aggregation"]
            value_field = parsed_expression.get("value_field")
            aggregation_expression = parsed_expression.get("normalized_expression") or aggregation_expression_raw
        else:
            aggregation = self._normalize_aggregation(payload.get("aggregation") or "sum")
            value_field = str(payload.get("value_field") or "").strip() or None
            aggregation_expression = ""

        if aggregation in {"sum", "avg", "min", "max", "count_distinct"} and not value_field:
            raise ValueError("value_field is required for current aggregation")

        if aggregation == "count":
            value_field = value_field or None

        normalized = {
            "metric_key": metric_key,
            "display_name": display_name,
            "description": description,
            "table_name": table_name,
            "time_field": time_field,
            "value_field": value_field or "",
            "aggregation_expression": aggregation_expression,
            "aggregation": aggregation,
            "default_grain": default_grain,
            "dimensions": dimensions,
        }
        return normalized

    def _inflate_metric_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        metric = dict(row or {})
        metric["dimensions"] = safe_json_loads(metric.get("dimensions"), [])
        if not isinstance(metric["dimensions"], list):
            metric["dimensions"] = []
        metric["value_field"] = metric.get("value_field") or ""
        metric["aggregation_expression"] = metric.get("aggregation_expression") or ""
        metric["aggregation"] = (metric.get("aggregation") or "").strip().lower()
        metric["default_grain"] = (metric.get("default_grain") or "").strip().lower()
        return metric

    def _get_table_field_types(self, table_name: str) -> Dict[str, str]:
        try:
            schema_rows = self.db.get_table_schema(table_name)
        except Exception:
            return {}
        field_types: Dict[str, str] = {}
        for row in schema_rows:
            field_name = row.get("Field")
            if not field_name:
                continue
            field_types[field_name] = str(row.get("Type") or "")
        return field_types

    @staticmethod
    def _is_temporal_type(type_name: str) -> bool:
        upper = str(type_name or "").upper()
        return "DATE" in upper or "TIME" in upper

    @staticmethod
    def _is_numeric_type(type_name: str) -> bool:
        upper = str(type_name or "").upper()
        numeric_markers = (
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
            "LARGEINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
            "REAL",
        )
        return any(marker in upper for marker in numeric_markers)

    @staticmethod
    def _is_string_like_type(type_name: str) -> bool:
        upper = str(type_name or "").upper()
        return any(marker in upper for marker in ("CHAR", "VARCHAR", "STRING", "TEXT"))

    @staticmethod
    def _is_remote_temporal_type(type_name: str) -> bool:
        lower = str(type_name or "").strip().lower()
        return lower in {"date", "datetime", "timestamp", "time"}

    @staticmethod
    def _quote_remote_identifier(identifier: str) -> str:
        candidate = str(identifier or "").strip()
        if not candidate:
            raise ValueError("identifier is required")
        if not re.match(r"^[A-Za-z0-9_\u4e00-\u9fa5$]+$", candidate):
            raise ValueError(f"invalid remote identifier: {identifier}")
        return f"`{candidate}`"

    def _normalize_sync_strategy(self, strategy: Optional[str]) -> str:
        normalized = str(strategy or "full").strip().lower()
        if normalized not in self._SUPPORTED_SYNC_STRATEGIES:
            supported = ", ".join(sorted(self._SUPPORTED_SYNC_STRATEGIES))
            raise ValueError(f"sync_strategy must be one of: {supported}")
        return normalized

    def _build_metric_time_normalization(
        self,
        table_name: str,
        time_field: str,
        field_types: Dict[str, str],
    ) -> Dict[str, Any]:
        if not time_field or time_field not in field_types:
            return {
                "supported": False,
                "code": "time_field_not_found",
                "strategy": None,
                "source_field_type": "",
                "normalized_expression": None,
            }

        source_field_type = str(field_types.get(time_field) or "")
        safe_time_field = self.db.validate_identifier(time_field)
        if self._is_temporal_type(source_field_type):
            return {
                "supported": True,
                "code": "ok",
                "strategy": "cast_datetime",
                "source_field_type": source_field_type,
                "normalized_expression": f"CAST({safe_time_field} AS DATETIME)",
            }
        if self._is_string_like_type(source_field_type):
            return {
                "supported": True,
                "code": "ok",
                "strategy": "try_cast_datetime",
                "source_field_type": source_field_type,
                "normalized_expression": f"TRY_CAST({safe_time_field} AS DATETIME)",
            }
        return {
            "supported": False,
            "code": "time_field_not_time_like",
            "strategy": None,
            "source_field_type": source_field_type,
            "normalized_expression": None,
        }

    def _sample_time_parseability(
        self,
        table_name: str,
        time_field: str,
        sample_limit: int = 1000,
    ) -> Dict[str, int]:
        safe_table_name = self.db.validate_identifier(table_name)
        safe_time_field = self.db.validate_identifier(time_field)
        sql = f"""
        SELECT
            SUM(CASE WHEN sampled_time IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count,
            SUM(
                CASE
                    WHEN sampled_time IS NOT NULL AND TRY_CAST(sampled_time AS DATETIME) IS NULL
                    THEN 1
                    ELSE 0
                END
            ) AS unparseable_count
        FROM (
            SELECT {safe_time_field} AS sampled_time
            FROM {safe_table_name}
            LIMIT %s
        ) sampled
        """
        rows = self.db.execute_query(sql, (max(1, self._safe_int(sample_limit, 1000)),))
        first_row = rows[0] if rows else {}
        return {
            "non_null_count": self._safe_int(first_row.get("non_null_count"), 0),
            "unparseable_count": self._safe_int(first_row.get("unparseable_count"), 0),
        }

    def evaluate_metric_availability(self, metric: Dict[str, Any]) -> Dict[str, Any]:
        table_name = str(metric.get("table_name") or "").strip()
        time_field = str(metric.get("time_field") or "").strip()
        value_field = str(metric.get("value_field") or "").strip()
        aggregation = str(metric.get("aggregation") or "").strip().lower()
        default_grain = str(metric.get("default_grain") or "").strip().lower()
        dimensions = metric.get("dimensions") if isinstance(metric.get("dimensions"), list) else []

        blocking_reasons: List[Dict[str, str]] = []
        warnings: List[Dict[str, str]] = []

        if not table_name:
            blocking_reasons.append({"code": "missing_table_name", "message": "table_name is required"})
        if not time_field:
            blocking_reasons.append({"code": "missing_time_field", "message": "time_field is required"})
        if aggregation not in self._SUPPORTED_METRIC_AGGREGATIONS:
            blocking_reasons.append(
                {
                    "code": "unsupported_aggregation",
                    "message": f"aggregation '{aggregation}' is not supported",
                }
            )
        if default_grain not in self._SUPPORTED_TIME_GRAINS:
            blocking_reasons.append(
                {
                    "code": "unsupported_default_grain",
                    "message": f"default_grain '{default_grain}' is not supported",
                }
            )
        if aggregation in {"sum", "avg", "min", "max", "count_distinct"} and not value_field:
            blocking_reasons.append(
                {
                    "code": "missing_value_field",
                    "message": f"value_field is required for aggregation '{aggregation}'",
                }
            )

        field_types = self._get_table_field_types(table_name) if table_name else {}
        if table_name and not field_types:
            blocking_reasons.append(
                {
                    "code": "table_schema_unavailable",
                    "message": f"table '{table_name}' schema is unavailable",
                }
            )

        if time_field and field_types:
            time_normalization = self._build_metric_time_normalization(table_name, time_field, field_types)
            if not time_normalization.get("supported"):
                source_field_type = time_normalization.get("source_field_type") or ""
                blocking_reasons.append(
                    {
                        "code": "time_field_not_temporal",
                        "message": (
                            f"time_field '{time_field}' type '{source_field_type}' cannot be normalized "
                            "as DATETIME"
                        ),
                    }
                )
            elif time_normalization.get("strategy") == "try_cast_datetime":
                parseability = self._sample_time_parseability(table_name, time_field)
                if parseability.get("non_null_count", 0) > 0 and parseability.get("unparseable_count", 0) > 0:
                    blocking_reasons.append(
                        {
                            "code": "time_field_unparseable_values",
                            "message": (
                                f"time_field '{time_field}' has "
                                f"{parseability.get('unparseable_count', 0)} unparseable values "
                                f"in sampled non-null rows ({parseability.get('non_null_count', 0)})"
                            ),
                        }
                    )

        if value_field and field_types and aggregation != "count":
            if value_field not in field_types:
                blocking_reasons.append(
                    {
                        "code": "value_field_not_found",
                        "message": f"value_field '{value_field}' not found in table '{table_name}'",
                    }
                )
            elif aggregation in {"sum", "avg", "min", "max"}:
                value_field_type = str(field_types.get(value_field) or "")
                if not self._is_numeric_type(value_field_type):
                    blocking_reasons.append(
                        {
                            "code": "value_field_not_numeric",
                            "message": (
                                f"value_field '{value_field}' type '{value_field_type}' is not numeric "
                                f"for aggregation '{aggregation}'"
                            ),
                        }
                    )

        for dimension in dimensions:
            if field_types and dimension not in field_types:
                blocking_reasons.append(
                    {
                        "code": "dimension_not_found",
                        "message": f"dimension '{dimension}' not found in table '{table_name}'",
                    }
                )

        if metric.get("aggregation_expression"):
            try:
                self._parse_aggregation_expression(metric.get("aggregation_expression"))
            except Exception as expression_error:
                blocking_reasons.append(
                    {
                        "code": "invalid_aggregation_expression",
                        "message": str(expression_error),
                    }
                )

        forecast_ready = len(blocking_reasons) == 0
        return {
            "forecast_ready": forecast_ready,
            "blocking_reasons": blocking_reasons,
            "warnings": warnings,
            "checked_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    def upsert_metric_definition(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metric = self._normalize_metric_definition_input(payload)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exists = self.db.execute_query(
            "SELECT metric_key FROM `_sys_metric_definitions` WHERE metric_key = %s LIMIT 1",
            (metric["metric_key"],),
        )

        if exists:
            update_sql = """
            UPDATE `_sys_metric_definitions`
            SET display_name = %s,
                description = %s,
                table_name = %s,
                time_field = %s,
                value_field = %s,
                aggregation_expression = %s,
                aggregation = %s,
                default_grain = %s,
                dimensions = %s,
                updated_at = %s
            WHERE metric_key = %s
            """
            self.db.execute_update(
                update_sql,
                (
                    metric["display_name"],
                    metric["description"],
                    metric["table_name"],
                    metric["time_field"],
                    metric["value_field"],
                    metric["aggregation_expression"],
                    metric["aggregation"],
                    metric["default_grain"],
                    json.dumps(metric["dimensions"], ensure_ascii=False),
                    now,
                    metric["metric_key"],
                ),
            )
            action = "updated"
        else:
            insert_sql = """
            INSERT INTO `_sys_metric_definitions`
            (`metric_key`, `display_name`, `description`, `table_name`, `time_field`,
             `value_field`, `aggregation_expression`, `aggregation`, `default_grain`,
             `dimensions`, `created_at`, `updated_at`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.db.execute_update(
                insert_sql,
                (
                    metric["metric_key"],
                    metric["display_name"],
                    metric["description"],
                    metric["table_name"],
                    metric["time_field"],
                    metric["value_field"],
                    metric["aggregation_expression"],
                    metric["aggregation"],
                    metric["default_grain"],
                    json.dumps(metric["dimensions"], ensure_ascii=False),
                    now,
                    now,
                ),
            )
            action = "created"

        availability = self.evaluate_metric_availability(metric)
        return {
            "success": True,
            "action": action,
            "metric": {**metric, "availability": availability},
        }

    def get_metric_definition(self, metric_key: str) -> Optional[Dict[str, Any]]:
        normalized_key = self._normalize_metric_key(metric_key)
        rows = self.db.execute_query(
            "SELECT * FROM `_sys_metric_definitions` WHERE metric_key = %s LIMIT 1",
            (normalized_key,),
        )
        if not rows:
            return None
        metric = self._inflate_metric_row(rows[0])
        metric["availability"] = self.evaluate_metric_availability(metric)
        return metric

    def list_metric_definitions(self, only_forecast_ready: bool = False) -> List[Dict[str, Any]]:
        rows = self.db.execute_query(
            """
            SELECT *
            FROM `_sys_metric_definitions`
            ORDER BY updated_at DESC, metric_key ASC
            """
        )
        metrics: List[Dict[str, Any]] = []
        for row in rows:
            metric = self._inflate_metric_row(row)
            metric["availability"] = self.evaluate_metric_availability(metric)
            if only_forecast_ready and not metric["availability"].get("forecast_ready"):
                continue
            metrics.append(metric)
        return metrics

    def delete_metric_definition(self, metric_key: str) -> Dict[str, Any]:
        normalized_key = self._normalize_metric_key(metric_key)
        self.db.execute_update(
            "DELETE FROM `_sys_metric_definitions` WHERE metric_key = %s",
            (normalized_key,),
        )
        return {"success": True, "metric_key": normalized_key}

    def _metric_period_expression(self, normalized_time_expression: str, grain: str) -> str:
        normalized_date_expression = f"DATE({normalized_time_expression})"
        if grain == "day":
            return f"DATE_FORMAT({normalized_date_expression}, '%%Y-%%m-%%d')"
        if grain == "week":
            return (
                f"DATE_FORMAT(DATE_SUB({normalized_date_expression}, "
                f"INTERVAL WEEKDAY({normalized_date_expression}) DAY), '%%Y-%%m-%%d')"
            )
        if grain == "month":
            return f"DATE_FORMAT({normalized_date_expression}, '%%Y-%%m-01')"
        raise ValueError(f"Unsupported grain: {grain}")

    def _metric_aggregation_sql(self, metric: Dict[str, Any]) -> str:
        aggregation = str(metric.get("aggregation") or "").strip().lower()
        value_field = str(metric.get("value_field") or "").strip()
        if aggregation == "count":
            return "COUNT(*)"
        if aggregation == "count_distinct":
            if not value_field:
                raise ValueError("value_field is required for count_distinct")
            safe_value_field = self.db.validate_identifier(value_field)
            return f"COUNT(DISTINCT {safe_value_field})"
        if aggregation in {"sum", "avg", "min", "max"}:
            if not value_field:
                raise ValueError(f"value_field is required for aggregation '{aggregation}'")
            safe_value_field = self.db.validate_identifier(value_field)
            return f"{aggregation.upper()}({safe_value_field})"
        raise ValueError(f"Unsupported aggregation: {aggregation}")

    def _metric_filter_clause(
        self,
        filters: Dict[str, Any],
        allowed_dimensions: List[str],
    ) -> Tuple[str, List[Any]]:
        if not filters:
            return "", []

        allowed_dimension_set = set(allowed_dimensions)
        clauses: List[str] = []
        params: List[Any] = []
        for key, value in filters.items():
            field_name = str(key or "").strip()
            if not field_name:
                continue
            if field_name not in allowed_dimension_set:
                raise ValueError(f"Filter field '{field_name}' is not in metric dimensions")

            safe_field = self.db.validate_identifier(field_name)
            if isinstance(value, list):
                if not value:
                    continue
                placeholders = ", ".join(["%s"] * len(value))
                clauses.append(f"{safe_field} IN ({placeholders})")
                params.extend(value)
                continue

            if isinstance(value, dict):
                if "min" in value and value.get("min") is not None:
                    clauses.append(f"{safe_field} >= %s")
                    params.append(value.get("min"))
                if "max" in value and value.get("max") is not None:
                    clauses.append(f"{safe_field} <= %s")
                    params.append(value.get("max"))
                if "eq" in value and value.get("eq") is not None:
                    clauses.append(f"{safe_field} = %s")
                    params.append(value.get("eq"))
                continue

            clauses.append(f"{safe_field} = %s")
            params.append(value)

        if not clauses:
            return "", []
        return " AND ".join(clauses), params

    def get_metric_series(
        self,
        metric_key: str,
        *,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        grain: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 5000,
    ) -> Dict[str, Any]:
        metric = self.get_metric_definition(metric_key)
        if not metric:
            raise ValueError(f"metric_key '{metric_key}' not found")

        availability = metric.get("availability") or self.evaluate_metric_availability(metric)
        if not availability.get("forecast_ready"):
            reasons = [item.get("message") for item in availability.get("blocking_reasons") or []]
            raise ValueError(
                "Metric is not forecast-ready: " + "; ".join(reason for reason in reasons if reason)
            )

        selected_grain = self._normalize_grain(grain or metric.get("default_grain") or "day", "grain")
        safe_table_name = self.db.validate_identifier(metric["table_name"])
        safe_time_field = self.db.validate_identifier(metric["time_field"])
        field_types = self._get_table_field_types(metric["table_name"])
        time_normalization = self._build_metric_time_normalization(
            metric["table_name"],
            metric["time_field"],
            field_types,
        )
        if not time_normalization.get("supported"):
            raise ValueError(
                f"time_field '{metric['time_field']}' cannot be normalized to DATETIME for metric series"
            )
        normalized_time_expression = str(time_normalization.get("normalized_expression") or "").strip()
        if not normalized_time_expression:
            raise ValueError("time normalization expression is empty")

        aggregation_sql = self._metric_aggregation_sql(metric)
        period_expression = self._metric_period_expression("_metric_time_value", selected_grain)
        limit_value = max(1, min(self._safe_int(limit, 5000), 20000))
        filters = filters or {}

        where_clauses: List[str] = ["_metric_time_value IS NOT NULL"]
        params: List[Any] = []
        if start_time:
            where_clauses.append("_metric_time_value >= CAST(%s AS DATETIME)")
            params.append(start_time)
        if end_time:
            where_clauses.append("_metric_time_value <= CAST(%s AS DATETIME)")
            params.append(end_time)

        dimension_clause, dimension_params = self._metric_filter_clause(
            filters,
            metric.get("dimensions") or [],
        )
        if dimension_clause:
            where_clauses.append(dimension_clause)
            params.extend(dimension_params)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql = f"""
        SELECT
            {period_expression} AS ts,
            {aggregation_sql} AS value,
            MIN(_metric_time_value) AS normalized_ts_min,
            MAX(_metric_time_value) AS normalized_ts_max
        FROM (
            SELECT
                *,
                {normalized_time_expression} AS _metric_time_value
            FROM {safe_table_name}
        ) metric_source
        {where_sql}
        GROUP BY ts
        ORDER BY ts ASC
        LIMIT %s
        """
        params.append(limit_value)
        rows = self.db.execute_query(sql, tuple(params))
        points = [
            {
                "ts": row.get("ts"),
                "value": row.get("value"),
            }
            for row in rows
        ]

        return {
            "success": True,
            "metric_key": metric["metric_key"],
            "display_name": metric.get("display_name") or metric["metric_key"],
            "table_name": metric.get("table_name"),
            "time_field": metric.get("time_field"),
            "value_field": metric.get("value_field") or None,
            "aggregation": metric.get("aggregation"),
            "default_grain": metric.get("default_grain"),
            "grain": selected_grain,
            "filters": filters,
            "time_range": {
                "start_time": start_time,
                "end_time": end_time,
            },
            "time_normalization": {
                "strategy": time_normalization.get("strategy"),
                "source_field_type": time_normalization.get("source_field_type"),
                "normalized_value_type": "DATETIME",
                "null_or_unparseable_filtered": True,
                "time_filter_field": safe_time_field,
            },
            "availability": availability,
            "points": points,
            "count": len(points),
        }

    def _get_remote_source_columns_sync(
        self,
        conn,
        database_name: str,
        source_table: str,
    ) -> Dict[str, str]:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(
                """
                SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (database_name, source_table),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
        return {
            str(row.get("column_name")): str(row.get("data_type") or "")
            for row in rows
            if row.get("column_name")
        }

    def _resolve_sync_execution_plan(
        self,
        *,
        requested_strategy: Optional[str],
        incremental_time_field: Optional[str],
        source_columns: Dict[str, str],
        target_table_exists: bool,
        incremental_start: Optional[str],
        incremental_end: Optional[str],
    ) -> Dict[str, Any]:
        normalized_strategy = self._normalize_sync_strategy(requested_strategy)
        normalized_incremental_time_field = str(incremental_time_field or "").strip() or None
        plan: Dict[str, Any] = {
            "requested_strategy": normalized_strategy,
            "effective_strategy": normalized_strategy,
            "incremental_time_field": normalized_incremental_time_field,
            "source_time_field_type": None,
            "incremental_start": incremental_start,
            "incremental_end": incremental_end,
            "fallback_reason": None,
            "explanation": "",
        }
        if normalized_strategy != "incremental":
            plan["explanation"] = "Requested full sync."
            return plan

        if not normalized_incremental_time_field:
            plan["effective_strategy"] = "full"
            plan["fallback_reason"] = "missing_incremental_time_field"
            plan["explanation"] = "incremental_time_field is required for incremental sync."
            return plan

        source_field_type = str(source_columns.get(normalized_incremental_time_field) or "")
        plan["source_time_field_type"] = source_field_type
        if not source_field_type:
            plan["effective_strategy"] = "full"
            plan["fallback_reason"] = "incremental_time_field_not_found"
            plan["explanation"] = (
                f"incremental_time_field '{normalized_incremental_time_field}' does not exist in source table."
            )
            return plan

        if not self._is_remote_temporal_type(source_field_type):
            plan["effective_strategy"] = "full"
            plan["fallback_reason"] = "incremental_time_field_not_temporal"
            plan["explanation"] = (
                f"incremental_time_field '{normalized_incremental_time_field}' type '{source_field_type}' "
                "is not temporal."
            )
            return plan

        if not target_table_exists and not incremental_start:
            plan["effective_strategy"] = "full"
            plan["fallback_reason"] = "incremental_bootstrap_requires_start_or_target"
            plan["explanation"] = (
                "target table is missing and incremental_start is not provided; fallback to full sync."
            )
            return plan

        plan["explanation"] = "Incremental sync is available."
        return plan

    def _get_target_incremental_anchor(
        self,
        *,
        target_table: str,
        incremental_time_field: str,
    ) -> Any:
        safe_target_table = self.db.validate_identifier(target_table)
        safe_incremental_field = self.db.validate_identifier(incremental_time_field)
        rows = self.db.execute_query(
            f"""
            SELECT MAX(CAST({safe_incremental_field} AS DATETIME)) AS incremental_anchor
            FROM {safe_target_table}
            """
        )
        if not rows:
            return None
        return rows[0].get("incremental_anchor")

    def finalize_table_ingestion(
        self,
        table_name: str,
        source_type: str,
        *,
        replace_existing: bool = False,
        clear_relationships: bool = True,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        origin_kind: Optional[str] = None,
        origin_id: Optional[str] = None,
        origin_label: Optional[str] = None,
        origin_path: Optional[str] = None,
        origin_table: Optional[str] = None,
        sync_task_id: Optional[str] = None,
        ingest_mode: Optional[str] = None,
        last_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        """缁熶竴鏀跺彛钀借〃鍚庣殑 registry/source/analysis 璧勪骇銆?"""
        safe_table_name = (table_name or "").strip()
        if not safe_table_name:
            raise ValueError("table_name is required")

        assets_reset = False
        if replace_existing:
            self._reset_table_analysis_assets_sync(
                safe_table_name,
                clear_relationships=clear_relationships,
            )
            assets_reset = True

        registry_result = self.ensure_table_registry(
            safe_table_name,
            source_type,
            display_name=display_name,
            description=description,
        )
        source_result = self.upsert_table_source(
            safe_table_name,
            source_type,
            origin_kind=origin_kind,
            origin_id=origin_id,
            origin_label=origin_label,
            origin_path=origin_path,
            origin_table=origin_table,
            sync_task_id=sync_task_id,
            ingest_mode=ingest_mode,
            last_rows=last_rows,
            analysis_status="pending",
        )

        return {
            "success": True,
            "table_name": safe_table_name,
            "registry_updated": bool(registry_result.get("success")),
            "assets_reset": assets_reset,
            "analysis_status": source_result.get("analysis_status"),
        }

    def list_query_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        sql = """
        SELECT `id`, `question`, `sql`, `table_names`, `is_empty_result`, `row_count`, `created_at`
        FROM `_sys_query_history`
        WHERE `quality_gate` = 1
        ORDER BY `created_at` DESC
        LIMIT %s
        """
        return self.db.execute_query(sql, (limit,))

    async def list_query_history_async(self, limit: int = 100) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self.list_query_history, limit)

    def update_query_feedback(self, query_id: str, quality_gate: int) -> Dict[str, Any]:
        sql = """
        UPDATE `_sys_query_history`
        SET `quality_gate` = %s
        WHERE `id` = %s
        """
        self.db.execute_update(sql, (quality_gate, query_id))
        return {"success": True, "id": query_id, "quality_gate": quality_gate}

    async def update_query_feedback_async(self, query_id: str, quality_gate: int) -> Dict[str, Any]:
        return await asyncio.to_thread(self.update_query_feedback, query_id, quality_gate)

    def create_relationship(
        self,
        table_a: str,
        column_a: str,
        table_b: str,
        column_b: str,
        rel_type: str = "logical",
        confidence: float = 1.0,
        is_manual: bool = True,
    ) -> Dict[str, Any]:
        rel_id = str(uuid.uuid4())
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        INSERT INTO `_sys_table_relationships`
        (`id`, `table_a`, `column_a`, `table_b`, `column_b`, `rel_type`, `confidence`, `is_manual`, `created_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.db.execute_update(
            sql,
            (rel_id, table_a, column_a, table_b, column_b, rel_type, confidence, is_manual, now),
        )
        return {
            "success": True,
            "relationship": {
                "id": rel_id,
                "table_a": table_a,
                "column_a": column_a,
                "table_b": table_b,
                "column_b": column_b,
                "rel_type": rel_type,
                "confidence": confidence,
                "is_manual": is_manual,
            },
        }

    async def create_relationship_async(self, **kwargs) -> Dict[str, Any]:
        return await asyncio.to_thread(self.create_relationship, **kwargs)

    def list_relationships(self, tables: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if tables:
            placeholders = ", ".join(["%s"] * len(tables))
            sql = f"""
            SELECT * FROM `_sys_table_relationships`
            WHERE `table_a` IN ({placeholders}) OR `table_b` IN ({placeholders})
            ORDER BY `is_manual` DESC, `confidence` DESC, `created_at` DESC
            """
            params = tuple(tables) + tuple(tables)
            return self.db.execute_query(sql, params)
        sql = """
        SELECT * FROM `_sys_table_relationships`
        ORDER BY `is_manual` DESC, `confidence` DESC, `created_at` DESC
        """
        return self.db.execute_query(sql)

    async def list_relationships_async(self, tables: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self.list_relationships, tables)

    def ensure_query_history_vector_support(self, dimension: int = 512) -> Dict[str, Any]:
        statements = [
            "ALTER TABLE `_sys_query_history` ADD COLUMN `question_embedding` ARRAY<FLOAT>",
            f"""
            CREATE INDEX IF NOT EXISTS idx_query_history_embedding
            ON `_sys_query_history` (`question_embedding`)
            USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="inner_product",
                "dim"="{dimension}"
            )
            """,
        ]

        executed = []
        for statement in statements:
            try:
                self.db.execute_update(statement)
                executed.append(statement.strip())
            except Exception:
                # Doris may reject duplicate ALTER/INDEX creation or ANN on UNIQUE KEY tables.
                pass

        return {"success": True, "dimension": dimension, "statements": executed}
    
    def _encrypt_password(self, password: str) -> str:
        """鍔犲瘑瀵嗙爜"""
        return self.cipher.encrypt(password.encode()).decode()
    
    def _decrypt_password(self, encrypted: str) -> str:
        """瑙ｅ瘑瀵嗙爜"""
        return self.cipher.decrypt(encrypted.encode()).decode()
    
    def test_connection(self, host: str, port: int, user: str, 
                       password: str, database: str = None) -> Dict[str, Any]:
        """娴嬭瘯鏁版嵁搴撹繛鎺?"""
        try:
            conn_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'connect_timeout': 30,
                'read_timeout': 30
            }
            if database:
                conn_params['database'] = database
                
            conn = pymysql.connect(**conn_params)
            cursor = conn.cursor()
            
            # 鑾峰彇鏁版嵁搴撳垪琛?
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'message': '杩炴帴鎴愬姛',
                'databases': databases
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'杩炴帴澶辫触: {str(e)}',
                'databases': []
            }
    
    def get_remote_tables(self, host: str, port: int, user: str,
                         password: str, database: str) -> Dict[str, Any]:
        """鑾峰彇杩滅▼鏁版嵁搴撶殑琛ㄥ垪琛?"""
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                connect_timeout=30,
                read_timeout=60
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            # 鑾峰彇琛ㄥ垪琛ㄥ拰鍩烘湰淇℃伅
            cursor.execute("""
                SELECT 
                    TABLE_NAME as name,
                    TABLE_ROWS as row_count,
                    TABLE_COMMENT as comment
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            """, (database,))
            tables = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'tables': tables,
                'count': len(tables)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'tables': []
            }

    def preview_remote_table(self, host: str, port: int, user: str,
                              password: str, database: str, table_name: str,
                              limit: int = 100) -> Dict[str, Any]:
        """棰勮杩滅▼琛ㄧ殑缁撴瀯鍜屾暟鎹?"""
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                connect_timeout=10
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            # 鑾峰彇琛ㄧ粨鏋?
            cursor.execute(f"""
                SELECT
                    COLUMN_NAME as name,
                    DATA_TYPE as type,
                    COLUMN_TYPE as full_type,
                    IS_NULLABLE as nullable,
                    COLUMN_COMMENT as comment
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table_name))
            columns = cursor.fetchall()

            # 鑾峰彇鍓?00琛屾暟鎹?
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s", (limit,))
            data = cursor.fetchall()

            # 鑾峰彇鎬昏鏁?
            cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
            total = cursor.fetchone()['total']

            cursor.close()
            conn.close()

            return {
                'success': True,
                'table_name': table_name,
                'columns': columns,
                'data': data,
                'total_rows': total,
                'preview_rows': len(data)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def save_datasource(self, name: str, host: str, port: int,
                       user: str, password: str, database: str) -> Dict[str, Any]:
        """淇濆瓨鏁版嵁婧愰厤缃?"""
        import uuid

        ds_id = str(uuid.uuid4())[:8]
        encrypted_pwd = self._encrypt_password(password)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = """
        INSERT INTO `_sys_datasources`
        (`id`, `name`, `host`, `port`, `user`, `password_encrypted`,
         `database_name`, `created_at`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.db.execute_update(sql, (
            ds_id, name, host, port, user, encrypted_pwd,
            database, now, now
        ))

        return {
            'success': True,
            'id': ds_id,
            'message': f'鏁版嵁婧?"{name}" 淇濆瓨鎴愬姛'
        }

    def list_datasources(self) -> List[Dict[str, Any]]:
        """鑾峰彇鎵€鏈夋暟鎹簮"""
        sql = """
        SELECT id, name, host, port, user, database_name, created_at
        FROM `_sys_datasources`
        ORDER BY created_at DESC
        """
        return self.db.execute_query(sql)

    def get_datasource(self, ds_id: str) -> Optional[Dict[str, Any]]:
        """鑾峰彇鍗曚釜鏁版嵁婧愰厤缃紙鍖呭惈瑙ｅ瘑瀵嗙爜锛?"""
        sql = "SELECT * FROM `_sys_datasources` WHERE id = %s"
        results = self.db.execute_query(sql, (ds_id,))
        if results:
            ds = results[0]
            ds['password'] = self._decrypt_password(ds['password_encrypted'])
            del ds['password_encrypted']
            return ds
        return None

    def delete_datasource(self, ds_id: str) -> Dict[str, Any]:
        """鍒犻櫎鏁版嵁婧?"""
        sql = "DELETE FROM `_sys_datasources` WHERE id = %s"
        self.db.execute_update(sql, (ds_id,))
        return {'success': True, 'message': '鏁版嵁婧愬凡鍒犻櫎'}

    def sync_table(self, ds_id: str, source_table: str,
                   target_table: str = None) -> Dict[str, Any]:
        """鍚屾鍗曚釜琛?"""
        ds = self.get_datasource(ds_id)
        if not ds:
            return {'success': False, 'error': '鏁版嵁婧愪笉瀛樺湪'}

        if not target_table:
            target_table = source_table

        try:
            # 杩炴帴杩滅▼鏁版嵁搴?(浣跨敤 SSCursor 瀹炵幇娴佸紡璇诲彇)
            conn = pymysql.connect(
                host=ds['host'], port=ds['port'],
                user=ds['user'], password=ds['password'],
                database=ds['database_name'],
                connect_timeout=60,  # 澧炲姞杩炴帴瓒呮椂
                cursorclass=pymysql.cursors.SSCursor  # 鍏抽敭锛氫娇鐢ㄦ湇鍔＄娓告爣
            )

            # 浣跨敤 chunksize 鍒嗘壒璇诲彇
            chunk_size = 10000  # 鍑忓皬鍒嗘壒澶у皬锛岄檷浣庡唴瀛樺帇鍔?
            total_rows_synced = 0
            table_created_in_this_process = False
            last_stream_load_result = None

            try:
                cursor = conn.cursor()
                source_table_safe = f"`{source_table}`"
                cursor.execute(f"SELECT * FROM {source_table_safe}")
                
                # 鑾峰彇鍒楀悕
                columns = [col[0] for col in cursor.description]
                
                batch_count = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    batch_count += 1
                    # 杞崲涓?DataFrame 浠ュ鐢ㄧ幇鏈夐€昏緫
                    df = pd.DataFrame(rows, columns=columns)

                    # 娓呯悊鍒楀悕
                    df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

                    # 浠呭湪绗竴鎵规妫€鏌ュ拰鍒涘缓琛?
                    if batch_count == 1:
                        # 妫€鏌ョ洰鏍囪〃鏄惁瀛樺湪
                        table_exists = self.db.table_exists(target_table)
                        if not table_exists:
                            # 鑷姩鎺ㄦ柇鍒楃被鍨嬪苟鍒涘缓琛?
                            column_types = {}
                            for col in df.columns:
                                dtype = df[col].dtype
                                if pd.api.types.is_integer_dtype(dtype):
                                    column_types[col] = 'BIGINT'
                                elif pd.api.types.is_float_dtype(dtype):
                                    column_types[col] = 'DECIMAL(18,2)'
                                elif pd.api.types.is_datetime64_any_dtype(dtype):
                                    column_types[col] = 'DATETIME'
                                else:
                                    column_types[col] = 'VARCHAR(500)'

                            excel_handler.create_table(target_table, column_types)
                            table_created_in_this_process = True
                        else:
                            safe_target = self.db.validate_identifier(target_table)
                            try:
                                self.db.execute_update(f"TRUNCATE TABLE {safe_target}")
                            except Exception:
                                self.db.execute_update(f"DELETE FROM {safe_target} WHERE 1=1")

                    # 浣跨敤 Stream Load 瀵煎叆褰撳墠鎵规
                    print(f"馃攧 Importing batch {batch_count} ({len(df)} rows) into {target_table}...")
                    last_stream_load_result = excel_handler.stream_load(df, target_table)
                    total_rows_synced += len(df)
            
            finally:
                conn.close()
            
            if total_rows_synced == 0:
                 return {
                    'success': True,
                    'message': '琛ㄤ负绌猴紝鏃犳暟鎹悓姝?',
                    'rows_synced': 0
                }

            return {
                'success': True,
                'source_table': source_table,
                'target_table': target_table,
                'rows_synced': total_rows_synced,
                'table_created': table_created_in_this_process,
                'stream_load_result': last_stream_load_result
            }

        except Exception as e:
            import traceback
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }

    def sync_multiple_tables(self, ds_id: str,
                            tables: List[Dict[str, str]]) -> Dict[str, Any]:
        """鍚屾澶氫釜琛?"""
        print(f"馃摝 寮€濮嬫壒閲忓悓姝?{len(tables)} 寮犺〃, ds_id={ds_id}")
        print(f"馃搵 tables: {tables}")

        results = []
        success_count = 0
        fail_count = 0

        for table_config in tables:
            source = table_config.get('source_table')
            target = table_config.get('target_table', source)
            print(f"馃攧 鍚屾琛? {source} -> {target}")

            result = self.sync_table(ds_id, source, target)
            print(f"馃搳 鍚屾缁撴灉: {result}")

            results.append({
                'source_table': source,
                'target_table': target,
                **result
            })

            if result.get('success'):
                success_count += 1
            else:
                fail_count += 1

        print(f"鉁?鎵归噺鍚屾瀹屾垚: 鎴愬姛={success_count}, 澶辫触={fail_count}")
        print(f"馃攳 璇︾粏缁撴灉: {json.dumps(results, indent=2, default=str)}")
        
        response = {
            'success': fail_count == 0,
            'total': len(tables),
            'success_count': success_count,
            'fail_count': fail_count,
            'results': results
        }

        if fail_count > 0:
            # 鎻愬彇绗竴涓け璐ョ殑閿欒淇℃伅浣滀负涓昏閿欒
            failed_results = [r for r in results if not r.get('success')]
            first_error = failed_results[0].get('error', 'Unknown error') if failed_results else 'Unknown error'
            response['error'] = f"鍚屾瀹屾垚锛屼絾鍦?{fail_count} 寮犺〃涓彂鐢熼敊璇? {first_error}"
            print(f"鉂?璁剧疆椤跺眰閿欒: {response['error']}")
            
        return response

    def save_sync_task(self, ds_id: str, source_table: str,
                       target_table: str, schedule_type: str,
                       schedule_minute: int = 0, schedule_hour: int = 0,
                       schedule_day_of_week: int = 1, schedule_day_of_month: int = 1,
                       enabled_for_ai: bool = True) -> Dict[str, Any]:
        """
        淇濆瓨鍚屾浠诲姟閰嶇疆锛堝寮虹増锛?

        Args:
            ds_id: 鏁版嵁婧怚D
            source_table: 婧愯〃鍚?
            target_table: 鐩爣琛ㄥ悕
            schedule_type: 璋冨害绫诲瀷 (hourly/daily/weekly/monthly)
            schedule_minute: 鍒嗛挓 (0-59)
            schedule_hour: 灏忔椂 (0-23)
            schedule_day_of_week: 鍛ㄥ嚑 (1-7, 1=鍛ㄤ竴)
            schedule_day_of_month: 鏃ユ湡 (1-31)
            enabled_for_ai: 鏄惁鍚敤AI鍒嗘瀽
        """
        import uuid

        task_id = str(uuid.uuid4())[:8]
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 璁＄畻涓嬫鍚屾鏃堕棿
        next_sync = self._calculate_next_sync_detailed(
            schedule_type, schedule_minute, schedule_hour,
            schedule_day_of_week, schedule_day_of_month
        )

        sql = """
        INSERT INTO `_sys_sync_tasks`
        (`id`, `datasource_id`, `source_table`, `target_table`,
         `schedule_type`, `schedule_minute`, `schedule_hour`,
         `schedule_day_of_week`, `schedule_day_of_month`,
         `schedule_value`, `last_sync_at`, `next_sync_at`,
         `status`, `enabled_for_ai`, `created_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.db.execute_update(sql, (
            task_id, ds_id, source_table, target_table or source_table,
            schedule_type, schedule_minute, schedule_hour,
            schedule_day_of_week, schedule_day_of_month,
            '', now, next_sync, 'active', 1 if enabled_for_ai else 0, now
        ))

        return {
            'success': True,
            'task_id': task_id,
            'next_sync_at': next_sync,
            'schedule_description': self._get_schedule_description(
                schedule_type, schedule_minute, schedule_hour,
                schedule_day_of_week, schedule_day_of_month
            )
        }

    def update_sync_task(self, task_id: str, schedule_type: str = None,
                         schedule_minute: int = None, schedule_hour: int = None,
                         schedule_day_of_week: int = None, schedule_day_of_month: int = None,
                         enabled_for_ai: bool = None) -> Dict[str, Any]:
        """鏇存柊鍚屾浠诲姟閰嶇疆"""
        updates = []
        params = []

        if schedule_type is not None:
            updates.append("schedule_type = %s")
            params.append(schedule_type)
        if schedule_minute is not None:
            updates.append("schedule_minute = %s")
            params.append(schedule_minute)
        if schedule_hour is not None:
            updates.append("schedule_hour = %s")
            params.append(schedule_hour)
        if schedule_day_of_week is not None:
            updates.append("schedule_day_of_week = %s")
            params.append(schedule_day_of_week)
        if schedule_day_of_month is not None:
            updates.append("schedule_day_of_month = %s")
            params.append(schedule_day_of_month)
        if enabled_for_ai is not None:
            updates.append("enabled_for_ai = %s")
            params.append(1 if enabled_for_ai else 0)

        if not updates:
            return {'success': False, 'error': '娌℃湁瑕佹洿鏂扮殑瀛楁'}

        params.append(task_id)
        sql = f"UPDATE `_sys_sync_tasks` SET {', '.join(updates)} WHERE id = %s"
        self.db.execute_update(sql, tuple(params))

        return {'success': True, 'message': '浠诲姟宸叉洿鏂?'}

    def toggle_ai_enabled(self, task_id: str, enabled: bool) -> Dict[str, Any]:
        """鍒囨崲琛ㄧ殑AI鍒嗘瀽鍚敤鐘舵€?"""
        sql = "UPDATE `_sys_sync_tasks` SET enabled_for_ai = %s WHERE id = %s"
        self.db.execute_update(sql, (1 if enabled else 0, task_id))
        return {
            'success': True,
            'enabled_for_ai': enabled,
            'message': ('ai_enabled' if enabled else 'ai_disabled')
        }

    def get_ai_enabled_tables(self) -> List[str]:
        """鑾峰彇鎵€鏈夊惎鐢ˋI鍒嗘瀽鐨勮〃鍚?"""
        sql = "SELECT DISTINCT target_table FROM `_sys_sync_tasks` WHERE enabled_for_ai = 1"
        results = self.db.execute_query(sql)
        return [r['target_table'] for r in results]

    def _get_schedule_description(self, schedule_type: str, minute: int, hour: int,
                                   day_of_week: int, day_of_month: int) -> str:
        """鐢熸垚璋冨害鎻忚堪"""
        weekdays = ['', '鍛ㄤ竴', '鍛ㄤ簩', '鍛ㄤ笁', '鍛ㄥ洓', '鍛ㄤ簲', '鍛ㄥ叚', '鍛ㄦ棩']
        time_str = f"{hour:02d}:{minute:02d}"

        if schedule_type == 'hourly':
            return f"every hour at minute {minute:02d}"
        elif schedule_type == 'daily':
            return f"daily at {time_str}"
        elif schedule_type == 'weekly':
            return f"weekly {weekdays[day_of_week]} {time_str}"
        elif schedule_type == 'monthly':
            return f"monthly day {day_of_month} {time_str}"
        return schedule_type

    def _calculate_next_sync_detailed(self, schedule_type: str, minute: int, hour: int,
                                       day_of_week: int, day_of_month: int) -> str:
        """璁＄畻涓嬫鍚屾鏃堕棿锛堣缁嗙増锛?"""
        from datetime import timedelta

        now = datetime.now()

        if schedule_type == 'hourly':
            # 涓嬩竴涓皬鏃剁殑绗琋鍒嗛挓
            next_time = now.replace(minute=minute, second=0, microsecond=0)
            if next_time <= now:
                next_time += timedelta(hours=1)

        elif schedule_type == 'daily':
            # 鏄庡ぉ鐨勬寚瀹氭椂闂?
            next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_time <= now:
                next_time += timedelta(days=1)

        elif schedule_type == 'weekly':
            # 涓嬩竴涓寚瀹氬懆鍑犵殑鎸囧畾鏃堕棿
            next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            days_ahead = day_of_week - now.isoweekday()
            if days_ahead < 0 or (days_ahead == 0 and next_time <= now):
                days_ahead += 7
            next_time += timedelta(days=days_ahead)

        elif schedule_type == 'monthly':
            # 涓嬩釜鏈堢殑鎸囧畾鏃ユ湡鏃堕棿
            next_time = now.replace(day=min(day_of_month, 28), hour=hour,
                                     minute=minute, second=0, microsecond=0)
            if next_time <= now:
                # 绉诲埌涓嬩釜鏈?
                if now.month == 12:
                    next_time = next_time.replace(year=now.year + 1, month=1)
                else:
                    next_time = next_time.replace(month=now.month + 1)
        else:
            next_time = now + timedelta(days=1)

        return next_time.strftime('%Y-%m-%d %H:%M:%S')

    def _calculate_next_sync(self, schedule_type: str) -> str:
        """璁＄畻涓嬫鍚屾鏃堕棿锛堢畝鍖栫増锛屼繚鎸佸悜鍚庡吋瀹癸級"""
        return self._calculate_next_sync_detailed(schedule_type, 0, 0, 1, 1)

    def list_sync_tasks(self) -> List[Dict[str, Any]]:
        """鑾峰彇鎵€鏈夊悓姝ヤ换鍔?"""
        sql = """
        SELECT t.*, d.name as datasource_name
        FROM `_sys_sync_tasks` t
        LEFT JOIN `_sys_datasources` d ON t.datasource_id = d.id
        ORDER BY t.created_at DESC
        """
        return self.db.execute_query(sql)

    def delete_sync_task(self, task_id: str) -> Dict[str, Any]:
        """鍒犻櫎鍚屾浠诲姟"""
        sql = "DELETE FROM `_sys_sync_tasks` WHERE id = %s"
        self.db.execute_update(sql, (task_id,))
        return {'success': True, 'message': '鍚屾浠诲姟宸插垹闄?'}

    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        """鑾峰彇寰呮墽琛岀殑鍚屾浠诲姟"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        SELECT * FROM `_sys_sync_tasks`
        WHERE status = 'active' AND next_sync_at <= %s
        """
        return self.db.execute_query(sql, (now,))

    def execute_scheduled_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """鎵ц瀹氭椂浠诲姟"""
        schedule_value = safe_json_loads(task.get("schedule_value"), {})
        sync_strategy = str(schedule_value.get("sync_strategy") or "full")
        incremental_time_field = schedule_value.get("incremental_time_field")
        result = self._sync_table_sync_v2(
            ds_id=task['datasource_id'],
            source_table=task['source_table'],
            target_table=task['target_table'],
            sync_strategy=sync_strategy,
            incremental_time_field=incremental_time_field,
        )

        # 鏇存柊浠诲姟鐘舵€?
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_sync = self._calculate_next_sync(task['schedule_type'])

        sql = """
        UPDATE `_sys_sync_tasks`
        SET last_sync_at = %s, next_sync_at = %s
        WHERE id = %s
        """
        self.db.execute_update(sql, (now, next_sync, task['id']))

        return result

    # ============ 寮傛鍖呰鏂规硶 (for FastAPI async endpoints) ============

    async def test_connection(self, host: str, port: int, user: str,
                              password: str, database: str = None) -> Dict[str, Any]:
        """娴嬭瘯鏁版嵁搴撹繛鎺?(寮傛)"""
        return await asyncio.to_thread(
            self._test_connection_sync, host, port, user, password, database
        )

    def _test_connection_sync(self, host: str, port: int, user: str,
                              password: str, database: str = None) -> Dict[str, Any]:
        """娴嬭瘯鏁版嵁搴撹繛鎺?(鍚屾)"""
        try:
            conn_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'connect_timeout': 30,
                'read_timeout': 30
            }
            if database:
                conn_params['database'] = database

            conn = pymysql.connect(**conn_params)
            cursor = conn.cursor()

            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall()]

            cursor.close()
            conn.close()

            return {
                'success': True,
                'message': '杩炴帴鎴愬姛',
                'databases': databases
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'杩炴帴澶辫触: {str(e)}',
                'databases': []
            }

    async def save_datasource(self, name: str, host: str, port: int,
                              user: str, password: str, database: str) -> Dict[str, Any]:
        """淇濆瓨鏁版嵁婧愰厤缃?(寮傛)"""
        return await asyncio.to_thread(
            self._save_datasource_sync, name, host, port, user, password, database
        )

    async def list_datasources(self) -> List[Dict[str, Any]]:
        """鑾峰彇鎵€鏈夋暟鎹簮 (寮傛)"""
        return await asyncio.to_thread(self._list_datasources_sync)

    async def get_datasource(self, ds_id: str) -> Optional[Dict[str, Any]]:
        """鑾峰彇鍗曚釜鏁版嵁婧愰厤缃?(寮傛)"""
        return await asyncio.to_thread(self._get_datasource_sync, ds_id)

    async def delete_datasource(self, ds_id: str) -> Dict[str, Any]:
        """鍒犻櫎鏁版嵁婧?(寮傛)"""
        return await asyncio.to_thread(self._delete_datasource_sync, ds_id)

    async def get_remote_tables(self, host: str, port: int, user: str,
                                password: str, database: str) -> Dict[str, Any]:
        """鑾峰彇杩滅▼鏁版嵁搴撶殑琛ㄥ垪琛?(寮傛)"""
        return await asyncio.to_thread(
            self._get_remote_tables_sync, host, port, user, password, database
        )

    async def sync_table(self, ds_id: str, source_table: str,
                         target_table: str = None, sync_strategy: str = "full",
                         incremental_time_field: Optional[str] = None,
                         incremental_start: Optional[str] = None,
                         incremental_end: Optional[str] = None) -> Dict[str, Any]:
        """鍚屾鍗曚釜琛?(寮傛)"""
        return await asyncio.to_thread(
            self._sync_table_sync_v2,
            ds_id,
            source_table,
            target_table,
            sync_strategy,
            incremental_time_field,
            incremental_start,
            incremental_end,
        )

    async def sync_multiple_tables(self, ds_id: str,
                                   tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """鍚屾澶氫釜琛?(寮傛)"""
        return await asyncio.to_thread(self._sync_multiple_tables_sync, ds_id, tables)

    async def preview_remote_table(self, host: str, port: int, user: str,
                                   password: str, database: str, table_name: str,
                                   limit: int = 100) -> Dict[str, Any]:
        """棰勮杩滅▼琛ㄧ殑缁撴瀯鍜屾暟鎹?(寮傛)"""
        return await asyncio.to_thread(
            self._preview_remote_table_sync, host, port, user, password, database, table_name, limit
        )

    async def save_sync_task(self, ds_id: str, source_table: str,
                             target_table: str, schedule_type: str,
                             schedule_minute: int = 0, schedule_hour: int = 0,
                             schedule_day_of_week: int = 1, schedule_day_of_month: int = 1,
                             enabled_for_ai: bool = True, sync_strategy: str = "full",
                             incremental_time_field: Optional[str] = None) -> Dict[str, Any]:
        """淇濆瓨鍚屾浠诲姟閰嶇疆 (寮傛)"""
        return await asyncio.to_thread(
            self._save_sync_task_sync, ds_id, source_table, target_table, schedule_type,
            schedule_minute, schedule_hour, schedule_day_of_week, schedule_day_of_month,
            enabled_for_ai, sync_strategy, incremental_time_field
        )

    async def update_sync_task(self, task_id: str, schedule_type: str,
                               schedule_minute: int = 0, schedule_hour: int = 0,
                               schedule_day_of_week: int = 1, schedule_day_of_month: int = 1,
                               enabled_for_ai: bool = True) -> Dict[str, Any]:
        """鏇存柊鍚屾浠诲姟閰嶇疆 (寮傛)"""
        return await asyncio.to_thread(
            self._update_sync_task_sync, task_id, schedule_type,
            schedule_minute, schedule_hour, schedule_day_of_week, schedule_day_of_month, enabled_for_ai
        )

    async def toggle_ai_enabled(self, task_id: str, enabled: bool) -> Dict[str, Any]:
        """鍒囨崲浠诲姟鐨凙I鍚敤鐘舵€?(寮傛)"""
        return await asyncio.to_thread(self._toggle_ai_enabled_sync, task_id, enabled)

    async def list_sync_tasks(self) -> List[Dict[str, Any]]:
        """鑾峰彇鎵€鏈夊悓姝ヤ换鍔?(寮傛)"""
        return await asyncio.to_thread(self._list_sync_tasks_sync)

    async def get_ai_enabled_tables(self) -> List[str]:
        """鑾峰彇鎵€鏈夊惎鐢ˋI鐨勮〃鍚?(寮傛)"""
        return await asyncio.to_thread(self._get_ai_enabled_tables_sync)

    async def delete_sync_task(self, task_id: str) -> Dict[str, Any]:
        """鍒犻櫎鍚屾浠诲姟 (寮傛)"""
        return await asyncio.to_thread(self._delete_sync_task_sync, task_id)

    async def list_table_registry(self, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """鑾峰彇琛ㄦ敞鍐屽垪琛?(寮傛)"""
        return await asyncio.to_thread(self._list_table_registry_sync, table_names)

    async def list_query_catalog(self) -> List[Dict[str, Any]]:
        """鑾峰彇涓氬姟璇箟鏌ヨ鐩綍 (寮傛)"""
        return await asyncio.to_thread(self._build_query_catalog_sync)

    async def list_foundation_tables(
        self,
        table_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """鑾峰彇绋冲畾鐨勫熀纭€灞傝〃鑱氬悎瑙嗗浘 (寮傛)銆?"""
        return await asyncio.to_thread(self._list_foundation_tables_sync, table_names)

    async def upsert_metric_definition_async(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """鍒涘缓鎴栨洿鏂版寚鏍囧畾涔?(寮傛)銆?"""
        return await asyncio.to_thread(self.upsert_metric_definition, payload)

    async def get_metric_definition_async(self, metric_key: str) -> Optional[Dict[str, Any]]:
        """鑾峰彇鎸囨爣瀹氫箟 (寮傛)銆?"""
        return await asyncio.to_thread(self.get_metric_definition, metric_key)

    async def list_metric_definitions_async(self, only_forecast_ready: bool = False) -> List[Dict[str, Any]]:
        """鑾峰彇鎸囨爣瀹氫箟鍒楄〃 (寮傛)銆?"""
        return await asyncio.to_thread(self.list_metric_definitions, only_forecast_ready)

    async def delete_metric_definition_async(self, metric_key: str) -> Dict[str, Any]:
        """鍒犻櫎鎸囨爣瀹氫箟 (寮傛)銆?"""
        return await asyncio.to_thread(self.delete_metric_definition, metric_key)

    async def get_metric_series_async(
        self,
        metric_key: str,
        *,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        grain: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 5000,
    ) -> Dict[str, Any]:
        """鎸夋寚鏍囧畾涔夎鍙栨椂闂村簭鍒楃偣鍒?(寮傛)銆?"""
        return await asyncio.to_thread(
            self.get_metric_series,
            metric_key,
            start_time=start_time,
            end_time=end_time,
            grain=grain,
            filters=filters,
            limit=limit,
        )

    async def get_table_profile_async(self, table_name: str) -> Optional[Dict[str, Any]]:
        """鑾峰彇鍗曡〃鍩虹灞傜敾鍍?(寮傛)銆?"""
        return await asyncio.to_thread(self.get_table_profile, table_name)

    async def list_relationship_models_async(
        self,
        tables: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """鑾峰彇绋冲畾鍏崇郴璇诲彇妯″瀷 (寮傛)銆?"""
        return await asyncio.to_thread(self.list_relationship_models, tables)

    async def update_table_registry(self, table_name: str, display_name: str = None,
                                    description: str = None) -> Dict[str, Any]:
        """鏇存柊琛ㄦ敞鍐屼俊鎭?(寮傛)"""
        return await asyncio.to_thread(
            self._update_table_registry_sync, table_name, display_name, description
        )

    async def reset_table_analysis_assets_async(
        self,
        table_name: str,
        clear_relationships: bool = True,
    ) -> Dict[str, Any]:
        """娓呯悊琛ㄧ殑娲剧敓鍒嗘瀽璧勪骇 (寮傛)"""
        return await asyncio.to_thread(
            self._reset_table_analysis_assets_sync,
            table_name,
            clear_relationships,
        )

    async def delete_registered_table_async(
        self,
        table_name: str,
        drop_physical: bool = True,
        cleanup_history: bool = True,
    ) -> Dict[str, Any]:
        """鍒犻櫎宸叉敞鍐岃〃鍙婂叾娲剧敓璧勪骇 (寮傛)"""
        return await asyncio.to_thread(
            self.delete_registered_table,
            table_name,
            drop_physical,
            cleanup_history,
        )

    async def finalize_table_ingestion_async(
        self,
        table_name: str,
        source_type: str,
        **kwargs,
    ) -> Dict[str, Any]:
        """缁熶竴鏀跺彛钀借〃鍚庣殑 registry/source/analysis 璧勪骇 (寮傛)銆?"""
        return await asyncio.to_thread(
            self.finalize_table_ingestion,
            table_name,
            source_type,
            **kwargs,
        )

    async def mark_table_analysis_status_async(
        self,
        table_name: str,
        status: str,
        *,
        analyzed_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """鏇存柊琛ㄥ垎鏋愮姸鎬?(寮傛)銆?"""
        return await asyncio.to_thread(
            self.mark_table_analysis_status,
            table_name,
            status,
            analyzed_at=analyzed_at,
        )

    async def ensure_table_registry_async(self, table_name: str, source_type: str) -> Dict[str, Any]:
        """纭繚琛ㄦ敞鍐屽瓨鍦?(寮傛)"""
        return await asyncio.to_thread(self.ensure_table_registry, table_name, source_type)

    # ============ 鍚屾鏂规硶鍒悕 (渚涘紓姝ユ柟娉曡皟鐢? ============
    # 杩欎簺鍒悕璁╁紓姝ュ寘瑁呭櫒鍙互璋冪敤鍘熸湁鐨勫悓姝ユ柟娉?

    def _save_datasource_sync(self, name, host, port, user, password, database):
        """淇濆瓨鏁版嵁婧愰厤缃?(鍚屾)"""
        import uuid
        ds_id = str(uuid.uuid4())[:8]
        encrypted_pwd = self._encrypt_password(password)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = """
        INSERT INTO `_sys_datasources`
        (`id`, `name`, `host`, `port`, `user`, `password_encrypted`,
         `database_name`, `created_at`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.db.execute_update(sql, (
            ds_id, name, host, port, user, encrypted_pwd,
            database, now, now
        ))
        return {'success': True, 'id': ds_id, 'message': f'鏁版嵁婧?"{name}" 淇濆瓨鎴愬姛'}

    def _list_datasources_sync(self):
        """鑾峰彇鎵€鏈夋暟鎹簮 (鍚屾)"""
        sql = """
        SELECT id, name, host, port, user, database_name, created_at
        FROM `_sys_datasources`
        ORDER BY created_at DESC
        """
        return self.db.execute_query(sql)

    def _get_datasource_sync(self, ds_id):
        """鑾峰彇鍗曚釜鏁版嵁婧愰厤缃?(鍚屾)"""
        sql = "SELECT * FROM `_sys_datasources` WHERE id = %s"
        results = self.db.execute_query(sql, (ds_id,))
        if results:
            ds = results[0]
            ds['password'] = self._decrypt_password(ds['password_encrypted'])
            del ds['password_encrypted']
            return ds
        return None

    def _delete_datasource_sync(self, ds_id):
        """鍒犻櫎鏁版嵁婧?(鍚屾)"""
        sql = "DELETE FROM `_sys_datasources` WHERE id = %s"
        self.db.execute_update(sql, (ds_id,))
        return {'success': True, 'message': '鏁版嵁婧愬凡鍒犻櫎'}

    def _get_remote_tables_sync(self, host, port, user, password, database):
        """鑾峰彇杩滅▼鏁版嵁搴撶殑琛ㄥ垪琛?(鍚屾)"""
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                connect_timeout=30,
                read_timeout=60
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""
                SELECT 
                    TABLE_NAME as name,
                    TABLE_ROWS as row_count,
                    TABLE_COMMENT as comment
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            """, (database,))
            tables = cursor.fetchall()
            cursor.close()
            conn.close()
            return {'success': True, 'tables': tables, 'count': len(tables)}
        except Exception as e:
            return {'success': False, 'error': str(e), 'tables': []}

    def _sync_table_sync(self, ds_id, source_table, target_table=None):
        """鍚屾鍗曚釜琛?(鍚屾) - 浣跨敤 SSCursor 娴佸紡鍒嗘壒璇诲彇锛岄伩鍏嶅ぇ琛?OOM 鍜岃秴鏃?"""
        ds = self._get_datasource_sync(ds_id)
        if not ds:
            return {'success': False, 'error': '鏁版嵁婧愪笉瀛樺湪'}

        if not target_table:
            target_table = source_table

        try:
            conn = pymysql.connect(
                host=ds['host'], port=ds['port'],
                user=ds['user'], password=ds['password'],
                database=ds['database_name'],
                connect_timeout=DB_CONNECT_TIMEOUT,
                read_timeout=DB_READ_TIMEOUT,
                write_timeout=DB_WRITE_TIMEOUT,
                cursorclass=pymysql.cursors.SSCursor,
            )

            chunk_size = 10000
            total_rows_synced = 0
            table_created_in_this_process = False
            last_stream_load_result = None

            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM `{source_table}`")
                columns = [col[0] for col in cursor.description]

                batch_count = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    batch_count += 1
                    df = pd.DataFrame(rows, columns=columns)
                    df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

                    if batch_count == 1:
                        table_exists = self.db.table_exists(target_table)
                        if not table_exists:
                            column_types = {}
                            for col in df.columns:
                                dtype = df[col].dtype
                                if pd.api.types.is_integer_dtype(dtype):
                                    column_types[col] = 'BIGINT'
                                elif pd.api.types.is_float_dtype(dtype):
                                    column_types[col] = 'DECIMAL(18,2)'
                                elif pd.api.types.is_datetime64_any_dtype(dtype):
                                    column_types[col] = 'DATETIME'
                                else:
                                    column_types[col] = 'VARCHAR(500)'
                            excel_handler.create_table(target_table, column_types)
                            table_created_in_this_process = True
                        else:
                            safe_target = self.db.validate_identifier(target_table)
                            try:
                                self.db.execute_update(f"TRUNCATE TABLE {safe_target}")
                            except Exception:
                                self.db.execute_update(f"DELETE FROM {safe_target} WHERE 1=1")

                    print(f"馃攧 Importing batch {batch_count} ({len(df)} rows) into {target_table}...")
                    last_stream_load_result = excel_handler.stream_load(df, target_table)
                    total_rows_synced += len(df)

            finally:
                conn.close()

            table_replaced = not table_created_in_this_process
            should_finalize = total_rows_synced > 0 or self.db.table_exists(target_table)
            finalize_result = None
            if should_finalize:
                finalize_result = self.finalize_table_ingestion(
                    target_table,
                    "database_sync",
                    replace_existing=table_replaced,
                    clear_relationships=True,
                    origin_kind="datasource",
                    origin_id=ds_id,
                    origin_label=ds.get("name") or ds_id,
                    origin_path=f"{ds.get('host')}:{ds.get('port')}/{ds.get('database_name')}",
                    origin_table=source_table,
                    ingest_mode="replace",
                    last_rows=total_rows_synced,
                )

            if total_rows_synced == 0:
                return {
                    'success': True,
                    'message': '琛ㄤ负绌猴紝鏃犳暟鎹悓姝?',
                    'source_table': source_table,
                    'target_table': target_table,
                    'rows_synced': 0,
                    'table_created': table_created_in_this_process,
                    'table_replaced': table_replaced,
                    'ingestion': finalize_result,
                }

            return {
                'success': True, 'source_table': source_table, 'target_table': target_table,
                'rows_synced': total_rows_synced, 'table_created': table_created_in_this_process,
                'table_replaced': table_replaced,
                'stream_load_result': last_stream_load_result,
                'ingestion': finalize_result,
            }
        except Exception as e:
            import traceback
            return {'success': False, 'error': str(e), 'traceback': traceback.format_exc()}

    def _sync_table_sync_v2(
        self,
        ds_id,
        source_table,
        target_table=None,
        sync_strategy: str = "full",
        incremental_time_field: Optional[str] = None,
        incremental_start: Optional[str] = None,
        incremental_end: Optional[str] = None,
    ):
        """V2 sync implementation with full/incremental strategy support."""
        ds = self._get_datasource_sync(ds_id)
        if not ds:
            return {'success': False, 'error': 'datasource_not_found'}

        if not target_table:
            target_table = source_table

        try:
            requested_strategy = self._normalize_sync_strategy(sync_strategy)
        except Exception as strategy_error:
            return {'success': False, 'error': str(strategy_error)}

        try:
            conn = pymysql.connect(
                host=ds['host'], port=ds['port'],
                user=ds['user'], password=ds['password'],
                database=ds['database_name'],
                connect_timeout=DB_CONNECT_TIMEOUT,
                read_timeout=DB_READ_TIMEOUT,
                write_timeout=DB_WRITE_TIMEOUT,
                cursorclass=pymysql.cursors.SSCursor,
            )

            chunk_size = 10000
            total_rows_synced = 0
            table_created_in_this_process = False
            last_stream_load_result = None
            target_table_exists = self.db.table_exists(target_table)
            target_table_preexisting = bool(target_table_exists)
            source_columns = self._get_remote_source_columns_sync(conn, ds['database_name'], source_table)
            sync_plan = self._resolve_sync_execution_plan(
                requested_strategy=requested_strategy,
                incremental_time_field=incremental_time_field,
                source_columns=source_columns,
                target_table_exists=target_table_exists,
                incremental_start=incremental_start,
                incremental_end=incremental_end,
            )
            effective_strategy = sync_plan.get("effective_strategy") or "full"
            incremental_anchor = incremental_start
            if effective_strategy == "incremental" and not incremental_anchor and target_table_exists:
                target_incremental_field = str(sync_plan.get("incremental_time_field") or "").strip()
                target_field_types = self._get_table_field_types(target_table)
                target_anchor_field_type = str(target_field_types.get(target_incremental_field) or "")

                if not target_anchor_field_type:
                    sync_plan["effective_strategy"] = "full"
                    sync_plan["fallback_reason"] = "incremental_anchor_field_unavailable"
                    sync_plan["explanation"] = (
                        f"target table '{target_table}' does not contain incremental field "
                        f"'{target_incremental_field}'; fallback to full sync."
                    )
                    effective_strategy = "full"
                elif not (
                    self._is_temporal_type(target_anchor_field_type)
                    or self._is_string_like_type(target_anchor_field_type)
                ):
                    sync_plan["effective_strategy"] = "full"
                    sync_plan["fallback_reason"] = "incremental_anchor_field_unavailable"
                    sync_plan["explanation"] = (
                        f"target incremental field '{target_incremental_field}' type "
                        f"'{target_anchor_field_type}' cannot be cast to DATETIME; fallback to full sync."
                    )
                    effective_strategy = "full"
                else:
                    try:
                        incremental_anchor = self._get_target_incremental_anchor(
                            target_table=target_table,
                            incremental_time_field=target_incremental_field,
                        )
                    except Exception as anchor_error:
                        incremental_anchor = None
                        sync_plan["effective_strategy"] = "full"
                        sync_plan["fallback_reason"] = "incremental_anchor_unavailable"
                        sync_plan["explanation"] = (
                            "failed to read target incremental anchor; fallback to full sync. "
                            f"reason={anchor_error}"
                        )
                        effective_strategy = "full"

                if effective_strategy == "incremental" and incremental_anchor in (None, ""):
                    sync_plan["effective_strategy"] = "full"
                    sync_plan["fallback_reason"] = "incremental_anchor_unavailable"
                    sync_plan["explanation"] = "target table has no incremental anchor; fallback to full sync."
                    effective_strategy = "full"

            source_where_clauses: List[str] = []
            source_query_params: List[Any] = []
            source_order_sql = ""
            safe_source_table = self._quote_remote_identifier(source_table)
            if effective_strategy == "incremental":
                safe_source_time_field = self._quote_remote_identifier(
                    str(sync_plan.get("incremental_time_field") or "")
                )
                source_where_clauses.append(f"{safe_source_time_field} IS NOT NULL")
                if incremental_anchor not in (None, ""):
                    source_where_clauses.append(
                        f"CAST({safe_source_time_field} AS DATETIME) > CAST(%s AS DATETIME)"
                    )
                    source_query_params.append(str(incremental_anchor))
                if incremental_end not in (None, ""):
                    source_where_clauses.append(
                        f"CAST({safe_source_time_field} AS DATETIME) <= CAST(%s AS DATETIME)"
                    )
                    source_query_params.append(str(incremental_end))
                source_order_sql = f" ORDER BY CAST({safe_source_time_field} AS DATETIME) ASC"

            source_where_sql = f" WHERE {' AND '.join(source_where_clauses)}" if source_where_clauses else ""
            source_sql = f"SELECT * FROM {safe_source_table}{source_where_sql}{source_order_sql}"

            try:
                if effective_strategy == "full" and target_table_preexisting:
                    safe_target = self.db.validate_identifier(target_table)
                    try:
                        self.db.execute_update(f"TRUNCATE TABLE {safe_target}")
                    except Exception:
                        self.db.execute_update(f"DELETE FROM {safe_target} WHERE 1=1")

                cursor = conn.cursor()
                if source_query_params:
                    cursor.execute(source_sql, tuple(source_query_params))
                else:
                    cursor.execute(source_sql)
                columns = [col[0] for col in cursor.description]

                batch_count = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    batch_count += 1
                    df = pd.DataFrame(rows, columns=columns)
                    df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

                    if batch_count == 1:
                        if not target_table_exists:
                            column_types = {}
                            for col in df.columns:
                                dtype = df[col].dtype
                                if pd.api.types.is_integer_dtype(dtype):
                                    column_types[col] = 'BIGINT'
                                elif pd.api.types.is_float_dtype(dtype):
                                    column_types[col] = 'DECIMAL(18,2)'
                                elif pd.api.types.is_datetime64_any_dtype(dtype):
                                    column_types[col] = 'DATETIME'
                                else:
                                    column_types[col] = 'VARCHAR(500)'
                            excel_handler.create_table(target_table, column_types)
                            table_created_in_this_process = True
                            target_table_exists = True

                    print(f"棣冩敡 Importing batch {batch_count} ({len(df)} rows) into {target_table}...")
                    last_stream_load_result = excel_handler.stream_load(df, target_table)
                    total_rows_synced += len(df)

            finally:
                conn.close()

            table_replaced = bool(
                effective_strategy == "full"
                and target_table_preexisting
                and not table_created_in_this_process
            )
            should_finalize = total_rows_synced > 0 or (effective_strategy == "full" and self.db.table_exists(target_table))
            finalize_result = None
            if should_finalize:
                finalize_result = self.finalize_table_ingestion(
                    target_table,
                    "database_sync",
                    replace_existing=table_replaced,
                    clear_relationships=True,
                    origin_kind="datasource",
                    origin_id=ds_id,
                    origin_label=ds.get("name") or ds_id,
                    origin_path=f"{ds.get('host')}:{ds.get('port')}/{ds.get('database_name')}",
                    origin_table=source_table,
                    ingest_mode="incremental" if effective_strategy == "incremental" else "replace",
                    last_rows=total_rows_synced,
                )

            capability = {
                "requested_strategy": requested_strategy,
                "effective_strategy": effective_strategy,
                "incremental_time_field": sync_plan.get("incremental_time_field"),
                "source_time_field_type": sync_plan.get("source_time_field_type"),
                "fallback_to_full": requested_strategy == "incremental" and effective_strategy == "full",
                "fallback_reason": sync_plan.get("fallback_reason"),
                "explanation": sync_plan.get("explanation"),
                "window": {
                    "start": str(incremental_anchor) if incremental_anchor not in (None, "") else None,
                    "end": incremental_end,
                },
            }

            if total_rows_synced == 0:
                return {
                    'success': True,
                    'message': (
                        'incremental window has no new rows'
                        if effective_strategy == "incremental"
                        else 'table is empty, no rows synced'
                    ),
                    'source_table': source_table,
                    'target_table': target_table,
                    'rows_synced': 0,
                    'table_created': table_created_in_this_process,
                    'table_replaced': table_replaced,
                    'sync_capability': capability,
                    'ingestion': finalize_result,
                }

            return {
                'success': True, 'source_table': source_table, 'target_table': target_table,
                'rows_synced': total_rows_synced, 'table_created': table_created_in_this_process,
                'table_replaced': table_replaced,
                'stream_load_result': last_stream_load_result,
                'sync_capability': capability,
                'ingestion': finalize_result,
            }
        except Exception as e:
            import traceback
            return {'success': False, 'error': str(e), 'traceback': traceback.format_exc()}

    def _sync_multiple_tables_sync(self, ds_id, tables):
        """鍚屾澶氫釜琛?(鍚屾)"""
        results = []
        success_count = 0
        fail_count = 0
        for table_config in tables:
            source = table_config.get('source_table')
            target = table_config.get('target_table', source)
            requested_strategy = table_config.get("sync_strategy") or "full"
            incremental_time_field = table_config.get("incremental_time_field")
            incremental_start = table_config.get("incremental_start")
            incremental_end = table_config.get("incremental_end")
            result = self._sync_table_sync_v2(
                ds_id,
                source,
                target,
                requested_strategy,
                incremental_time_field,
                incremental_start,
                incremental_end,
            )
            results.append({'source_table': source, 'target_table': target, **result})
            if result.get('success'):
                success_count += 1
            else:
                fail_count += 1
        return {'success': fail_count == 0, 'total': len(tables), 'success_count': success_count,
                'fail_count': fail_count, 'results': results}

    def _preview_remote_table_sync(self, host, port, user, password, database, table_name, limit=100):
        """棰勮杩滅▼琛?(鍚屾)"""
        try:
            conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database,
                                   connect_timeout=30, read_timeout=60)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""SELECT COLUMN_NAME as name, DATA_TYPE as type FROM information_schema.COLUMNS
                              WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION""", (database, table_name))
            columns = cursor.fetchall()
            safe_table_name = self.db.validate_identifier(table_name)
            cursor.execute(f"SELECT * FROM {safe_table_name} LIMIT %s", (limit,))
            data = cursor.fetchall()
            cursor.execute(f"SELECT COUNT(*) as total FROM {safe_table_name}")
            total = cursor.fetchone()['total']
            cursor.close(); conn.close()
            return {'success': True, 'table_name': table_name, 'columns': columns, 'data': data,
                    'total_rows': total, 'preview_rows': len(data)}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _save_sync_task_sync(self, ds_id, source_table, target_table, schedule_type,
                             schedule_minute=0, schedule_hour=0, schedule_day_of_week=1,
                             schedule_day_of_month=1, enabled_for_ai=True,
                             sync_strategy: str = "full",
                             incremental_time_field: Optional[str] = None):
        """淇濆瓨鍚屾浠诲姟 (鍚屾)"""
        import uuid
        normalized_strategy = self._normalize_sync_strategy(sync_strategy)
        normalized_incremental_time_field = str(incremental_time_field or "").strip() or None
        if normalized_strategy == "incremental" and not normalized_incremental_time_field:
            raise ValueError("incremental_time_field is required when sync_strategy=incremental")
        task_id = str(uuid.uuid4())[:8]
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        schedule_value = json.dumps(
            {
                "sync_strategy": normalized_strategy,
                "incremental_time_field": normalized_incremental_time_field,
            },
            ensure_ascii=False,
        )
        sql = """INSERT INTO `_sys_sync_tasks` (`id`, `datasource_id`, `source_table`, `target_table`,
                 `schedule_type`, `schedule_minute`, `schedule_hour`, `schedule_day_of_week`,
                 `schedule_day_of_month`, `schedule_value`, `enabled_for_ai`, `status`, `created_at`)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)"""
        self.db.execute_update(sql, (task_id, ds_id, source_table, target_table, schedule_type,
                                      schedule_minute, schedule_hour, schedule_day_of_week,
                                      schedule_day_of_month, schedule_value, 1 if enabled_for_ai else 0, now))
        return {
            'success': True,
            'id': task_id,
            'message': 'sync_task_saved',
            'sync_strategy': normalized_strategy,
            'incremental_time_field': normalized_incremental_time_field,
        }

    def _update_sync_task_sync(self, task_id, schedule_type, schedule_minute=0,
                               schedule_hour=0, schedule_day_of_week=1,
                               schedule_day_of_month=1, enabled_for_ai=True):
        """鏇存柊鍚屾浠诲姟 (鍚屾)"""
        sql = """UPDATE `_sys_sync_tasks` SET schedule_type = %s, schedule_minute = %s,
                 schedule_hour = %s, schedule_day_of_week = %s, schedule_day_of_month = %s,
                 enabled_for_ai = %s WHERE id = %s"""
        self.db.execute_update(sql, (schedule_type, schedule_minute, schedule_hour, schedule_day_of_week,
                                      schedule_day_of_month, 1 if enabled_for_ai else 0, task_id))
        return {'success': True, 'message': '浠诲姟宸叉洿鏂?'}

    def _toggle_ai_enabled_sync(self, task_id, enabled):
        """鍒囨崲AI鍚敤鐘舵€?(鍚屾)"""
        sql = "UPDATE `_sys_sync_tasks` SET enabled_for_ai = %s WHERE id = %s"
        self.db.execute_update(sql, (1 if enabled else 0, task_id))
        return {'success': True, 'enabled_for_ai': enabled, 'message': ('ai_enabled' if enabled else 'ai_disabled')}


    def _list_sync_tasks_sync(self):
        """鑾峰彇鎵€鏈夊悓姝ヤ换鍔?(鍚屾)"""
        sql = """
        SELECT t.*, d.name as datasource_name
        FROM `_sys_sync_tasks` t
        LEFT JOIN `_sys_datasources` d ON t.datasource_id = d.id
        ORDER BY t.created_at DESC
        """
        return self.db.execute_query(sql)

    def _get_ai_enabled_tables_sync(self):
        """鑾峰彇鍚敤AI鐨勮〃 (鍚屾)"""
        sql = """
        SELECT DISTINCT target_table
        FROM `_sys_sync_tasks`
        WHERE enabled_for_ai = 1
        """
        results = self.db.execute_query(sql)
        return [r['target_table'] for r in results]

    def _delete_sync_task_sync(self, task_id):
        """鍒犻櫎鍚屾浠诲姟 (鍚屾)"""
        sql = "DELETE FROM `_sys_sync_tasks` WHERE id = %s"
        self.db.execute_update(sql, (task_id,))
        return {'success': True, 'message': '鍚屾浠诲姟宸插垹闄?'}

    def _query_with_optional_tables(self, sql: str, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not table_names:
            return self.db.execute_query(sql)

        placeholders = ", ".join(["%s"] * len(table_names))
        return self.db.execute_query(f"{sql} WHERE table_name IN ({placeholders})", tuple(table_names))

    def _get_registry_context_rows_sync(self, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            r.table_name,
            r.display_name,
            r.description,
            r.source_type,
            r.created_at,
            r.updated_at,
            m.description AS auto_description,
            m.columns_info,
            m.sample_queries,
            m.analyzed_at,
            m.source_type AS metadata_source_type,
            a.agent_config,
            a.source_hash,
            s.origin_kind,
            s.origin_id,
            s.origin_label,
            s.origin_path,
            s.origin_table,
            s.sync_task_id,
            s.ingest_mode,
            s.last_rows,
            s.analysis_status,
            s.last_ingested_at,
            s.last_analyzed_at
        FROM `_sys_table_registry` r
        LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
        LEFT JOIN `_sys_table_agents` a ON r.table_name = a.table_name
        LEFT JOIN `_sys_table_sources` s ON r.table_name = s.table_name
        """
        if table_names:
            placeholders = ", ".join(["%s"] * len(table_names))
            sql += f" WHERE r.table_name IN ({placeholders})"
            params = tuple(table_names)
        else:
            params = None
        sql += " ORDER BY COALESCE(m.analyzed_at, r.updated_at) DESC"
        return self.db.execute_query(sql, params)

    def _get_metadata_rows_sync(self, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        sql = """
        SELECT `table_name`, `description`, `columns_info`, `sample_queries`, `analyzed_at`, `source_type`
        FROM `_sys_table_metadata`
        """
        return self._query_with_optional_tables(sql, table_names)

    def _get_field_catalog_rows_sync(self, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        sql = """
        SELECT `table_name`, `field_name`, `field_type`, `enum_values`, `value_range`
        FROM `_sys_field_catalog`
        """
        return self._query_with_optional_tables(sql, table_names)

    def _get_table_source_rows_sync(self, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            `table_name`, `source_type`, `origin_kind`, `origin_id`, `origin_label`,
            `origin_path`, `origin_table`, `sync_task_id`, `ingest_mode`, `last_rows`,
            `analysis_status`, `last_ingested_at`, `last_analyzed_at`, `created_at`, `updated_at`
        FROM `_sys_table_sources`
        """
        return self._query_with_optional_tables(sql, table_names)

    def _build_field_items_by_table_sync(
        self,
        table_names: List[str],
        metadata_by_table: Dict[str, Dict[str, Any]],
        field_catalog_by_table: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        result: Dict[str, List[Dict[str, Any]]] = {}

        for table_name in table_names:
            columns_info = (metadata_by_table.get(table_name) or {}).get("columns_info") or {}
            try:
                schema_rows = self.db.get_table_schema(table_name)
            except Exception:
                schema_rows = []

            seen_fields = set()
            field_items: List[Dict[str, Any]] = []
            for schema_row in schema_rows:
                field_name = schema_row.get("Field")
                if not field_name or field_name in seen_fields:
                    continue
                seen_fields.add(field_name)
                field_meta = field_catalog_by_table.get(table_name, {}).get(field_name, {})
                field_items.append(
                    build_field_payload(
                        field_name=field_name,
                        description=columns_info.get(field_name) or "",
                        schema_type=schema_row.get("Type") or "",
                        semantic=field_meta.get("semantic") or "",
                        enum_values=field_meta.get("enum_values") or [],
                        value_range=field_meta.get("value_range"),
                    )
                )

            for field_name, description in columns_info.items():
                if field_name in seen_fields:
                    continue
                seen_fields.add(field_name)
                field_meta = field_catalog_by_table.get(table_name, {}).get(field_name, {})
                field_items.append(
                    build_field_payload(
                        field_name=field_name,
                        description=description or "",
                        schema_type="",
                        semantic=field_meta.get("semantic") or "",
                        enum_values=field_meta.get("enum_values") or [],
                        value_range=field_meta.get("value_range"),
                    )
                )

            result[table_name] = field_items

        return result

    def _list_table_registry_sync(self, table_names: Optional[List[str]] = None):
        """鑾峰彇琛ㄦ敞鍐屽垪琛?(鍚屾)"""
        registry_rows = self._get_registry_context_rows_sync(table_names)
        if not registry_rows:
            return []

        table_names = [row.get("table_name") for row in registry_rows if row.get("table_name")]
        metadata_by_table = {
            row.get("table_name"): {
                "table_name": row.get("table_name"),
                "description": row.get("auto_description") or "",
                "columns_info": safe_json_loads(row.get("columns_info"), {}),
                "sample_queries": safe_json_loads(row.get("sample_queries"), []),
                "analyzed_at": row.get("analyzed_at"),
                "source_type": row.get("metadata_source_type") or row.get("source_type") or "",
            }
            for row in registry_rows
            if row.get("table_name")
        }
        source_by_table = {
            row.get("table_name"): build_source_payload(row)
            for row in registry_rows
            if row.get("table_name")
        }

        relationship_counts: Dict[str, int] = {}
        for rel in self.list_relationships(table_names):
            for key in (rel.get("table_a"), rel.get("table_b")):
                if key:
                    relationship_counts[key] = relationship_counts.get(key, 0) + 1

        tables = []
        for row in registry_rows:
            table_name = row.get("table_name")
            if not table_name:
                continue
            metadata_payload = build_metadata_payload(
                table_name,
                metadata_by_table.get(table_name),
                fields=[],
                source_payload=source_by_table.get(table_name),
            )
            tables.append(
                build_registry_payload(
                    row,
                    metadata_payload=metadata_payload,
                    source_payload=source_by_table.get(table_name),
                    relationship_count=relationship_counts.get(table_name, 0),
                )
            )
        return tables

    @staticmethod
    def _safe_json_loads(value, default):
        return safe_json_loads(value, default)

    @staticmethod
    def _semantic_label(semantic: str) -> str:
        return semantic_label(semantic)

    @staticmethod
    def _short_field_display_name(field_name: str, description: str) -> str:
        return short_field_display_name(field_name, description)

    @staticmethod
    def _relation_type_label(rel_type: str) -> str:
        return relation_type_label(rel_type)

    def _build_query_catalog_sync(self) -> List[Dict[str, Any]]:
        """鏋勫缓浠呴潰鍚戜笟鍔¤〃鐨勬煡璇㈢洰褰曘€?"""
        registry_rows = self._list_table_registry_sync()
        if not registry_rows:
            return []

        table_names = [row.get("table_name") for row in registry_rows if row.get("table_name")]
        if not table_names:
            return []

        metadata_rows = self.db.execute_query(
            """
            SELECT `table_name`, `description`, `columns_info`
            FROM `_sys_table_metadata`
            """
        )
        metadata_by_table = {}
        for row in metadata_rows:
            table_name = row.get("table_name")
            if table_name:
                metadata_by_table[table_name] = {
                    "description": row.get("description") or "",
                    "columns_info": self._safe_json_loads(row.get("columns_info"), {}),
                }

        field_catalog_rows = self.db.execute_query(
            """
            SELECT `table_name`, `field_name`, `field_type`, `enum_values`, `value_range`
            FROM `_sys_field_catalog`
            """
        )
        field_catalog_by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in field_catalog_rows:
            table_name = row.get("table_name")
            field_name = row.get("field_name")
            if not table_name or not field_name:
                continue
            field_catalog_by_table.setdefault(table_name, {})[field_name] = {
                "semantic": row.get("field_type") or "",
                "semantic_label": self._semantic_label(row.get("field_type") or ""),
                "enum_values": self._safe_json_loads(row.get("enum_values"), []),
                "value_range": self._safe_json_loads(row.get("value_range"), None),
            }

        relationships = self.list_relationships(table_names)
        registry_by_table = {row["table_name"]: row for row in registry_rows if row.get("table_name")}

        catalog = []
        fields_by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in registry_rows:
            table_name = row.get("table_name")
            if not table_name:
                continue

            meta = metadata_by_table.get(table_name, {})
            columns_info = meta.get("columns_info") or {}
            try:
                schema_rows = self.db.get_table_schema(table_name)
            except Exception:
                schema_rows = []

            seen_fields = set()
            field_items = []
            for schema_row in schema_rows:
                field_name = schema_row.get("Field")
                if not field_name or field_name in seen_fields:
                    continue
                seen_fields.add(field_name)
                field_meta = field_catalog_by_table.get(table_name, {}).get(field_name, {})
                description = columns_info.get(field_name) or ""
                display_name = self._short_field_display_name(field_name, description)
                field_items.append(
                    {
                        "field_name": field_name,
                        "display_name": display_name,
                        "description": description,
                        "field_type": schema_row.get("Type") or "",
                        "semantic": field_meta.get("semantic") or "",
                        "semantic_label": field_meta.get("semantic_label") or "",
                        "enum_values": field_meta.get("enum_values") or [],
                        "value_range": field_meta.get("value_range"),
                    }
                )

            for field_name, description in columns_info.items():
                if field_name in seen_fields:
                    continue
                seen_fields.add(field_name)
                field_meta = field_catalog_by_table.get(table_name, {}).get(field_name, {})
                field_items.append(
                    {
                        "field_name": field_name,
                        "display_name": self._short_field_display_name(field_name, description),
                        "description": description or "",
                        "field_type": "",
                        "semantic": field_meta.get("semantic") or "",
                        "semantic_label": field_meta.get("semantic_label") or "",
                        "enum_values": field_meta.get("enum_values") or [],
                        "value_range": field_meta.get("value_range"),
                    }
                )

            fields_by_table[table_name] = {field["field_name"]: field for field in field_items}
            catalog.append(
                {
                    "table_name": table_name,
                    "display_name": row.get("display_name") or table_name,
                    "description": row.get("description") or meta.get("description") or row.get("auto_description") or "",
                    "source_type": row.get("source_type") or "",
                    "fields": field_items,
                    "relationships": [],
                }
            )

        catalog_by_table = {item["table_name"]: item for item in catalog}
        for rel in relationships:
            table_a = rel.get("table_a")
            table_b = rel.get("table_b")
            if table_a not in catalog_by_table or table_b not in catalog_by_table:
                continue

            normalized_pairs = (
                (table_a, table_b, rel.get("column_a"), rel.get("column_b")),
                (table_b, table_a, rel.get("column_b"), rel.get("column_a")),
            )
            for current_table, related_table, source_field_name, target_field_name in normalized_pairs:
                source_field = fields_by_table.get(current_table, {}).get(
                    source_field_name,
                    {"field_name": source_field_name, "display_name": source_field_name or ""},
                )
                target_field = fields_by_table.get(related_table, {}).get(
                    target_field_name,
                    {"field_name": target_field_name, "display_name": target_field_name or ""},
                )
                related_display_name = registry_by_table.get(related_table, {}).get("display_name") or related_table
                relation_description = (
                    f"join on {source_field.get('display_name') or source_field_name} = "
                    f"{target_field.get('display_name') or target_field_name} to {related_display_name}"
                )
                catalog_by_table[current_table]["relationships"].append(
                    {
                        "id": rel.get("id"),
                        "related_table_name": related_table,
                        "related_display_name": related_display_name,
                        "relation_type": rel.get("rel_type") or "logical",
                        "relation_type_label": self._relation_type_label(rel.get("rel_type") or "logical"),
                        "relation_label": f"{related_display_name} 路 {relation_description}",
                        "relation_description": relation_description,
                        "source_field_name": source_field_name,
                        "source_field_display_name": source_field.get("display_name") or source_field_name,
                        "target_field_name": target_field_name,
                        "target_field_display_name": target_field.get("display_name") or target_field_name,
                    }
                )

        for item in catalog:
            item["relationships"].sort(
                key=lambda rel: (
                    rel.get("related_display_name") or rel.get("related_table_name") or "",
                    rel.get("source_field_display_name") or "",
                    rel.get("target_field_display_name") or "",
                )
            )

        return catalog

    def list_relationship_models(self, tables: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """杩斿洖绋冲畾鐨勫叧绯昏鍙栨ā鍨嬨€?"""
        requested_registry_rows = self._list_table_registry_sync(tables)
        requested_table_names = [
            row.get("table_name")
            for row in requested_registry_rows
            if row.get("table_name")
        ]
        if not requested_table_names:
            return []

        relationship_rows = self.list_relationships(requested_table_names)
        if not relationship_rows:
            return []

        context_table_names = set(requested_table_names)
        for rel_row in relationship_rows:
            table_a = rel_row.get("table_a")
            table_b = rel_row.get("table_b")
            if table_a:
                context_table_names.add(table_a)
            if table_b:
                context_table_names.add(table_b)

        context_table_names_list = sorted(context_table_names)
        context_registry_rows = self._list_table_registry_sync(context_table_names_list)
        registry_by_table = {
            row["table_name"]: row
            for row in context_registry_rows
            if row.get("table_name")
        }

        metadata_rows = self._get_metadata_rows_sync(context_table_names_list)
        metadata_by_table = {
            row.get("table_name"): {
                "columns_info": safe_json_loads(row.get("columns_info"), {}),
            }
            for row in metadata_rows
            if row.get("table_name")
        }
        field_catalog_rows = self._get_field_catalog_rows_sync(context_table_names_list)
        field_catalog_by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in field_catalog_rows:
            table_name = row.get("table_name")
            field_name = row.get("field_name")
            if not table_name or not field_name:
                continue
            field_catalog_by_table.setdefault(table_name, {})[field_name] = {
                "semantic": row.get("field_type") or "",
                "enum_values": safe_json_loads(row.get("enum_values"), []),
                "value_range": safe_json_loads(row.get("value_range"), None),
            }

        field_items_by_table = self._build_field_items_by_table_sync(
            context_table_names_list,
            metadata_by_table,
            field_catalog_by_table,
        )
        field_maps_by_table = {
            table_name: {field["field_name"]: field for field in fields}
            for table_name, fields in field_items_by_table.items()
        }

        return [
            build_relationship_payload(
                rel_row,
                registry_by_table=registry_by_table,
                fields_by_table=field_maps_by_table,
            )
            for rel_row in relationship_rows
        ]

    def get_table_profile(self, table_name: str) -> Optional[Dict[str, Any]]:
        """鑾峰彇鍗曡〃绋冲畾鍩虹灞傜敾鍍忋€?"""
        safe_table_name = (table_name or "").strip()
        if not safe_table_name:
            return None

        registry_rows = self._list_table_registry_sync([safe_table_name])
        if not registry_rows:
            return None

        registry_payload = registry_rows[0]
        source_rows = self._get_table_source_rows_sync([safe_table_name])
        source_payload = build_source_payload(source_rows[0]) if source_rows else registry_payload.get("source")

        metadata_rows = self._get_metadata_rows_sync([safe_table_name])
        metadata_row = metadata_rows[0] if metadata_rows else None

        field_catalog_rows = self._get_field_catalog_rows_sync([safe_table_name])
        field_catalog_by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in field_catalog_rows:
            field_name = row.get("field_name")
            if not field_name:
                continue
            field_catalog_by_table.setdefault(safe_table_name, {})[field_name] = {
                "semantic": row.get("field_type") or "",
                "enum_values": safe_json_loads(row.get("enum_values"), []),
                "value_range": safe_json_loads(row.get("value_range"), None),
            }

        metadata_by_table = {
            safe_table_name: {
                "columns_info": safe_json_loads((metadata_row or {}).get("columns_info"), {}),
            }
        }
        fields = self._build_field_items_by_table_sync(
            [safe_table_name],
            metadata_by_table,
            field_catalog_by_table,
        ).get(safe_table_name, [])

        metadata_payload = build_metadata_payload(
            safe_table_name,
            metadata_row,
            fields,
            source_payload=source_payload,
        )
        registry_payload = build_registry_payload(
            registry_payload,
            metadata_payload=metadata_payload,
            source_payload=source_payload,
            relationship_count=registry_payload.get("relationship_count", 0),
        )

        relationships = self.list_relationship_models([safe_table_name])
        return build_table_profile_payload(
            safe_table_name,
            registry_payload=registry_payload,
            metadata_payload=metadata_payload,
            relationship_payloads=relationships,
            source_payload=source_payload,
        )

    def _list_foundation_tables_sync(self, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """鑾峰彇绋冲畾鐨勫熀纭€灞傝〃鑱氬悎瑙嗗浘銆?"""
        registry_rows = self._list_table_registry_sync(table_names)
        profiles = []
        for row in registry_rows:
            table_name = row.get("table_name")
            if not table_name:
                continue
            profile = self.get_table_profile(table_name)
            if profile:
                profiles.append(profile)
        return profiles

    def _update_table_registry_sync(self, table_name, display_name=None, description=None):
        """鏇存柊琛ㄦ敞鍐屼俊鎭?(鍚屾)"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        UPDATE `_sys_table_registry`
        SET display_name = COALESCE(%s, display_name),
            description = COALESCE(%s, description),
            updated_at = %s
        WHERE table_name = %s
        """
        self.db.execute_update(sql, (display_name, description, now, table_name))
        return {'success': True, 'message': '琛ㄤ俊鎭凡鏇存柊'}

    def _reset_table_analysis_assets_sync(self, table_name, clear_relationships=True):
        """娓呯悊琛ㄧ殑娲剧敓鍒嗘瀽璧勪骇 (鍚屾)"""
        safe_table_name = (table_name or "").strip()
        if not safe_table_name:
            raise ValueError("table_name is required")

        self.db.execute_update("DELETE FROM `_sys_table_metadata` WHERE table_name = %s", (safe_table_name,))
        self.db.execute_update("DELETE FROM `_sys_table_agents` WHERE table_name = %s", (safe_table_name,))
        self.db.execute_update("DELETE FROM `_sys_field_catalog` WHERE table_name = %s", (safe_table_name,))
        if clear_relationships:
            self.db.execute_update(
                "DELETE FROM `_sys_table_relationships` WHERE table_a = %s OR table_b = %s",
                (safe_table_name, safe_table_name),
            )
        return {
            'success': True,
            'table_name': safe_table_name,
            'relationships_cleared': bool(clear_relationships),
        }

    def delete_registered_table(self, table_name, drop_physical=True, cleanup_history=True):
        """鍒犻櫎宸叉敞鍐岃〃鍙婂叾娲剧敓璧勪骇 (鍚屾)"""
        safe_table_name = (table_name or "").strip()
        if not safe_table_name:
            raise ValueError("table_name is required")
        if safe_table_name.startswith("_sys_"):
            raise ValueError("system tables cannot be deleted via this API")

        physical_deleted = False
        if drop_physical and self._physical_table_exists(safe_table_name):
            validated_table_name = self.db.validate_identifier(safe_table_name)
            self.db.execute_update(f"DROP TABLE {validated_table_name}")
            physical_deleted = True

        self._reset_table_analysis_assets_sync(safe_table_name, clear_relationships=True)
        if cleanup_history:
            self.db.execute_update(
                "DELETE FROM `_sys_query_history` WHERE FIND_IN_SET(%s, `table_names`) > 0",
                (safe_table_name,),
            )
        self.db.execute_update("DELETE FROM `_sys_table_sources` WHERE table_name = %s", (safe_table_name,))
        self.db.execute_update("DELETE FROM `_sys_table_registry` WHERE table_name = %s", (safe_table_name,))
        self._wait_for_registry_absence(safe_table_name)
        if drop_physical:
            self._wait_for_physical_table_absence(safe_table_name)
        return {
            'success': True,
            'table_name': safe_table_name,
            'physical_table_deleted': physical_deleted,
            'history_cleaned': bool(cleanup_history),
        }

    def _wait_for_registry_absence(self, table_name, timeout_seconds=5.0):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            rows = self.db.execute_query(
                "SELECT table_name FROM `_sys_table_registry` WHERE table_name = %s LIMIT 1",
                (table_name,),
            )
            if not rows:
                return
            time.sleep(0.2)
        raise RuntimeError(f"table registry entry still visible after delete: {table_name}")

    def _wait_for_physical_table_absence(self, table_name, timeout_seconds=5.0):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if not self._physical_table_exists(table_name):
                return
            time.sleep(0.2)
        raise RuntimeError(f"physical table still visible after delete: {table_name}")

    def _physical_table_exists(self, table_name):
        rows = self.db.execute_query("SHOW TABLES LIKE %s", (table_name,))
        return bool(rows)



# 鍏ㄥ眬瀹炰緥
datasource_handler = DataSourceHandler()


# ============ 瀹氭椂璋冨害鍣?============

class SyncScheduler:
    """鍚屾浠诲姟璋冨害鍣?"""

    def __init__(self, handler: DataSourceHandler):
        self.handler = handler
        self.scheduler = None

    def register(self, shared_scheduler):
        """鍦ㄥ叡浜皟搴﹀櫒涓婃敞鍐屽悓姝ヤ换鍔°€?"""
        shared_scheduler.register_interval(
            self._check_and_execute_tasks,
            minutes=1,
            job_id="sync_checker",
        )
        shared_scheduler.register_cron(
            self._refresh_agent_catalogs,
            job_id="field_catalog_refresh",
            hour=0,
            minute=0,
        )
        print("鉁?鍚屾璋冨害鍣ㄥ凡娉ㄥ唽")

    def start(self):
        """鍏煎鏃ф祦绋嬬殑鏈湴璋冨害鍣ㄥ惎鍔ㄣ€?"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler

            if self.scheduler is not None:
                return
            print("鈿狅笍 SyncScheduler.start() is deprecated; use register(app_scheduler) instead.")
            self.scheduler = BackgroundScheduler()
            self.scheduler.add_job(
                self._check_and_execute_tasks,
                'interval',
                minutes=1,
                id='sync_checker'
            )
            self.scheduler.add_job(
                self._refresh_agent_catalogs,
                'cron',
                hour=0,
                minute=0,
                id='field_catalog_refresh'
            )
            self.scheduler.start()
            print("鉁?鍚屾璋冨害鍣ㄥ凡鍚姩")
        except Exception as e:
            print(f"鈿狅笍 鍚屾璋冨害鍣ㄥ惎鍔ㄥけ璐? {e}")

    def stop(self):
        """鍋滄璋冨害鍣?"""
        if self.scheduler:
            self.scheduler.shutdown()
            print("馃洃 鍚屾璋冨害鍣ㄥ凡鍋滄")

    def _check_and_execute_tasks(self):
        """妫€鏌ュ苟鎵ц寰呭悓姝ヤ换鍔?"""
        try:
            tasks = self.handler.get_pending_tasks()
            for task in tasks:
                print(f"鈴?鎵ц瀹氭椂鍚屾: {task['source_table']} -> {task['target_table']}")
                result = self.handler.execute_scheduled_task(task)
                if result.get('success'):
                    print(f"sync success: {result.get('rows_synced', 0)} rows")
                else:
                    print(f"鉂?鍚屾澶辫触: {result.get('error')}")
        except Exception as e:
            print(f"鉂?浠诲姟妫€鏌ュけ璐? {e}")

    def _refresh_agent_catalogs(self):
        try:
            from metadata_analyzer import metadata_analyzer

            metadata_analyzer.refresh_all_field_catalogs()
        except Exception as e:
            print(f"鈿狅笍 瀛楁鐩綍鍒锋柊澶辫触: {e}")


# 鍏ㄥ眬璋冨害鍣ㄥ疄渚?
sync_scheduler = SyncScheduler(datasource_handler)
