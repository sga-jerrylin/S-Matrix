"""
Stable read-model helpers for the data foundation layer.
"""
import json
import re
from typing import Any, Dict, List, Optional


def safe_json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def semantic_label(semantic: str) -> str:
    labels = {
        "geographic-city": "地理位置/城市",
        "categorical": "分类字段",
        "temporal-year": "时间/年份",
        "financial-income": "数值/金额",
        "text": "文本字段",
        "id": "标识字段",
    }
    return labels.get((semantic or "").strip(), semantic or "")


def short_field_display_name(field_name: str, description: str) -> str:
    candidate = (description or "").strip()
    if not candidate:
        return field_name

    candidate = re.split(r"[，,]\s*数据类型[:：]", candidate, maxsplit=1)[0].strip()
    candidate = re.sub(r"\s*[（(]\s*数据类型[:：].*?[)）]\s*$", "", candidate).strip()
    return candidate or field_name


def relation_type_label(rel_type: str) -> str:
    labels = {
        "logical": "逻辑关联",
        "fk": "主外键关联",
        "lookup": "维表映射",
    }
    return labels.get((rel_type or "").strip(), rel_type or "关联")


def build_source_payload(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None

    return {
        "table_name": row.get("table_name"),
        "source_type": row.get("source_type") or "unknown",
        "origin_kind": row.get("origin_kind") or "",
        "origin_id": row.get("origin_id") or "",
        "origin_label": row.get("origin_label") or "",
        "origin_path": row.get("origin_path") or "",
        "origin_table": row.get("origin_table") or "",
        "sync_task_id": row.get("sync_task_id") or "",
        "ingest_mode": row.get("ingest_mode") or "",
        "last_rows": row.get("last_rows"),
        "analysis_status": row.get("analysis_status") or "missing",
        "last_ingested_at": row.get("last_ingested_at"),
        "last_analyzed_at": row.get("last_analyzed_at"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def build_field_payload(
    field_name: str,
    description: str = "",
    schema_type: str = "",
    semantic: str = "",
    enum_values: Optional[List[Any]] = None,
    value_range: Any = None,
) -> Dict[str, Any]:
    semantic_value = semantic or ""
    return {
        "field_name": field_name,
        "display_name": short_field_display_name(field_name, description),
        "description": description or "",
        "field_type": schema_type or "",
        "semantic": semantic_value,
        "semantic_label": semantic_label(semantic_value),
        "enum_values": enum_values or [],
        "value_range": value_range,
    }


def build_metadata_payload(
    table_name: str,
    metadata_row: Optional[Dict[str, Any]],
    fields: List[Dict[str, Any]],
    source_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    metadata_row = metadata_row or {}
    source_payload = source_payload or {}

    description = metadata_row.get("description") or ""
    metadata_ready = bool(metadata_row)
    metadata_status = "ready" if metadata_ready else (source_payload.get("analysis_status") or "missing")

    return {
        "table_name": table_name,
        "description": description,
        "columns_info": safe_json_loads(metadata_row.get("columns_info"), {}),
        "sample_queries": safe_json_loads(metadata_row.get("sample_queries"), []),
        "source_type": (
            metadata_row.get("source_type")
            or source_payload.get("source_type")
            or "unknown"
        ),
        "analyzed_at": metadata_row.get("analyzed_at"),
        "status": metadata_status,
        "ready": metadata_ready,
        "fields": fields,
    }


def build_registry_payload(
    row: Dict[str, Any],
    metadata_payload: Optional[Dict[str, Any]] = None,
    source_payload: Optional[Dict[str, Any]] = None,
    relationship_count: int = 0,
) -> Dict[str, Any]:
    metadata_payload = metadata_payload or {}
    source_payload = source_payload or {}
    description = (
        row.get("description")
        or metadata_payload.get("description")
        or row.get("auto_description")
        or ""
    )
    display_name = row.get("display_name") or row.get("table_name") or ""
    metadata_status = metadata_payload.get("status") or source_payload.get("analysis_status") or "missing"

    return {
        "table_name": row.get("table_name"),
        "display_name": display_name,
        "description": description,
        "source_type": (
            row.get("source_type")
            or source_payload.get("source_type")
            or metadata_payload.get("source_type")
            or "unknown"
        ),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "auto_description": row.get("auto_description") or "",
        "analyzed_at": metadata_payload.get("analyzed_at") or row.get("analyzed_at"),
        "columns_info": metadata_payload.get("columns_info") or safe_json_loads(row.get("columns_info"), {}),
        "sample_queries": metadata_payload.get("sample_queries") or safe_json_loads(row.get("sample_queries"), []),
        "agent_config": safe_json_loads(row.get("agent_config"), {}),
        "source": source_payload or None,
        "metadata_status": metadata_status,
        "metadata_ready": metadata_payload.get("ready", False),
        "relationship_count": relationship_count,
    }


def build_relationship_payload(
    rel_row: Dict[str, Any],
    registry_by_table: Optional[Dict[str, Dict[str, Any]]] = None,
    fields_by_table: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    registry_by_table = registry_by_table or {}
    fields_by_table = fields_by_table or {}

    table_a = rel_row.get("table_a")
    table_b = rel_row.get("table_b")
    column_a = rel_row.get("column_a")
    column_b = rel_row.get("column_b")
    table_a_display = registry_by_table.get(table_a, {}).get("display_name") or table_a
    table_b_display = registry_by_table.get(table_b, {}).get("display_name") or table_b
    field_a_display = fields_by_table.get(table_a, {}).get(column_a, {}).get("display_name") or column_a
    field_b_display = fields_by_table.get(table_b, {}).get(column_b, {}).get("display_name") or column_b
    rel_type = rel_row.get("rel_type") or "logical"

    return {
        "id": rel_row.get("id"),
        "table_a": table_a,
        "table_a_display_name": table_a_display,
        "column_a": column_a,
        "column_a_display_name": field_a_display,
        "table_b": table_b,
        "table_b_display_name": table_b_display,
        "column_b": column_b,
        "column_b_display_name": field_b_display,
        "relation_type": rel_type,
        "relation_type_label": relation_type_label(rel_type),
        "confidence": rel_row.get("confidence"),
        "is_manual": bool(rel_row.get("is_manual")),
        "created_at": rel_row.get("created_at"),
        "relation_label": (
            f"{table_a_display}.{field_a_display} -> {table_b_display}.{field_b_display}"
        ),
    }


def build_table_profile_payload(
    table_name: str,
    registry_payload: Dict[str, Any],
    metadata_payload: Dict[str, Any],
    relationship_payloads: List[Dict[str, Any]],
    source_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source_payload = source_payload or {}
    field_count = len(metadata_payload.get("fields") or [])

    return {
        "table_name": table_name,
        "registry": registry_payload,
        "source": source_payload or None,
        "metadata": metadata_payload,
        "relationships": relationship_payloads,
        "stats": {
            "field_count": field_count,
            "relationship_count": len(relationship_payloads),
            "metadata_status": metadata_payload.get("status") or source_payload.get("analysis_status") or "missing",
            "metadata_ready": bool(metadata_payload.get("ready")),
            "last_ingested_at": source_payload.get("last_ingested_at"),
            "last_analyzed_at": source_payload.get("last_analyzed_at") or metadata_payload.get("analyzed_at"),
        },
    }
