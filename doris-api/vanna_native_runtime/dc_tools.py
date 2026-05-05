"""
DC query-intelligence tools mapped into Vanna2 ToolRegistry.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Type

from pydantic import BaseModel, Field

from .models import NativeRuntimeBackbone


def _json_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _coerce_error_message(error: Exception) -> str:
    message = str(error or "").strip()
    return message or error.__class__.__name__


def _normalize_groups(groups: Iterable[str]) -> List[str]:
    normalized = [str(group).strip() for group in groups if str(group).strip()]
    return normalized or ["user"]


def _build_tool_payload(
    *,
    tool_name: str,
    success: bool,
    data: Optional[Dict[str, Any]] = None,
    error: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "tool_name": tool_name,
        "success": bool(success),
        "data": dict(data or {}),
        "error": dict(error or {}) if error else None,
    }


def _safe_context_payload(context: Any) -> Dict[str, Any]:
    user = getattr(context, "user", None)
    return {
        "request_id": str(getattr(context, "request_id", "") or ""),
        "conversation_id": str(getattr(context, "conversation_id", "") or ""),
        "user_id": str(getattr(user, "id", "") or ""),
        "groups": list(getattr(user, "group_memberships", []) or []),
    }


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


class DCCatalogRetrievalArgs(BaseModel):
    table_names: List[str] = Field(default_factory=list, description="Optional table scope")
    include_table_registry: bool = Field(default=True)
    include_foundation_tables: bool = Field(default=True)
    include_query_catalog: bool = Field(default=True)


class DCMemoryRetrievalArgs(BaseModel):
    question: str = Field(..., min_length=1)
    limit: int = Field(default=5, ge=1, le=20)
    api_config: Dict[str, Any] = Field(default_factory=dict)


class DCDDLDocRetrievalArgs(BaseModel):
    question: str = Field(..., min_length=1)
    api_config: Dict[str, Any] = Field(default_factory=dict)


class DCSqlGenerationArgs(BaseModel):
    question: str = Field(..., min_length=1)
    table_name: Optional[str] = Field(default=None)
    subtask: Dict[str, Any] = Field(default_factory=dict)
    api_config: Dict[str, Any] = Field(default_factory=dict)


class DCSqlValidationArgs(BaseModel):
    sql: str = Field(..., min_length=1)


class DCSqlRepairArgs(BaseModel):
    question: str = Field(..., min_length=1)
    failed_sql: str = Field(..., min_length=1)
    error_message: str = Field(..., min_length=1)
    ddl_list: List[str] = Field(default_factory=list)
    api_config: Dict[str, Any] = Field(default_factory=dict)


class DCSqlExecutionArgs(BaseModel):
    sql: str = Field(..., min_length=1)
    params: List[Any] = Field(default_factory=list)


@dataclass
class DCToolRuntimeAdapter:
    """
    Runtime adapter that bridges existing DorisClaw query modules to native tools.
    """

    datasource_handler: Any = None
    doris_client: Any = None
    table_admin_factory: Optional[Callable[[], Any]] = None
    repair_agent_factory: Optional[Callable[[Dict[str, Any]], Any]] = None
    vanna_factory: Optional[Callable[[Dict[str, Any]], Any]] = None
    sql_validator: Optional[Callable[[str], Dict[str, Any]]] = None

    def __post_init__(self) -> None:
        if self.datasource_handler is None:
            from datasource_handler import datasource_handler as default_handler

            self.datasource_handler = default_handler
        if self.doris_client is None:
            from db import doris_client as default_client

            self.doris_client = default_client
        if self.table_admin_factory is None:
            from table_admin_agent import TableAdminAgent

            self.table_admin_factory = lambda: TableAdminAgent(
                doris_client_override=self.doris_client
            )
        if self.repair_agent_factory is None:
            from repair_agent import RepairAgent

            self.repair_agent_factory = lambda api_config: RepairAgent(
                doris_client=self.doris_client,
                api_key=api_config.get("api_key"),
                model=api_config.get("model"),
                base_url=api_config.get("base_url"),
            )
        if self.vanna_factory is None:
            from vanna_doris import VannaDorisOpenAI

            self.vanna_factory = lambda api_config: VannaDorisOpenAI(
                doris_client=self.doris_client,
                api_key=api_config.get("api_key"),
                model=api_config.get("model"),
                base_url=api_config.get("base_url"),
                api_config=api_config,
                config={"temperature": 0.1},
            )
        if self.sql_validator is None:
            self.sql_validator = self._default_sql_validator

    async def catalog_retrieval(self, args: DCCatalogRetrievalArgs) -> Dict[str, Any]:
        table_scope = list(args.table_names or [])
        registry_rows: List[Dict[str, Any]] = []
        foundation_rows: List[Dict[str, Any]] = []
        catalog_rows: List[Dict[str, Any]] = []
        source_labels: List[str] = []
        warnings: List[str] = []

        if args.include_table_registry:
            try:
                registry_rows = list(
                    await _call_maybe_async(
                        self.datasource_handler.list_table_registry,
                        table_scope or None,
                    )
                    or []
                )
                source_labels.append("table_registry")
            except Exception as exc:
                warnings.append(f"table_registry unavailable: {_coerce_error_message(exc)}")

        if args.include_foundation_tables and hasattr(
            self.datasource_handler, "list_foundation_tables"
        ):
            try:
                foundation_rows = list(
                    await _call_maybe_async(
                        self.datasource_handler.list_foundation_tables,
                        table_scope or None,
                    )
                    or []
                )
                source_labels.append("foundation_tables")
            except Exception as exc:
                warnings.append(
                    f"foundation_tables unavailable: {_coerce_error_message(exc)}"
                )

        if args.include_query_catalog:
            try:
                catalog_rows = list(
                    await _call_maybe_async(self.datasource_handler.list_query_catalog) or []
                )
                source_labels.append("query_catalog")
            except Exception as exc:
                warnings.append(f"query_catalog unavailable: {_coerce_error_message(exc)}")

        return {
            "success": True,
            "table_registry": registry_rows,
            "foundation_tables": foundation_rows,
            "query_catalog": catalog_rows,
            "trace": {
                "table_scope": table_scope,
                "table_registry_count": len(registry_rows),
                "foundation_table_count": len(foundation_rows),
                "query_catalog_count": len(catalog_rows),
                "source_labels": sorted(set(source_labels)),
                "warnings": warnings,
                "fallback_used": bool(warnings),
            },
        }

    async def memory_retrieval(self, args: DCMemoryRetrievalArgs) -> Dict[str, Any]:
        api_config = dict(args.api_config or {})
        try:
            vanna = self.vanna_factory(api_config)
            if hasattr(vanna, "get_similar_question_sql_with_trace"):
                result = await asyncio.to_thread(
                    vanna.get_similar_question_sql_with_trace,
                    args.question,
                    limit=args.limit,
                )
                examples = list((result or {}).get("examples") or [])
                trace = dict((result or {}).get("trace") or {})
                return {
                    "success": True,
                    "examples": examples,
                    "trace": trace,
                }

            examples = await asyncio.to_thread(
                vanna.get_similar_question_sql,
                args.question,
                args.limit,
            )
            examples = list(examples or [])
            return {
                "success": True,
                "examples": examples,
                "trace": {
                    "memory_hit": bool(examples),
                    "fallback_used": False,
                    "selected_source": "query_history.legacy",
                    "source_labels": ["query_history.legacy"] if examples else [],
                    "sources_attempted": ["query_history.legacy"],
                    "candidate_count": len(examples),
                    "limit": args.limit,
                    "errors": [],
                },
            }
        except Exception as exc:
            error_message = _coerce_error_message(exc)
            return {
                "success": True,
                "examples": [],
                "trace": {
                    "memory_hit": False,
                    "fallback_used": True,
                    "selected_source": "",
                    "source_labels": [],
                    "sources_attempted": ["query_history.adapter"],
                    "candidate_count": 0,
                    "limit": args.limit,
                    "errors": [
                        {"source": "query_history.adapter", "error": error_message}
                    ],
                },
                "error": {
                    "code": "memory_retrieval_failed",
                    "message": error_message,
                },
            }

    async def ddl_doc_retrieval(self, args: DCDDLDocRetrievalArgs) -> Dict[str, Any]:
        api_config = dict(args.api_config or {})
        try:
            from vanna_doris import LegacyVannaAdapter

            vanna = self.vanna_factory(api_config)
            adapter = LegacyVannaAdapter(vanna)
            result = await asyncio.to_thread(adapter.ddl_doc_retrieval, args.question)
            result = dict(result or {})
            trace = dict(result.get("trace") or {})
            return {
                "success": True,
                "ddl": list(result.get("ddl") or []),
                "documentation": list(result.get("documentation") or []),
                "trace": {
                    "ddl_count": int(trace.get("ddl_count", 0)),
                    "documentation_count": int(trace.get("documentation_count", 0)),
                    "source_labels": list(trace.get("source_labels") or []),
                    "ddl_cache_hit": bool(trace.get("ddl_cache_hit", False)),
                },
            }
        except Exception as exc:
            error_message = _coerce_error_message(exc)
            return {
                "success": True,
                "ddl": [],
                "documentation": [],
                "trace": {
                    "ddl_count": 0,
                    "documentation_count": 0,
                    "source_labels": [],
                    "ddl_cache_hit": False,
                    "fallback_used": True,
                },
                "error": {
                    "code": "ddl_doc_retrieval_failed",
                    "message": error_message,
                },
            }

    async def sql_generation(self, args: DCSqlGenerationArgs) -> Dict[str, Any]:
        api_config = dict(args.api_config or {})
        subtask = dict(args.subtask or {})
        if args.table_name and "table" not in subtask:
            subtask["table"] = args.table_name
        if "question" not in subtask:
            subtask["question"] = args.question

        try:
            table_admin = self.table_admin_factory()
            if hasattr(table_admin, "generate_sql_for_subtask_with_trace"):
                result = await asyncio.to_thread(
                    table_admin.generate_sql_for_subtask_with_trace,
                    subtask,
                    args.question,
                    api_config,
                )
                result = dict(result or {})
                return {
                    "success": True,
                    "sql": str(result.get("sql") or ""),
                    "trace": dict(result.get("trace") or {}),
                }

            sql = await asyncio.to_thread(
                table_admin.generate_sql_for_subtask,
                subtask,
                args.question,
                api_config,
            )
            return {
                "success": True,
                "sql": str(sql or ""),
                "trace": {
                    "strategy": "legacy_generate_sql_for_subtask",
                    "table_name": subtask.get("table"),
                },
            }
        except Exception as exc:
            return {
                "success": False,
                "sql": "",
                "trace": {},
                "error": {
                    "code": "sql_generation_failed",
                    "message": _coerce_error_message(exc),
                },
            }

    async def sql_validation(self, args: DCSqlValidationArgs) -> Dict[str, Any]:
        try:
            validation = self.sql_validator(str(args.sql or ""))
            return {
                "success": True,
                "validation": validation,
            }
        except Exception as exc:
            return {
                "success": False,
                "validation": {"is_valid": False, "errors": [_coerce_error_message(exc)]},
                "error": {
                    "code": "sql_validation_failed",
                    "message": _coerce_error_message(exc),
                },
            }

    async def sql_repair(self, args: DCSqlRepairArgs) -> Dict[str, Any]:
        api_config = dict(args.api_config or {})
        try:
            repair_agent = self.repair_agent_factory(api_config)
            if hasattr(repair_agent, "repair_sql_with_trace"):
                result = await asyncio.to_thread(
                    repair_agent.repair_sql_with_trace,
                    args.question,
                    args.failed_sql,
                    args.error_message,
                    list(args.ddl_list or []),
                    api_config=api_config,
                )
                result = dict(result or {})
                return {
                    "success": True,
                    "sql": str(result.get("sql") or ""),
                    "trace": dict(result.get("trace") or {}),
                }

            repaired_sql = await asyncio.to_thread(
                repair_agent.repair_sql,
                args.question,
                args.failed_sql,
                args.error_message,
                list(args.ddl_list or []),
                api_config=api_config,
            )
            return {
                "success": True,
                "sql": str(repaired_sql or ""),
                "trace": {},
            }
        except Exception as exc:
            return {
                "success": False,
                "sql": "",
                "trace": {},
                "error": {
                    "code": "sql_repair_failed",
                    "message": _coerce_error_message(exc),
                },
            }

    async def sql_execution(self, args: DCSqlExecutionArgs) -> Dict[str, Any]:
        started = time.perf_counter()
        sql = str(args.sql or "").strip()
        params = tuple(args.params or [])
        try:
            if hasattr(self.doris_client, "execute_query_async"):
                rows = await _call_maybe_async(self.doris_client.execute_query_async, sql, params)
            else:
                rows = await asyncio.to_thread(self.doris_client.execute_query, sql, params)
            rows = list(rows or [])
            return {
                "success": True,
                "rows": rows,
                "row_count": len(rows),
                "execution_ms": int((time.perf_counter() - started) * 1000),
            }
        except Exception as exc:
            return {
                "success": False,
                "rows": [],
                "row_count": 0,
                "execution_ms": int((time.perf_counter() - started) * 1000),
                "error": {
                    "code": "sql_execution_failed",
                    "message": _coerce_error_message(exc),
                },
            }

    def _default_sql_validator(self, sql: str) -> Dict[str, Any]:
        normalized_sql = (sql or "").strip()
        errors: List[str] = []
        warnings: List[str] = []
        blocked_keywords = [
            "drop",
            "truncate",
            "alter",
            "create",
            "insert",
            "update",
            "delete",
            "replace",
            "grant",
            "revoke",
        ]

        if not normalized_sql:
            errors.append("sql is empty")
        if normalized_sql.count(";") > 1 or (
            normalized_sql.endswith(";") and ";" in normalized_sql[:-1]
        ):
            errors.append("multiple SQL statements are not allowed")
        if ";" in normalized_sql[:-1]:
            errors.append("multi-statement SQL is not allowed")

        lowered = normalized_sql.lower()
        first_token_match = re.match(r"^\s*([a-zA-Z]+)", normalized_sql)
        first_token = (first_token_match.group(1).lower() if first_token_match else "")
        if first_token and first_token not in {"select", "with"}:
            errors.append(f"unsupported SQL command: {first_token}")

        for keyword in blocked_keywords:
            if re.search(rf"\b{re.escape(keyword)}\b", lowered):
                errors.append(f"blocked keyword detected: {keyword}")

        if "select *" in lowered:
            warnings.append("SELECT * detected; prefer explicit columns")

        return {
            "is_valid": not errors,
            "errors": errors,
            "warnings": warnings,
            "normalized_sql": normalized_sql.rstrip(";"),
            "read_only": not errors,
        }


def _make_tool_class(
    *,
    imports: Any,
    tool_name: str,
    description: str,
    args_schema: Type[BaseModel],
    handler: Callable[[Any], Awaitable[Dict[str, Any]]],
    audit_bridge: Any,
) -> Any:
    tool_base = imports.Tool
    tool_result_cls = imports.ToolResult

    class RuntimeMappedTool(tool_base[args_schema]):  # type: ignore[name-defined]
        @property
        def name(self) -> str:
            return tool_name

        @property
        def description(self) -> str:
            return description

        def get_args_schema(self) -> Type[BaseModel]:
            return args_schema

        async def execute(self, context: Any, args: Any) -> Any:
            context_payload = _safe_context_payload(context)
            request_id = str(context_payload.get("request_id") or "")
            audit_bridge.record(
                "tool_invocation",
                {"tool_name": tool_name, "context": context_payload},
                request_id=request_id,
            )
            try:
                result = await handler(args)
            except Exception as exc:
                error_payload = {
                    "code": "tool_execute_exception",
                    "message": _coerce_error_message(exc),
                }
                payload = _build_tool_payload(
                    tool_name=tool_name,
                    success=False,
                    error=error_payload,
                )
                audit_bridge.record(
                    "tool_error",
                    {
                        "tool_name": tool_name,
                        "context": context_payload,
                        "error": error_payload,
                    },
                    request_id=request_id,
                )
                return tool_result_cls(
                    success=False,
                    result_for_llm=_json_dumps(payload),
                    metadata=payload,
                    error=error_payload["message"],
                )

            success = bool(result.get("success", True))
            data = {key: value for key, value in result.items() if key not in {"success", "error"}}
            error_payload = dict(result.get("error") or {}) if result.get("error") else None
            payload = _build_tool_payload(
                tool_name=tool_name,
                success=success,
                data=data,
                error=error_payload,
            )
            audit_bridge.record(
                "tool_result",
                {
                    "tool_name": tool_name,
                    "context": context_payload,
                    "success": success,
                    "error": error_payload,
                },
                request_id=request_id,
            )
            return tool_result_cls(
                success=success,
                result_for_llm=_json_dumps(payload),
                metadata=payload,
                error=(error_payload or {}).get("message"),
            )

    return RuntimeMappedTool()


def register_dc_query_tools(
    backbone: NativeRuntimeBackbone,
    *,
    adapter: Optional[DCToolRuntimeAdapter] = None,
    access_policy: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    Register DorisClaw query-intelligence tools into native ToolRegistry.
    """

    adapter = adapter or DCToolRuntimeAdapter()
    imports = backbone.imports
    policy = dict(
        access_policy
        or {
            "dc_catalog_retrieval": ["user", "admin"],
            "dc_memory_retrieval": ["user", "admin"],
            "dc_ddl_doc_retrieval": ["user", "admin"],
            "dc_sql_generation": ["user", "admin"],
            "dc_sql_validation": ["user", "admin"],
            "dc_sql_repair": ["admin"],
            "dc_sql_execution": ["user", "admin"],
        }
    )

    tool_specs = [
        (
            "dc_catalog_retrieval",
            "Retrieve table registry, foundation tables, and query catalog context",
            DCCatalogRetrievalArgs,
            adapter.catalog_retrieval,
        ),
        (
            "dc_memory_retrieval",
            "Retrieve similar NLQ question-SQL history examples",
            DCMemoryRetrievalArgs,
            adapter.memory_retrieval,
        ),
        (
            "dc_ddl_doc_retrieval",
            "Retrieve DDL and documentation context",
            DCDDLDocRetrievalArgs,
            adapter.ddl_doc_retrieval,
        ),
        (
            "dc_sql_generation",
            "Generate SQL for a query subtask",
            DCSqlGenerationArgs,
            adapter.sql_generation,
        ),
        (
            "dc_sql_validation",
            "Validate SQL safety and read-only constraints",
            DCSqlValidationArgs,
            adapter.sql_validation,
        ),
        (
            "dc_sql_repair",
            "Repair failed SQL using error and schema context",
            DCSqlRepairArgs,
            adapter.sql_repair,
        ),
        (
            "dc_sql_execution",
            "Execute SQL on Doris and return rows",
            DCSqlExecutionArgs,
            adapter.sql_execution,
        ),
    ]

    registered: List[str] = []
    for name, description, args_schema, handler in tool_specs:
        tool = _make_tool_class(
            imports=imports,
            tool_name=name,
            description=description,
            args_schema=args_schema,
            handler=handler,
            audit_bridge=backbone.audit_bridge,
        )
        groups = _normalize_groups(policy.get(name) or ["user"])
        backbone.tool_registry.register_local_tool(tool, access_groups=groups)
        registered.append(name)

    return {
        "tool_names": registered,
        "access_policy": {name: _normalize_groups(policy.get(name) or ["user"]) for name in registered},
    }
