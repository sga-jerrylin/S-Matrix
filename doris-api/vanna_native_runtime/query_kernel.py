"""
Native query kernel orchestrated by Vanna2 ToolRegistry mappings.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from coordinator_agent import CoordinatorAgent
from planner_agent import PlannerAgent

from .backbone import build_native_runtime_backbone
from .dc_tools import register_dc_query_tools
from .memory_backend import DCAgentMemoryAdapter
from .models import NativeRuntimeBackbone


class NativeKernelExecutionError(RuntimeError):
    """Structured native-kernel failure."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass
class NativeKernelResult:
    intent: str
    plan: Dict[str, Any]
    subtask_traces: List[Dict[str, Any]]
    retrieval_summary: Dict[str, Any]
    orchestration_trace: Dict[str, Any]
    repair_trace: Dict[str, Any]
    sql: str
    data: List[Dict[str, Any]]
    history_id: Optional[str]
    history_status: str
    warnings: List[str]
    phase_traces: List[Dict[str, Any]]
    native_trace: Dict[str, Any]

    @property
    def table_names(self) -> List[str]:
        return list(self.plan.get("tables") or [])


@dataclass
class _RuntimeCacheEntry:
    backbone: NativeRuntimeBackbone
    expires_at: float
    cache_key: str


_NATIVE_RUNTIME_CACHE: Dict[str, _RuntimeCacheEntry] = {}
_NATIVE_RUNTIME_CACHE_LOCK = asyncio.Lock()
_DEFAULT_MEMORY_REUSE_CONFIDENCE = 0.86


def _sanitize_error(error: Exception) -> str:
    message = str(error or "").strip()
    return message or error.__class__.__name__


def _tool_payload(result: Any, tool_name: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    raw = getattr(result, "result_for_llm", "")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}
    if not payload:
        metadata = getattr(result, "metadata", None)
        if isinstance(metadata, dict):
            payload = dict(metadata)
    if not payload:
        payload = {
            "tool_name": tool_name,
            "success": bool(getattr(result, "success", False)),
            "data": {},
            "error": None,
        }
    payload.setdefault("tool_name", tool_name)
    payload.setdefault("success", bool(getattr(result, "success", False)))
    payload.setdefault("data", {})
    if not isinstance(payload.get("data"), dict):
        payload["data"] = {}
    if payload.get("error") is None and getattr(result, "error", None):
        payload["error"] = {
            "code": f"{tool_name}_failed",
            "message": str(getattr(result, "error")),
        }
    return payload


def _extract_sql_tables(sql: str) -> List[str]:
    text = str(sql or "")
    pattern = re.compile(r"(?:from|join)\s+`?([a-zA-Z0-9_\-\u4e00-\u9fff]+)`?", re.IGNORECASE)
    return sorted({match.group(1) for match in pattern.finditer(text) if match.group(1)})


def _build_memory_context_line(candidates: List[Dict[str, Any]], limit: int = 2) -> str:
    snippets: List[str] = []
    for candidate in candidates[: max(1, int(limit))]:
        sql = str(candidate.get("sql") or "").strip()
        if not sql:
            continue
        question = str(candidate.get("question") or "").strip()
        score = float(candidate.get("confidence", 0.0))
        snippets.append(f"- question={question}; confidence={score:.2f}; sql={sql}")
    if not snippets:
        return ""
    return (
        "\nRelevant historical examples (semantic context only; do not directly reuse unless table and intent match):\n"
        + "\n".join(snippets)
    )


def _candidate_matches_table(candidate: Dict[str, Any], table_name: str) -> bool:
    table_names = [str(item).strip() for item in list(candidate.get("table_names") or []) if str(item).strip()]
    return bool(table_name and table_names and table_name in table_names)


def _memory_candidate_view(candidate: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "question": str(candidate.get("question") or ""),
        "table_names": [str(item).strip() for item in list(candidate.get("table_names") or []) if str(item).strip()],
        "confidence": float(candidate.get("confidence", 0.0)),
        "source": str(candidate.get("source") or ""),
        "sql": str(candidate.get("sql") or ""),
        "intent": str(candidate.get("intent") or "unknown"),
    }


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").lower())


def _classify_query_intent(question: str) -> str:
    compact = _compact_text(question)
    if not compact:
        return "unknown"

    trend_markers = ("每天", "每日", "按天", "趋势", "走势", "变化", "trend", "daily", "perday")
    ranking_markers = ("排行", "排名", "top", "最高", "最低")
    detail_markers = ("前", "明细", "详情", "列表", "list", "detail", "sample", "limit")
    amount_markers = ("金额总和", "总金额", "总额", "合计", "sum(", "sum")
    count_markers = ("总数", "数量", "多少", "count", "几条", "几笔")

    if any(marker in compact for marker in trend_markers):
        return "trend"
    if any(marker in compact for marker in ranking_markers):
        return "ranking"
    if any(marker in compact for marker in amount_markers):
        return "amount_sum"
    if any(marker in compact for marker in detail_markers) and "总数" not in compact and "count" not in compact:
        return "detail_listing"
    if any(marker in compact for marker in count_markers):
        return "aggregate_count"
    return "unknown"


def _classify_sql_intent(sql: str) -> str:
    normalized = " ".join(str(sql or "").lower().split())
    if not normalized:
        return "unknown"

    has_count = "count(" in normalized
    has_sum = "sum(" in normalized
    has_group_by = " group by " in normalized
    has_order_by = " order by " in normalized
    has_limit = " limit " in normalized
    has_time_bucket = any(
        token in normalized
        for token in ("date_trunc", "to_date(", "day(", "week(", "month(", "date_format(", "created_at")
    )

    if has_group_by and has_time_bucket:
        return "trend"
    if has_order_by and has_limit and has_sum:
        return "ranking"
    if has_count and not has_group_by:
        return "aggregate_count"
    if has_sum and not has_group_by:
        return "amount_sum"
    if has_order_by and has_limit:
        return "detail_listing"
    if has_limit:
        return "detail_listing"
    return "unknown"


def _classify_candidate_intent(candidate: Dict[str, Any]) -> str:
    sql_intent = _classify_sql_intent(str(candidate.get("sql") or ""))
    if sql_intent != "unknown":
        return sql_intent
    return _classify_query_intent(str(candidate.get("question") or ""))


def _intent_matched(query_intent: str, candidate_intent: str) -> bool:
    if query_intent == "unknown" or candidate_intent == "unknown":
        return False
    return query_intent == candidate_intent


def _parse_confidence_threshold() -> float:
    raw = str(os.getenv("DC_NATIVE_MEMORY_REUSE_CONFIDENCE", str(_DEFAULT_MEMORY_REUSE_CONFIDENCE))).strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _DEFAULT_MEMORY_REUSE_CONFIDENCE
    if value <= 0:
        return _DEFAULT_MEMORY_REUSE_CONFIDENCE
    if value > 1:
        return 1.0
    return value


def _runtime_cache_ttl_seconds() -> float:
    raw = str(os.getenv("DC_NATIVE_RUNTIME_CACHE_TTL_SECONDS", "600")).strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 600.0
    return max(1.0, value)


def _runtime_cache_key(*, runtime_groups: List[str], doris_client: Any) -> str:
    normalized_groups = ",".join(sorted({str(group).strip() for group in runtime_groups if str(group).strip()}))
    return f"doris_client:{id(doris_client)}|groups:{normalized_groups}"


async def _get_cached_runtime_backbone(
    *,
    user_id: Optional[str],
    runtime_groups: List[str],
    doris_client: Any,
) -> Tuple[NativeRuntimeBackbone, bool, str]:
    cache_key = _runtime_cache_key(runtime_groups=runtime_groups, doris_client=doris_client)
    ttl_seconds = _runtime_cache_ttl_seconds()
    now = time.monotonic()

    async with _NATIVE_RUNTIME_CACHE_LOCK:
        stale_keys = [key for key, entry in _NATIVE_RUNTIME_CACHE.items() if entry.expires_at <= now]
        for key in stale_keys:
            _NATIVE_RUNTIME_CACHE.pop(key, None)

        cached = _NATIVE_RUNTIME_CACHE.get(cache_key)
        if cached and cached.expires_at > now:
            return cached.backbone, True, cache_key

        memory_backend = DCAgentMemoryAdapter(doris_client=doris_client)
        backbone = build_native_runtime_backbone(
            default_user_id=user_id or "dc-native-user",
            default_user_groups=runtime_groups,
            agent_memory=memory_backend,
        )
        register_dc_query_tools(backbone)
        _NATIVE_RUNTIME_CACHE[cache_key] = _RuntimeCacheEntry(
            backbone=backbone,
            expires_at=now + ttl_seconds,
            cache_key=cache_key,
        )
        return backbone, False, cache_key


async def run_native_query_kernel(
    *,
    query: str,
    requested_tables: List[str],
    api_config: Dict[str, Any],
    doris_client: Any,
    datasource_handler: Any,
    request_id: str,
    session_id: str,
    user_id: Optional[str],
    max_repair_attempts: int = 2,
) -> NativeKernelResult:
    warnings: List[str] = []
    tools_called: List[Dict[str, Any]] = []
    phase_traces: List[Dict[str, Any]] = []
    native_memory: Dict[str, Any] = {
        "example_count": 0,
        "memory_hit": False,
        "sources_attempted": [],
        "vanna_memory_hit": False,
        "vanna_memory_source": "",
        "confidence": 0.0,
        "used_as": "none",
        "rejected_reason": "",
        "candidate_count": 0,
        "candidate_examples": [],
        "chosen_candidate": {},
        "chosen_candidates": [],
        "rejected_candidates": [],
        "query_intent": "unknown",
        "candidate_intent": "unknown",
        "intent_matched": False,
        "reuse_gate_reason": "",
    }
    reuse_confidence_threshold = _parse_confidence_threshold()

    runtime_groups = ["admin", "user"]
    backbone, runtime_reused, runtime_cache_key = await _get_cached_runtime_backbone(
        user_id=user_id,
        runtime_groups=runtime_groups,
        doris_client=doris_client,
    )
    if hasattr(backbone.audit_bridge, "clear"):
        try:
            backbone.audit_bridge.clear(request_id=request_id)
        except TypeError:
            backbone.audit_bridge.clear()
    imports = backbone.imports
    tool_context = imports.ToolContext(
        user=imports.User(
            id=user_id or "dc-native-user",
            username=user_id or "dc-native-user",
            group_memberships=runtime_groups,
            metadata={"source": "native_query_kernel"},
        ),
        conversation_id=session_id,
        request_id=request_id,
        agent_memory=backbone.agent_memory,
        metadata={
            "kernel": "native",
            "runtime_reused": runtime_reused,
            "runtime_cache_key": runtime_cache_key,
        },
    )

    async def execute_tool(
        name: str,
        arguments: Dict[str, Any],
        *,
        fail_hard: bool = True,
    ) -> Dict[str, Any]:
        call = imports.ToolCall(
            id=f"{name}-{uuid.uuid4().hex[:10]}",
            name=name,
            arguments=dict(arguments or {}),
        )
        result = await backbone.tool_registry.execute(call, tool_context)
        payload = _tool_payload(result, name)
        success = bool(payload.get("success", False))
        error_payload = payload.get("error") if isinstance(payload.get("error"), dict) else None
        tools_called.append(
            {
                "tool_name": name,
                "success": success,
                "error": dict(error_payload or {}),
            }
        )
        if fail_hard and not success:
            error_message = str((error_payload or {}).get("message") or getattr(result, "error", "") or name)
            raise NativeKernelExecutionError(
                code=f"{name}_failed",
                message=error_message,
                details={"tool_name": name, "arguments": dict(arguments or {})},
            )
        return payload

    catalog_payload = await execute_tool(
        "dc_catalog_retrieval",
        {
            "table_names": list(requested_tables or []),
            "include_table_registry": True,
            "include_foundation_tables": True,
            "include_query_catalog": True,
        },
    )
    tables_context = list(((catalog_payload.get("data") or {}).get("table_registry") or []))
    if requested_tables and not tables_context:
        raise NativeKernelExecutionError(
            code="selected_scope_not_found",
            message="Selected query scope does not match any registered tables",
            details={"requested_tables": list(requested_tables)},
        )
    if not tables_context:
        raise NativeKernelExecutionError(
            code="table_registry_empty",
            message="Table registry unavailable for native kernel planning",
        )
    phase_traces.append(
        {
            "phase": "catalog_retrieval",
            "status": "ok",
            "details": {"table_registry_count": len(tables_context)},
            "source_labels": list((((catalog_payload.get("data") or {}).get("trace") or {}).get("source_labels") or []),
            ),
        }
    )

    planner = PlannerAgent(tables_context=tables_context)
    plan = await asyncio.to_thread(planner.plan, query)
    subtasks = plan.get("subtasks") or [{"table": table, "question": query} for table in plan.get("tables", [])]
    if not subtasks:
        raise NativeKernelExecutionError(
            code="planner_no_subtasks",
            message="Planner could not resolve any target tables",
            details={"query": query},
        )
    phase_traces.append(
        {
            "phase": "planner",
            "status": "ok",
            "details": {
                "intent": str(plan.get("intent") or "list"),
                "selected_table_count": len(plan.get("tables") or []),
                "needs_join": bool(plan.get("needs_join", False)),
            },
            "source_labels": ["planner.tables_context"],
        }
    )

    sql_map: Dict[str, str] = {}
    subtask_traces: List[Dict[str, Any]] = []
    ddl_context_for_repair: List[str] = []
    per_subtask_memory: List[Dict[str, Any]] = []

    for subtask in subtasks:
        table_name = str(subtask.get("table") or "").strip()
        if not table_name:
            continue
        question = str(subtask.get("question") or query)
        query_intent = _classify_query_intent(question)

        memory_candidates: List[Dict[str, Any]] = []
        memory_trace = {
            "selected_source": "",
            "sources_attempted": [],
            "errors": [],
            "degraded": False,
            "candidate_count": 0,
        }
        memory_decision = {
            "vanna_memory_hit": False,
            "vanna_memory_source": "",
            "confidence": 0.0,
            "used_as": "none",
            "rejected_reason": "",
            "candidate_count": 0,
            "reused_sql": "",
            "candidate_examples": [],
            "chosen_candidate": {},
            "rejected_candidates": [],
            "query_intent": query_intent,
            "candidate_intent": "unknown",
            "intent_matched": False,
            "reuse_gate_reason": "",
        }

        try:
            retrieval = await backbone.agent_memory.retrieve_similar_sql_candidates(  # type: ignore[attr-defined]
                question=question,
                context=tool_context,
                limit=5,
            )
            memory_trace = dict(retrieval.get("trace") or {})
            results = list(retrieval.get("results") or [])
            for item in results:
                memory = getattr(item, "memory", None)
                if not memory:
                    continue
                args = dict(getattr(memory, "args", {}) or {})
                sql = str(args.get("sql") or "").strip()
                if not sql:
                    continue
                candidate_tables = list(args.get("table_names") or [])
                if not candidate_tables:
                    candidate_tables = _extract_sql_tables(sql)
                score = float(getattr(item, "similarity_score", 0.0) or 0.0)
                source = str((getattr(memory, "metadata", {}) or {}).get("source") or memory_trace.get("selected_source") or "")
                memory_candidates.append(
                    {
                        "question": str(getattr(memory, "question", "") or ""),
                        "sql": sql,
                        "table_names": candidate_tables,
                        "confidence": score,
                        "source": source,
                        "intent": _classify_candidate_intent(
                            {
                                "question": str(getattr(memory, "question", "") or ""),
                                "sql": sql,
                            }
                        ),
                    }
                )
        except Exception as memory_error:
            memory_trace = {
                "selected_source": "",
                "sources_attempted": ["vanna.agent_memory"],
                "errors": [{"source": "vanna.agent_memory", "error": _sanitize_error(memory_error)}],
                "degraded": True,
                "candidate_count": 0,
            }
            memory_candidates = []

        memory_candidates.sort(key=lambda item: float(item.get("confidence", 0.0)), reverse=True)
        memory_trace["candidate_count"] = len(memory_candidates)
        memory_decision["candidate_count"] = len(memory_candidates)
        memory_decision["candidate_examples"] = [_memory_candidate_view(candidate) for candidate in memory_candidates[:5]]

        same_table_candidates = [candidate for candidate in memory_candidates if _candidate_matches_table(candidate, table_name)]
        cross_table_candidates = [candidate for candidate in memory_candidates if not _candidate_matches_table(candidate, table_name)]
        chosen_candidate = same_table_candidates[0] if same_table_candidates else None

        if chosen_candidate:
            source = str(chosen_candidate.get("source") or "")
            confidence = float(chosen_candidate.get("confidence", 0.0))
            candidate_intent = str(chosen_candidate.get("intent") or "unknown")
            intent_matched = _intent_matched(query_intent, candidate_intent)
            memory_decision.update(
                {
                    "vanna_memory_source": source,
                    "confidence": confidence,
                    "chosen_candidate": _memory_candidate_view(chosen_candidate),
                    "rejected_candidates": [_memory_candidate_view(candidate) for candidate in cross_table_candidates[:3]],
                    "candidate_intent": candidate_intent,
                    "intent_matched": intent_matched,
                }
            )
            memory_decision["vanna_memory_hit"] = True
            if confidence < reuse_confidence_threshold:
                memory_decision["used_as"] = "prompt_context"
                memory_decision["reuse_gate_reason"] = "reuse_blocked_low_confidence"
            elif not intent_matched:
                memory_decision["used_as"] = "prompt_context"
                memory_decision["reuse_gate_reason"] = "reuse_blocked_intent_mismatch"
            elif candidate_intent == "unknown":
                memory_decision["used_as"] = "prompt_context"
                memory_decision["reuse_gate_reason"] = "reuse_blocked_candidate_intent_unknown"
            elif query_intent == "unknown":
                memory_decision["used_as"] = "prompt_context"
                memory_decision["reuse_gate_reason"] = "reuse_blocked_query_intent_unknown"
            else:
                memory_decision["used_as"] = "sql_reuse"
                memory_decision["reused_sql"] = str(chosen_candidate.get("sql") or "").strip()
                memory_decision["reuse_gate_reason"] = "reuse_allowed"
        elif memory_candidates:
            top_candidate = memory_candidates[0]
            memory_decision["candidate_intent"] = str(top_candidate.get("intent") or "unknown")
            memory_decision["intent_matched"] = _intent_matched(
                str(memory_decision.get("query_intent") or "unknown"),
                str(memory_decision.get("candidate_intent") or "unknown"),
            )
            memory_decision["used_as"] = "rejected"
            memory_decision["rejected_reason"] = "cross_table_memory_mismatch"
            memory_decision["reuse_gate_reason"] = "reuse_blocked_cross_table"
            memory_decision["rejected_candidates"] = [_memory_candidate_view(candidate) for candidate in cross_table_candidates[:3]]
        elif bool(memory_trace.get("degraded", False)):
            memory_decision["used_as"] = "degraded"
            memory_decision["reuse_gate_reason"] = "reuse_blocked_memory_backend_degraded"
            first_error = list(memory_trace.get("errors") or [])
            if first_error:
                memory_decision["rejected_reason"] = str(first_error[0].get("error") or "memory_backend_error")
        else:
            memory_decision["reuse_gate_reason"] = "reuse_blocked_no_candidate"

        native_memory["example_count"] += len(memory_candidates)
        native_memory["memory_hit"] = bool(native_memory["memory_hit"] or bool(memory_candidates))
        native_memory["sources_attempted"] = sorted(
            set(
                list(native_memory.get("sources_attempted") or [])
                + list(memory_trace.get("sources_attempted") or [])
            )
        )
        native_memory["candidate_count"] = int(native_memory.get("candidate_count", 0)) + len(memory_candidates)
        if memory_decision["vanna_memory_hit"]:
            native_memory["vanna_memory_hit"] = True
            if not native_memory.get("vanna_memory_source"):
                native_memory["vanna_memory_source"] = memory_decision["vanna_memory_source"]
            native_memory["confidence"] = max(float(native_memory.get("confidence", 0.0)), float(memory_decision["confidence"]))
            if memory_decision["used_as"] == "sql_reuse":
                native_memory["used_as"] = "sql_reuse"
            elif native_memory.get("used_as") != "sql_reuse":
                native_memory["used_as"] = "prompt_context"
        elif memory_decision["used_as"] == "degraded" and native_memory.get("used_as") not in {"sql_reuse", "prompt_context"}:
            native_memory["used_as"] = "degraded"
        elif memory_decision["used_as"] == "rejected" and native_memory.get("used_as") in {"none", "rejected"}:
            native_memory["used_as"] = "rejected"
        if memory_decision["rejected_reason"] and not native_memory.get("rejected_reason"):
            native_memory["rejected_reason"] = memory_decision["rejected_reason"]
        if str(memory_decision.get("query_intent") or "") and native_memory.get("query_intent") in {"", "unknown"}:
            native_memory["query_intent"] = str(memory_decision.get("query_intent") or "unknown")
        if memory_decision.get("candidate_intent") and native_memory.get("candidate_intent") in {"", "unknown"}:
            native_memory["candidate_intent"] = str(memory_decision.get("candidate_intent") or "unknown")
        if memory_decision.get("intent_matched"):
            native_memory["intent_matched"] = True
        if memory_decision.get("reuse_gate_reason"):
            gate_reason = str(memory_decision.get("reuse_gate_reason") or "")
            if native_memory.get("used_as") == "sql_reuse" and gate_reason == "reuse_allowed":
                native_memory["reuse_gate_reason"] = gate_reason
            elif not native_memory.get("reuse_gate_reason"):
                native_memory["reuse_gate_reason"] = gate_reason
        if memory_decision.get("candidate_examples"):
            existing = list(native_memory.get("candidate_examples") or [])
            existing_keys = {
                (
                    str(item.get("question") or ""),
                    str(item.get("source") or ""),
                    str(item.get("sql") or ""),
                    tuple(item.get("table_names") or []),
                )
                for item in existing
                if isinstance(item, dict)
            }
            for item in list(memory_decision.get("candidate_examples") or []):
                key = (
                    str(item.get("question") or ""),
                    str(item.get("source") or ""),
                    str(item.get("sql") or ""),
                    tuple(item.get("table_names") or []),
                )
                if key in existing_keys:
                    continue
                existing.append(item)
                existing_keys.add(key)
                if len(existing) >= 10:
                    break
            native_memory["candidate_examples"] = existing
        if memory_decision.get("chosen_candidate"):
            chosen_candidates = list(native_memory.get("chosen_candidates") or [])
            chosen_item = {"table_name": table_name, **dict(memory_decision.get("chosen_candidate") or {})}
            chosen_item["used_as"] = str(memory_decision.get("used_as") or "none")
            chosen_candidates.append(chosen_item)
            native_memory["chosen_candidates"] = chosen_candidates
            if not native_memory.get("chosen_candidate"):
                native_memory["chosen_candidate"] = dict(chosen_item)
        if memory_decision.get("rejected_candidates"):
            rejected_items = list(native_memory.get("rejected_candidates") or [])
            for item in list(memory_decision.get("rejected_candidates") or []):
                rejected_items.append({"table_name": table_name, **dict(item or {})})
                if len(rejected_items) >= 10:
                    break
            native_memory["rejected_candidates"] = rejected_items[:10]

        per_subtask_memory.append(
            {
                "table_name": table_name,
                "candidate_count": int(memory_decision.get("candidate_count", len(memory_candidates))),
                "memory_hit": bool(memory_decision.get("vanna_memory_hit", False)),
                "selected_source": str(memory_decision.get("vanna_memory_source") or ""),
                "fallback_used": bool(memory_trace.get("degraded", False)),
                "vanna_memory_hit": bool(memory_decision.get("vanna_memory_hit", False)),
                "vanna_memory_source": str(memory_decision.get("vanna_memory_source") or ""),
                "confidence": float(memory_decision.get("confidence", 0.0)),
                "used_as": str(memory_decision.get("used_as") or "none"),
                "rejected_reason": str(memory_decision.get("rejected_reason") or ""),
                "query_intent": str(memory_decision.get("query_intent") or "unknown"),
                "candidate_intent": str(memory_decision.get("candidate_intent") or "unknown"),
                "intent_matched": bool(memory_decision.get("intent_matched", False)),
                "reuse_gate_reason": str(memory_decision.get("reuse_gate_reason") or ""),
                "candidate_examples": list(memory_decision.get("candidate_examples") or []),
                "chosen_candidate": dict(memory_decision.get("chosen_candidate") or {}),
                "rejected_candidates": list(memory_decision.get("rejected_candidates") or []),
            }
        )

        ddl_doc_payload = await execute_tool(
            "dc_ddl_doc_retrieval",
            {"question": question, "api_config": dict(api_config or {})},
            fail_hard=False,
        )
        ddl_doc_data = dict(ddl_doc_payload.get("data") or {})
        ddl_list = list(ddl_doc_data.get("ddl") or [])
        ddl_trace = dict(ddl_doc_data.get("trace") or {})
        ddl_context_for_repair.extend(ddl_list[:3])

        generation_trace: Dict[str, Any] = {}
        if memory_decision.get("used_as") == "sql_reuse" and memory_decision.get("reused_sql"):
            sql_text = str(memory_decision.get("reused_sql") or "").strip()
            sql_map[table_name] = sql_text
            generation_trace = {
                "table_name": table_name,
                "question": question,
                "strategy": "native_memory_sql_reuse",
                "prompt_attempts": 0,
                "example_count": len(memory_candidates),
                "ddl_count": len(ddl_list),
                "documentation_count": len(ddl_doc_data.get("documentation") or []),
                "retrieval_source_labels": [str(memory_decision.get("vanna_memory_source") or "")] if memory_decision.get("vanna_memory_source") else [],
                "candidate_retrieval_source_labels": [str(memory_decision.get("vanna_memory_source") or "")] if memory_decision.get("vanna_memory_source") else [],
                "memory_hit": True,
                "candidate_memory_hit": bool(memory_candidates),
                "memory_fallback_used": bool(memory_trace.get("degraded", False)),
                "memory_source": str(memory_decision.get("vanna_memory_source") or ""),
                "query_intent": str(memory_decision.get("query_intent") or "unknown"),
                "candidate_intent": str(memory_decision.get("candidate_intent") or "unknown"),
                "intent_matched": bool(memory_decision.get("intent_matched", False)),
                "reuse_gate_reason": str(memory_decision.get("reuse_gate_reason") or ""),
                "phases": ["vanna_memory_retrieval", "sql_generation_reuse"],
                "target_only": True,
                "referenced_tables": list(_extract_sql_tables(sql_text) or [table_name]),
            }
        else:
            generation_subtask = dict(subtask or {})
            generation_question = query
            if memory_decision.get("used_as") == "prompt_context" and memory_candidates:
                generation_question = f"{query}{_build_memory_context_line(memory_candidates)}"
                generation_subtask["question"] = generation_question

            sql_payload = await execute_tool(
                "dc_sql_generation",
                {
                    "question": generation_question,
                    "table_name": table_name,
                    "subtask": generation_subtask,
                    "api_config": dict(api_config or {}),
                },
            )
            sql_data = dict(sql_payload.get("data") or {})
            sql_text = str(sql_data.get("sql") or "").strip()
            if not sql_text:
                raise NativeKernelExecutionError(
                    code="sql_generation_empty",
                    message=f"Native SQL generation returned empty SQL for table '{table_name}'",
                )
            sql_map[table_name] = sql_text
            generation_trace = dict(sql_data.get("trace") or {})
            generation_trace.setdefault("query_intent", str(memory_decision.get("query_intent") or "unknown"))
            generation_trace.setdefault("candidate_intent", str(memory_decision.get("candidate_intent") or "unknown"))
            generation_trace.setdefault("intent_matched", bool(memory_decision.get("intent_matched", False)))
            generation_trace.setdefault("reuse_gate_reason", str(memory_decision.get("reuse_gate_reason") or ""))

        candidate_labels = list(generation_trace.get("candidate_retrieval_source_labels") or [])
        used_labels = list(generation_trace.get("retrieval_source_labels") or [])
        subtask_trace = {
            "table_name": table_name,
            "question": str(generation_trace.get("question") or question),
            "strategy": str(generation_trace.get("strategy") or "native_tool_sql_generation"),
            "sql": sql_text,
            "prompt_attempts": int(generation_trace.get("prompt_attempts", 0)),
            "metadata_available": bool(generation_trace.get("metadata_available", False)),
            "schema_column_count": int(generation_trace.get("schema_column_count", 0)),
            "example_count": int(generation_trace.get("example_count", len(memory_candidates))),
            "ddl_count": int(generation_trace.get("ddl_count", len(ddl_list))),
            "documentation_count": int(generation_trace.get("documentation_count", len(ddl_doc_data.get("documentation") or []))),
            "retrieval_source_labels": used_labels,
            "candidate_retrieval_source_labels": candidate_labels,
            "memory_hit": bool(generation_trace.get("memory_hit", False)),
            "candidate_memory_hit": bool(generation_trace.get("candidate_memory_hit", bool(memory_candidates))),
            "memory_fallback_used": bool(generation_trace.get("memory_fallback_used", memory_trace.get("degraded", False))),
            "memory_source": str(generation_trace.get("memory_source") or ""),
            "query_intent": str(generation_trace.get("query_intent") or memory_decision.get("query_intent") or "unknown"),
            "candidate_intent": str(generation_trace.get("candidate_intent") or memory_decision.get("candidate_intent") or "unknown"),
            "intent_matched": bool(generation_trace.get("intent_matched", memory_decision.get("intent_matched", False))),
            "reuse_gate_reason": str(generation_trace.get("reuse_gate_reason") or memory_decision.get("reuse_gate_reason") or ""),
            "phases": list(generation_trace.get("phases") or ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"]),
            "target_only": bool(generation_trace.get("target_only", True)),
            "referenced_tables": list(generation_trace.get("referenced_tables") or [table_name]),
        }
        subtask_traces.append(subtask_trace)

    if not sql_map:
        raise NativeKernelExecutionError(
            code="no_sql_generated",
            message="Planner resolved tables but SQL generation returned no statements",
        )

    retrieval_source_labels = sorted(
        {
            source
            for trace in subtask_traces
            for source in list(trace.get("retrieval_source_labels") or [])
            if source
        }
    )
    retrieval_summary = {
        "example_count": sum(int(trace.get("example_count", 0)) for trace in subtask_traces),
        "ddl_count": sum(int(trace.get("ddl_count", 0)) for trace in subtask_traces),
        "documentation_count": sum(int(trace.get("documentation_count", 0)) for trace in subtask_traces),
        "source_labels": retrieval_source_labels,
        "memory_hit": any(bool(trace.get("memory_hit", False)) for trace in subtask_traces),
        "memory_fallback_used": any(bool(trace.get("memory_fallback_used", False)) for trace in subtask_traces),
    }
    phase_traces.extend(
        [
            {
                "phase": "memory_retrieval",
                "status": "ok",
                "details": {
                    "example_count": retrieval_summary["example_count"],
                    "memory_hit": retrieval_summary["memory_hit"],
                    "memory_fallback_used": retrieval_summary["memory_fallback_used"],
                },
                "source_labels": [source for source in retrieval_source_labels if source.startswith("query_history.")],
            },
            {
                "phase": "ddl_doc_retrieval",
                "status": "ok",
                "details": {
                    "ddl_count": retrieval_summary["ddl_count"],
                    "documentation_count": retrieval_summary["documentation_count"],
                },
                "source_labels": retrieval_source_labels,
            },
            {
                "phase": "sql_generation",
                "status": "ok",
                "details": {"subtask_count": len(sql_map)},
                "source_labels": ["dc_sql_generation"],
            },
        ]
    )

    try:
        relationships = await datasource_handler.list_relationships_async(plan.get("tables"))
    except Exception as relationship_error:
        relationships = []
        warnings.append(f"relationships unavailable: {_sanitize_error(relationship_error)}")
    coordinator = CoordinatorAgent()
    coordination_result = await asyncio.to_thread(
        coordinator.coordinate_with_trace,
        plan,
        sql_map,
        relationships,
    )
    orchestration_trace = dict((coordination_result or {}).get("trace") or {})
    generated_sql = str((coordination_result or {}).get("sql") or "").strip()
    if not generated_sql:
        raise NativeKernelExecutionError(
            code="orchestration_empty_sql",
            message="Native orchestration returned empty SQL",
        )
    phase_traces.append(
        {
            "phase": "orchestration",
            "status": "ok",
            "details": {"strategy": str(orchestration_trace.get("strategy") or "passthrough")},
            "source_labels": ["coordinator.relationships"],
        }
    )

    repair_trace: Dict[str, Any] = {
        "attempted": False,
        "max_attempts": int(max_repair_attempts),
        "attempts": [],
    }
    final_sql = generated_sql

    validation_payload = await execute_tool(
        "dc_sql_validation",
        {"sql": final_sql},
        fail_hard=False,
    )
    validation = dict((validation_payload.get("data") or {}).get("validation") or {})
    validation_errors = list(validation.get("errors") or [])
    if validation_errors:
        warnings.append(f"native validation warnings: {'; '.join(str(item) for item in validation_errors[:2])}")

    execution_payload = await execute_tool(
        "dc_sql_execution",
        {"sql": final_sql, "params": []},
        fail_hard=False,
    )
    execution_data = dict(execution_payload.get("data") or {})
    query_result = list(execution_data.get("rows") or [])
    last_error_message = str(((execution_payload.get("error") or {}).get("message") or "")).strip()

    if not bool(execution_payload.get("success", False)):
        repair_trace["attempted"] = True
        for attempt_index in range(int(max_repair_attempts)):
            failed_sql = final_sql
            repair_payload = await execute_tool(
                "dc_sql_repair",
                {
                    "question": query,
                    "failed_sql": failed_sql,
                    "error_message": last_error_message or "sql execution failed",
                    "ddl_list": list(ddl_context_for_repair)[:10],
                    "api_config": dict(api_config or {}),
                },
                fail_hard=False,
            )
            repair_data = dict(repair_payload.get("data") or {})
            repair_error = dict(repair_payload.get("error") or {})
            repaired_sql = str(repair_data.get("sql") or "").strip().rstrip(";")
            repair_attempt_meta = dict(repair_data.get("trace") or {})

            attempt_trace = {
                "attempt": attempt_index + 1,
                "error_message": last_error_message or "sql execution failed",
                "failed_sql": failed_sql,
                "repaired_sql": repaired_sql,
                "succeeded": False,
                "ddl_count": int(repair_attempt_meta.get("ddl_count", 0)),
                "model": str(repair_attempt_meta.get("model") or (api_config or {}).get("model") or ""),
                "base_url": str(repair_attempt_meta.get("base_url") or (api_config or {}).get("base_url") or ""),
            }
            if not bool(repair_payload.get("success", False)) or not repaired_sql:
                repair_trace["attempts"].append(attempt_trace)
                last_error_message = str(repair_error.get("message") or last_error_message or "sql repair failed")
                continue

            final_sql = repaired_sql
            validation_payload = await execute_tool(
                "dc_sql_validation",
                {"sql": final_sql},
                fail_hard=False,
            )
            validation = dict((validation_payload.get("data") or {}).get("validation") or {})
            if list(validation.get("errors") or []):
                repair_trace["attempts"].append(attempt_trace)
                last_error_message = "; ".join(str(item) for item in list(validation.get("errors") or []))
                continue

            execution_payload = await execute_tool(
                "dc_sql_execution",
                {"sql": final_sql, "params": []},
                fail_hard=False,
            )
            execution_data = dict(execution_payload.get("data") or {})
            if bool(execution_payload.get("success", False)):
                query_result = list(execution_data.get("rows") or [])
                attempt_trace["succeeded"] = True
                repair_trace["attempts"].append(attempt_trace)
                break

            repair_trace["attempts"].append(attempt_trace)
            last_error_message = str(((execution_payload.get("error") or {}).get("message") or "sql execution failed"))
        else:
            raise NativeKernelExecutionError(
                code="sql_execution_failed",
                message=last_error_message or "Native kernel SQL execution failed after repair attempts",
            )

    phase_traces.append(
        {
            "phase": "validation_repair",
            "status": "repaired" if repair_trace.get("attempted") else "ok",
            "details": {
                "attempted": bool(repair_trace.get("attempted", False)),
                "attempt_count": len(repair_trace.get("attempts", [])),
                "max_attempts": int(repair_trace.get("max_attempts", 0)),
            },
            "source_labels": ["dc_sql_validation", "dc_sql_repair"],
        }
    )

    from vanna_doris import VannaDorisOpenAI

    history_vanna = VannaDorisOpenAI(
        doris_client=doris_client,
        api_key=api_config.get("api_key"),
        model=api_config.get("model"),
        base_url=api_config.get("base_url"),
        api_config=api_config,
        config={"temperature": 0.1},
    )
    try:
        history_result = await asyncio.to_thread(
            history_vanna.add_question_sql,
            question=query,
            sql=final_sql,
            row_count=len(query_result),
            is_empty_result=len(query_result) == 0,
        )
    except Exception as history_error:
        warnings.append(f"history persistence failed: {_sanitize_error(history_error)}")
        history_result = {"status": "error", "id": None}

    phase_traces.append(
        {
            "phase": "execution",
            "status": "ok",
            "details": {
                "row_count": len(query_result),
                "history_status": str((history_result or {}).get("status") or ""),
            },
            "source_labels": ["dc_sql_execution"],
        }
    )

    if hasattr(backbone.audit_bridge, "snapshot"):
        try:
            audit_events = backbone.audit_bridge.snapshot(request_id=request_id)
        except TypeError:
            audit_events = backbone.audit_bridge.snapshot()
    else:
        audit_events = []
    if hasattr(backbone.audit_bridge, "clear"):
        try:
            backbone.audit_bridge.clear(request_id=request_id)
        except TypeError:
            backbone.audit_bridge.clear()

    native_trace = {
        "kernel": "native",
        "runtime_reused": bool(runtime_reused),
        "runtime_cache_key": runtime_cache_key,
        "tools_called": tools_called,
        "memory": {
            "example_count": int(native_memory.get("example_count", 0)),
            "memory_hit": bool(native_memory.get("memory_hit", False)),
            "sources_attempted": list(native_memory.get("sources_attempted") or []),
            "vanna_memory_hit": bool(native_memory.get("vanna_memory_hit", False)),
            "vanna_memory_source": str(native_memory.get("vanna_memory_source") or ""),
            "confidence": float(native_memory.get("confidence", 0.0)),
            "used_as": str(native_memory.get("used_as") or "none"),
            "rejected_reason": str(native_memory.get("rejected_reason") or ""),
            "candidate_count": int(native_memory.get("candidate_count", 0)),
            "candidate_examples": list(native_memory.get("candidate_examples") or []),
            "chosen_candidate": dict(native_memory.get("chosen_candidate") or {}),
            "chosen_candidates": list(native_memory.get("chosen_candidates") or []),
            "rejected_candidates": list(native_memory.get("rejected_candidates") or []),
            "query_intent": str(native_memory.get("query_intent") or "unknown"),
            "candidate_intent": str(native_memory.get("candidate_intent") or "unknown"),
            "intent_matched": bool(native_memory.get("intent_matched", False)),
            "reuse_gate_reason": str(native_memory.get("reuse_gate_reason") or ""),
            "subtasks": per_subtask_memory,
        },
        "audit_events": audit_events,
        "fallback_reason": "",
    }

    return NativeKernelResult(
        intent=str(plan.get("intent") or "list"),
        plan=plan,
        subtask_traces=subtask_traces,
        retrieval_summary=retrieval_summary,
        orchestration_trace=orchestration_trace,
        repair_trace=repair_trace,
        sql=final_sql,
        data=query_result,
        history_id=(history_result or {}).get("id") if isinstance(history_result, dict) else None,
        history_status=str((history_result or {}).get("status") or ""),
        warnings=warnings,
        phase_traces=phase_traces,
        native_trace=native_trace,
    )
