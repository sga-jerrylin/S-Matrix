"""
DC-backed AgentMemory implementation for Vanna2 native runtime.
"""

from __future__ import annotations

import asyncio
import difflib
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from vanna.capabilities.agent_memory import AgentMemory
from vanna.capabilities.agent_memory.models import (
    TextMemory,
    TextMemorySearchResult,
    ToolMemory,
    ToolMemorySearchResult,
)


class DCAgentMemoryAdapter(AgentMemory):
    """
    AgentMemory adapter backed by DorisClaw system tables.

    Primary source:
    - `_sys_query_history` for question/sql memory

    Secondary source:
    - `_sys_table_registry` + `_sys_table_metadata` + information_schema for text memories
    """

    def __init__(self, *, doris_client: Any, max_items: int = 10_000):
        self.doris_client = doris_client
        self._runtime_memories: List[ToolMemory] = []
        self._runtime_text_memories: List[TextMemory] = []
        self._max_items = max(100, int(max_items))
        self._lock = asyncio.Lock()
        self._last_search_trace_by_request: Dict[str, Dict[str, Any]] = {}
        self._text_cache: Dict[str, Any] = {"expires_at": 0.0, "items": []}

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().isoformat()

    @staticmethod
    def _normalize(text: str) -> str:
        return " ".join(str(text or "").lower().split())

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        compact = DCAgentMemoryAdapter._normalize(text)
        if not compact:
            return set()
        if " " in compact:
            return {token for token in compact.split(" ") if token}
        if len(compact) <= 4:
            return {compact}
        # CJK fallback: bigrams
        return {compact[index : index + 2] for index in range(len(compact) - 1)}

    @classmethod
    def _similarity(cls, a: str, b: str) -> float:
        a_norm = cls._normalize(a)
        b_norm = cls._normalize(b)
        if not a_norm or not b_norm:
            return 0.0

        token_a = cls._tokenize(a_norm)
        token_b = cls._tokenize(b_norm)
        if token_a and token_b:
            jaccard = len(token_a & token_b) / max(1, len(token_a | token_b))
        else:
            jaccard = 0.0

        ratio = difflib.SequenceMatcher(None, a_norm, b_norm).ratio()
        return float(min(1.0, max(jaccard, ratio)))

    @staticmethod
    def _extract_keywords(question: str) -> List[str]:
        compact = re.sub(r"\s+", "", str(question or ""))
        compact = re.sub(r"[，。！？,.!?;；:：]", "", compact)
        if not compact:
            return []
        keywords = {compact}
        if len(compact) > 4:
            keywords.update(compact[index : index + 2] for index in range(len(compact) - 1))
        return sorted(keyword for keyword in keywords if keyword)

    @staticmethod
    def _parse_table_names(raw: Any) -> List[str]:
        if isinstance(raw, list):
            names = [str(item).strip() for item in raw if str(item).strip()]
            return names
        if raw is None:
            return []
        text = str(raw).strip()
        if not text:
            return []
        return [segment.strip() for segment in text.split(",") if segment.strip()]

    def _remember_search_trace(self, context: Any, trace: Dict[str, Any]) -> None:
        request_id = str(getattr(context, "request_id", "") or "")
        if request_id:
            self._last_search_trace_by_request[request_id] = dict(trace)

    def get_last_search_trace(self, request_id: str) -> Dict[str, Any]:
        return dict(self._last_search_trace_by_request.get(str(request_id or ""), {}))

    def _build_tool_memory(self, row: Dict[str, Any], *, source: str, similarity: float) -> ToolMemory:
        table_names = self._parse_table_names(row.get("table_names"))
        sql = str(row.get("sql") or "").strip()
        metadata = {
            "source": source,
            "question_hash": row.get("question_hash"),
            "row_count": int(row.get("row_count", 0) or 0),
            "is_empty_result": bool(row.get("is_empty_result", False)),
            "table_names": table_names,
            "confidence": float(similarity),
        }
        return ToolMemory(
            memory_id=str(row.get("id") or f"history-{uuid.uuid4().hex}"),
            question=str(row.get("question") or ""),
            tool_name="dc_sql_generation",
            args={
                "sql": sql,
                "table_names": table_names,
                "table_name": table_names[0] if table_names else "",
            },
            timestamp=str(row.get("created_at") or ""),
            success=bool(int(row.get("quality_gate", 1) or 1)),
            metadata=metadata,
        )

    def _normalize_rows(self, rows: Any) -> List[Dict[str, Any]]:
        if not isinstance(rows, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                normalized.append(row)
        return normalized

    def _fetch_query_history_candidates(
        self,
        question: str,
        *,
        limit: int,
    ) -> Dict[str, Any]:
        question = str(question or "").strip()
        sources_attempted: List[str] = []
        errors: List[Dict[str, str]] = []
        selected_source = ""
        rows: List[Dict[str, Any]] = []

        # Stage 1: MATCH_ANY
        match_any_sql = """
        SELECT `id`, `question`, `sql`, `table_names`, `question_hash`, `quality_gate`,
               `is_empty_result`, `row_count`, `created_at`
        FROM `_sys_query_history`
        WHERE `quality_gate` = 1
          AND `question` MATCH_ANY %s
        ORDER BY `is_empty_result` ASC, `created_at` DESC
        LIMIT %s
        """
        sources_attempted.append("query_history.match_any")
        try:
            rows = self._normalize_rows(self.doris_client.execute_query(match_any_sql, (question, max(limit * 3, 20))))
            if rows:
                selected_source = "query_history.match_any"
        except Exception as exc:
            errors.append({"source": "query_history.match_any", "error": str(exc)})

        # Stage 2: LIKE keywords
        if not rows:
            keywords = self._extract_keywords(question)
            if keywords:
                like_clauses = " OR ".join(["`question` LIKE %s"] * len(keywords))
                like_sql = f"""
                SELECT `id`, `question`, `sql`, `table_names`, `question_hash`, `quality_gate`,
                       `is_empty_result`, `row_count`, `created_at`
                FROM `_sys_query_history`
                WHERE `quality_gate` = 1
                  AND ({like_clauses})
                ORDER BY `is_empty_result` ASC, `created_at` DESC
                LIMIT %s
                """
                params = tuple(f"%{keyword}%" for keyword in keywords) + (max(limit * 3, 20),)
                sources_attempted.append("query_history.like_keyword")
                try:
                    rows = self._normalize_rows(self.doris_client.execute_query(like_sql, params))
                    if rows:
                        selected_source = "query_history.like_keyword"
                except Exception as exc:
                    errors.append({"source": "query_history.like_keyword", "error": str(exc)})

        # Stage 3: recent fallback
        if not rows:
            recent_sql = """
            SELECT `id`, `question`, `sql`, `table_names`, `question_hash`, `quality_gate`,
                   `is_empty_result`, `row_count`, `created_at`
            FROM `_sys_query_history`
            WHERE `quality_gate` = 1
            ORDER BY `created_at` DESC
            LIMIT %s
            """
            sources_attempted.append("query_history.recent")
            try:
                rows = self._normalize_rows(self.doris_client.execute_query(recent_sql, (max(limit * 5, 50),)))
                if rows:
                    selected_source = "query_history.recent"
            except Exception as exc:
                errors.append({"source": "query_history.recent", "error": str(exc)})

        return {
            "rows": rows,
            "selected_source": selected_source,
            "sources_attempted": sources_attempted,
            "errors": errors,
            "degraded": bool(errors) and not rows,
        }

    def _build_doc_text_memories(self) -> List[TextMemory]:
        now_ts = datetime.now().timestamp()
        cache_items = self._text_cache.get("items") or []
        expires_at = float(self._text_cache.get("expires_at") or 0.0)
        if cache_items and now_ts < expires_at:
            return list(cache_items)

        memories: List[TextMemory] = []
        # Documentation context from registry/metadata
        registry_sql = """
        SELECT r.table_name, r.display_name, r.description,
               m.description AS auto_description, m.columns_info
        FROM `_sys_table_registry` r
        LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
        ORDER BY r.updated_at DESC
        LIMIT 300
        """
        try:
            rows = self._normalize_rows(self.doris_client.execute_query(registry_sql))
        except Exception:
            rows = []
        for row in rows:
            table_name = str(row.get("table_name") or "").strip()
            if not table_name:
                continue
            display_name = str(row.get("display_name") or table_name)
            description = str(row.get("description") or row.get("auto_description") or "")
            columns_info = str(row.get("columns_info") or "")
            content = (
                f"Table: {table_name}\n"
                f"Display Name: {display_name}\n"
                f"Description: {description}\n"
                f"Columns Info: {columns_info}"
            )
            memories.append(
                TextMemory(
                    memory_id=f"doc:{table_name}",
                    content=content,
                    timestamp=self._now_iso(),
                )
            )

        # DDL context via information_schema
        ddl_sql = """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        LIMIT 2000
        """
        try:
            schema_name = str(self.doris_client.config.get("database") or "")
            ddl_rows = self._normalize_rows(self.doris_client.execute_query(ddl_sql, (schema_name,)))
        except Exception:
            ddl_rows = []
        ddl_by_table: Dict[str, List[str]] = {}
        for row in ddl_rows:
            table_name = str(row.get("TABLE_NAME") or "").strip()
            if not table_name:
                continue
            ddl_by_table.setdefault(table_name, []).append(
                f"`{row.get('COLUMN_NAME')}` {row.get('DATA_TYPE')}"
            )
        for table_name, columns in ddl_by_table.items():
            ddl_content = "CREATE TABLE `" + table_name + "` (\n  " + ",\n  ".join(columns[:50]) + "\n)"
            memories.append(
                TextMemory(
                    memory_id=f"ddl:{table_name}",
                    content=ddl_content,
                    timestamp=self._now_iso(),
                )
            )

        self._text_cache = {
            "expires_at": now_ts + 300.0,
            "items": list(memories),
        }
        return memories

    async def retrieve_similar_sql_candidates(
        self,
        *,
        question: str,
        context: Any,
        limit: int = 10,
    ) -> Dict[str, Any]:
        limit = max(1, int(limit))
        search = self._fetch_query_history_candidates(question, limit=limit)
        rows = list(search.get("rows") or [])
        scored: List[ToolMemorySearchResult] = []
        for row in rows:
            score = self._similarity(str(question or ""), str(row.get("question") or ""))
            memory = self._build_tool_memory(
                row,
                source=str(search.get("selected_source") or "query_history"),
                similarity=score,
            )
            scored.append(
                ToolMemorySearchResult(
                    memory=memory,
                    similarity_score=score,
                    rank=0,
                )
            )
        scored.sort(key=lambda item: float(item.similarity_score), reverse=True)
        ranked: List[ToolMemorySearchResult] = []
        for index, item in enumerate(scored[:limit], start=1):
            ranked.append(
                ToolMemorySearchResult(
                    memory=item.memory,
                    similarity_score=float(item.similarity_score),
                    rank=index,
                )
            )

        trace = {
            "selected_source": str(search.get("selected_source") or ""),
            "sources_attempted": list(search.get("sources_attempted") or []),
            "errors": list(search.get("errors") or []),
            "degraded": bool(search.get("degraded", False)),
            "candidate_count": len(ranked),
        }
        self._remember_search_trace(context, trace)
        return {"results": ranked, "trace": trace}

    async def save_tool_usage(
        self,
        question: str,
        tool_name: str,
        args: Dict[str, Any],
        context: Any,
        success: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        memory = ToolMemory(
            memory_id=str(uuid.uuid4()),
            question=str(question or ""),
            tool_name=str(tool_name or "unknown_tool"),
            args=dict(args or {}),
            timestamp=self._now_iso(),
            success=bool(success),
            metadata=dict(metadata or {}),
        )
        async with self._lock:
            self._runtime_memories.append(memory)
            if len(self._runtime_memories) > self._max_items:
                overflow = len(self._runtime_memories) - self._max_items
                del self._runtime_memories[:overflow]

    async def save_text_memory(self, content: str, context: Any) -> TextMemory:
        memory = TextMemory(
            memory_id=str(uuid.uuid4()),
            content=str(content or ""),
            timestamp=self._now_iso(),
        )
        async with self._lock:
            self._runtime_text_memories.append(memory)
            if len(self._runtime_text_memories) > self._max_items:
                overflow = len(self._runtime_text_memories) - self._max_items
                del self._runtime_text_memories[:overflow]
        return memory

    async def search_similar_usage(
        self,
        question: str,
        context: Any,
        *,
        limit: int = 10,
        similarity_threshold: float = 0.7,
        tool_name_filter: Optional[str] = None,
    ) -> List[ToolMemorySearchResult]:
        if tool_name_filter and tool_name_filter != "dc_sql_generation":
            return []

        retrieval = await self.retrieve_similar_sql_candidates(
            question=question,
            context=context,
            limit=limit,
        )
        threshold = max(0.0, float(similarity_threshold))
        results = list(retrieval.get("results") or [])
        filtered = [result for result in results if float(result.similarity_score) >= threshold]
        ranked: List[ToolMemorySearchResult] = []
        for index, item in enumerate(filtered[: max(1, int(limit))], start=1):
            ranked.append(
                ToolMemorySearchResult(
                    memory=item.memory,
                    similarity_score=float(item.similarity_score),
                    rank=index,
                )
            )
        return ranked

    async def search_text_memories(
        self,
        query: str,
        context: Any,
        *,
        limit: int = 10,
        similarity_threshold: float = 0.7,
    ) -> List[TextMemorySearchResult]:
        runtime_items = list(self._runtime_text_memories)
        doc_items = self._build_doc_text_memories()
        candidates = runtime_items + doc_items
        scored: List[Tuple[TextMemory, float]] = []
        for item in candidates:
            score = self._similarity(str(query or ""), str(item.content or ""))
            scored.append((item, score))
        scored.sort(key=lambda pair: float(pair[1]), reverse=True)
        threshold = max(0.0, float(similarity_threshold))
        results: List[TextMemorySearchResult] = []
        for item, score in scored:
            if score < threshold:
                continue
            results.append(
                TextMemorySearchResult(
                    memory=item,
                    similarity_score=float(score),
                    rank=len(results) + 1,
                )
            )
            if len(results) >= max(1, int(limit)):
                break
        return results

    async def get_recent_memories(self, context: Any, limit: int = 10) -> List[ToolMemory]:
        limit = max(1, int(limit))
        runtime = list(reversed(self._runtime_memories[-limit:]))
        remaining = max(0, limit - len(runtime))
        if remaining <= 0:
            return runtime

        recent_sql = """
        SELECT `id`, `question`, `sql`, `table_names`, `question_hash`, `quality_gate`,
               `is_empty_result`, `row_count`, `created_at`
        FROM `_sys_query_history`
        WHERE `quality_gate` = 1
        ORDER BY `created_at` DESC
        LIMIT %s
        """
        try:
            rows = self._normalize_rows(self.doris_client.execute_query(recent_sql, (remaining,)))
        except Exception:
            rows = []

        db_items: List[ToolMemory] = []
        for row in rows:
            db_items.append(
                self._build_tool_memory(
                    row,
                    source="query_history.recent",
                    similarity=1.0,
                )
            )
        return runtime + db_items

    async def get_recent_text_memories(self, context: Any, limit: int = 10) -> List[TextMemory]:
        limit = max(1, int(limit))
        runtime = list(reversed(self._runtime_text_memories[-limit:]))
        if len(runtime) >= limit:
            return runtime
        doc_items = self._build_doc_text_memories()
        remain = limit - len(runtime)
        return runtime + doc_items[:remain]

    async def delete_by_id(self, context: Any, memory_id: str) -> bool:
        target = str(memory_id or "")
        async with self._lock:
            for index, item in enumerate(self._runtime_memories):
                if str(item.memory_id or "") == target:
                    del self._runtime_memories[index]
                    return True
        return False

    async def delete_text_memory(self, context: Any, memory_id: str) -> bool:
        target = str(memory_id or "")
        async with self._lock:
            for index, item in enumerate(self._runtime_text_memories):
                if str(item.memory_id or "") == target:
                    del self._runtime_text_memories[index]
                    return True
        return False

    async def clear_memories(
        self,
        context: Any,
        tool_name: Optional[str] = None,
        before_date: Optional[str] = None,
    ) -> int:
        deleted = 0
        async with self._lock:
            if tool_name is None and before_date is None:
                deleted = len(self._runtime_memories) + len(self._runtime_text_memories)
                self._runtime_memories = []
                self._runtime_text_memories = []
                return deleted

            kept: List[ToolMemory] = []
            for item in self._runtime_memories:
                should_delete = True
                if tool_name and str(item.tool_name or "") != str(tool_name):
                    should_delete = False
                if should_delete and before_date and item.timestamp and str(item.timestamp) >= str(before_date):
                    should_delete = False
                if should_delete:
                    deleted += 1
                else:
                    kept.append(item)
            self._runtime_memories = kept

            kept_text: List[TextMemory] = []
            for item in self._runtime_text_memories:
                should_delete = tool_name is None
                if should_delete and before_date and item.timestamp and str(item.timestamp) >= str(before_date):
                    should_delete = False
                if should_delete:
                    deleted += 1
                else:
                    kept_text.append(item)
            self._runtime_text_memories = kept_text
        return deleted

