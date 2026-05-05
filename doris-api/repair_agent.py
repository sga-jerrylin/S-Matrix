"""
Auto-repair agent for failed SQL execution.
"""
import json
from typing import Any, Dict, List, Optional

from db import DorisClient
from llm_executor import LLMExecutor


class RepairAgent:
    """Use an LLM to repair failed SQL with schema context."""

    def __init__(
        self,
        doris_client: DorisClient,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.doris_client = doris_client
        self.api_key = api_key
        self.model = model or "deepseek-chat"
        self.base_url = base_url or "https://api.deepseek.com"

    def repair_sql(
        self,
        question: str,
        failed_sql: str,
        error_message: str,
        ddl_list: Optional[List[str]] = None,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        runtime_config = dict(api_config or {})
        runtime_config.setdefault("api_key", self.api_key)
        runtime_config.setdefault("model", self.model)
        runtime_config.setdefault("base_url", self.base_url)
        runtime_config.setdefault("llm_execution_mode", "direct_api")

        prompt = self._build_prompt(question, failed_sql, error_message, ddl_list or [])
        executor = LLMExecutor(
            doris_client=self.doris_client,
            api_config=runtime_config,
        )
        content = executor.call(
            prompt=prompt,
            system_prompt="You repair Apache Doris SQL. Return only the corrected SQL.",
            temperature=0.0,
            max_tokens=2000,
        )
        return self._clean_sql(content)

    def repair_sql_with_trace(
        self,
        question: str,
        failed_sql: str,
        error_message: str,
        ddl_list: Optional[List[str]] = None,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        runtime_config = dict(api_config or {})
        runtime_config.setdefault("api_key", self.api_key)
        runtime_config.setdefault("model", self.model)
        runtime_config.setdefault("base_url", self.base_url)
        runtime_config.setdefault("llm_execution_mode", "direct_api")
        model = runtime_config.get("model") or self.model
        base_url = runtime_config.get("base_url") or self.base_url

        sql = self.repair_sql(
            question,
            failed_sql,
            error_message,
            ddl_list=ddl_list,
            api_config=runtime_config,
        )
        return {
            "sql": sql,
            "trace": {
                "model": model,
                "base_url": base_url,
                "ddl_count": len(ddl_list or []),
            },
        }

    def _build_prompt(
        self,
        question: str,
        failed_sql: str,
        error_message: str,
        ddl_list: List[str],
    ) -> str:
        ddl_block = "\n\n".join(ddl_list[:10])
        return (
            "修复下面这条 Apache Doris SQL，并只返回最终 SQL。\n\n"
            f"用户问题:\n{question}\n\n"
            f"失败 SQL:\n{failed_sql}\n\n"
            f"错误信息:\n{error_message}\n\n"
            f"相关 DDL:\n{ddl_block}\n"
        )

    def _clean_sql(self, sql: str) -> str:
        cleaned = (sql or "").strip()
        if cleaned.startswith("```sql"):
            cleaned = cleaned[6:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()
