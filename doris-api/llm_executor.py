"""
Unified LLM execution adapter for NLQ query intelligence.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, Optional

import pymysql
import requests

from db import DorisClient


_RESOURCE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]{0,127}$")
logger = logging.getLogger(__name__)


class LLMExecutionError(Exception):
    """Structured error for LLM execution failures."""

    def __init__(
        self,
        message: str,
        *,
        llm_execution_mode: str,
        resource_name: Optional[str] = None,
        error_code: str = "llm_execution_error",
    ):
        super().__init__(message)
        self.llm_execution_mode = llm_execution_mode
        self.resource_name = resource_name
        self.error_code = error_code

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": False,
            "error_code": self.error_code,
            "message": str(self),
            "llm_execution_mode": self.llm_execution_mode,
            "resource_name": self.resource_name,
        }


def escape_sql_string(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "''")


class LLMExecutor:
    """
    Execute LLM prompts via a unified mode:
    - direct_api: OpenAI-compatible HTTP endpoint
    - doris_resource: Doris AI_GENERATE(resource_name, prompt)
    """

    def __init__(
        self,
        *,
        doris_client: DorisClient,
        api_config: Optional[Dict[str, Any]] = None,
    ):
        self.doris_client = doris_client
        self.api_config = dict(api_config or {})

    def call(
        self,
        *,
        prompt: str,
        system_prompt: str = "",
        temperature: float = 0.1,
        max_tokens: int = 2000,
    ) -> str:
        mode = str(self.api_config.get("llm_execution_mode") or "direct_api")
        if mode == "doris_resource":
            return self._call_doris_resource(prompt=prompt, system_prompt=system_prompt)
        if mode == "direct_api":
            return self._call_direct_api(
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        raise LLMExecutionError(
            f"Unsupported llm_execution_mode: {mode}",
            llm_execution_mode=mode,
            resource_name=self._resource_name(),
            error_code="invalid_llm_execution_mode",
        )

    def _resource_name(self) -> Optional[str]:
        value = self.api_config.get("resource_name")
        return str(value) if value else None

    def _positive_int(self, value: Any, default: int = 0) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return int(default)
        return parsed if parsed > 0 else int(default)

    def _resource_timeout_seconds(self) -> int:
        configured = self.api_config.get("resource_timeout_seconds")
        if configured is None:
            configured = os.getenv("LLM_RESOURCE_TIMEOUT_SECONDS")
        return self._positive_int(configured, 0)

    def _resource_query_timeout_seconds(self, hard_timeout_seconds: int) -> int:
        configured = self.api_config.get("resource_query_timeout_seconds")
        if configured is None:
            configured = os.getenv("LLM_RESOURCE_QUERY_TIMEOUT_SECONDS")
        default_timeout = hard_timeout_seconds if hard_timeout_seconds > 0 else 0
        return self._positive_int(configured, default_timeout)

    @staticmethod
    def _is_timeout_error(exc: Exception) -> bool:
        if isinstance(exc, TimeoutError):
            return True
        lowered = str(exc).lower()
        return "timeout" in lowered or "timed out" in lowered

    @staticmethod
    def _open_doris_connection(
        config: Dict[str, Any],
        *,
        connect_timeout: int,
        read_timeout: int,
        write_timeout: int,
        include_database: bool,
    ):
        kwargs: Dict[str, Any] = {
            "host": config.get("host"),
            "port": int(config.get("port") or 9030),
            "user": config.get("user"),
            "password": config.get("password", ""),
            "charset": config.get("charset", "utf8mb4"),
            "autocommit": True,
            "connect_timeout": int(connect_timeout),
            "read_timeout": int(read_timeout),
            "write_timeout": int(write_timeout),
        }
        if include_database and config.get("database"):
            kwargs["database"] = config.get("database")
        return pymysql.connect(**kwargs)

    def _is_doris_connection_active(self, config: Dict[str, Any], connection_id: int) -> bool:
        inspector_conn = None
        inspector_cursor = None
        safe_conn_id = int(connection_id)
        try:
            inspector_conn = self._open_doris_connection(
                config,
                connect_timeout=3,
                read_timeout=3,
                write_timeout=3,
                include_database=False,
            )
            inspector_cursor = inspector_conn.cursor()
            inspector_cursor.execute("SHOW FULL PROCESSLIST")
            for row in inspector_cursor.fetchall() or []:
                if isinstance(row, dict):
                    raw_id = row.get("Id") or row.get("id")
                elif isinstance(row, (list, tuple)) and row:
                    raw_id = row[0]
                else:
                    raw_id = None
                try:
                    if int(raw_id) == safe_conn_id:
                        return True
                except Exception:
                    continue
            return False
        except Exception:
            # If we cannot inspect processlist, keep cancel path best-effort and proceed.
            return True
        finally:
            try:
                if inspector_cursor is not None:
                    inspector_cursor.close()
            finally:
                if inspector_conn is not None:
                    inspector_conn.close()

    def _cancel_doris_connection(self, config: Dict[str, Any], connection_id: int) -> None:
        safe_conn_id = int(connection_id)
        if safe_conn_id <= 0:
            return

        max_attempts = 3
        last_exc: Optional[Exception] = None
        for attempt in range(max_attempts):
            if not self._is_doris_connection_active(config, safe_conn_id):
                return

            killer_conn = None
            killer_cursor = None
            try:
                killer_conn = self._open_doris_connection(
                    config,
                    connect_timeout=3,
                    read_timeout=3,
                    write_timeout=3,
                    include_database=False,
                )
                killer_cursor = killer_conn.cursor()
                killer_cursor.execute(f"KILL {safe_conn_id}")
            except Exception as exc:
                last_exc = exc
            finally:
                try:
                    if killer_cursor is not None:
                        killer_cursor.close()
                finally:
                    if killer_conn is not None:
                        killer_conn.close()

            if attempt < max_attempts - 1:
                time.sleep(0.2)

        if self._is_doris_connection_active(config, safe_conn_id):
            logger.warning(
                "Failed to cancel Doris connection %s after timeout: %s",
                safe_conn_id,
                repr(last_exc) if last_exc is not None else "unknown_error",
            )

    def _execute_doris_resource_query_with_timeouts(
        self,
        sql: str,
        *,
        hard_timeout_seconds: int,
        query_timeout_seconds: int,
    ) -> list:
        config = dict(getattr(self.doris_client, "config", {}) or {})
        if not config:
            raise RuntimeError("Doris client config is unavailable for timeout-guarded resource execution")

        connect_timeout = max(1, min(hard_timeout_seconds, 15))
        read_timeout = max(1, hard_timeout_seconds)
        write_timeout = max(1, min(hard_timeout_seconds, 30))
        query_timeout = max(1, query_timeout_seconds or hard_timeout_seconds)

        connection = self._open_doris_connection(
            config,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
            include_database=True,
        )

        cursor = None
        connection_id = 0
        try:
            try:
                connection_id = int(connection.thread_id())
            except Exception:
                connection_id = 0
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"SET query_timeout = {int(query_timeout)}")
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as exc:
            if self._is_timeout_error(exc) and connection_id > 0:
                self._cancel_doris_connection(config, connection_id)
            raise
        finally:
            try:
                if cursor is not None:
                    cursor.close()
            finally:
                connection.close()

    def _call_doris_resource(self, *, prompt: str, system_prompt: str) -> str:
        resource_name = self._resource_name()
        if not resource_name:
            raise LLMExecutionError(
                "resource_name is required for doris_resource mode",
                llm_execution_mode="doris_resource",
                error_code="missing_resource_name",
            )
        if not _RESOURCE_NAME_RE.match(resource_name):
            raise LLMExecutionError(
                "Invalid resource_name format",
                llm_execution_mode="doris_resource",
                resource_name=resource_name,
                error_code="invalid_resource_name",
            )

        # Keep system intent while using a single Doris AI_GENERATE prompt.
        final_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        escaped_resource = escape_sql_string(resource_name)
        escaped_prompt = escape_sql_string(final_prompt)
        sql = f"SELECT AI_GENERATE('{escaped_resource}', '{escaped_prompt}') AS llm_response"
        hard_timeout_seconds = self._resource_timeout_seconds()
        query_timeout_seconds = self._resource_query_timeout_seconds(hard_timeout_seconds)
        try:
            if hard_timeout_seconds > 0:
                rows = self._execute_doris_resource_query_with_timeouts(
                    sql,
                    hard_timeout_seconds=hard_timeout_seconds,
                    query_timeout_seconds=query_timeout_seconds,
                )
            else:
                rows = self.doris_client.execute_query(sql)
        except Exception as exc:
            if self._is_timeout_error(exc):
                timeout_budget = hard_timeout_seconds or query_timeout_seconds or 0
                timeout_note = f" after {timeout_budget}s" if timeout_budget else ""
                raise LLMExecutionError(
                    f"Doris AI_GENERATE timeout{timeout_note}: {exc}",
                    llm_execution_mode="doris_resource",
                    resource_name=resource_name,
                    error_code="resource_timeout",
                ) from exc
            raise LLMExecutionError(
                f"Doris AI_GENERATE failed: {exc}",
                llm_execution_mode="doris_resource",
                resource_name=resource_name,
                error_code="doris_resource_failed",
            ) from exc

        if not rows:
            return ""

        first = rows[0]
        if isinstance(first, dict):
            value = first.get("llm_response")
            if value is None and first:
                value = next(iter(first.values()))
            return str(value or "")
        return str(first)

    def _call_direct_api(
        self,
        *,
        prompt: str,
        system_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> str:
        api_key = self.api_config.get("api_key")
        if not api_key:
            raise LLMExecutionError(
                "API key not provided for direct_api mode",
                llm_execution_mode="direct_api",
                resource_name=self._resource_name(),
                error_code="missing_api_key",
            )

        base_url = str(self.api_config.get("base_url") or "https://api.deepseek.com").rstrip("/")
        model = str(self.api_config.get("model") or "deepseek-chat")
        url = f"{base_url}/chat/completions"

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt or "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            response = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return str(content or "")
        except Exception as exc:
            raise LLMExecutionError(
                f"Direct API call failed: {exc}",
                llm_execution_mode="direct_api",
                resource_name=self._resource_name(),
                error_code="direct_api_failed",
            ) from exc
