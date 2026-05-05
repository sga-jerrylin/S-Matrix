"""
表格元数据分析器
使用 LLM 分析表格结构和用途
"""
import os
import json
import asyncio
import hashlib
import logging
import re
from urllib.parse import urlsplit, urlunsplit
from typing import Dict, Any, Optional
from datetime import datetime
from db import doris_client
from llm_executor import LLMExecutionError, LLMExecutor


logger = logging.getLogger(__name__)
_RESOURCE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]{0,127}$")
_METADATA_DEFAULT_RESOURCE = os.getenv("METADATA_DEFAULT_RESOURCE", "openrutor")
_METADATA_RESOURCE_TIMEOUT_SECONDS = int(os.getenv("METADATA_RESOURCE_TIMEOUT_SECONDS", "45"))
_METADATA_RESOURCE_QUERY_TIMEOUT_SECONDS = int(
    os.getenv("METADATA_RESOURCE_QUERY_TIMEOUT_SECONDS", "45")
)


class MetadataAnalyzer:
    """表格元数据分析器"""
    
    def __init__(self):
        self.db = doris_client
        # DeepSeek API 配置
        self.api_key = os.getenv('DEEPSEEK_API_KEY') or os.getenv('OPENAI_API_KEY')
        self.model = os.getenv('DEEPSEEK_MODEL', 'deepseek-chat')
        self.base_url = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')

    @staticmethod
    def _derive_base_url(endpoint: str) -> str:
        if not endpoint:
            return ""
        parts = urlsplit(endpoint)
        path = parts.path or ""
        suffix = "/chat/completions"
        if path.endswith(suffix):
            path = path[: -len(suffix)] or "/"
        return urlunsplit((parts.scheme, parts.netloc, path, "", ""))

    @staticmethod
    def _normalize_llm_resource(row_group: Dict[str, Any]) -> Dict[str, Any]:
        properties = dict(row_group.get("properties") or {})
        name = row_group.get("ResourceName") or row_group.get("Name") or ""
        provider = (
            properties.get("ai.provider_type")
            or properties.get("provider")
            or row_group.get("ResourceType")
            or ""
        )
        model = properties.get("ai.model_name") or properties.get("model_name") or ""
        endpoint = properties.get("ai.endpoint") or properties.get("endpoint") or ""
        api_key_value = properties.get("ai.api_key") or properties.get("api_key") or ""
        api_key_configured = bool(api_key_value and str(api_key_value).strip("* "))
        if not api_key_configured and api_key_value:
            api_key_configured = True
        return {
            **row_group,
            "name": name,
            "provider": provider,
            "model": model,
            "endpoint": endpoint,
            "base_url": MetadataAnalyzer._derive_base_url(endpoint),
            "api_key_configured": api_key_configured,
            "properties": properties,
        }

    def _load_llm_resources(self) -> list:
        sql = 'SHOW RESOURCES WHERE NAME LIKE "%"'
        rows = self.db.execute_query(sql) or []
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            name = row.get("Name")
            resource_type = row.get("ResourceType")
            if resource_type != "ai" or not name:
                continue
            if name not in grouped:
                grouped[name] = {"ResourceName": name, "ResourceType": resource_type, "properties": {}}
            item = row.get("Item")
            value = row.get("Value")
            if item and value is not None:
                grouped[name]["properties"][item] = value
        return [self._normalize_llm_resource(item) for item in grouped.values()]

    def _resolve_resource_for_metadata(self, resource_name: Optional[str]) -> Dict[str, Any]:
        requested = (resource_name or "").strip()
        if requested and not _RESOURCE_NAME_RE.match(requested):
            return {
                "success": False,
                "error_code": "invalid_resource_name",
                "message": "resource_name format is invalid",
                "status_code": 400,
                "resource_name": requested,
            }

        resources = self._load_llm_resources()
        selected = None
        if requested:
            selected = next((item for item in resources if item.get("name") == requested), None)
            if not selected:
                return {
                    "success": False,
                    "error_code": "resource_not_found",
                    "message": f"LLM resource '{requested}' not found",
                    "status_code": 400,
                    "resource_name": requested,
                }
        else:
            preferred = _METADATA_DEFAULT_RESOURCE.strip()
            if preferred:
                selected = next(
                    (
                        item
                        for item in resources
                        if str(item.get("name") or "").strip().lower() == preferred.lower()
                    ),
                    None,
                )
            if selected is None and resources:
                selected = resources[0]

        return {"success": True, "selected": selected, "resources": resources}

    def _build_runtime_api_config(self, resource_name: Optional[str]) -> Dict[str, Any]:
        resource_resolution = self._resolve_resource_for_metadata(resource_name)
        if not resource_resolution.get("success"):
            return resource_resolution

        selected = resource_resolution.get("selected") or {}
        env_api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
        resource_api_key_configured = bool(selected.get("api_key_configured"))
        has_resource = bool(selected)

        llm_execution_mode = "direct_api"
        if has_resource and resource_api_key_configured:
            llm_execution_mode = "doris_resource"

        runtime = {
            "api_key": env_api_key,
            "model": selected.get("model") or os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            "base_url": selected.get("base_url") or os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            "resource_name": selected.get("name") if has_resource else None,
            "endpoint": selected.get("endpoint") if has_resource else None,
            "provider": selected.get("provider") if has_resource else None,
            "resource_found": has_resource,
            "resource_api_key_configured": resource_api_key_configured,
            "llm_execution_mode": llm_execution_mode,
            "resource_timeout_seconds": max(1, int(_METADATA_RESOURCE_TIMEOUT_SECONDS)),
            "resource_query_timeout_seconds": max(1, int(_METADATA_RESOURCE_QUERY_TIMEOUT_SECONDS)),
        }

        if llm_execution_mode == "direct_api" and not env_api_key:
            return {
                "success": False,
                "error_code": "missing_llm_configuration",
                "message": (
                    "No usable LLM resource or direct API key is configured for metadata analysis."
                ),
                "status_code": 500,
                "resource_name": runtime.get("resource_name"),
                "llm_execution_mode": llm_execution_mode,
            }

        return {"success": True, "api_config": runtime}

    @staticmethod
    def _structured_error(
        *,
        code: str,
        message: str,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "success": False,
            "status_code": status_code,
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
        }

    @staticmethod
    def _is_timeout_llm_error(llm_error: LLMExecutionError) -> bool:
        error_code = str(getattr(llm_error, "error_code", "") or "").strip().lower()
        if error_code in {"resource_timeout", "timeout"}:
            return True
        return "timeout" in str(llm_error).lower()

    def _parse_llm_json(self, content: str) -> Dict[str, Any]:
        candidate = str(content or "")
        if '```json' in candidate:
            candidate = candidate.split('```json', 1)[1].split('```', 1)[0]
        elif '```' in candidate:
            candidate = candidate.split('```', 1)[1].split('```', 1)[0]
        try:
            parsed = json.loads(candidate.strip())
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        return {
            "description": str(content or ""),
            "columns": {},
            "suggested_queries": [],
            "raw_response": str(content or ""),
        }

    def _call_llm_with_runtime(self, prompt: str, api_config: Dict[str, Any]) -> Dict[str, Any]:
        executor = LLMExecutor(doris_client=self.db, api_config=api_config)
        content = executor.call(
            prompt=prompt,
            system_prompt="你是一个数据分析专家，擅长分析数据表结构和用途。请用中文回答。",
            temperature=0.3,
            max_tokens=2000,
        )
        return self._parse_llm_json(content)

    @staticmethod
    def _fallback_display_name(table_name: str) -> str:
        explicit = {
            "orders": "订单主表",
            "order_items": "订单商品明细表",
            "order_payment_trade": "订单支付流水表",
            "member": "会员主表",
            "inventory_realtime": "实时库存表",
        }
        return explicit.get(table_name, table_name)

    def _build_fallback_analysis(
        self,
        table_name: str,
        columns: Optional[list] = None,
        sample_data: Optional[list] = None,
    ) -> Dict[str, Any]:
        columns = list(columns or [])
        sample_row = (sample_data or [{}])[0] or {}
        column_descriptions: Dict[str, str] = {}
        for column_name in columns:
            lower_name = str(column_name or "").lower()
            sample_value = sample_row.get(column_name)
            if isinstance(sample_value, bool):
                type_hint = "布尔"
            elif isinstance(sample_value, int):
                type_hint = "整数"
            elif isinstance(sample_value, float):
                type_hint = "数值"
            elif sample_value is None:
                type_hint = "未知类型"
            else:
                type_hint = "文本"

            if any(marker in lower_name for marker in ["time", "date", "created", "updated"]):
                semantic_hint = "时间字段"
            elif any(marker in lower_name for marker in ["amount", "paid", "price", "fee", "total", "balance"]):
                semantic_hint = "金额字段"
            elif lower_name.endswith("_id") or any(marker in lower_name for marker in ["member", "user", "tenant"]):
                semantic_hint = "标识字段"
            else:
                semantic_hint = "业务字段"

            column_descriptions[column_name] = f"{semantic_hint}，{type_hint}"

        return {
            "display_name": self._fallback_display_name(table_name),
            "description": f"{table_name} 的业务数据明细表（fallback semantic）",
            "columns": column_descriptions,
            "suggested_queries": [
                f"{table_name} 按时间趋势统计",
                f"{table_name} 核心金额指标分析",
                f"{table_name} 关键维度分组统计",
            ],
            "data_domain": "业务数据",
            "key_dimensions": ["时间", "门店/租户", "金额/数量"],
        }

    def _ensure_analysis_column_coverage(
        self,
        table_name: str,
        analysis: Dict[str, Any],
        columns: Optional[list] = None,
        sample_data: Optional[list] = None,
    ) -> Dict[str, Any]:
        merged = dict(analysis or {})
        fallback = self._build_fallback_analysis(table_name, columns, sample_data)
        existing_columns = merged.get("columns")
        if not isinstance(existing_columns, dict):
            existing_columns = {}

        fallback_columns = fallback.get("columns") or {}
        for column_name, description in fallback_columns.items():
            if not str(existing_columns.get(column_name) or "").strip():
                existing_columns[column_name] = description

        merged["columns"] = existing_columns
        if not str(merged.get("display_name") or "").strip():
            merged["display_name"] = fallback.get("display_name")
        if not str(merged.get("description") or "").strip():
            merged["description"] = fallback.get("description")
        if not isinstance(merged.get("suggested_queries"), list) or not merged.get("suggested_queries"):
            merged["suggested_queries"] = fallback.get("suggested_queries") or []
        if not str(merged.get("data_domain") or "").strip():
            merged["data_domain"] = fallback.get("data_domain")
        if not isinstance(merged.get("key_dimensions"), list) or not merged.get("key_dimensions"):
            merged["key_dimensions"] = fallback.get("key_dimensions") or []
        return merged

    def _json_dumps_safe(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, default=str)

    def _mark_table_analysis_status(
        self,
        table_name: str,
        status: str,
        *,
        analyzed_at: Optional[str] = None,
    ) -> None:
        safe_table_name = (table_name or "").strip()
        if not safe_table_name:
            return

        try:
            exists = self.db.execute_query(
                "SELECT table_name FROM `_sys_table_sources` WHERE table_name = %s LIMIT 1",
                (safe_table_name,),
            )
        except Exception:
            return

        if not exists:
            return

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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

        try:
            self.db.execute_update(sql, params)
        except Exception:
            pass
    
    async def analyze_table_async(
        self,
        table_name: str,
        source_type: str = 'excel',
        *,
        resource_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """异步分析表格元数据"""
        return await asyncio.to_thread(
            self.analyze_table,
            table_name,
            source_type,
            resource_name=resource_name,
        )

    def analyze_table(
        self,
        table_name: str,
        source_type: str = 'excel',
        *,
        resource_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        分析表格元数据
        
        Args:
            table_name: 表名
            source_type: 来源类型 (excel/database_sync)
        
        Returns:
            分析结果
        """
        runtime_resolution = self._build_runtime_api_config(resource_name)
        if not runtime_resolution.get("success"):
            self._mark_table_analysis_status(table_name, "failed")
            return self._structured_error(
                code=str(runtime_resolution.get("error_code") or "llm_config_error"),
                message=str(runtime_resolution.get("message") or "Failed to resolve LLM runtime configuration"),
                status_code=int(runtime_resolution.get("status_code") or 500),
                details={
                    "resource_name": runtime_resolution.get("resource_name"),
                    "llm_execution_mode": runtime_resolution.get("llm_execution_mode"),
                },
            )
        runtime_api_config = runtime_resolution.get("api_config") or {}
        columns: list = []
        sample_data: list = []
        
        try:
            # 校验表名
            safe_table_name = self.db.validate_identifier(table_name)
            
            # 1. 获取表结构
            schema = self.db.get_table_schema(table_name)
            columns = [col['Field'] for col in schema]
            
            # 2. 获取样本数据 (前10行)
            # 使用 safe_table_name，注意它已经包含了反引号，但我们的 validate_identifier 返回的是 `table`
            # 这里的 sample_sql 是 f"SELECT * FROM `{table_name}` ..."
            # 如果 validate_identifier 返回 "`table`"，那么 SQL 变成 "SELECT * FROM `table`" 是对的
            # 但如果 validate_identifier 返回的是不带反引号的 safe string？
            # 看 db.py: return f"`{identifier}`"
            # 所以这里不需要再加反引号
            sample_sql = f"SELECT * FROM {safe_table_name} LIMIT 10"
            sample_data = self.db.execute_query(sample_sql)
            
            # 3. 构造 prompt
            prompt = self._build_compact_analysis_prompt(table_name, columns, sample_data)
            
            # 4. 调用 LLM
            analysis = self._call_llm_with_runtime(prompt, runtime_api_config)
            analysis = self._ensure_analysis_column_coverage(table_name, analysis, columns, sample_data)
             
            # 5. 保存元数据
            self._save_metadata(table_name, analysis, source_type)
            self._update_registry_semantics(table_name, analysis)
            self.refresh_agent_assets(
                table_name,
                source_type,
                runtime_api_config=runtime_api_config,
            )
             
            return {
                'success': True,
                'table_name': table_name,
                'analysis': analysis,
                'llm_execution_mode': runtime_api_config.get("llm_execution_mode"),
                'resource_name': runtime_api_config.get("resource_name"),
            }
             
        except LLMExecutionError as llm_error:
            if (
                self._is_timeout_llm_error(llm_error)
                and runtime_api_config.get("llm_execution_mode") == "doris_resource"
            ):
                fallback_analysis = self._build_fallback_analysis(table_name, columns, sample_data)
                fallback_analysis = self._ensure_analysis_column_coverage(
                    table_name,
                    fallback_analysis,
                    columns,
                    sample_data,
                )
                fallback_analysis["fallback_reason"] = "resource_timeout"
                fallback_analysis["fallback_error"] = str(llm_error)
                try:
                    self._save_metadata(table_name, fallback_analysis, source_type)
                    self._update_registry_semantics(table_name, fallback_analysis)
                    self.refresh_agent_assets(
                        table_name,
                        source_type,
                        runtime_api_config=runtime_api_config,
                    )
                    return {
                        "success": True,
                        "table_name": table_name,
                        "analysis": fallback_analysis,
                        "llm_execution_mode": runtime_api_config.get("llm_execution_mode"),
                        "resource_name": runtime_api_config.get("resource_name"),
                        "fallback_used": True,
                        "fallback_reason": "resource_timeout",
                    }
                except Exception:
                    pass

            self._mark_table_analysis_status(table_name, "failed")
            return self._structured_error(
                code=str(llm_error.error_code or "llm_execution_failed"),
                message=str(llm_error),
                status_code=502,
                details={
                    "resource_name": llm_error.resource_name,
                    "llm_execution_mode": llm_error.llm_execution_mode,
                },
            )
        except Exception as e:
            import traceback
            self._mark_table_analysis_status(table_name, "failed")
            return self._structured_error(
                code="metadata_analysis_failed",
                message=str(e),
                status_code=500,
                details={"traceback": traceback.format_exc()},
            )
    
    def _build_analysis_prompt(self, table_name: str, columns: list, 
                               sample_data: list) -> str:
        """构造分析 prompt"""
        # 格式化样本数据
        sample_str = ""
        for i, row in enumerate(sample_data[:5], 1):  # 只取前5行避免太长
            row_str = ", ".join([f"{k}: {v}" for k, v in row.items()])
            sample_str += f"  行{i}: {row_str}\n"
        
        prompt = f"""请分析以下数据表，提供结构化的元数据信息。

表名: {table_name}
列名: {', '.join(columns)}

样本数据:
{sample_str}

请以 JSON 格式返回以下信息：
{{
    "display_name": "适合业务人员阅读的中文表名，不超过20字",
    "description": "一句话描述这张表的用途和内容",
    "columns": {{
        "列名1": "该列的含义和数据类型说明",
        "列名2": "该列的含义和数据类型说明"
    }},
    "suggested_queries": [
        "可以基于这张表回答的问题示例1",
        "可以基于这张表回答的问题示例2",
        "可以基于这张表回答的问题示例3"
    ],
    "data_domain": "数据领域（如：销售、用户、财务、环保等）",
    "key_dimensions": ["主要分析维度1", "主要分析维度2"]
}}

只返回 JSON，不要其他解释。"""
        
        return prompt
    
    def _build_compact_analysis_prompt(self, table_name: str, columns: list,
                                       sample_data: list) -> str:
        """Build a compact prompt to avoid oversized AI_GENERATE payloads."""
        column_limit = 48
        sample_row_limit = 3
        sample_column_limit = 18
        sample_value_limit = 120
        max_prompt_chars = 12000

        def _short_value(value: Any) -> str:
            text = str(value if value is not None else "null")
            if len(text) > sample_value_limit:
                return text[:sample_value_limit] + "...(truncated)"
            return text

        safe_columns = list(columns or [])
        preview_columns = safe_columns[:column_limit]
        column_hint = ", ".join(preview_columns)
        if len(safe_columns) > column_limit:
            column_hint = f"{column_hint}, ... (+{len(safe_columns) - column_limit} more columns)"

        sample_lines = []
        for idx, row in enumerate((sample_data or [])[:sample_row_limit], 1):
            row = row or {}
            row_columns = [name for name in preview_columns[:sample_column_limit] if name in row]
            if not row_columns:
                row_columns = list(row.keys())[:sample_column_limit]
            fields = [f"{name}: {_short_value(row.get(name))}" for name in row_columns]
            omitted = max(0, len(row.keys()) - len(row_columns))
            if omitted:
                fields.append(f"... (+{omitted} more fields)")
            sample_lines.append(f"  row{idx}: " + ", ".join(fields))

        sample_block = "\n".join(sample_lines) if sample_lines else "  (no sample rows)"
        prompt = f"""请分析以下数据表并返回结构化元数据（仅输出 JSON，不要额外解释）。

table_name: {table_name}
columns_preview: {column_hint}
columns_total: {len(safe_columns)}

sample_rows:
{sample_block}

输出 JSON:
{{
  "display_name": "适合业务阅读的中文表名，不超过20字",
  "description": "一句话描述该表业务用途",
  "columns": {{
    "列名1": "列含义和数据类型说明",
    "列名2": "列含义和数据类型说明"
  }},
  "suggested_queries": [
    "可基于该表回答的问题示例1",
    "可基于该表回答的问题示例2",
    "可基于该表回答的问题示例3"
  ],
  "data_domain": "数据领域",
  "key_dimensions": ["主要分析维度1", "主要分析维度2"]
}}
"""
        if len(prompt) > max_prompt_chars:
            prompt = prompt[:max_prompt_chars].rstrip()
            prompt += "\n\n(提示: 输入已截断，请基于可见字段稳健推断。)"
        return prompt

    def _call_llm(self, prompt: str) -> Dict[str, Any]:
        """调用 LLM API"""
        import requests

        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "你是一个数据分析专家，擅长分析数据表结构和用途。请用中文回答。"},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.3,
                "max_tokens": 2000,
            },
            timeout=60,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return self._parse_llm_json(content)
    
    def _save_metadata(self, table_name: str, analysis: Dict[str, Any], 
                       source_type: str):
        """保存元数据到系统表"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 先删除旧记录
        delete_sql = "DELETE FROM `_sys_table_metadata` WHERE table_name = %s"
        self.db.execute_update(delete_sql, (table_name,))
        
        # 插入新记录
        sql = """
        INSERT INTO `_sys_table_metadata` 
        (`table_name`, `description`, `columns_info`, `sample_queries`, `analyzed_at`, `source_type`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        self.db.execute_update(sql, (
            table_name,
            analysis.get('description', ''),
            json.dumps(analysis.get('columns', {}), ensure_ascii=False),
            json.dumps(analysis.get('suggested_queries', []), ensure_ascii=False),
            now,
            source_type
        ))
        self._mark_table_analysis_status(table_name, "ready", analyzed_at=now)

    def _update_registry_semantics(self, table_name: str, analysis: Dict[str, Any]) -> None:
        display_name = str(analysis.get("display_name") or "").strip() or self._fallback_display_name(table_name)
        description = str(analysis.get("description") or "").strip()
        if not display_name and not description:
            return
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        UPDATE `_sys_table_registry`
        SET display_name = COALESCE(NULLIF(%s, ''), display_name),
            description = COALESCE(NULLIF(%s, ''), description),
            updated_at = %s
        WHERE table_name = %s
        """
        self.db.execute_update(sql, (display_name, description, now, table_name))

    def _save_agent_config(self, table_name: str, agent_config: Dict[str, Any], source_hash: str):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        delete_sql = "DELETE FROM `_sys_table_agents` WHERE table_name = %s"
        self.db.execute_update(delete_sql, (table_name,))
        insert_sql = """
        INSERT INTO `_sys_table_agents`
        (`table_name`, `agent_config`, `source_hash`, `created_at`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.db.execute_update(
            insert_sql,
            (table_name, json.dumps(agent_config, ensure_ascii=False), source_hash, now, now),
        )

    def _save_field_catalog(self, table_name: str, agent_config: Dict[str, Any]):
        self.db.execute_update("DELETE FROM `_sys_field_catalog` WHERE table_name = %s", (table_name,))
        fields = agent_config.get("fields", {})
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_sql = """
        INSERT INTO `_sys_field_catalog`
        (`table_name`, `field_name`, `field_type`, `enum_values`, `value_range`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for field_name, field_cfg in fields.items():
            enum_values = field_cfg.get("values", [])
            value_range = field_cfg.get("range")
            self.db.execute_update(
                insert_sql,
                (
                    table_name,
                    field_name,
                    field_cfg.get("semantic", ""),
                    json.dumps(enum_values, ensure_ascii=False),
                    json.dumps(value_range, ensure_ascii=False) if value_range is not None else None,
                    now,
                ),
            )

    def _build_agent_prompt(self, table_name: str, metadata: Dict[str, Any], sample_data: list) -> str:
        return f"""请将以下原始表元数据转换为结构化表管理员配置 JSON。

表名: {table_name}
表描述: {metadata.get('description', '')}
字段说明: {self._json_dumps_safe(metadata.get('columns_info', {}))}
样本数据: {self._json_dumps_safe(sample_data[:5])}

返回 JSON:
{{
  "table_description": "表描述",
  "fields": {{
    "字段名": {{
      "semantic": "geographic-city|categorical|temporal-year|financial-income|text|id",
      "match": "fuzzy|exact|range|like",
      "values": ["可选枚举值"],
      "range": [0, 100]
    }}
  }},
  "cot_template": "处理此表查询的推理步骤"
}}

只返回 JSON。"""

    def _fallback_agent_config(self, metadata: Dict[str, Any], sample_data: list) -> Dict[str, Any]:
        fields = {}
        columns_info = metadata.get("columns_info", {}) or {}
        sample_row = sample_data[0] if sample_data else {}

        for field_name in columns_info.keys() or sample_row.keys():
            sample_value = sample_row.get(field_name)
            field_cfg = {"semantic": "text", "match": "like"}
            lower_name = str(field_name or "").strip().lower()
            if any(keyword in field_name for keyword in ["市", "省", "区", "县", "城市"]):
                field_cfg = {"semantic": "geographic-city", "match": "fuzzy"}
            elif any(keyword in field_name for keyword in ["年", "日期", "时间"]) or any(
                marker in lower_name for marker in ["date", "time", "created", "updated"]
            ):
                field_cfg = {"semantic": "temporal-year", "match": "range"}
            elif any(marker in lower_name for marker in ["member", "user", "tenant"]) or lower_name.endswith("_id"):
                field_cfg = {"semantic": "id", "match": "exact"}
            elif any(marker in lower_name for marker in ["amount", "paid", "price", "balance", "fee", "total", "num"]):
                field_cfg = {"semantic": "financial-income", "match": "range"}
            elif isinstance(sample_value, (int, float)):
                field_cfg = {"semantic": "financial-income", "match": "range"}
            if isinstance(sample_value, str):
                field_cfg["values"] = [sample_value]
            fields[field_name] = field_cfg

        return {
            "table_description": metadata.get("description", ""),
            "fields": fields,
            "cot_template": "识别查询维度，按字段匹配策略生成 WHERE 和聚合条件。",
        }

    def refresh_agent_assets(
        self,
        table_name: str,
        source_type: str = "excel",
        *,
        runtime_api_config: Optional[Dict[str, Any]] = None,
        resource_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        metadata = self.get_metadata(table_name)
        if not metadata:
            return self._structured_error(
                code="metadata_not_found",
                message="metadata not found",
                status_code=404,
            )

        safe_table_name = self.db.validate_identifier(table_name)
        sample_data = self.db.execute_query(f"SELECT * FROM {safe_table_name} LIMIT 10")
        source_hash = hashlib.md5(
            json.dumps(metadata, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

        prompt = self._build_agent_prompt(table_name, metadata, sample_data)
        try:
            if runtime_api_config and runtime_api_config.get("llm_execution_mode") == "doris_resource":
                # Keep metadata analysis on LLM resource, but avoid a second expensive
                # AI_GENERATE round for agent assets on wide/large tables.
                agent_config = self._fallback_agent_config(metadata, sample_data)
            elif runtime_api_config:
                agent_config = self._call_llm_with_runtime(prompt, runtime_api_config)
            elif resource_name:
                runtime_resolution = self._build_runtime_api_config(resource_name)
                if runtime_resolution.get("success"):
                    agent_config = self._call_llm_with_runtime(
                        prompt,
                        runtime_resolution.get("api_config") or {},
                    )
                else:
                    raise ValueError(
                        str(runtime_resolution.get("message") or "Failed to resolve runtime config for agent assets")
                    )
            else:
                agent_config = self._call_llm(prompt)
        except Exception:
            agent_config = self._fallback_agent_config(metadata, sample_data)

        self._save_agent_config(table_name, agent_config, source_hash)
        self._save_field_catalog(table_name, agent_config)
        return {"success": True, "table_name": table_name, "agent_config": agent_config, "source_type": source_type}

    def get_agent_config(self, table_name: str) -> Optional[Dict[str, Any]]:
        sql = "SELECT * FROM `_sys_table_agents` WHERE table_name = %s"
        rows = self.db.execute_query(sql, (table_name,))
        if not rows:
            return None
        row = rows[0]
        try:
            row["agent_config"] = json.loads(row.get("agent_config") or "{}")
        except Exception:
            row["agent_config"] = {}
        return row

    def refresh_all_field_catalogs(self) -> Dict[str, Any]:
        metadata_rows = self.list_all_metadata()
        refreshed = 0
        for row in metadata_rows:
            table_name = row.get("table_name")
            if not table_name:
                continue
            result = self.refresh_agent_assets(table_name, row.get("source_type") or "excel")
            if result.get("success"):
                refreshed += 1
        return {"success": True, "refreshed": refreshed}

    def get_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表格元数据"""
        sql = "SELECT * FROM `_sys_table_metadata` WHERE table_name = %s"
        results = self.db.execute_query(sql, (table_name,))

        if results:
            meta = results[0]
            # 解析 JSON 字段
            try:
                meta['columns_info'] = json.loads(meta.get('columns_info', '{}'))
            except:
                meta['columns_info'] = {}
            try:
                meta['sample_queries'] = json.loads(meta.get('sample_queries', '[]'))
            except:
                meta['sample_queries'] = []
            return meta
        return None

    def list_all_metadata(self) -> list:
        """获取所有表格元数据"""
        sql = "SELECT * FROM `_sys_table_metadata` ORDER BY analyzed_at DESC"
        results = self.db.execute_query(sql)

        for meta in results:
            try:
                meta['columns_info'] = json.loads(meta.get('columns_info', '{}'))
            except:
                meta['columns_info'] = {}
            try:
                meta['sample_queries'] = json.loads(meta.get('sample_queries', '[]'))
            except:
                meta['sample_queries'] = []

        return results

    def _list_table_registry(self) -> list:
        """List table registry entries for display name/description."""
        try:
            sql = "SELECT table_name, display_name, description FROM `_sys_table_registry`"
            return self.db.execute_query(sql)
        except Exception:
            return []

    def get_all_tables_context(self) -> str:
        """
        ??????????????????????????????????????????????????? AI ??????
        """
        registry_list = self._list_table_registry()
        if not registry_list:
            return ""

        metadata_map = {meta.get('table_name'): meta for meta in self.list_all_metadata()}
        context_parts = ["?????????????????????????????????????????????\n"]

        for reg in registry_list:
            table_name = reg.get('table_name')
            if not table_name:
                continue
            meta = metadata_map.get(table_name, {})
            display_name = reg.get('display_name') or table_name
            description = reg.get('description') or meta.get('description') or '?????????'

            context_parts.append(f"- ??????: {table_name}")
            if display_name != table_name:
                context_parts.append(f"  ?????????: {display_name}")
            context_parts.append(f"  ??????: {description}")
            if meta.get('columns_info'):
                cols = ", ".join(meta['columns_info'].keys())
                context_parts.append(f"  ????????????: {cols}")
            context_parts.append("")

        return "\n".join(context_parts)


# 全局实例
metadata_analyzer = MetadataAnalyzer()
