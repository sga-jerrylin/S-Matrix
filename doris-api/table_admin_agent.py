"""
Table admin agent that specializes SQL generation for one table/subtask.
"""

import json
import re
from typing import Dict, Any, Optional, List

from db import doris_client
from vanna_doris import LegacyVannaAdapter, VannaDorisOpenAI


class TableAdminAgent:
    def __init__(self, doris_client_override=None):
        self.doris_client = doris_client_override or doris_client

    def generate_sql_for_subtask(
        self,
        subtask: Dict[str, Any],
        question: str,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        return self.generate_sql_for_subtask_with_trace(subtask, question, api_config)["sql"]

    def generate_sql_for_subtask_with_trace(
        self,
        subtask: Dict[str, Any],
        question: str,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        api_config = api_config or {}
        vanna = VannaDorisOpenAI(
            doris_client=self.doris_client,
            api_key=api_config.get("api_key"),
            model=api_config.get("model"),
            base_url=api_config.get("base_url"),
            api_config=api_config,
            config={"temperature": 0.1},
        )
        task_question = subtask.get("question") or question
        target_table = (subtask.get("table") or "").strip()
        if target_table:
            sql, trace = self._generate_sql_for_target_table(vanna, target_table, task_question)
            return {"sql": sql, "trace": trace}

        sql = vanna.generate_sql(task_question)
        if self._sql_targets_only_table(vanna, sql, target_table):
            return {
                "sql": sql,
                "trace": {
                    "table_name": target_table or None,
                    "question": task_question,
                    "strategy": "generic_generate_sql",
                    "prompt_attempts": 0,
                    "metadata_available": False,
                    "schema_column_count": 0,
                    "example_count": 0,
                    "ddl_count": 0,
                    "documentation_count": 0,
                    "retrieval_source_labels": [],
                    "candidate_retrieval_source_labels": [],
                    "memory_hit": False,
                    "candidate_memory_hit": False,
                    "memory_fallback_used": False,
                    "memory_source": "",
                    "phases": ["sql_generation"],
                    "target_only": True,
                    "referenced_tables": vanna.extract_table_names(sql or ""),
                },
            }

        constrained_prompt = self._build_single_table_prompt(vanna, target_table, task_question)
        constrained_sql = self._clean_sql(vanna.submit_prompt(constrained_prompt))
        constrained_sql = vanna.auto_fuzzy_match_locations(constrained_sql)
        if not self._sql_targets_only_table(vanna, constrained_sql, target_table):
            raise ValueError(f"generated SQL references tables outside target table '{target_table}'")
        return {
            "sql": constrained_sql,
            "trace": {
                "table_name": target_table or None,
                "question": task_question,
                "strategy": "generic_then_constrained",
                "prompt_attempts": 1,
                "metadata_available": False,
                "schema_column_count": 0,
                "example_count": 0,
                "ddl_count": 0,
                "documentation_count": 0,
                "retrieval_source_labels": [],
                "candidate_retrieval_source_labels": [],
                "memory_hit": False,
                "candidate_memory_hit": False,
                "memory_fallback_used": False,
                "memory_source": "",
                "phases": ["sql_generation"],
                "target_only": True,
                "referenced_tables": vanna.extract_table_names(constrained_sql or ""),
            },
        }

    def _generate_sql_for_target_table(self, vanna, table_name: str, question: str) -> tuple[str, Dict[str, Any]]:
        template_sql = self._try_template_sql(table_name=table_name, question=question)
        if template_sql:
            return template_sql, {
                "table_name": table_name,
                "question": question,
                "strategy": "rule_template",
                "prompt_attempts": 0,
                "metadata_available": False,
                "schema_column_count": 0,
                "example_count": 0,
                "ddl_count": 0,
                "documentation_count": 0,
                "retrieval_source_labels": [],
                "candidate_retrieval_source_labels": [],
                "memory_hit": False,
                "candidate_memory_hit": False,
                "memory_fallback_used": False,
                "memory_source": "",
                "phases": ["sql_generation_template"],
                "target_only": True,
                "referenced_tables": [table_name],
            }

        prompt_context = self._build_single_table_context(vanna, table_name, question)
        prompt = self._build_single_table_prompt(vanna, table_name, question, prompt_context)
        sql = self._clean_sql(vanna.submit_prompt(prompt))
        sql = vanna.auto_fuzzy_match_locations(sql)
        if self._sql_targets_only_table(vanna, sql, table_name):
            return sql, self._build_generation_trace(
                table_name=table_name,
                question=question,
                strategy="single_table_prompt",
                prompt_attempts=1,
                prompt_context=prompt_context,
                referenced_tables=vanna.extract_table_names(sql or ""),
            )

        referenced_tables = ", ".join(vanna.extract_table_names(sql or "")) or "(none)"
        retry_prompt = (
            f"{prompt}\n\n"
            "Your previous answer referenced the wrong table.\n"
            f"Previous referenced tables: {referenced_tables}\n"
            f"Rewrite the SQL so it uses ONLY `{table_name}`.\n"
            "Return only executable SQL."
        )
        retry_sql = self._clean_sql(vanna.submit_prompt(retry_prompt))
        retry_sql = vanna.auto_fuzzy_match_locations(retry_sql)
        if self._sql_targets_only_table(vanna, retry_sql, table_name):
            return retry_sql, self._build_generation_trace(
                table_name=table_name,
                question=question,
                strategy="single_table_prompt_retry",
                prompt_attempts=2,
                prompt_context=prompt_context,
                referenced_tables=vanna.extract_table_names(retry_sql or ""),
            )
        raise ValueError(f"generated SQL references tables outside target table '{table_name}'")

    def _sql_targets_only_table(self, vanna, sql: str, table_name: str) -> bool:
        if not table_name:
            return True
        referenced_tables = vanna.extract_table_names(sql or "")
        return bool(referenced_tables) and set(referenced_tables) == {table_name}

    def _clean_sql(self, sql: str) -> str:
        cleaned = (sql or "").strip()
        if cleaned.startswith("```sql"):
            cleaned = cleaned[6:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()

    def _build_single_table_context(self, vanna, table_name: str, question: str) -> Dict[str, Any]:
        adapter = LegacyVannaAdapter(vanna)
        memory_result = adapter.memory_retrieval(question, limit=5)
        retrieval_result = adapter.ddl_doc_retrieval(question)
        relevant_examples = self._filter_examples_for_table(
            vanna=vanna,
            table_name=table_name,
            examples=list(memory_result.get("examples") or []),
            limit=2,
        )
        related_ddl = self._filter_ddl_for_table(table_name, retrieval_result.get("ddl") or [])
        related_docs = self._filter_docs_for_table(table_name, retrieval_result.get("documentation") or [])
        candidate_retrieval_source_labels = self._merge_retrieval_source_labels(
            memory_trace=memory_result.get("trace") or {},
            retrieval_trace=retrieval_result.get("trace") or {},
        )
        used_retrieval_source_labels = self._build_used_retrieval_source_labels(
            memory_trace=memory_result.get("trace") or {},
            retrieval_trace=retrieval_result.get("trace") or {},
            used_example_count=len(relevant_examples),
            used_ddl_count=len(related_ddl),
            used_documentation_count=len(related_docs),
        )
        return {
            "schema": self.doris_client.get_table_schema(table_name),
            "sample_rows": self.doris_client.execute_query(f"SELECT * FROM `{table_name}` LIMIT 2"),
            "metadata_lines": self._get_table_metadata_lines(table_name),
            "examples": self._format_examples(relevant_examples),
            "example_rows": relevant_examples,
            "related_ddl": related_ddl,
            "related_docs": related_docs,
            "memory_trace": memory_result.get("trace") or {},
            "retrieval_trace": retrieval_result.get("trace") or {},
            "candidate_retrieval_source_labels": candidate_retrieval_source_labels,
            "used_retrieval_source_labels": used_retrieval_source_labels,
        }

    def _build_single_table_prompt(
        self,
        vanna,
        table_name: str,
        question: str,
        prompt_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        prompt_context = prompt_context or self._build_single_table_context(vanna, table_name, question)
        schema = prompt_context.get("schema") or []
        sample_rows = prompt_context.get("sample_rows") or []
        metadata_lines = prompt_context.get("metadata_lines") or []
        examples = prompt_context.get("examples") or []
        related_ddl = prompt_context.get("related_ddl") or []
        related_docs = prompt_context.get("related_docs") or []
        column_lines = [
            f"- `{column.get('Field')}`: {column.get('Type')}"
            for column in schema[:24]
            if column.get("Field")
        ]

        prompt = [
            "You are generating Apache Doris SQL for a single-table subtask.",
            f"ONLY use table `{table_name}`.",
            "Do not invent, merge, or infer any other table names.",
            "",
            f"Question: {question}",
            "",
            "Table metadata:",
            "\n".join(metadata_lines) if metadata_lines else "- (not available)",
            "",
            "Available columns:",
            "\n".join(column_lines) if column_lines else "- (unknown)",
            "",
            "Sample rows:",
            self._truncate_text(self._format_sample_rows(sample_rows), 1000),
            "",
            "Relevant examples:",
            "\n".join(examples) if examples else "- (none)",
            "",
            "Related DDL:",
            self._truncate_text("\n\n".join(related_ddl[:1]) if related_ddl else "- (none)", 900),
            "",
            "Related documentation:",
            self._truncate_text("\n\n".join(related_docs[:1]) if related_docs else "- (none)", 900),
            "",
            "Rules:",
            "1. Return only executable Apache Doris SQL.",
            "2. Use backticks around Chinese column names.",
            "3. Prefer LIKE '%keyword%' for city/province text filters.",
            "4. Do not use markdown fences or explanations.",
            "",
            "Return only executable SQL. No markdown.",
        ]
        return "\n".join(prompt)

    def _build_generation_trace(
        self,
        *,
        table_name: str,
        question: str,
        strategy: str,
        prompt_attempts: int,
        prompt_context: Dict[str, Any],
        referenced_tables: List[str],
    ) -> Dict[str, Any]:
        schema = prompt_context.get("schema") or []
        metadata_lines = prompt_context.get("metadata_lines") or []
        examples = prompt_context.get("examples") or []
        related_ddl = prompt_context.get("related_ddl") or []
        related_docs = prompt_context.get("related_docs") or []
        memory_trace = prompt_context.get("memory_trace") or {}
        candidate_retrieval_source_labels = list(prompt_context.get("candidate_retrieval_source_labels") or [])
        used_retrieval_source_labels = list(prompt_context.get("used_retrieval_source_labels") or [])
        memory_hit = bool(examples)
        candidate_memory_hit = bool(memory_trace.get("memory_hit", False))
        selected_memory_source = str(memory_trace.get("selected_source") or "")
        return {
            "table_name": table_name,
            "question": question,
            "strategy": strategy,
            "prompt_attempts": prompt_attempts,
            "metadata_available": bool(metadata_lines),
            "schema_column_count": len(schema),
            "example_count": len(examples),
            "ddl_count": len(related_ddl),
            "documentation_count": len(related_docs),
            "retrieval_source_labels": used_retrieval_source_labels,
            "candidate_retrieval_source_labels": candidate_retrieval_source_labels,
            "memory_hit": memory_hit,
            "candidate_memory_hit": candidate_memory_hit,
            "memory_fallback_used": bool(memory_trace.get("fallback_used", False)),
            "memory_source": selected_memory_source if memory_hit else "",
            "phases": ["memory_retrieval", "ddl_doc_retrieval", "sql_generation"],
            "target_only": True,
            "referenced_tables": referenced_tables,
        }

    def _get_table_metadata_lines(self, table_name: str) -> List[str]:
        sql = """
        SELECT r.display_name, r.description, m.description AS auto_description, m.columns_info
        FROM `_sys_table_registry` r
        LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
        WHERE r.table_name = %s
        LIMIT 1
        """
        try:
            rows = self.doris_client.execute_query(sql, (table_name,))
        except Exception:
            return []

        if not rows:
            return []

        row = rows[0]
        lines: List[str] = []
        display_name = row.get("display_name")
        description = row.get("description") or row.get("auto_description")
        if display_name:
            lines.append(f"- Display name: {display_name}")
        if description:
            lines.append(f"- Description: {description}")

        columns_info = row.get("columns_info")
        if columns_info:
            try:
                decoded = json.loads(columns_info) if isinstance(columns_info, str) else columns_info
            except Exception:
                decoded = columns_info
            if isinstance(decoded, dict) and decoded:
                preview = ", ".join(list(decoded.keys())[:10])
                if preview:
                    lines.append(f"- Key columns: {preview}")
        return lines

    def _filter_examples_for_table(
        self,
        *,
        vanna,
        table_name: str,
        examples: List[Dict[str, Any]],
        limit: int = 2,
    ) -> List[Dict[str, Any]]:
        relevant_examples = []
        for example in examples:
            example_sql = (example or {}).get("sql", "")
            if self._sql_targets_only_table(vanna, example_sql, table_name):
                relevant_examples.append(
                    {
                        "question": (example or {}).get("question", ""),
                        "sql": example_sql,
                    }
                )
            if len(relevant_examples) >= limit:
                break
        return relevant_examples

    def _format_examples(self, examples: List[Dict[str, Any]]) -> List[str]:
        return [
            f"Question: {(example or {}).get('question', '')}\nSQL: {(example or {}).get('sql', '')}"
            for example in examples
        ]

    def _filter_ddl_for_table(self, table_name: str, ddl_list: List[str]) -> List[str]:
        table_token = f"`{table_name}`"
        targeted = [ddl for ddl in ddl_list if table_token in (ddl or "")]
        if targeted:
            return [self._truncate_text(targeted[0], 1600)]
        return list(ddl_list[:1])

    def _filter_docs_for_table(self, table_name: str, docs: List[str]) -> List[str]:
        target_prefix = f"Table: {table_name}"
        targeted = [doc for doc in docs if target_prefix in (doc or "")]
        if targeted:
            return targeted[:3]
        return list(docs[:1])

    def _merge_retrieval_source_labels(
        self,
        *,
        memory_trace: Dict[str, Any],
        retrieval_trace: Dict[str, Any],
    ) -> List[str]:
        memory_labels = list(memory_trace.get("source_labels") or [])
        retrieval_labels = list(retrieval_trace.get("source_labels") or [])
        return sorted(set(memory_labels + retrieval_labels))

    def _build_used_retrieval_source_labels(
        self,
        *,
        memory_trace: Dict[str, Any],
        retrieval_trace: Dict[str, Any],
        used_example_count: int,
        used_ddl_count: int,
        used_documentation_count: int,
    ) -> List[str]:
        used_labels: List[str] = []

        if used_example_count > 0:
            selected_source = str(memory_trace.get("selected_source") or "")
            if selected_source:
                used_labels.append(selected_source)
            else:
                used_labels.extend(list(memory_trace.get("source_labels") or []))

        if used_ddl_count > 0 or used_documentation_count > 0:
            used_labels.extend(list(retrieval_trace.get("source_labels") or []))

        return sorted(set(label for label in used_labels if label))

    def _normalize_question(self, question: str) -> str:
        return re.sub(r"\s+", "", (question or "").lower())

    def _extract_recent_days(self, normalized_question: str) -> Optional[int]:
        match = re.search(r"(最近|近)(\d{1,3})天", normalized_question)
        if match:
            return int(match.group(2))
        return None

    def _date_field_for_table(self, table_name: str) -> str:
        mapping = {
            "orders": "created_at",
            "order_items": "created_at",
            "order_payment_trade": "created_at",
            "member": "register_at",
            "warehouse_stock_in_items": "created_at",
            "warehouse_stock_out_items": "created_at",
        }
        return mapping.get(table_name, "")

    def _date_filter_sql(self, table_name: str, days: Optional[int]) -> str:
        if not days:
            return ""
        date_field = self._date_field_for_table(table_name)
        if not date_field:
            return ""
        return f" WHERE `{date_field}` >= DATE_SUB(CURDATE(), INTERVAL {int(days)} DAY)"

    def _try_template_sql(self, *, table_name: str, question: str) -> str:
        q = self._normalize_question(question)
        days = self._extract_recent_days(q)
        has = lambda *words: any(word in q for word in words)  # noqa: E731

        if table_name == "orders":
            if has("门店", "店铺", "排行"):
                return (
                    "SELECT `shop_id`, SUM(`paid_amount`) AS `paid_amount_total` "
                    "FROM `orders` GROUP BY `shop_id` ORDER BY `paid_amount_total` DESC LIMIT 10"
                )
            if has("每天", "每日", "趋势") and has("金额", "实付", "付款", "支付"):
                day_window = days or 30
                return (
                    "SELECT DATE(`created_at`) AS `day`, SUM(`paid_amount`) AS `paid_amount_total` "
                    "FROM `orders` "
                    f"WHERE `created_at` >= DATE_SUB(CURDATE(), INTERVAL {day_window} DAY) "
                    "GROUP BY DATE(`created_at`) ORDER BY `day`"
                )
            if has("金额", "实付", "付款", "支付", "总额", "总和"):
                return (
                    "SELECT SUM(`paid_amount`) AS `paid_amount_total` "
                    f"FROM `orders`{self._date_filter_sql('orders', days or (30 if has('最近30天', '近30天') else None))}"
                )
            if has("总数", "数量", "多少", "订单数", "订单总数"):
                return f"SELECT COUNT(*) AS `order_count` FROM `orders`{self._date_filter_sql('orders', days)}"

        if table_name == "order_items":
            if has("销量", "商品", "sku", "明细", "top", "最高", "排行"):
                where_clause = self._date_filter_sql("order_items", days)
                return (
                    "SELECT `item_id`, `item_name`, SUM(`num`) AS `total_num` "
                    f"FROM `order_items`{where_clause} "
                    "GROUP BY `item_id`, `item_name` ORDER BY `total_num` DESC LIMIT 10"
                )

        if table_name == "order_payment_trade":
            if has("退款", "退单", "refund"):
                if has("每天", "每日", "趋势"):
                    day_window = days or 30
                    return (
                        "SELECT DATE(`created_at`) AS `day`, SUM(`refund_amount`) AS `refund_amount_total` "
                        "FROM `order_payment_trade` "
                        f"WHERE `created_at` >= DATE_SUB(CURDATE(), INTERVAL {day_window} DAY) "
                        "GROUP BY DATE(`created_at`) ORDER BY `day`"
                    )
                return (
                    "SELECT SUM(`refund_amount`) AS `refund_amount_total` "
                    f"FROM `order_payment_trade`{self._date_filter_sql('order_payment_trade', days or 30)}"
                )
            if has("每天", "每日", "趋势") and has("金额", "实付", "付款", "支付", "流水"):
                day_window = days or 30
                return (
                    "SELECT DATE(`created_at`) AS `day`, SUM(`paid_amount`) AS `paid_amount_total` "
                    "FROM `order_payment_trade` "
                    f"WHERE `created_at` >= DATE_SUB(CURDATE(), INTERVAL {day_window} DAY) "
                    "GROUP BY DATE(`created_at`) ORDER BY `day`"
                )
            if has("金额", "实付", "付款", "支付", "流水", "总额", "总和"):
                return (
                    "SELECT SUM(`paid_amount`) AS `paid_amount_total` "
                    f"FROM `order_payment_trade`{self._date_filter_sql('order_payment_trade', days or 30)}"
                )

        if table_name == "member":
            if has("消费", "排行", "top", "最高"):
                return (
                    "SELECT `member_id`, `username`, `nickname`, `consume_amount` "
                    "FROM `member` ORDER BY `consume_amount` DESC LIMIT 10"
                )
            if has("新增", "新会员", "注册", "增长"):
                lookback = days or 30
                return (
                    "SELECT COUNT(*) AS `new_member_count` FROM `member` "
                    f"WHERE `register_at` >= DATE_SUB(CURDATE(), INTERVAL {lookback} DAY)"
                )
            if has("总数", "数量", "多少", "会员数"):
                return "SELECT COUNT(*) AS `member_count` FROM `member`"

        if table_name == "inventory_realtime":
            if has("库存", "在库", "最高", "top", "排行"):
                return (
                    "SELECT `product_id`, `product_name`, CAST(`stock` AS DECIMAL(18,2)) AS `stock_value` "
                    "FROM `inventory_realtime` ORDER BY `stock_value` DESC LIMIT 10"
                )

        if table_name == "warehouse_stock_in_items":
            if has("入库", "数量", "总量", "总数"):
                if has("每天", "每日", "趋势"):
                    day_window = days or 30
                    return (
                        "SELECT DATE(`created_at`) AS `day`, SUM(CAST(`num` AS DECIMAL(18,2))) AS `stock_in_num` "
                        "FROM `warehouse_stock_in_items` "
                        f"WHERE `created_at` >= DATE_SUB(CURDATE(), INTERVAL {day_window} DAY) "
                        "GROUP BY DATE(`created_at`) ORDER BY `day`"
                    )
                return (
                    "SELECT SUM(CAST(`num` AS DECIMAL(18,2))) AS `stock_in_num` "
                    f"FROM `warehouse_stock_in_items`{self._date_filter_sql('warehouse_stock_in_items', days or 30)}"
                )

        if table_name == "warehouse_stock_out_items":
            if has("出库", "数量", "总量", "总数"):
                if has("每天", "每日", "趋势"):
                    day_window = days or 30
                    return (
                        "SELECT DATE(`created_at`) AS `day`, SUM(CAST(`num` AS DECIMAL(18,2))) AS `stock_out_num` "
                        "FROM `warehouse_stock_out_items` "
                        f"WHERE `created_at` >= DATE_SUB(CURDATE(), INTERVAL {day_window} DAY) "
                        "GROUP BY DATE(`created_at`) ORDER BY `day`"
                    )
                return (
                    "SELECT SUM(CAST(`num` AS DECIMAL(18,2))) AS `stock_out_num` "
                    f"FROM `warehouse_stock_out_items`{self._date_filter_sql('warehouse_stock_out_items', days or 30)}"
                )

        return ""

    def _format_sample_rows(self, rows: List[Dict[str, Any]]) -> str:
        formatted_rows: List[str] = []
        for row in list(rows or [])[:2]:
            if isinstance(row, dict):
                compact_row = {key: row.get(key) for key in list(row.keys())[:12]}
                formatted_rows.append(json.dumps(compact_row, ensure_ascii=False))
            else:
                formatted_rows.append(str(row))
        return "\n".join(formatted_rows) if formatted_rows else "- (none)"

    def _truncate_text(self, text: str, max_len: int) -> str:
        value = str(text or "")
        if len(value) <= max_len:
            return value
        return value[:max_len] + " ...[truncated]"
