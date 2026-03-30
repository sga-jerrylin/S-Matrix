"""
Table admin agent that specializes SQL generation for one table/subtask.
"""

import os
import json
from typing import Dict, Any, Optional, List

from db import doris_client
from vanna_doris import VannaDorisOpenAI


class TableAdminAgent:
    def __init__(self, doris_client_override=None):
        self.doris_client = doris_client_override or doris_client

    def generate_sql_for_subtask(
        self,
        subtask: Dict[str, Any],
        question: str,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        api_config = api_config or {}
        vanna = VannaDorisOpenAI(
            doris_client=self.doris_client,
            api_key=api_config.get("api_key") or os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY"),
            model=api_config.get("model") or os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            base_url=api_config.get("base_url") or os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            config={"temperature": 0.1},
        )
        task_question = subtask.get("question") or question
        target_table = (subtask.get("table") or "").strip()
        if target_table:
            return self._generate_sql_for_target_table(vanna, target_table, task_question)

        sql = vanna.generate_sql(task_question)
        if self._sql_targets_only_table(vanna, sql, target_table):
            return sql

        constrained_prompt = self._build_single_table_prompt(vanna, target_table, task_question)
        constrained_sql = self._clean_sql(vanna.submit_prompt(constrained_prompt))
        constrained_sql = vanna.auto_fuzzy_match_locations(constrained_sql)
        if not self._sql_targets_only_table(vanna, constrained_sql, target_table):
            raise ValueError(f"generated SQL references tables outside target table '{target_table}'")
        return constrained_sql

    def _generate_sql_for_target_table(self, vanna, table_name: str, question: str) -> str:
        prompt = self._build_single_table_prompt(vanna, table_name, question)
        sql = self._clean_sql(vanna.submit_prompt(prompt))
        sql = vanna.auto_fuzzy_match_locations(sql)
        if self._sql_targets_only_table(vanna, sql, table_name):
            return sql

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
            return retry_sql
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

    def _build_single_table_prompt(self, vanna, table_name: str, question: str) -> str:
        schema = self.doris_client.get_table_schema(table_name)
        sample_rows = self.doris_client.execute_query(f"SELECT * FROM `{table_name}` LIMIT 3")
        metadata_lines = self._get_table_metadata_lines(table_name)
        examples = self._get_relevant_examples(vanna, table_name, question)
        column_lines = [
            f"- `{column.get('Field')}`: {column.get('Type')}"
            for column in schema
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
            str(sample_rows[:3]),
            "",
            "Relevant examples:",
            "\n".join(examples) if examples else "- (none)",
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

    def _get_relevant_examples(self, vanna, table_name: str, question: str) -> List[str]:
        if not hasattr(vanna, "get_similar_question_sql"):
            return []
        try:
            examples = vanna.get_similar_question_sql(question, limit=5)
        except Exception:
            return []

        relevant_examples = []
        for example in examples:
            example_sql = (example or {}).get("sql", "")
            if self._sql_targets_only_table(vanna, example_sql, table_name):
                relevant_examples.append(
                    f"Question: {(example or {}).get('question', '')}\nSQL: {example_sql}"
                )
            if len(relevant_examples) >= 2:
                break
        return relevant_examples
