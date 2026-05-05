"""
Planner agent for multi-table natural language queries.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple


class PlannerAgent:
    def __init__(self, tables_context: Optional[List[Dict[str, Any]]] = None):
        self.tables_context = tables_context or []

    def plan(self, question: str) -> Dict[str, Any]:
        normalized_question = self._normalize_text((question or "").lower())
        question_tokens = self._tokens(normalized_question)
        table_names = [str((table or {}).get("table_name") or "") for table in self.tables_context]
        available_tables = {name for name in table_names if name}

        scored_tables: List[Dict[str, Any]] = []
        for index, table in enumerate(self.tables_context):
            table_name = str((table or {}).get("table_name") or "")
            if not table_name:
                continue

            haystack = self._build_haystack(table)
            score = 0
            matched_terms: List[str] = []
            if table_name.lower() in normalized_question:
                score += 6
                matched_terms.append(table_name)

            signal_score, signal_terms = self._business_signal_score(table_name, normalized_question)
            if signal_score > 0:
                score += signal_score
                matched_terms.extend(signal_terms)

            for token in question_tokens:
                if token and token in haystack:
                    score += 1
                    matched_terms.append(token)

            if score > 0:
                scored_tables.append(
                    {
                        "score": score,
                        "index": index,
                        "table_name": table_name,
                        "matched_terms": sorted(set(matched_terms)),
                    }
                )

        scored_tables.sort(key=lambda item: (-int(item["score"]), int(item["index"])))

        fallback_used = False
        routed_by_rule = False
        tables = self._rule_based_tables(normalized_question, available_tables)
        if tables:
            routed_by_rule = True
        else:
            tables = self._select_tables(scored_tables, normalized_question)
            if not tables:
                fallback_table = self._select_fallback_table(available_tables, table_names)
                tables = [fallback_table] if fallback_table else []
                fallback_used = bool(tables)

        intent = self._detect_intent(normalized_question)
        resolved_tables = [table for table in tables if table]
        selected_table_set = set(resolved_tables)
        subtasks = [{"table": table, "question": question} for table in resolved_tables]

        candidates = [
            {
                "table_name": item["table_name"],
                "score": int(item["score"]),
                "matched_terms": item.get("matched_terms", []),
                "selected": item["table_name"] in selected_table_set,
                "rank": rank,
            }
            for rank, item in enumerate(scored_tables, start=1)
        ]
        if fallback_used and resolved_tables and not candidates:
            candidates = [
                {
                    "table_name": resolved_tables[0],
                    "score": 0,
                    "matched_terms": [],
                    "selected": True,
                    "rank": 1,
                }
            ]

        return {
            "intent": intent,
            "tables": resolved_tables,
            "subtasks": subtasks,
            "needs_join": len(resolved_tables) > 1,
            "normalized_question": normalized_question,
            "candidates": candidates,
            "fallback_used": fallback_used,
            "routing_reason": self._build_routing_reason(
                intent=intent,
                tables=resolved_tables,
                fallback_used=fallback_used,
                routed_by_rule=routed_by_rule,
            ),
        }

    def _rule_based_tables(self, question: str, available_tables: Sequence[str]) -> List[str]:
        available = set(available_tables)

        def has_any(keywords: Sequence[str]) -> bool:
            return any(keyword in question for keyword in keywords)

        out_keywords = ("出库", "出倉", "出倉", "stock out", "出货", "发货")
        in_keywords = ("入库", "入倉", "stock in", "收货", "采购入库")
        inventory_keywords = ("库存", "在库", "存货", "库存量", "stock")
        payment_keywords = ("支付", "付款", "实付", "流水", "交易", "收款", "pay", "payment")
        refund_keywords = ("退款", "退单", "refund")
        member_keywords = ("会员", "用户", "member", "客户")
        item_keywords = ("商品", "明细", "sku", "销量", "item")
        order_keywords = ("订单", "order")
        shop_keywords = ("门店", "店铺", "shop")

        if has_any(out_keywords) and "warehouse_stock_out_items" in available:
            return ["warehouse_stock_out_items"]
        if has_any(in_keywords) and "warehouse_stock_in_items" in available:
            return ["warehouse_stock_in_items"]
        if has_any(inventory_keywords) and "inventory_realtime" in available:
            return ["inventory_realtime"]
        if (has_any(payment_keywords) or has_any(refund_keywords)) and "order_payment_trade" in available:
            return ["order_payment_trade"]
        if has_any(member_keywords) and "member" in available:
            return ["member"]
        if has_any(item_keywords) and "order_items" in available:
            return ["order_items"]
        if (has_any(order_keywords) or has_any(shop_keywords)) and "orders" in available:
            return ["orders"]
        return []

    def _business_signal_score(self, table_name: str, question: str) -> Tuple[int, List[str]]:
        rules = {
            "orders": (("订单", "下单", "门店", "店铺", "总金额", "订单量"), 8),
            "order_items": (("商品", "明细", "sku", "销量", "件数"), 10),
            "order_payment_trade": (("支付", "付款", "实付", "流水", "交易", "退款"), 12),
            "member": (("会员", "用户", "消费"), 10),
            "inventory_realtime": (("库存", "在库", "存货"), 12),
            "warehouse_stock_in_items": (("入库", "采购入库", "收货"), 14),
            "warehouse_stock_out_items": (("出库", "发货", "出货"), 14),
        }
        config = rules.get(table_name)
        if not config:
            return 0, []
        keywords, weight = config
        matched = [keyword for keyword in keywords if keyword in question]
        if not matched:
            return 0, []
        return weight + len(matched), matched

    def _select_tables(self, scored_tables: List[Dict[str, Any]], question: str) -> List[str]:
        if not scored_tables:
            return []
        if len(scored_tables) == 1:
            return [str(scored_tables[0]["table_name"])]
        if not self._has_multi_table_signal(question):
            return [str(scored_tables[0]["table_name"])]
        return [str(item["table_name"]) for item in scored_tables[:3]]

    def _select_fallback_table(self, available_tables: Sequence[str], all_tables_ordered: Sequence[str]) -> str:
        preferred = [
            "orders",
            "member",
            "order_payment_trade",
            "order_items",
            "inventory_realtime",
            "warehouse_stock_in_items",
            "warehouse_stock_out_items",
        ]
        available = set(available_tables)
        for table_name in preferred:
            if table_name in available:
                return table_name
        for table_name in all_tables_ordered:
            if table_name:
                return table_name
        return ""

    def _has_multi_table_signal(self, question: str) -> bool:
        multi_table_keywords = [
            "join",
            "关联",
            "关系",
            "连接",
            "匹配",
            "联表",
            "跨表",
            "对应",
            "参加",
            "分支",
            "网点",
        ]
        if any(keyword in question for keyword in multi_table_keywords):
            return True

        if "和" in question and any(keyword in question for keyword in ["活动", "分支", "网点", "机构"]):
            return True
        if "以及" in question and any(keyword in question for keyword in ["活动", "分支", "网点"]):
            return True
        return False

    def _build_haystack(self, table: Dict[str, Any]) -> str:
        parts = [
            table.get("table_name", ""),
            table.get("display_name", ""),
            table.get("description", ""),
            table.get("auto_description", ""),
            self._stringify(table.get("columns_info")),
            self._stringify(table.get("agent_config")),
        ]
        return self._normalize_text(" ".join(part for part in parts if part).lower())

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except Exception:
                return value
            return self._stringify(decoded)
        if isinstance(value, dict):
            return " ".join(
                filter(
                    None,
                    [self._stringify(key) + " " + self._stringify(item) for key, item in value.items()],
                )
            )
        if isinstance(value, list):
            return " ".join(self._stringify(item) for item in value)
        return str(value)

    def _detect_intent(self, question: str) -> str:
        if any(token in question for token in ["多少", "数量", "count", "总数"]):
            return "count"
        if any(token in question for token in ["占比", "比例", "平均", "sum", "总和", "金额", "趋势", "排行"]):
            return "aggregate"
        return "list"

    def _build_routing_reason(
        self,
        *,
        intent: str,
        tables: List[str],
        fallback_used: bool,
        routed_by_rule: bool,
    ) -> str:
        if fallback_used and tables:
            return f"no high-confidence table match found, fallback to `{tables[0]}` for intent `{intent}`"
        if not tables:
            return f"no table matched intent `{intent}`"
        if routed_by_rule:
            return f"selected table `{tables[0]}` using business routing rules for intent `{intent}`"
        if len(tables) == 1:
            return f"selected single table `{tables[0]}` for intent `{intent}`"
        return f"selected {len(tables)} tables for intent `{intent}` with join orchestration"

    def _normalize_text(self, text: str) -> str:
        return (
            text.replace("？", "")
            .replace("?", "")
            .replace("，", "")
            .replace(",", "")
            .replace("。", "")
            .replace(".", "")
            .replace("、", "")
            .replace(" ", "")
        )

    def _tokens(self, question: str) -> List[str]:
        if len(question) <= 4:
            return [question] if question else []

        tokens = {question[i : i + 2] for i in range(len(question) - 1)}
        tokens.update(question[i : i + 3] for i in range(len(question) - 2))
        return sorted(token for token in tokens if token)
