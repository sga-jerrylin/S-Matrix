"""
ToolRegistry permission and LegacyVannaAdapter bridge POC.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

import pandas as pd
from pydantic import BaseModel, Field
from vanna.capabilities.agent_memory import AgentMemory
from vanna.core.registry import ToolRegistry
from vanna.core.tool import Tool, ToolCall, ToolContext, ToolResult
from vanna.core.user.models import User
from vanna.legacy.adapter import LegacyVannaAdapter


class EchoArgs(BaseModel):
    text: str = Field(description="text to echo")


class EchoTool(Tool[EchoArgs]):
    @property
    def name(self) -> str:
        return "echo"

    @property
    def description(self) -> str:
        return "Echo text and caller identity"

    def get_args_schema(self):
        return EchoArgs

    async def execute(self, context: ToolContext, args: EchoArgs) -> ToolResult:
        return ToolResult(
            success=True,
            result_for_llm=f"{context.user.id}:{args.text}",
            metadata={"groups": context.user.group_memberships},
        )


class FakeLegacyVanna:
    """Minimal legacy object consumed by LegacyVannaAdapter."""

    def run_sql(self, sql: str) -> pd.DataFrame:
        return pd.DataFrame([{"sql": sql, "ok": 1}])

    def add_question_sql(self, question: str, sql: str) -> None:
        return None

    def get_similar_question_sql(self, question: str) -> List[Dict[str, str]]:
        return [{"question": "historical question", "sql": "SELECT 1"}]

    def add_documentation(self, documentation: str) -> str:
        return "doc-1"

    def get_related_documentation(self, question: str) -> List[str]:
        return ["sample documentation"]

    def remove_training_data(self, id: str) -> bool:  # noqa: A002 - legacy API shape
        return True


async def run_tool_registry_check(agent_memory: AgentMemory) -> Dict[str, Any]:
    registry = ToolRegistry()
    registry.register_local_tool(EchoTool(), access_groups=["admin"])

    user_ctx = ToolContext(
        user=User(id="u-user", group_memberships=["user"]),
        conversation_id="c1",
        request_id="r1",
        agent_memory=agent_memory,
    )
    admin_ctx = ToolContext(
        user=User(id="u-admin", group_memberships=["admin"]),
        conversation_id="c2",
        request_id="r2",
        agent_memory=agent_memory,
    )
    call = ToolCall(id="tc-1", name="echo", arguments={"text": "hello"})

    denied = await registry.execute(call, user_ctx)
    allowed = await registry.execute(call, admin_ctx)
    return {
        "denied": {"success": denied.success, "error": denied.error},
        "allowed": {
            "success": allowed.success,
            "result_for_llm": allowed.result_for_llm,
            "has_execution_time": "execution_time_ms" in allowed.metadata,
        },
    }


async def run_legacy_adapter_check() -> Dict[str, Any]:
    adapter = LegacyVannaAdapter(FakeLegacyVanna())
    tools = await adapter.list_tools()
    ctx = ToolContext(
        user=User(id="u1", group_memberships=["user"]),
        conversation_id="lc1",
        request_id="lr1",
        agent_memory=adapter,
    )
    search_result = await adapter.execute(
        ToolCall(
            id="legacy-1",
            name="search_saved_correct_tool_uses",
            arguments={"question": "count total orders"},
        ),
        ctx,
    )
    return {
        "registered_tools": sorted(tools),
        "search_success": search_result.success,
        "search_error": search_result.error,
    }


async def main_async() -> Dict[str, Any]:
    adapter = LegacyVannaAdapter(FakeLegacyVanna())
    return {
        "registry_permission_check": await run_tool_registry_check(adapter),
        "legacy_adapter_check": await run_legacy_adapter_check(),
    }


def main() -> int:
    output = asyncio.run(main_async())
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
