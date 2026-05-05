"""
Minimal native Vanna Agent + FastAPI server self-check.

This script is intentionally isolated from DorisClaw `/api/query/natural`.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from fastapi.testclient import TestClient
from vanna import Agent, ToolRegistry
from vanna.core.user.models import User
from vanna.core.user.request_context import RequestContext
from vanna.core.user.resolver import UserResolver
from vanna.integrations.local.agent_memory.in_memory import DemoAgentMemory
from vanna.integrations.mock import MockLlmService
from vanna.servers.fastapi import VannaFastAPIServer


class HeaderUserResolver(UserResolver):
    """Resolve users from HTTP headers for spike verification."""

    async def resolve_user(self, request_context: RequestContext) -> User:
        user_id = request_context.get_header("x-user-id", "spike-user")
        raw_groups = request_context.get_header("x-user-groups", "user")
        groups = [part.strip() for part in raw_groups.split(",") if part.strip()]
        return User(
            id=user_id,
            username=user_id,
            metadata={"source": "experimental.vanna2_native_spike"},
            group_memberships=groups or ["user"],
        )


def build_agent() -> Agent:
    return Agent(
        llm_service=MockLlmService("native agent response"),
        tool_registry=ToolRegistry(),
        user_resolver=HeaderUserResolver(),
        agent_memory=DemoAgentMemory(),
    )


def run_poll_check(client: TestClient) -> Dict[str, Any]:
    resp = client.post(
        "/api/vanna/v2/chat_poll",
        json={"message": "hello native vanna"},
        headers={"x-user-id": "agent-b-spike", "x-user-groups": "user"},
    )
    data = resp.json()
    return {
        "status_code": resp.status_code,
        "top_level_keys": sorted(data.keys()) if isinstance(data, dict) else [],
        "total_chunks": data.get("total_chunks") if isinstance(data, dict) else None,
        "chunk_keys": sorted((data.get("chunks") or [{}])[0].keys())
        if isinstance(data, dict)
        else [],
    }


def run_sse_check(client: TestClient) -> Dict[str, Any]:
    with client.stream(
        "POST",
        "/api/vanna/v2/chat_sse",
        json={"message": "hello sse"},
        headers={"x-user-id": "agent-b-spike", "x-user-groups": "user"},
    ) as resp:
        lines = [line for line in resp.iter_lines() if line]
    done_seen = any(line.strip() == "data: [DONE]" for line in lines)
    first_payload = next((line for line in lines if line.startswith("data: {")), "")
    return {
        "status_code": resp.status_code,
        "line_count": len(lines),
        "done_seen": done_seen,
        "first_payload_prefix": first_payload[:120],
    }


def main() -> int:
    app = VannaFastAPIServer(build_agent()).create_app()
    client = TestClient(app)
    output = {
        "poll": run_poll_check(client),
        "sse": run_sse_check(client),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
