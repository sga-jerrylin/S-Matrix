"""
Native runtime backbone bootstrap for DorisClaw query intelligence.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .imports import load_native_imports
from .models import NativeAuditTraceBridge, NativeRuntimeBackbone


def _normalize_groups(groups: Optional[Iterable[str]]) -> List[str]:
    normalized = [str(group).strip() for group in (groups or []) if str(group).strip()]
    return normalized or ["user"]


def build_native_runtime_backbone(
    *,
    llm_service: Any = None,
    tool_registry: Any = None,
    agent_memory: Any = None,
    user_resolver: Any = None,
    default_user_id: str = "dc-native-user",
    default_user_groups: Optional[Iterable[str]] = None,
    default_user_email: Optional[str] = None,
    mock_response_content: str = "native runtime initialized",
    memory_max_items: int = 10_000,
    agent_kwargs: Optional[Dict[str, Any]] = None,
) -> NativeRuntimeBackbone:
    """
    Build minimal native Agent/ToolRegistry/Memory/RequestContext backbone.

    This is phase-1 bootstrap only and is intentionally not connected to
    `/api/query/natural` execution path yet.
    """

    imports = load_native_imports()
    groups = _normalize_groups(default_user_groups)

    if tool_registry is None:
        tool_registry = imports.ToolRegistry()
    if agent_memory is None:
        agent_memory = imports.DemoAgentMemory(max_items=int(memory_max_items))
    if llm_service is None:
        llm_service = imports.MockLlmService(response_content=mock_response_content)

    if user_resolver is None:
        user_cls = imports.User
        resolver_base = imports.UserResolver

        class DefaultUserResolver(resolver_base):
            async def resolve_user(self, request_context: Any) -> Any:
                metadata = dict(getattr(request_context, "metadata", {}) or {})
                user_id = str(
                    metadata.get("user_id")
                    or metadata.get("user")
                    or default_user_id
                )
                email = metadata.get("email") or default_user_email
                memberships = metadata.get("group_memberships") or metadata.get("groups") or groups
                return user_cls(
                    id=user_id,
                    email=email,
                    username=user_id,
                    group_memberships=_normalize_groups(memberships),
                    metadata={"source": "vanna_native_runtime"},
                )

        user_resolver = DefaultUserResolver()

    kwargs = dict(agent_kwargs or {})
    kwargs.update(
        {
            "llm_service": llm_service,
            "tool_registry": tool_registry,
            "user_resolver": user_resolver,
            "agent_memory": agent_memory,
        }
    )
    agent = imports.Agent(**kwargs)

    return NativeRuntimeBackbone(
        imports=imports,
        agent=agent,
        tool_registry=tool_registry,
        agent_memory=agent_memory,
        user_resolver=user_resolver,
        llm_service=llm_service,
        audit_bridge=NativeAuditTraceBridge(),
    )
