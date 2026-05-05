"""
Runtime data models for Vanna native kernel bootstrap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class NativeRuntimeProbe:
    """Probe payload for module/version/signature checks."""

    python_version: str
    vanna_dist_version: Optional[str]
    vanna_module_version: Optional[str]
    modules: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    signatures: Dict[str, str] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "python_version": self.python_version,
            "vanna_dist_version": self.vanna_dist_version,
            "vanna_module_version": self.vanna_module_version,
            "modules": dict(self.modules),
            "signatures": dict(self.signatures),
            "notes": list(self.notes),
        }


@dataclass
class NativeRuntimeImports:
    """Imported Vanna classes/symbols needed by native kernel."""

    Agent: Any
    ToolRegistry: Any
    Tool: Any
    ToolCall: Any
    ToolContext: Any
    ToolResult: Any
    User: Any
    UserResolver: Any
    RequestContext: Any
    DemoAgentMemory: Any
    MockLlmService: Any
    VannaFastAPIServer: Any
    LegacyVannaAdapter: Any


@dataclass
class NativeAuditTraceBridge:
    """Minimal in-memory audit bridge placeholder for phase-1."""

    events: List[Dict[str, Any]] = field(default_factory=list)

    def record(
        self,
        phase: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        self.events.append(
            {
                "phase": phase,
                "request_id": str(request_id or ""),
                "payload": dict(payload or {}),
            }
        )

    def snapshot(self, request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        rid = str(request_id or "").strip()
        if not rid:
            return list(self.events)
        return [event for event in self.events if str(event.get("request_id") or "") == rid]

    def clear(self, request_id: Optional[str] = None) -> None:
        rid = str(request_id or "").strip()
        if not rid:
            self.events = []
            return
        self.events = [
            event
            for event in self.events
            if str(event.get("request_id") or "") != rid
        ]


@dataclass
class NativeRuntimeBackbone:
    """Bootstrapped native runtime components."""

    imports: NativeRuntimeImports
    agent: Any
    tool_registry: Any
    agent_memory: Any
    user_resolver: Any
    llm_service: Any
    audit_bridge: NativeAuditTraceBridge

    def new_request_context(
        self,
        *,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        remote_addr: Optional[str] = None,
    ) -> Any:
        request_context_cls = self.imports.RequestContext
        return request_context_cls(
            headers=dict(headers or {}),
            cookies=dict(cookies or {}),
            query_params=dict(query_params or {}),
            metadata=dict(metadata or {}),
            remote_addr=remote_addr,
        )

    def create_sse_sidecar_app(self, config: Optional[Dict[str, Any]] = None) -> Any:
        server = self.imports.VannaFastAPIServer(self.agent, config=config)
        return server.create_app()
