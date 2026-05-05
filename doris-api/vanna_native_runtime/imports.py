"""
Centralized Vanna symbol imports for DorisClaw query intelligence.
"""

from __future__ import annotations

import importlib
from typing import Any, Tuple

from .errors import VannaRuntimeImportError
from .models import NativeRuntimeImports


def _import_attr(module_name: str, attr_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise VannaRuntimeImportError(
            f"failed to import module '{module_name}': {exc}"
        ) from exc

    try:
        return getattr(module, attr_name)
    except AttributeError as exc:
        raise VannaRuntimeImportError(
            f"module '{module_name}' has no attribute '{attr_name}'"
        ) from exc


def resolve_legacy_vanna_base() -> Tuple[Any, str]:
    """
    Resolve VannaBase from legacy-compatible import paths.

    Preferred order:
    1) vanna.legacy.base.VannaBase
    2) vanna.base.VannaBase
    """

    try:
        return _import_attr("vanna.legacy.base", "VannaBase"), "vanna.legacy.base"
    except Exception:
        pass

    try:
        return _import_attr("vanna.base", "VannaBase"), "vanna.base"
    except Exception as exc:
        raise VannaRuntimeImportError(
            "neither 'vanna.legacy.base' nor 'vanna.base' is importable"
        ) from exc


def load_native_imports() -> NativeRuntimeImports:
    """Load native runtime symbols from Vanna 2."""

    # Keep imports flexible across package layouts by preferring top-level exports,
    # then falling back to domain modules.
    try:
        Agent = _import_attr("vanna", "Agent")
    except Exception:
        Agent = _import_attr("vanna.core.agent.agent", "Agent")

    try:
        ToolRegistry = _import_attr("vanna", "ToolRegistry")
    except Exception:
        ToolRegistry = _import_attr("vanna.core.registry", "ToolRegistry")

    Tool = _import_attr("vanna.core.tool.base", "Tool")
    ToolCall = _import_attr("vanna.core.tool.models", "ToolCall")
    ToolContext = _import_attr("vanna.core.tool.models", "ToolContext")
    ToolResult = _import_attr("vanna.core.tool.models", "ToolResult")
    User = _import_attr("vanna.core.user.models", "User")
    UserResolver = _import_attr("vanna.core.user.resolver", "UserResolver")
    RequestContext = _import_attr("vanna.core.user.request_context", "RequestContext")
    DemoAgentMemory = _import_attr(
        "vanna.integrations.local.agent_memory.in_memory", "DemoAgentMemory"
    )
    MockLlmService = _import_attr("vanna.integrations.mock", "MockLlmService")
    VannaFastAPIServer = _import_attr("vanna.servers.fastapi", "VannaFastAPIServer")
    LegacyVannaAdapter = _import_attr("vanna.legacy.adapter", "LegacyVannaAdapter")

    return NativeRuntimeImports(
        Agent=Agent,
        ToolRegistry=ToolRegistry,
        Tool=Tool,
        ToolCall=ToolCall,
        ToolContext=ToolContext,
        ToolResult=ToolResult,
        User=User,
        UserResolver=UserResolver,
        RequestContext=RequestContext,
        DemoAgentMemory=DemoAgentMemory,
        MockLlmService=MockLlmService,
        VannaFastAPIServer=VannaFastAPIServer,
        LegacyVannaAdapter=LegacyVannaAdapter,
    )
