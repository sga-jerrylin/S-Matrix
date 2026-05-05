"""
Version/module/signature probe for Vanna native runtime.
"""

from __future__ import annotations

import importlib
import inspect
import sys
from importlib import metadata
from typing import Any, Dict, List

from .imports import load_native_imports
from .models import NativeRuntimeProbe


def _module_probe(module_name: str) -> Dict[str, Any]:
    try:
        module = importlib.import_module(module_name)
        return {"available": True, "file": getattr(module, "__file__", None)}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def _signature_of(obj: Any) -> str:
    try:
        return str(inspect.signature(obj))
    except Exception as exc:
        return f"<unavailable: {exc}>"


def probe_vanna_native_runtime() -> NativeRuntimeProbe:
    """Collect runtime probe data for diagnostics and phase-gating."""

    notes: List[str] = []
    vanna_dist_version = None
    vanna_module_version = None

    try:
        vanna_dist_version = metadata.version("vanna")
    except Exception as exc:
        notes.append(f"cannot read distribution version: {exc}")

    try:
        vanna_module = importlib.import_module("vanna")
        vanna_module_version = getattr(vanna_module, "__version__", None)
    except Exception as exc:
        notes.append(f"cannot import vanna module: {exc}")

    module_targets = [
        "vanna",
        "vanna.legacy.base",
        "vanna.legacy.adapter",
        "vanna.core.agent.agent",
        "vanna.core.registry",
        "vanna.core.user.request_context",
        "vanna.servers.fastapi",
        "vanna.server.fastapi",
    ]
    modules = {name: _module_probe(name) for name in module_targets}

    signatures: Dict[str, str] = {}
    try:
        imports = load_native_imports()
        signatures = {
            "Agent.__init__": _signature_of(imports.Agent.__init__),
            "Agent.send_message": _signature_of(imports.Agent.send_message),
            "ToolRegistry.register_local_tool": _signature_of(
                imports.ToolRegistry.register_local_tool
            ),
            "VannaFastAPIServer.__init__": _signature_of(
                imports.VannaFastAPIServer.__init__
            ),
            "VannaFastAPIServer.create_app": _signature_of(
                imports.VannaFastAPIServer.create_app
            ),
        }
    except Exception as exc:
        notes.append(f"cannot inspect native signatures: {exc}")

    if vanna_dist_version and vanna_module_version and vanna_dist_version != vanna_module_version:
        notes.append(
            "distribution version and vanna.__version__ mismatch "
            f"({vanna_dist_version} != {vanna_module_version})"
        )

    agent_sig = signatures.get("Agent.__init__", "")
    for required_param in ("llm_service", "tool_registry", "user_resolver", "agent_memory"):
        if required_param and required_param not in agent_sig:
            notes.append(f"Agent.__init__ missing expected parameter '{required_param}'")

    return NativeRuntimeProbe(
        python_version=sys.version,
        vanna_dist_version=vanna_dist_version,
        vanna_module_version=vanna_module_version,
        modules=modules,
        signatures=signatures,
        notes=notes,
    )
