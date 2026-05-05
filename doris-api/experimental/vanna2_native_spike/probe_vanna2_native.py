"""
Probe native Vanna 2 capabilities without touching DorisClaw production chain.
"""

from __future__ import annotations

import importlib
import inspect
import json
import sys
from importlib import metadata
from typing import Any, Dict, List


def module_available(module_name: str) -> Dict[str, Any]:
    try:
        mod = importlib.import_module(module_name)
        return {"available": True, "file": getattr(mod, "__file__", None)}
    except Exception as exc:  # pragma: no cover - probe path only
        return {"available": False, "error": str(exc)}


def signature_of(obj: Any) -> str:
    try:
        return str(inspect.signature(obj))
    except Exception as exc:  # pragma: no cover - probe path only
        return f"<unavailable: {exc}>"


def main() -> int:
    result: Dict[str, Any] = {
        "python_version": sys.version,
        "vanna_dist_version": None,
        "vanna_module_version": None,
        "modules": {},
        "signatures": {},
        "notes": [],
    }

    try:
        result["vanna_dist_version"] = metadata.version("vanna")
    except Exception as exc:  # pragma: no cover - probe path only
        result["notes"].append(f"cannot read distribution version: {exc}")

    try:
        import vanna  # noqa: WPS433

        result["vanna_module_version"] = getattr(vanna, "__version__", None)
    except Exception as exc:  # pragma: no cover - probe path only
        result["notes"].append(f"cannot import vanna: {exc}")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    module_targets: List[str] = [
        "vanna.legacy.base",
        "vanna.legacy.adapter",
        "vanna.core.agent.agent",
        "vanna.core.registry",
        "vanna.servers.fastapi",
        "vanna.servers.flask",
        "vanna.server.fastapi",
        "vanna.web_components",
    ]
    for name in module_targets:
        result["modules"][name] = module_available(name)

    try:
        from vanna import Agent, ToolRegistry  # noqa: WPS433
        from vanna.servers.fastapi import VannaFastAPIServer  # noqa: WPS433

        result["signatures"] = {
            "Agent.__init__": signature_of(Agent.__init__),
            "Agent.send_message": signature_of(Agent.send_message),
            "ToolRegistry.register_local_tool": signature_of(ToolRegistry.register_local_tool),
            "VannaFastAPIServer.__init__": signature_of(VannaFastAPIServer.__init__),
            "VannaFastAPIServer.create_app": signature_of(VannaFastAPIServer.create_app),
        }
    except Exception as exc:  # pragma: no cover - probe path only
        result["notes"].append(f"cannot inspect signatures: {exc}")

    dist_v = result.get("vanna_dist_version")
    mod_v = result.get("vanna_module_version")
    if dist_v and mod_v and dist_v != mod_v:
        result["notes"].append(
            "distribution version and vanna.__version__ mismatch "
            f"({dist_v} != {mod_v})"
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
