"""
Vanna 2 native runtime entrypoint for DorisClaw query intelligence.

All business-side Vanna imports should go through this package.
"""

from .backbone import build_native_runtime_backbone
from .dc_tools import DCToolRuntimeAdapter, register_dc_query_tools
from .errors import VannaRuntimeError, VannaRuntimeImportError
from .imports import load_native_imports, resolve_legacy_vanna_base
from .memory_backend import DCAgentMemoryAdapter
from .models import (
    NativeAuditTraceBridge,
    NativeRuntimeBackbone,
    NativeRuntimeImports,
    NativeRuntimeProbe,
)
from .probe import probe_vanna_native_runtime
from .query_kernel import NativeKernelExecutionError, NativeKernelResult, run_native_query_kernel

__all__ = [
    "VannaRuntimeError",
    "VannaRuntimeImportError",
    "NativeRuntimeProbe",
    "NativeRuntimeImports",
    "NativeAuditTraceBridge",
    "NativeRuntimeBackbone",
    "load_native_imports",
    "resolve_legacy_vanna_base",
    "DCAgentMemoryAdapter",
    "probe_vanna_native_runtime",
    "build_native_runtime_backbone",
    "DCToolRuntimeAdapter",
    "register_dc_query_tools",
    "NativeKernelExecutionError",
    "NativeKernelResult",
    "run_native_query_kernel",
]
