"""
Error types for Vanna native runtime bootstrap.
"""


class VannaRuntimeError(RuntimeError):
    """Base runtime error for vanna_native_runtime."""


class VannaRuntimeImportError(VannaRuntimeError):
    """Raised when required Vanna symbols cannot be imported."""
