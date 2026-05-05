"""
Compatibility helpers for Vanna package layout across 0.x and 2.x.
"""

from typing import Type

from vanna_native_runtime import resolve_legacy_vanna_base

VANNA_BASE_IMPORT_PATH = ""

_VannaBase, VANNA_BASE_IMPORT_PATH = resolve_legacy_vanna_base()


VannaBase: Type = _VannaBase
