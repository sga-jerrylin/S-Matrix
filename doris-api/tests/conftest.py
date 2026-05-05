import importlib
import sys
import types
import base64
from pathlib import Path
from unittest.mock import AsyncMock


ROOT = Path(__file__).resolve().parents[2]
API_DIR = ROOT / "doris-api"

if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))


def _install_optional_dependency_stubs():
    try:
        import cryptography.fernet  # noqa: F401
    except Exception:
        cryptography_module = types.ModuleType("cryptography")
        fernet_module = types.ModuleType("cryptography.fernet")

        class FakeFernet:
            def __init__(self, key):
                self.key = key

            @staticmethod
            def generate_key():
                return base64.urlsafe_b64encode(b"codex-test-fernet-key-32-bytes!")

            def encrypt(self, value: bytes) -> bytes:
                return base64.urlsafe_b64encode(value)

            def decrypt(self, value: bytes) -> bytes:
                return base64.urlsafe_b64decode(value)

        fernet_module.Fernet = FakeFernet
        cryptography_module.fernet = fernet_module
        sys.modules.setdefault("cryptography", cryptography_module)
        sys.modules["cryptography.fernet"] = fernet_module

    has_vanna_legacy_base = False
    has_vanna_base = False
    try:
        import vanna.legacy.base  # noqa: F401
        has_vanna_legacy_base = True
    except Exception:
        pass

    if not has_vanna_legacy_base:
        try:
            import vanna.base  # noqa: F401
            has_vanna_base = True
        except Exception:
            pass

    if not has_vanna_legacy_base and not has_vanna_base:
        vanna_module = types.ModuleType("vanna")
        base_module = types.ModuleType("vanna.base")
        legacy_module = types.ModuleType("vanna.legacy")
        legacy_base_module = types.ModuleType("vanna.legacy.base")

        class FakeVannaBase:
            def __init__(self, config=None):
                self.config = config or {}

        base_module.VannaBase = FakeVannaBase
        legacy_base_module.VannaBase = FakeVannaBase
        vanna_module.base = base_module
        legacy_module.base = legacy_base_module
        vanna_module.legacy = legacy_module
        sys.modules.setdefault("vanna", vanna_module)
        sys.modules["vanna.base"] = base_module
        sys.modules["vanna.legacy"] = legacy_module
        sys.modules["vanna.legacy.base"] = legacy_base_module

    try:
        import apscheduler.schedulers.background  # noqa: F401
    except Exception:
        apscheduler_module = types.ModuleType("apscheduler")
        schedulers_module = types.ModuleType("apscheduler.schedulers")
        background_module = types.ModuleType("apscheduler.schedulers.background")

        class FakeBackgroundScheduler:
            def __init__(self, *args, **kwargs):
                self.jobs = []

            def add_job(self, *args, **kwargs):
                self.jobs.append((args, kwargs))

            def start(self):
                return None

            def shutdown(self):
                return None

        background_module.BackgroundScheduler = FakeBackgroundScheduler
        schedulers_module.background = background_module
        apscheduler_module.schedulers = schedulers_module
        sys.modules.setdefault("apscheduler", apscheduler_module)
        sys.modules.setdefault("apscheduler.schedulers", schedulers_module)
        sys.modules["apscheduler.schedulers.background"] = background_module


_install_optional_dependency_stubs()


def reload_main():
    import main

    module = importlib.reload(main)
    module.app.router.on_startup.clear()
    module.doris_ready = True
    module.resolve_llm_resource_config = lambda resource_name=None: None
    module.datasource_handler.list_table_registry = AsyncMock(return_value=[])
    module.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    return module
