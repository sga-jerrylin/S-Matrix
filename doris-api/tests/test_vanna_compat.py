import importlib

import vanna_compat


def test_vanna_base_compat_import_path_is_supported():
    assert vanna_compat.VANNA_BASE_IMPORT_PATH in {"vanna.legacy.base", "vanna.base"}
    assert vanna_compat.VannaBase is not None


def test_vanna_base_comes_from_real_vanna_package_when_installed():
    module_name = vanna_compat.VannaBase.__module__
    assert module_name.startswith("vanna."), module_name
    assert module_name not in {"conftest", "vanna_compat"}

    module = importlib.import_module(module_name)
    module_file = getattr(module, "__file__", "")
    assert module_file, "expected real vanna module file, got stub module"
