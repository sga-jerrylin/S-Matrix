from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import os
import sys


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def parse_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}

    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _strip_quotes(value)
    return values


@dataclass(frozen=True)
class RuntimeSettings:
    repo_root: Path
    api_base_url: str
    api_key: str
    timeout: int
    frontend_url: str
    fe_bootstrap_url: str
    be_health_url: str
    python_executable: str
    env_file: Optional[Path]

    @classmethod
    def load(cls) -> "RuntimeSettings":
        repo_root = Path(__file__).resolve().parents[2]
        env_file = repo_root / ".env"
        env_values = parse_env_file(env_file)

        def lookup(*keys: str, default: str = "") -> str:
            for key in keys:
                value = os.getenv(key)
                if value not in (None, ""):
                    return value
            for key in keys:
                value = env_values.get(key)
                if value not in (None, ""):
                    return value
            return default

        timeout_text = lookup("DC_TIMEOUT", default="30")
        try:
            timeout = max(int(timeout_text), 1)
        except ValueError:
            timeout = 30

        return cls(
            repo_root=repo_root,
            api_base_url=lookup("DC_API_BASE_URL", "SMATRIX_API_URL", default="http://localhost:38018").rstrip("/"),
            api_key=lookup("DC_API_KEY", "SMATRIX_API_KEY"),
            timeout=timeout,
            frontend_url=lookup("DC_FRONTEND_URL", default="http://localhost:35173"),
            fe_bootstrap_url=lookup("DC_FE_BOOTSTRAP_URL", default="http://localhost:38030/api/bootstrap"),
            be_health_url=lookup("DC_BE_HEALTH_URL", default="http://localhost:38040/api/health"),
            python_executable=lookup("PYTHON", default=sys.executable),
            env_file=env_file if env_file.exists() else None,
        )
