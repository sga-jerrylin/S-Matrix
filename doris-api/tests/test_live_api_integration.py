import os
from pathlib import Path

import pytest
import requests


pytestmark = pytest.mark.live


def _load_env_value(key: str) -> str:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return ""

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key.strip() == key:
            return value.strip().strip("'").strip('"')
    return ""


@pytest.fixture(scope="session")
def live_config():
    if os.getenv("RUN_LIVE_INTEGRATION") != "1":
        pytest.skip("set RUN_LIVE_INTEGRATION=1 to run live API integration tests")

    base_url = os.getenv("LIVE_BASE_URL", "http://localhost:38018").rstrip("/")
    api_key = os.getenv("LIVE_API_KEY") or _load_env_value("SMATRIX_API_KEY")
    if not api_key:
        pytest.skip("LIVE_API_KEY/SMATRIX_API_KEY is required for live integration tests")

    session = requests.Session()
    session.trust_env = False
    return {
        "base_url": base_url,
        "api_key": api_key,
        "session": session,
    }


def test_live_health_endpoint(live_config):
    response = live_config["session"].get(f"{live_config['base_url']}/api/health", timeout=15)

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["doris_connected"] is True


def test_live_history_requires_auth(live_config):
    response = live_config["session"].get(f"{live_config['base_url']}/api/query/history", timeout=15)

    assert response.status_code == 401


def test_live_history_returns_contract_with_auth(live_config):
    response = live_config["session"].get(
        f"{live_config['base_url']}/api/query/history",
        headers={"X-API-Key": live_config["api_key"]},
        timeout=15,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert isinstance(payload["history"], list)
    assert isinstance(payload["count"], int)


def test_live_natural_query_contract_when_enabled(live_config):
    if os.getenv("RUN_LIVE_NATURAL_QUERY") != "1":
        pytest.skip("set RUN_LIVE_NATURAL_QUERY=1 to run live natural-query integration")

    query = os.getenv("LIVE_NATURAL_QUERY", "广州有多少机构？")
    response = live_config["session"].post(
        f"{live_config['base_url']}/api/query/natural",
        headers={
            "Content-Type": "application/json",
            "X-API-Key": live_config["api_key"],
        },
        json={"query": query},
        timeout=120,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["query"] == query
    assert isinstance(payload["sql"], str) and payload["sql"].strip()
    assert isinstance(payload["data"], list)
    assert isinstance(payload["count"], int)
