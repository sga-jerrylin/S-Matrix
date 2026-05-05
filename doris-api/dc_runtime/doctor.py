from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import subprocess

from .client import RuntimeClient, RuntimeFailure
from .config import RuntimeSettings


REQUIRED_SYSTEM_TABLES = {
    "_sys_datasources",
    "_sys_sync_tasks",
    "_sys_table_metadata",
    "_sys_table_registry",
    "_sys_query_history",
    "_sys_table_agents",
    "_sys_field_catalog",
    "_sys_table_relationships",
}

CANONICAL_BACKEND_HOST = "smatrix-be"
CANONICAL_BACKEND_PORT = "9050"


@dataclass
class CheckResult:
    name: str
    status: str
    message: str
    details: Optional[Any] = None

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "message": self.message,
        }
        if self.details is not None:
            payload["details"] = self.details
        return payload


def run_doctor(settings: RuntimeSettings, client: Optional[RuntimeClient] = None) -> Dict[str, Any]:
    client = client or RuntimeClient(settings)
    checks: List[CheckResult] = [
        _check_env_file(settings),
        _check_api_key(settings),
        _check_compose_file(settings),
        _check_command(["docker", "--version"], "docker"),
        _check_command(["docker", "compose", "version"], "docker_compose"),
    ]

    checks.append(_check_api_health(client))
    checks.append(_check_authenticated_route(client))

    return _summarize("doctor", checks)


def run_smoke(
    settings: RuntimeSettings,
    *,
    compose_up: bool = False,
    skip_frontend: bool = False,
) -> Dict[str, Any]:
    checks: List[CheckResult] = []
    if compose_up:
        compose_result = _check_subprocess(
            ["docker", "compose", "up", "-d", "--build", "--remove-orphans"],
            "compose_up",
            cwd=settings.repo_root,
            success_message="docker compose up -d --build --remove-orphans completed",
        )
        checks.append(compose_result)
        if compose_result.status != "pass":
            return _summarize("smoke", checks)

    services = ["smatrix-fe", "smatrix-be", "smatrix-api"]
    if not skip_frontend:
        services.append("smatrix-frontend")
    for service in services:
        checks.append(_wait_for_container_health(service))

    canonical_backend_check = _check_canonical_backend(settings)
    checks.append(canonical_backend_check)
    if canonical_backend_check.status != "pass":
        return _summarize("smoke", checks)

    checks.append(_wait_for_http(settings.fe_bootstrap_url, "fe_bootstrap"))
    checks.append(_wait_for_http(settings.be_health_url, "be_health"))
    checks.append(_wait_for_http(f"{settings.api_base_url}/api/health", "api_health"))

    if not skip_frontend:
        checks.append(_wait_for_http(settings.frontend_url, "frontend"))

    auth_client = RuntimeClient(settings)
    checks.append(_check_unauthorized_history(auth_client))
    checks.append(_check_authenticated_route(auth_client))
    checks.append(_check_system_tables(settings))
    checks.append(_check_system_table_write(settings))

    return _summarize("smoke", checks)


def _check_env_file(settings: RuntimeSettings) -> CheckResult:
    if settings.env_file is None:
        return CheckResult(
            name="env_file",
            status="warn",
            message="No .env file found at repository root; runtime will rely on process environment only.",
        )
    return CheckResult(
        name="env_file",
        status="pass",
        message=f"Using environment file: {settings.env_file}",
    )


def _check_api_key(settings: RuntimeSettings) -> CheckResult:
    if settings.api_key:
        return CheckResult(
            name="api_key",
            status="pass",
            message="Authenticated API key is configured for dc runtime calls.",
        )
    return CheckResult(
        name="api_key",
        status="fail",
        message="SMATRIX_API_KEY/DC_API_KEY is not configured; authenticated commands will fail.",
    )


def _check_compose_file(settings: RuntimeSettings) -> CheckResult:
    compose_file = settings.repo_root / "docker-compose.yml"
    if compose_file.exists():
        return CheckResult(
            name="compose_file",
            status="pass",
            message=f"Found compose file: {compose_file}",
        )
    return CheckResult(
        name="compose_file",
        status="fail",
        message=f"Missing compose file: {compose_file}",
    )


def _check_command(command: List[str], name: str) -> CheckResult:
    return _check_subprocess(command, name, success_message="Command is available")


def _check_subprocess(
    command: List[str],
    name: str,
    *,
    cwd=None,
    success_message: str,
) -> CheckResult:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except FileNotFoundError as exc:
        return CheckResult(name=name, status="warn", message=str(exc))

    if completed.returncode == 0:
        details = completed.stdout.strip() or completed.stderr.strip() or None
        return CheckResult(name=name, status="pass", message=success_message, details=details)

    details = completed.stdout.strip() or completed.stderr.strip() or None
    return CheckResult(
        name=name,
        status="fail",
        message=f"Command failed with exit code {completed.returncode}",
        details=details,
    )


def _check_api_health(client: RuntimeClient) -> CheckResult:
    try:
        payload = client.health()
    except RuntimeFailure as exc:
        return CheckResult(name="api_health", status="fail", message=exc.message, details=exc.to_payload())

    success = bool(payload.get("success")) and bool(payload.get("doris_connected"))
    return CheckResult(
        name="api_health",
        status="pass" if success else "fail",
        message="API health endpoint responded" if success else "API health endpoint reported degraded state",
        details=payload,
    )


def _check_authenticated_route(client: RuntimeClient) -> CheckResult:
    try:
        payload = client.query_history(limit=1)
    except RuntimeFailure as exc:
        return CheckResult(
            name="api_auth",
            status="fail",
            message=exc.message,
            details=exc.to_payload(),
        )

    return CheckResult(
        name="api_auth",
        status="pass",
        message="Authenticated API route responded successfully.",
        details={"count": payload.get("count"), "success": payload.get("success")},
    )


def _wait_for_http(url: str, name: str, *, expected_status: int = 200) -> CheckResult:
    import time
    import requests

    session = requests.Session()
    session.trust_env = False

    last_status: Optional[int] = None
    last_body: Optional[str] = None
    for _ in range(60):
        try:
            response = session.get(url, timeout=5)
            last_status = response.status_code
            last_body = response.text[:500]
            if response.status_code == expected_status:
                return CheckResult(name=name, status="pass", message=f"{url} returned {expected_status}")
        except requests.RequestException as exc:
            last_body = str(exc)
        time.sleep(2)

    return CheckResult(
        name=name,
        status="fail",
        message=f"{url} did not return {expected_status} within the retry window.",
        details={"last_status": last_status, "last_body": last_body},
    )


def _wait_for_container_health(service: str, *, attempts: int = 120, sleep_seconds: int = 5) -> CheckResult:
    import time

    command = [
        "docker",
        "inspect",
        "--format={{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}",
        service,
    ]
    last_status: Optional[str] = None

    for _ in range(attempts):
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        except FileNotFoundError as exc:
            return CheckResult(name=f"{service}_health", status="fail", message=str(exc))

        stderr_text = (completed.stderr or "").strip()
        stdout_text = (completed.stdout or "").strip()
        last_status = stdout_text or stderr_text or "unknown"

        if completed.returncode != 0:
            lowered = last_status.lower()
            if (
                "failed to connect to the docker api" in lowered
                or "error during connect" in lowered
                or "is the docker daemon running" in lowered
                or "open //./pipe/dockerdesktoplinuxengine" in lowered
                or "no such file or directory" in lowered
            ):
                return CheckResult(
                    name=f"{service}_health",
                    status="fail",
                    message="Docker daemon is not reachable.",
                    details={"raw_error": last_status},
                )

        if completed.returncode == 0 and last_status == "healthy":
            return CheckResult(
                name=f"{service}_health",
                status="pass",
                message=f"Container is healthy: {service}",
            )
        if last_status == "unhealthy":
            return CheckResult(
                name=f"{service}_health",
                status="fail",
                message=f"Container is unhealthy: {service}",
            )
        time.sleep(sleep_seconds)

    return CheckResult(
        name=f"{service}_health",
        status="fail",
        message=f"Timed out waiting for healthy container: {service}",
        details={"last_status": last_status},
    )


def _check_unauthorized_history(client: RuntimeClient) -> CheckResult:
    import requests

    try:
        response = client.session.get(
            f"{client.settings.api_base_url}/api/query/history",
            timeout=client.settings.timeout,
        )
    except requests.RequestException as exc:
        return CheckResult(name="api_unauthorized", status="fail", message=str(exc))

    if response.status_code == 401:
        return CheckResult(
            name="api_unauthorized",
            status="pass",
            message="Unauthenticated query-history call is correctly rejected with 401.",
        )

    return CheckResult(
        name="api_unauthorized",
        status="fail",
        message=f"Expected 401 from unauthenticated query-history call, got {response.status_code}",
        details=response.text[:500],
    )


def _check_system_tables(settings: RuntimeSettings) -> CheckResult:
    script = (
        "from db import doris_client; "
        f"required = {sorted(REQUIRED_SYSTEM_TABLES)!r}; "
        "rows = doris_client.execute_query('SHOW TABLES'); "
        "tables = set(); "
        "[tables.update(row.values()) for row in rows]; "
        "missing = sorted(set(required) - tables); "
        "import sys; "
        "print('system tables ready' if not missing else 'missing system tables: ' + ', '.join(missing)); "
        "sys.exit(0 if not missing else 1)"
    )

    return _check_subprocess(
        ["docker", "exec", "smatrix-api", "python", "-c", script],
        "system_tables",
        cwd=settings.repo_root,
        success_message="Required system tables are present in Doris.",
    )


def _check_canonical_backend(settings: RuntimeSettings) -> CheckResult:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "smatrix-fe",
        "mysql",
        "-hsmatrix-fe",
        "-P9030",
        "-uroot",
        "--batch",
        "--skip-column-names",
        "-e",
        "SHOW BACKENDS;",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=settings.repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except FileNotFoundError as exc:
        return CheckResult(name="canonical_backend", status="fail", message=str(exc))

    if completed.returncode != 0:
        details = completed.stdout.strip() or completed.stderr.strip() or None
        return CheckResult(
            name="canonical_backend",
            status="fail",
            message="Failed to query Doris backends from FE.",
            details={
                "command": "docker compose exec -T smatrix-fe mysql ... SHOW BACKENDS;",
                "output": details,
                "recovery": ["./init.sh --reset --yes", "./init.sh"],
            },
        )

    rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if len(rows) != 1:
        return CheckResult(
            name="canonical_backend",
            status="fail",
            message=(
                f"Expected exactly one Doris backend ({CANONICAL_BACKEND_HOST}:{CANONICAL_BACKEND_PORT}), "
                f"but found {len(rows)}."
            ),
            details={
                "rows": rows,
                "recovery": ["./init.sh --reset --yes", "./init.sh"],
            },
        )

    columns = rows[0].split("\t")
    if len(columns) < 10:
        return CheckResult(
            name="canonical_backend",
            status="fail",
            message="Unable to parse SHOW BACKENDS output.",
            details={"row": rows[0]},
        )

    host = columns[1].strip()
    heartbeat_port = columns[2].strip()
    alive = columns[9].strip().lower()
    if host != CANONICAL_BACKEND_HOST or heartbeat_port != CANONICAL_BACKEND_PORT:
        return CheckResult(
            name="canonical_backend",
            status="fail",
            message=(
                f"Detected stale Doris backend {host}:{heartbeat_port}; "
                f"expected {CANONICAL_BACKEND_HOST}:{CANONICAL_BACKEND_PORT}."
            ),
            details={
                "rows": rows,
                "recovery": ["./init.sh --reset --yes", "./init.sh"],
            },
        )
    if alive != "true":
        return CheckResult(
            name="canonical_backend",
            status="fail",
            message=(
                f"Canonical Doris backend {host}:{heartbeat_port} is not alive "
                f"(Alive={alive})."
            ),
            details={
                "rows": rows,
                "recovery": ["./init.sh --reset --yes", "./init.sh"],
            },
        )

    return CheckResult(
        name="canonical_backend",
        status="pass",
        message=(
            f"Doris backend is canonical and alive: "
            f"{CANONICAL_BACKEND_HOST}:{CANONICAL_BACKEND_PORT}"
        ),
    )


def _check_system_table_write(settings: RuntimeSettings) -> CheckResult:
    script = (
        "import uuid, datetime; "
        "from db import doris_client; "
        "probe_id = 'smoke_probe_' + uuid.uuid4().hex[:8]; "
        "now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'); "
        "doris_client.execute_update("
        "\"INSERT INTO `_sys_datasources` "
        "(`id`, `name`, `host`, `port`, `user`, `password_encrypted`, `database_name`, `created_at`, `updated_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)\", "
        "(probe_id, 'smoke_probe', '127.0.0.1', 3306, 'probe', 'probe', 'probe_db', now, now)"
        "); "
        "doris_client.execute_update(\"DELETE FROM `_sys_datasources` WHERE `id` = %s\", (probe_id,)); "
        "print('system table write probe ok: ' + probe_id)"
    )

    return _check_subprocess(
        ["docker", "exec", "smatrix-api", "python", "-c", script],
        "system_table_write",
        cwd=settings.repo_root,
        success_message="System table write probe succeeded on _sys_datasources.",
    )


def _summarize(command: str, checks: List[CheckResult]) -> Dict[str, Any]:
    has_failures = any(check.status == "fail" for check in checks)
    has_warnings = any(check.status == "warn" for check in checks)
    if has_failures:
        status = "fail"
    elif has_warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "success": not has_failures,
        "command": command,
        "status": status,
        "checks": [check.to_payload() for check in checks],
    }
