from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
import json

import requests

from .config import RuntimeSettings


@dataclass
class RuntimeFailure(Exception):
    code: str
    message: str
    status_code: Optional[int] = None
    details: Optional[Any] = None

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.status_code is not None:
            payload["status_code"] = self.status_code
        if self.details is not None:
            payload["details"] = self.details
        return payload

    def __str__(self) -> str:
        return self.message


class RuntimeClient:
    """Thin HTTP client over the existing FastAPI surface."""

    def __init__(self, settings: RuntimeSettings):
        self.settings = settings
        self.session = requests.Session()
        self.session.trust_env = False

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/api/health", require_auth=False)

    def list_tables(self) -> Dict[str, Any]:
        return self._request("GET", "/api/tables")

    def query_catalog(self) -> Dict[str, Any]:
        return self._request("GET", "/api/query/catalog")

    def table_schema(self, table_name: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/tables/{table_name}/schema")

    def query_history(self, *, limit: int = 20) -> Dict[str, Any]:
        return self._request("GET", "/api/query/history", params={"limit": limit})

    def query_natural(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/api/query/natural", json_body=payload)

    def analyze_table(self, table_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", f"/api/analysis/table/{table_name}", json_body=payload)

    def analyze_replay(self, history_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", f"/api/analysis/replay/{history_id}", json_body=payload)

    def report_list(
        self,
        *,
        table_names: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if table_names:
            params["table_names"] = table_names
        return self._request("GET", "/api/analysis/reports", params=params)

    def report_detail(self, report_id: str, *, include_reasoning: bool = False) -> Dict[str, Any]:
        params = {"include_reasoning": "true"} if include_reasoning else None
        return self._request("GET", f"/api/analysis/reports/{report_id}", params=params)

    def report_summary(self, report_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/analysis/reports/{report_id}/summary")

    def report_latest(self, table_name: str, *, include_reasoning: bool = False) -> Dict[str, Any]:
        params = {"include_reasoning": "true"} if include_reasoning else None
        return self._request("GET", f"/api/analysis/reports/latest/{table_name}", params=params)

    def forecast(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized_payload = normalize_forecast_payload(payload)
        return self._request("POST", "/api/analysis/forecast", json_body=normalized_payload)

    def _request(
        self,
        method: str,
        path: str,
        *,
        require_auth: bool = True,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        expected_statuses: Iterable[int] = (200,),
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        headers: Dict[str, str] = {}
        if require_auth:
            if not self.settings.api_key:
                raise RuntimeFailure(
                    code="configuration_error",
                    message="SMATRIX_API_KEY/DC_API_KEY is not configured for authenticated API calls.",
                )
            headers["X-API-Key"] = self.settings.api_key
            headers["Authorization"] = f"Bearer {self.settings.api_key}"

        try:
            response = self.session.request(
                method=method,
                url=f"{self.settings.api_base_url}{path}",
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout or self.settings.timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeFailure(
                code="transport_error",
                message=f"Request to {path} failed: {exc}",
            ) from exc

        payload = _parse_response_payload(response)
        if response.status_code not in set(expected_statuses):
            message = _extract_error_message(payload, response)
            raise RuntimeFailure(
                code=f"http_{response.status_code}",
                message=message,
                status_code=response.status_code,
                details=payload,
            )

        if isinstance(payload, dict):
            return payload

        return {"success": True, "data": payload}


def _parse_response_payload(response: requests.Response) -> Any:
    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type.lower():
        try:
            return response.json()
        except json.JSONDecodeError:
            return {"raw_text": response.text}

    text = response.text.strip()
    if not text:
        return {}

    try:
        return response.json()
    except ValueError:
        return {"raw_text": text}


def _extract_error_message(payload: Any, response: requests.Response) -> str:
    if isinstance(payload, dict):
        for key in ("detail", "message", "error"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return response.reason or f"HTTP {response.status_code}"


def normalize_forecast_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeFailure(code="input_error", message="forecast payload must be a JSON object.")

    normalized = dict(payload)
    granularity = _normalize_grain(normalized.get("granularity"), field_name="granularity")
    horizon_unit = _normalize_grain(normalized.get("horizon_unit"), field_name="horizon_unit", allow_none=True)

    if horizon_unit is not None and granularity is None:
        granularity = horizon_unit
    elif horizon_unit is not None and granularity is not None and horizon_unit != granularity:
        raise RuntimeFailure(
            code="input_error",
            message="horizon_unit must equal granularity when both are provided.",
        )

    if granularity is not None:
        normalized["granularity"] = granularity
    if horizon_unit is not None:
        normalized["horizon_unit"] = horizon_unit

    filters = normalized.get("filters")
    if filters is not None and not isinstance(filters, dict):
        raise RuntimeFailure(code="input_error", message="filters must be a JSON object.")

    return normalized


def _normalize_grain(value: Any, *, field_name: str, allow_none: bool = True) -> Optional[str]:
    if value in (None, ""):
        if allow_none:
            return None
        raise RuntimeFailure(code="input_error", message=f"{field_name} must not be empty.")

    normalized = str(value).strip().lower()
    if normalized not in {"day", "week", "month"}:
        raise RuntimeFailure(
            code="input_error",
            message=f"{field_name} must be one of day, week, month.",
        )
    return normalized
