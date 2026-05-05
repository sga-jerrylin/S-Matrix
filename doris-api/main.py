"""
Doris API Gateway - 主程序
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Dict, Any, List, Optional
import uvicorn
import traceback
import os
import re
import json
import asyncio
import logging
import inspect
import uuid
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from config import API_HOST, API_PORT, DORIS_CONFIG, ANALYST_DEFAULT_DEPTH
from handlers import action_handler
from db import doris_client
from upload_handler import excel_handler
from vanna_doris import VannaDorisOpenAI
from datasource_handler import datasource_handler, sync_scheduler
from metadata_analyzer import metadata_analyzer
from planner_agent import PlannerAgent
from table_admin_agent import TableAdminAgent
from coordinator_agent import CoordinatorAgent
from repair_agent import RepairAgent
from analyst_agent import AnalystAgent
from llm_executor import LLMExecutionError
from app_scheduler import app_scheduler
from analysis_dispatcher import AnalysisDispatcher
from analysis_scheduler import AnalysisScheduler
from vanna_native_runtime import (
    NativeKernelExecutionError,
    run_native_query_kernel,
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Application startup/shutdown lifecycle."""
    async def init_in_background():
        global doris_ready
        loop = asyncio.get_running_loop()
        if doris_ready:
            if analysis_scheduler is not None:
                analysis_scheduler.set_event_loop(loop)
            return
        ready = await asyncio.to_thread(_init_doris_sync)
        if ready:
            doris_ready = True
            sync_scheduler.register(app_scheduler)
            if analysis_scheduler is not None:
                analysis_scheduler.set_event_loop(loop)
                analysis_scheduler.register(app_scheduler)
            app_scheduler.start()

    asyncio.create_task(init_in_background())
    yield
    app_scheduler.stop()


app = FastAPI(
    title="Doris API Gateway",
    description="极简的 HTTP API Gateway for Apache Doris",
    version="1.0.0",
    lifespan=lifespan,
)


# Global readiness flag for Doris init to avoid 502s after reboot.
doris_ready = False
analyst_agent: Optional[AnalystAgent] = None
analysis_scheduler: Optional[AnalysisScheduler] = None
analysis_dispatcher: Optional[AnalysisDispatcher] = None
auto_analysis_executor = ThreadPoolExecutor(
    max_workers=int(os.getenv("ANALYST_AUTO_ANALYZE_WORKERS", "2")),
    thread_name_prefix="analyst-auto",
)
logger = logging.getLogger(__name__)
cors_origins = [
    origin.strip()
    for origin in os.getenv("SMATRIX_CORS_ORIGINS", "http://localhost:35173").split(",")
    if origin.strip()
]

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _normalize_llm_resource(row_group: Dict[str, Any]) -> Dict[str, Any]:
    properties = dict(row_group.get("properties") or {})
    name = row_group.get("ResourceName") or row_group.get("Name") or ""
    provider = (
        properties.get("ai.provider_type")
        or properties.get("provider")
        or row_group.get("ResourceType")
        or ""
    )
    model = properties.get("ai.model_name") or properties.get("model_name") or ""
    endpoint = properties.get("ai.endpoint") or properties.get("endpoint") or ""
    temperature = properties.get("ai.temperature") or properties.get("temperature")
    max_tokens = properties.get("ai.max_tokens") or properties.get("max_tokens")
    api_key_value = properties.get("ai.api_key") or properties.get("api_key") or ""
    api_key_configured = bool(api_key_value and str(api_key_value).strip("* "))

    if not api_key_configured and api_key_value:
        api_key_configured = True

    normalized = {
        **row_group,
        "name": name,
        "provider": provider,
        "model": model,
        "endpoint": endpoint,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "api_key_configured": api_key_configured,
        "properties": properties,
    }
    return normalized


def load_llm_resources() -> List[Dict[str, Any]]:
    sql = 'SHOW RESOURCES WHERE NAME LIKE "%"'
    all_resources = doris_client.execute_query(sql)

    resources_dict: Dict[str, Dict[str, Any]] = {}
    for row in all_resources:
        name = row.get('Name')
        resource_type = row.get('ResourceType')
        if resource_type != 'ai' or not name:
            continue

        if name not in resources_dict:
            resources_dict[name] = {
                'ResourceName': name,
                'ResourceType': resource_type,
                'properties': {}
            }

        item = row.get('Item')
        value = row.get('Value')
        if item and value is not None:
            resources_dict[name]['properties'][item] = value

    return [_normalize_llm_resource(resource) for resource in resources_dict.values()]


def _derive_base_url(endpoint: str) -> str:
    if not endpoint:
        return ""

    parts = urlsplit(endpoint)
    path = parts.path or ""
    suffix = "/chat/completions"
    if path.endswith(suffix):
        path = path[: -len(suffix)] or "/"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def resolve_llm_resource_config(resource_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    resources = load_llm_resources()
    if not resources:
        return None

    selected = None
    if resource_name:
        selected = next((resource for resource in resources if resource.get("name") == resource_name), None)
    if selected is None:
        selected = resources[0]
    if selected is None:
        return None

    endpoint = selected.get("endpoint") or ""
    return {
        "resource_name": selected.get("name"),
        "provider": selected.get("provider"),
        "model": selected.get("model"),
        "endpoint": endpoint,
        "base_url": _derive_base_url(endpoint),
        "api_key_configured": selected.get("api_key_configured", False),
    }


def build_api_config(
    resource_name: Optional[str] = None,
    *,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    resource_config = resolve_llm_resource_config(resource_name)
    resolved_api_key = api_key or os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    resource_found = bool(resource_config)
    resource_api_key_configured = bool((resource_config or {}).get("api_key_configured", False))
    resolved_resource_name = (resource_config or {}).get("resource_name") or resource_name
    llm_execution_mode = "direct_api"
    if not resolved_api_key and resolved_resource_name and resource_api_key_configured:
        llm_execution_mode = "doris_resource"

    return {
        "api_key": resolved_api_key,
        "model": model or (resource_config or {}).get("model") or os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        "base_url": base_url or (resource_config or {}).get("base_url") or os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        "resource_name": resolved_resource_name,
        "endpoint": (resource_config or {}).get("endpoint"),
        "provider": (resource_config or {}).get("provider"),
        "resource_found": resource_found,
        "resource_api_key_configured": resource_api_key_configured,
        "llm_execution_mode": llm_execution_mode,
    }


# ============ 启动事件 ============

def _init_doris_sync():
    import time
    import pymysql
    global analyst_agent, analysis_scheduler, analysis_dispatcher

    retry_interval = int(os.getenv("DORIS_INIT_RETRY_INTERVAL", "2"))
    db_name = DORIS_CONFIG["database"]

    print("=" * 60)
    print("Doris API Gateway starting...")
    print("=" * 60)

    while True:
        try:
            print("Waiting for Doris FE...")

            conn = pymysql.connect(
                host=DORIS_CONFIG['host'],
                port=DORIS_CONFIG['port'],
                user=DORIS_CONFIG['user'],
                password=DORIS_CONFIG['password'],
                connect_timeout=5
            )

            cursor = conn.cursor()

            cursor.execute("SHOW BACKENDS")
            backends = cursor.fetchall()
            if not backends:
                be_host = os.getenv('DORIS_STREAM_LOAD_HOST', 'doris-be')
                be_heartbeat_port = 9050
                print(f"No BE registered, trying to add: {be_host}:{be_heartbeat_port}")
                try:
                    cursor.execute(f'ALTER SYSTEM ADD BACKEND "{be_host}:{be_heartbeat_port}"')
                    print(f"Sent add BE: {be_host}:{be_heartbeat_port}")
                    time.sleep(5)
                except Exception as be_err:
                    print(f"Add BE failed (may already exist): {be_err}")

            print(f"Ensure database {db_name}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall()]

            if db_name in databases:
                print(f"Database '{db_name}' ready")
            else:
                print(f"Database '{db_name}' create failed")

            cursor.close()
            conn.close()

            if not datasource_handler.init_tables():
                print("System tables are not ready yet")
                print(f"Retry in {retry_interval}s...")
                time.sleep(retry_interval)
                continue
            if analyst_agent is None:
                analyst_agent = AnalystAgent(
                    doris_client,
                    build_api_config,
                    metric_provider=datasource_handler,
                )
            if not analyst_agent.init_tables():
                print("Analysis system tables are not ready yet")
                print(f"Retry in {retry_interval}s...")
                time.sleep(retry_interval)
                continue
            if analysis_dispatcher is None:
                analysis_dispatcher = AnalysisDispatcher()
            if analysis_scheduler is None:
                analysis_scheduler = AnalysisScheduler(
                    analyst_agent,
                    doris_client,
                    dispatcher=analysis_dispatcher,
                )
            else:
                analysis_scheduler.agent = analyst_agent
                analysis_scheduler.db = doris_client
                analysis_scheduler.dispatcher = analysis_dispatcher
            if not analysis_scheduler.init_tables():
                print("Analysis schedule tables are not ready yet")
                print(f"Retry in {retry_interval}s...")
                time.sleep(retry_interval)
                continue
            print("System tables initialized")

            print("=" * 60)
            print("Doris API Gateway ready")
            print(f"Database: {db_name}")
            print(f"API: http://{API_HOST}:{API_PORT}")
            print(f"Docs: http://{API_HOST}:{API_PORT}/docs")
            print("=" * 60)
            return True

        except Exception as e:
            print(f"Connect failed: {e}")
            print(f"Retry in {retry_interval}s...")
            time.sleep(retry_interval)

@app.middleware("http")
async def api_guard_middleware(request: Request, call_next):
    _no_auth_paths = {"/api/health", "/api/config"}
    if request.url.path.startswith("/api") and request.url.path not in _no_auth_paths:
        expected_api_key = os.getenv("SMATRIX_API_KEY")
        if not expected_api_key:
            return JSONResponse(
                status_code=503,
                content={"success": False, "message": "SMATRIX_API_KEY is not configured."},
            )

        provided_api_key = request.headers.get("X-API-Key")
        authorization = request.headers.get("Authorization", "")
        if authorization.lower().startswith("bearer "):
            provided_api_key = authorization.split(" ", 1)[1].strip()

        if provided_api_key != expected_api_key:
            return JSONResponse(
                status_code=401,
                content={"success": False, "message": "Unauthorized"},
            )

        if not doris_ready:
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "message": "Doris FE is not ready yet, please retry later."
                },
            )
    return await call_next(request)


# ============ 数据模型 ============

class ExecuteRequest(BaseModel):
    """统一执行请求"""
    action: str = Field(..., description="操作类型: query/sentiment/classify/extract/stats/similarity/translate/summarize/mask/fixgrammar/generate/filter")
    table: Optional[str] = Field(None, description="表名")
    column: Optional[str] = Field(None, description="列名")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="其他参数")
    
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "action": "sentiment",
            "table": "customer_feedback",
            "column": "feedback_text",
            "params": {
                "limit": 50
            }
        }
    })


_RESOURCE_NAME_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_\-]{0,127}$')


def _escape_sql_str(value: str) -> str:
    """转义 SQL 字符串值中的单引号，防止注入。"""
    return str(value).replace("'", "''")


def _redact_sensitive(value: str) -> str:
    text = str(value)
    text = re.sub(
        r"(?i)(api[_\.-]?key['\"\s:=]+)([^'\"\s,\)]+)",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(
        r"(?i)(authorization['\"\s:=]+bearer\s+)([^'\"\s,\)]+)",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(r"sk-[A-Za-z0-9_\-]{6,}", "[REDACTED_API_KEY]", text)
    return text


def _summarize_error(error: Exception) -> str:
    return _redact_sensitive(str(error).splitlines()[0])[:500]


def _format_sse_event(event: str, data: Dict[str, Any]) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def _ensure_llm_route_available(
    *,
    request_resource_name: Optional[str],
    llm_execution_mode: str,
    api_key: Optional[str],
    api_config: Dict[str, Any],
) -> None:
    if llm_execution_mode != "direct_api" or api_key:
        return

    if request_resource_name and not api_config.get("resource_found", False):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error_code": "llm_resource_not_found",
                "message": f"LLM resource '{request_resource_name}' not found",
                "llm_execution_mode": llm_execution_mode,
                "resource_name": request_resource_name,
            },
        )

    if request_resource_name and not api_config.get("resource_api_key_configured", False):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error_code": "llm_resource_key_not_configured",
                "message": (
                    f"LLM resource '{request_resource_name}' has no usable key and direct API key is missing"
                ),
                "llm_execution_mode": llm_execution_mode,
                "resource_name": request_resource_name,
            },
        )

    raise HTTPException(
        status_code=400,
        detail={
            "success": False,
            "error_code": "missing_api_key",
            "message": (
                "API key not provided. Please provide 'api_key' in request "
                "or set DEEPSEEK_API_KEY/OPENAI_API_KEY environment variable"
            ),
            "llm_execution_mode": llm_execution_mode,
            "resource_name": api_config.get("resource_name"),
        },
    )


def _resolve_default_nlq_kernel() -> str:
    configured = str(os.getenv("DC_NLQ_DEFAULT_KERNEL", "auto") or "").strip().lower()
    if configured not in {"legacy", "native", "auto"}:
        return "auto"
    return configured


DEFAULT_AI_DIMENSIONS = 1536
DEEPSEEK_CANONICAL_ENDPOINT = "https://api.deepseek.com/chat/completions"


def _normalize_llm_endpoint(provider_type: str, endpoint: str) -> str:
    raw = str(endpoint or "").strip()
    provider = str(provider_type or "").strip().lower()
    if provider != "deepseek":
        return raw
    if not raw:
        return DEEPSEEK_CANONICAL_ENDPOINT

    try:
        parsed = urlsplit(raw)
        path = (parsed.path or "").rstrip("/").lower()
        if parsed.scheme and parsed.netloc and path in {"", "/", "/v1", "/v1/chat/completions", "/chat/completions"}:
            return f"{parsed.scheme}://{parsed.netloc}/chat/completions"
    except Exception:
        return raw
    return raw


def _normalize_llm_temperature(temperature: Optional[float]) -> Optional[float]:
    if temperature is None:
        return None
    try:
        value = float(temperature)
    except (TypeError, ValueError):
        return None
    if value < 0 or value > 1:
        return None
    return value


def _normalize_doris_provider_type(provider_type: str) -> str:
    normalized = str(provider_type or "").strip()
    return normalized.upper()


def _build_llm_resource_properties(
    req: "LLMConfigRequest",
    *,
    include_api_key: bool,
    include_type: bool = True,
) -> List[str]:
    properties = []
    if include_type:
        properties.append("'type' = 'ai'")
    normalized_endpoint = _normalize_llm_endpoint(req.provider_type, req.endpoint)
    provider_for_doris = _normalize_doris_provider_type(req.provider_type)
    properties.extend([
        f"'ai.provider_type' = '{_escape_sql_str(provider_for_doris)}'",
        f"'ai.endpoint' = '{_escape_sql_str(normalized_endpoint)}'",
        f"'ai.model_name' = '{_escape_sql_str(req.model_name)}'",
        f"'ai.dimensions' = {DEFAULT_AI_DIMENSIONS}",
    ])

    if include_api_key and req.api_key:
        properties.append(f"'ai.api_key' = '{_escape_sql_str(req.api_key)}'")
    normalized_temperature = _normalize_llm_temperature(req.temperature)
    if normalized_temperature is not None:
        properties.append(f"'ai.temperature' = {normalized_temperature}")
    if req.max_tokens is not None:
        properties.append(f"'ai.max_tokens' = {req.max_tokens}")
    return properties


def _log_llm_config_action(action: str, req: "LLMConfigRequest") -> None:
    logger.info(
        "LLM resource %s requested: resource_name=%s provider=%s endpoint=%s model=%s api_key_configured=%s",
        action,
        req.resource_name,
        req.provider_type,
        req.endpoint,
        req.model_name,
        bool(req.api_key),
    )


class LLMConfigRequest(BaseModel):
    """LLM 配置请求"""
    resource_name: str = Field(..., description="资源名称（仅允许字母、数字、下划线、连字符）")
    provider_type: str = Field(..., description="厂商类型: openai/deepseek/qwen/zhipu/local等")
    endpoint: str = Field(..., description="API 端点")
    model_name: str = Field(..., description="模型名称")
    api_key: Optional[str] = Field(None, description="API 密钥")
    temperature: Optional[float] = Field(None, description="温度参数 0-1")
    max_tokens: Optional[int] = Field(None, description="最大 token 数")

    @field_validator('resource_name')
    @classmethod
    def validate_resource_name(cls, v: str) -> str:
        if not _RESOURCE_NAME_RE.match(v):
            raise ValueError(
                "resource_name 只允许字母开头，包含字母、数字、下划线或连字符，长度 1-128"
            )
        return v

    @field_validator('provider_type')
    @classmethod
    def normalize_provider_type(cls, v: str) -> str:
        return str(v or "").strip().lower()

    @model_validator(mode="after")
    def normalize_fields(self):
        self.endpoint = _normalize_llm_endpoint(self.provider_type, self.endpoint)
        self.temperature = _normalize_llm_temperature(self.temperature)
        if self.max_tokens is not None and self.max_tokens <= 0:
            self.max_tokens = None
        if self.api_key is not None:
            self.api_key = str(self.api_key).strip() or None
        self.model_name = str(self.model_name or "").strip()
        return self

    model_config = ConfigDict(
        protected_namespaces=(),
        json_schema_extra={
            "example": {
                "resource_name": "my_openai",
                "provider_type": "openai",
                "endpoint": "https://api.openai.com/v1/chat/completions",
                "model_name": "gpt-4",
                "api_key": "sk-xxxxx"
            }
        },
    )


class AnalysisTableRequest(BaseModel):
    depth: str = Field(default=ANALYST_DEFAULT_DEPTH, description="Analysis depth: quick/standard/deep/expert")
    resource_name: Optional[str] = Field(default=None, description="Optional LLM resource name")

    @field_validator("depth")
    @classmethod
    def validate_depth(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"quick", "standard", "deep", "expert"}:
            raise ValueError("depth must be one of quick, standard, deep, expert")
        return normalized


class AnalysisReplayRequest(BaseModel):
    resource_name: Optional[str] = Field(default=None, description="Optional LLM resource name")


class AnalysisForecastExternalSignal(BaseModel):
    source: str = Field(..., description="External signal source identifier")
    signal_key: str = Field(..., description="Signal name from the external source")
    granularity: Optional[str] = Field(default=None, description="Optional external signal granularity")


class AnalysisForecastRequest(BaseModel):
    metric_key: str = Field(..., description="Internal metric key generated by the insight module")
    horizon_steps: int = Field(default=7, ge=1, le=180, description="Forecast horizon step count")
    granularity: str = Field(default="day", description="Forecast granularity: day/week/month")
    horizon_unit: Optional[str] = Field(default=None, description="Optional alias of granularity for compatibility")
    start_at: Optional[str] = Field(default=None, description="Optional ISO datetime inclusive start")
    end_at: Optional[str] = Field(default=None, description="Optional ISO datetime inclusive end")
    lookback_points: int = Field(default=180, ge=30, le=2000, description="Default lookback bucket count")
    filters: Dict[str, Any] = Field(default_factory=dict, description="Optional equality/IN filter map")
    resource_name: Optional[str] = Field(default=None, description="Optional LLM resource name")
    external_signals: List[AnalysisForecastExternalSignal] = Field(
        default_factory=list,
        description="Optional external signals for future algorithm integration",
    )

    @field_validator("metric_key")
    @classmethod
    def validate_metric_key(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("metric_key must not be empty")
        return normalized

    @field_validator("granularity")
    @classmethod
    def validate_granularity(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"day", "week", "month"}:
            raise ValueError("granularity must be one of day, week, month")
        return normalized

    @field_validator("horizon_unit")
    @classmethod
    def validate_horizon_unit(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        normalized = str(value).strip().lower()
        if normalized not in {"day", "week", "month"}:
            raise ValueError("horizon_unit must be one of day, week, month")
        return normalized

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("filters must be an object")
        validated: Dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key or "").strip()
            if not key:
                raise ValueError("filter key must not be empty")
            if isinstance(raw_value, list):
                validated[key] = raw_value
            elif raw_value is None or isinstance(raw_value, (str, int, float, bool)):
                validated[key] = raw_value
            else:
                raise ValueError(f"unsupported filter value type for '{key}'")
        return validated

    @model_validator(mode="after")
    def sync_horizon_unit_and_granularity(self):
        if not self.horizon_unit:
            self.horizon_unit = self.granularity
            return self
        if self.horizon_unit != self.granularity:
            raise ValueError("horizon_unit must equal granularity when provided")
        return self


class MetricDefinitionRequest(BaseModel):
    metric_key: str = Field(..., description="Metric identifier")
    display_name: str = Field(..., description="Human-readable metric name")
    description: Optional[str] = Field(default="", description="Metric description")
    table_name: str = Field(..., description="Physical source table")
    time_field: str = Field(..., description="Time field used for series axis")
    value_field: Optional[str] = Field(default=None, description="Value field for aggregation")
    aggregation_expression: Optional[str] = Field(
        default=None,
        description="Optional explicit aggregation expression, e.g. SUM(amount)",
    )
    aggregation: str = Field(default="sum", description="Aggregation method")
    default_grain: str = Field(default="day", description="Default time grain")
    dimensions: List[str] = Field(default_factory=list, description="Allowed filter dimensions")

    @field_validator("metric_key", "display_name", "table_name", "time_field")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("aggregation")
    @classmethod
    def validate_metric_aggregation(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"sum", "avg", "min", "max", "count", "count_distinct"}:
            raise ValueError("aggregation must be one of sum, avg, min, max, count, count_distinct")
        return normalized

    @field_validator("default_grain")
    @classmethod
    def validate_metric_default_grain(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"day", "week", "month"}:
            raise ValueError("default_grain must be one of day, week, month")
        return normalized

    @field_validator("aggregation_expression")
    @classmethod
    def normalize_aggregation_expression(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("value_field")
    @classmethod
    def normalize_value_field(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("dimensions")
    @classmethod
    def validate_metric_dimensions(cls, value: List[str]) -> List[str]:
        if value is None:
            return []
        normalized: List[str] = []
        seen = set()
        for item in value:
            candidate = str(item or "").strip()
            if not candidate or candidate in seen:
                continue
            normalized.append(candidate)
            seen.add(candidate)
        return normalized


class MetricSeriesRequest(BaseModel):
    metric_key: str = Field(..., description="Metric identifier")
    start_time: Optional[str] = Field(default=None, description="Inclusive series start time")
    end_time: Optional[str] = Field(default=None, description="Inclusive series end time")
    grain: Optional[str] = Field(default=None, description="Override time grain: day/week/month")
    filters: Dict[str, Any] = Field(default_factory=dict, description="Optional dimension filters")
    limit: int = Field(default=5000, ge=1, le=20000, description="Maximum point count")

    @field_validator("metric_key")
    @classmethod
    def validate_series_metric_key(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("metric_key must not be empty")
        return normalized

    @field_validator("grain")
    @classmethod
    def validate_series_grain(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        normalized = str(value).strip().lower()
        if normalized not in {"day", "week", "month"}:
            raise ValueError("grain must be one of day, week, month")
        return normalized

    @field_validator("filters")
    @classmethod
    def validate_series_filters(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("filters must be an object")
        validated: Dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key or "").strip()
            if not key:
                raise ValueError("filter key must not be empty")
            if isinstance(raw_value, list):
                validated[key] = raw_value
            elif isinstance(raw_value, dict):
                validated[key] = raw_value
            elif raw_value is None or isinstance(raw_value, (str, int, float, bool)):
                validated[key] = raw_value
            else:
                raise ValueError(f"unsupported filter value type for '{key}'")
        return validated


ANALYSIS_CONTRACT_VERSION = "insight.report.read.v1"
ANALYSIS_SUMMARY_CONTRACT_VERSION = "insight.report.summary.v1"
FORECAST_BOUNDARY_CONTRACT_VERSION = "insight.forecast.boundary.v1"
COLLABORATION_BOUNDARY_CONTRACT_VERSION = "insight.collaboration.boundary.v1"
METRIC_FOUNDATION_CONTRACT_VERSION = "foundation.metric.read.v1"


def _analysis_contract_blueprint() -> Dict[str, Any]:
    return {
        "contract_version": ANALYSIS_CONTRACT_VERSION,
        "summary_contract_version": ANALYSIS_SUMMARY_CONTRACT_VERSION,
        "report_read_model": {
            "required_fields": [
                "summary",
                "insights",
                "top_insights",
                "anomalies",
                "recommendations",
                "action_items",
                "insight_count",
                "anomaly_count",
            ],
            "identity_fields": [
                "id",
                "trigger_type",
                "depth",
                "schedule_id",
                "history_id",
                "table_names",
            ],
            "status_fields": [
                "status",
                "failed_step_count",
                "error_message",
                "duration_ms",
                "created_at",
            ],
        },
        "contracts": {
            "table_analysis": {
                "endpoint": "/api/analysis/table/{table_name}",
                "response_model": "report_read_model",
            },
            "replay_analysis": {
                "endpoint": "/api/analysis/replay/{history_id}",
                "response_model": "report_read_model",
                "notes": [
                    "trigger_type is fixed to history_replay",
                    "history_id is required in response payload",
                    "summary fields are aligned with report detail and list contracts",
                ],
            },
            "report_list": {
                "endpoint": "/api/analysis/reports",
                "item_model": "report_summary_model",
                "summary_fields": [
                    "summary",
                    "top_insights",
                    "anomalies",
                    "recommendations",
                    "action_items",
                    "insight_count",
                    "anomaly_count",
                ],
            },
            "report_detail": {
                "endpoint": "/api/analysis/reports/{report_id}",
                "response_model": "report_read_model",
            },
            "report_summary": {
                "endpoint": "/api/analysis/reports/{report_id}/summary",
                "response_model": "report_summary_model",
            },
            "latest_report": {
                "endpoint": "/api/analysis/reports/latest/{table_name}",
                "response_model": "report_read_model",
            },
        },
    }


def _forecast_boundary_blueprint() -> Dict[str, Any]:
    return {
        "contract_version": FORECAST_BOUNDARY_CONTRACT_VERSION,
        "status": "mvp_available",
        "metric_key_semantics": {
            "primary": "registered metric_key from /api/internal/metrics",
            "compatibility": "legacy <table>.<agg>(<value_column>|*)@<time_column> (controlled fallback)",
        },
        "input_boundary": {
            "internal_metrics": [
                "metric_key",
                "granularity",
                "horizon_steps",
                "horizon_unit",
                "start_at",
                "end_at",
                "lookback_points",
                "filters",
            ],
            "future_external_signals": [
                "external_signals[].source",
                "external_signals[].signal_key",
                "external_signals[].granularity",
            ],
        },
        "minimal_output_model": {
            "fields": [
                "forecast_id",
                "metric_key",
                "horizon",
                "points",
                "assumptions",
                "backtest_summary",
                "model_info",
            ],
            "point_fields": ["ts", "value", "lower", "upper", "confidence"],
            "backtest_fields": ["status", "holdout_points", "train_points", "mae", "rmse", "mape", "residual_std"],
            "model_info_fields": [
                "name",
                "version",
                "status",
                "granularity",
                "aggregation",
                "table_name",
                "time_column",
                "value_column",
                "training_points",
                "history_points",
            ],
        },
    }


def _collaboration_boundary_blueprint() -> Dict[str, Any]:
    return {
        "contract_version": COLLABORATION_BOUNDARY_CONTRACT_VERSION,
        "boundaries": {
            "SGA-Web": {
                "role": "Provide external business context and structured supplement metadata.",
                "input_from_insight": ["report_id", "summary", "top_insights", "action_items"],
                "output_to_insight": ["context_cards", "operator_feedback", "business_annotations"],
            },
            "SGA-EastFactory": {
                "role": "Provide trend/sentiment/exogenous signals and source confidence.",
                "input_from_insight": ["metric_key", "table_names", "time_window"],
                "output_to_insight": ["signal_series", "event_tags", "confidence_notes"],
            },
            "Evole": {
                "role": "Future optimization operator layer behind forecast boundary.",
                "placement": "Downstream of forecast interface, not inside query or insight core pipeline.",
                "input_contract": ["forecast points", "constraints", "objective hints"],
                "output_contract": ["candidate actions", "expected impact", "risk tags"],
            },
        },
    }


def _metric_foundation_blueprint() -> Dict[str, Any]:
    return {
        "contract_version": METRIC_FOUNDATION_CONTRACT_VERSION,
        "status": "mvp_available",
        "metric_definition_model": {
            "required_fields": [
                "metric_key",
                "display_name",
                "table_name",
                "time_field",
                "aggregation",
                "default_grain",
                "dimensions",
            ],
            "optional_fields": [
                "description",
                "value_field",
                "aggregation_expression",
            ],
            "supported_aggregations": ["sum", "avg", "min", "max", "count", "count_distinct"],
            "supported_grains": ["day", "week", "month"],
        },
        "series_read_model": {
            "input_fields": ["metric_key", "start_time", "end_time", "grain", "filters", "limit"],
            "output_fields": [
                "metric_key",
                "display_name",
                "table_name",
                "time_field",
                "value_field",
                "aggregation",
                "default_grain",
                "grain",
                "filters",
                "time_range",
                "time_normalization",
                "availability",
                "points",
                "count",
            ],
            "point_fields": ["ts", "value"],
            "time_normalization_fields": [
                "strategy",
                "source_field_type",
                "normalized_value_type",
                "null_or_unparseable_filtered",
                "time_filter_field",
            ],
            "blocking_reason_codes": [
                "missing_time_field",
                "time_field_not_found",
                "time_field_not_temporal",
                "time_field_unparseable_values",
                "unsupported_default_grain",
                "value_field_not_found",
                "value_field_not_numeric",
            ],
        },
        "series_time_semantics": {
            "normalization_rule": (
                "All time filters and buckets use normalized DATETIME values (CAST/TRY_CAST), "
                "not raw string comparison."
            ),
            "bucket_rule": "day/week/month use one normalized time bucket path in datasource_handler.get_metric_series().",
            "invalid_time_handling": (
                "Rows with null or unparseable normalized time are filtered out in series query; "
                "metrics with sampled unparseable time values are blocked before forecast_ready."
            ),
        },
        "forecast_mvp_boundary": {
            "included_when": [
                "table schema is readable",
                "time_field exists and is temporal",
                "aggregation is supported",
                "default_grain is day/week/month",
                "required value_field exists for aggregation",
                "dimensions all exist in source table",
                "string-like time_field has no sampled unparseable values",
            ],
            "excluded_when": [
                "time_field missing/not temporal",
                "time_field has unparseable sampled values",
                "aggregation or grain unsupported",
                "value_field missing/not found for required aggregations",
                "dimension field not found",
                "aggregation_expression invalid",
            ],
        },
        "sync_incremental_mvp": {
            "supported_strategies": ["full", "incremental"],
            "incremental_requirements": [
                "sync_strategy=incremental",
                "incremental_time_field provided",
                "incremental_time_field exists in source table",
                "incremental_time_field type is temporal (date/datetime/timestamp/time)",
            ],
            "fallback_behavior": (
                "If incremental requirements are not met, execution falls back to full sync and "
                "returns fallback_reason/explanation in sync_capability."
            ),
            "capability_fields": [
                "requested_strategy",
                "effective_strategy",
                "fallback_to_full",
                "fallback_reason",
                "explanation",
                "window.start",
                "window.end",
                "incremental_time_field",
                "source_time_field_type",
            ],
        },
        "stable_read_surfaces": {
            "foundation": {
                "contracts": "/api/foundation/metrics/contracts",
                "metrics": "/api/foundation/metrics",
                "metric_detail": "/api/foundation/metrics/{metric_key}",
                "series": "/api/foundation/metrics/series",
            },
            "internal_for_agent_c": {
                "contracts": "/api/internal/metrics/contracts",
                "metrics": "/api/internal/metrics",
                "metric_detail": "/api/internal/metrics/{metric_key}",
                "series": "/api/internal/metrics/series",
            },
        },
    }


def _require_analyst_agent() -> AnalystAgent:
    if analyst_agent is None:
        raise HTTPException(status_code=503, detail="Analyst agent is not initialized yet")
    return analyst_agent


class AnalysisScheduleCreateRequest(BaseModel):
    name: str = Field(..., description="Schedule name")
    tables: List[str] = Field(..., description="One or more table names")
    depth: str = Field(default=ANALYST_DEFAULT_DEPTH, description="Analysis depth: quick/standard/deep/expert")
    resource_name: Optional[str] = Field(default=None, description="Optional LLM resource name")
    schedule_type: str = Field(..., description="Schedule type: hourly/daily/weekly/monthly")
    schedule_hour: int = Field(default=8, description="Hour (0-23)")
    schedule_minute: int = Field(default=0, description="Minute (0-59)")
    schedule_day_of_week: int = Field(default=1, description="ISO day of week (1-7)")
    schedule_day_of_month: int = Field(default=1, description="Day of month (1-31)")
    timezone: str = Field(default="UTC", description="IANA timezone name")
    delivery: Optional[Dict[str, Any]] = Field(default=None, description="Delivery configuration")
    enabled: bool = Field(default=True, description="Whether the schedule is enabled")

    @field_validator("depth")
    @classmethod
    def validate_schedule_depth(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"quick", "standard", "deep", "expert"}:
            raise ValueError("depth must be one of quick, standard, deep, expert")
        return normalized

    @field_validator("schedule_type")
    @classmethod
    def validate_schedule_type(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"hourly", "daily", "weekly", "monthly"}:
            raise ValueError("schedule_type must be one of hourly, daily, weekly, monthly")
        return normalized

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        ZoneInfo(value or "UTC")
        return value


class AnalysisScheduleUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, description="Schedule name")
    tables: Optional[List[str]] = Field(default=None, description="One or more table names")
    depth: Optional[str] = Field(default=None, description="Analysis depth: quick/standard/deep/expert")
    resource_name: Optional[str] = Field(default=None, description="Optional LLM resource name")
    schedule_type: Optional[str] = Field(default=None, description="Schedule type: hourly/daily/weekly/monthly")
    schedule_hour: Optional[int] = Field(default=None, description="Hour (0-23)")
    schedule_minute: Optional[int] = Field(default=None, description="Minute (0-59)")
    schedule_day_of_week: Optional[int] = Field(default=None, description="ISO day of week (1-7)")
    schedule_day_of_month: Optional[int] = Field(default=None, description="Day of month (1-31)")
    timezone: Optional[str] = Field(default=None, description="IANA timezone name")
    delivery: Optional[Dict[str, Any]] = Field(default=None, description="Delivery configuration")
    enabled: Optional[bool] = Field(default=None, description="Whether the schedule is enabled")

    @field_validator("depth")
    @classmethod
    def validate_optional_schedule_depth(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        normalized = value.strip().lower()
        if normalized not in {"quick", "standard", "deep", "expert"}:
            raise ValueError("depth must be one of quick, standard, deep, expert")
        return normalized

    @field_validator("schedule_type")
    @classmethod
    def validate_optional_schedule_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        normalized = value.strip().lower()
        if normalized not in {"hourly", "daily", "weekly", "monthly"}:
            raise ValueError("schedule_type must be one of hourly, daily, weekly, monthly")
        return normalized

    @field_validator("timezone")
    @classmethod
    def validate_optional_timezone(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        ZoneInfo(value)
        return value


def _require_analysis_scheduler() -> AnalysisScheduler:
    if analysis_scheduler is None:
        raise HTTPException(status_code=503, detail="Analysis scheduler is not initialized yet")
    return analysis_scheduler


def _require_analysis_dispatcher() -> AnalysisDispatcher:
    if analysis_dispatcher is None:
        raise HTTPException(status_code=503, detail="Analysis dispatcher is not initialized yet")
    return analysis_dispatcher


def _log_auto_analysis_result(future) -> None:
    try:
        future.result()
    except Exception as exc:
        logger.warning("auto analysis failed: %s", exc)


def _run_auto_analysis(history_id: str, resource_name: Optional[str]) -> None:
    if analyst_agent is None:
        return
    analyst_agent.replay_from_history(history_id, resource_name)


class NLQueryContextEnvelope(BaseModel):
    user_id: Optional[str] = None
    source: str = "api"
    labels: List[str] = Field(default_factory=list)
    attributes: Dict[str, Any] = Field(default_factory=dict)


class NLQueryRequest(BaseModel):
    """自然语言查询请求"""
    query: str = Field(..., min_length=1, description="自然语言问题")
    table_names: List[str] = Field(default_factory=list, description="可选：限定查询范围的表名列表")
    resource_name: Optional[str] = Field(None, description="LLM 资源名称,不指定则使用第一个可用资源")
    api_key: Optional[str] = Field(None, description="可选：覆盖默认 API Key")
    model: Optional[str] = Field(None, description="可选：覆盖默认模型")
    base_url: Optional[str] = Field(None, description="可选：覆盖默认 OpenAI-compatible Base URL")
    include_trace: bool = Field(default=True, description="是否返回查询解释链路")
    kernel: Optional[str] = Field(
        default=None,
        description="Query kernel selector: native|legacy|auto. Empty means using DC_NLQ_DEFAULT_KERNEL",
    )
    max_repair_attempts: int = Field(default=2, ge=0, le=3, description="SQL 修复最大重试次数")

    request_id: Optional[str] = Field(None, description="Optional request id for audit trace")
    session_id: Optional[str] = Field(None, description="Optional session id for audit trace")
    context: NLQueryContextEnvelope = Field(default_factory=NLQueryContextEnvelope)

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        normalized = dict(value)
        if not normalized.get("query") and normalized.get("question"):
            normalized["query"] = normalized.get("question")

        if not normalized.get("table_names") and normalized.get("table_name"):
            normalized["table_names"] = [normalized.get("table_name")]

        return normalized

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        cleaned = (value or "").strip()
        if not cleaned:
            raise ValueError("query must not be empty")
        return cleaned

    @field_validator("kernel")
    @classmethod
    def validate_kernel(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if not normalized:
            return None
        if normalized not in {"native", "legacy", "auto"}:
            raise ValueError("kernel must be one of native, legacy, auto")
        return normalized

    @field_validator("table_names", mode="before")
    @classmethod
    def normalize_table_names(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            raise ValueError("table_names must be a string or a list of strings")

        normalized: List[str] = []
        seen = set()
        for item in value:
            table_name = str(item or "").strip()
            if table_name and table_name not in seen:
                normalized.append(table_name)
                seen.add(table_name)
        return normalized

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "query": "2022年的机构中来自于广东的有多少个?分别是来自于广东那几个城市每个城市的占比是多少?",
            "table_names": ["中国环保公益组织现状调研数据2022"],
            "resource_name": "my_deepseek",
            "include_trace": True,
        }
    })


class NativeSSEQueryRequest(NLQueryRequest):
    kernel: str = Field(default="native", description="Native SSE kernel selector: native|auto")
    include_trace: bool = Field(default=False, description="SSE endpoint streams events and ignores trace payload")


class PlannerCandidateResponse(BaseModel):
    table_name: str
    score: int
    matched_terms: List[str] = Field(default_factory=list)
    selected: bool = False
    rank: int = 0


class PlannerTraceResponse(BaseModel):
    normalized_query: str
    intent: str
    requested_tables: List[str] = Field(default_factory=list)
    selected_tables: List[str] = Field(default_factory=list)
    needs_join: bool = False
    fallback_used: bool = False
    routing_reason: str = ""
    candidates: List[PlannerCandidateResponse] = Field(default_factory=list)


class SubtaskTraceResponse(BaseModel):
    table_name: str
    question: str
    strategy: str
    sql: str
    prompt_attempts: int = 0
    metadata_available: bool = False
    schema_column_count: int = 0
    example_count: int = 0
    ddl_count: int = 0
    documentation_count: int = 0
    retrieval_source_labels: List[str] = Field(default_factory=list)
    candidate_retrieval_source_labels: List[str] = Field(default_factory=list)
    memory_hit: bool = False
    candidate_memory_hit: bool = False
    memory_fallback_used: bool = False
    memory_source: str = ""
    query_intent: str = "unknown"
    candidate_intent: str = "unknown"
    intent_matched: bool = False
    reuse_gate_reason: str = ""
    phases: List[str] = Field(default_factory=list)
    target_only: bool = False
    referenced_tables: List[str] = Field(default_factory=list)


class OrchestrationTraceResponse(BaseModel):
    strategy: str
    input_tables: List[str] = Field(default_factory=list)
    candidate_relationship_count: int = 0
    selected_relationship: Optional[Dict[str, Any]] = None


class RepairAttemptTraceResponse(BaseModel):
    attempt: int
    error_message: str
    failed_sql: str
    repaired_sql: str
    succeeded: bool = False
    ddl_count: int = 0
    model: str = ""
    base_url: str = ""


class RepairTraceResponse(BaseModel):
    attempted: bool = False
    max_attempts: int = 0
    attempts: List[RepairAttemptTraceResponse] = Field(default_factory=list)


class ExecutionTraceResponse(BaseModel):
    row_count: int = 0
    history_id: Optional[str] = None
    history_status: str = ""
    llm_execution_mode: str = ""
    resource_name: Optional[str] = None
    model: str = ""


class NativeToolCallTraceResponse(BaseModel):
    tool_name: str
    success: bool = False
    error: Dict[str, Any] = Field(default_factory=dict)


class NativeMemoryTraceResponse(BaseModel):
    example_count: int = 0
    memory_hit: bool = False
    sources_attempted: List[str] = Field(default_factory=list)
    vanna_memory_hit: bool = False
    vanna_memory_source: str = ""
    confidence: float = 0.0
    used_as: str = "none"
    rejected_reason: str = ""
    candidate_count: int = 0
    candidate_examples: List[Dict[str, Any]] = Field(default_factory=list)
    chosen_candidate: Dict[str, Any] = Field(default_factory=dict)
    chosen_candidates: List[Dict[str, Any]] = Field(default_factory=list)
    rejected_candidates: List[Dict[str, Any]] = Field(default_factory=list)
    query_intent: str = "unknown"
    candidate_intent: str = "unknown"
    intent_matched: bool = False
    reuse_gate_reason: str = ""
    subtasks: List[Dict[str, Any]] = Field(default_factory=list)


class NativeTraceResponse(BaseModel):
    kernel: str = "legacy"
    runtime_reused: bool = False
    runtime_cache_key: str = ""
    tools_called: List[NativeToolCallTraceResponse] = Field(default_factory=list)
    memory: NativeMemoryTraceResponse = Field(default_factory=NativeMemoryTraceResponse)
    audit_events: List[Dict[str, Any]] = Field(default_factory=list)
    fallback_reason: str = ""


class RetrievalTraceResponse(BaseModel):
    example_count: int = 0
    ddl_count: int = 0
    documentation_count: int = 0
    source_labels: List[str] = Field(default_factory=list)
    memory_hit: bool = False
    memory_fallback_used: bool = False


class ContextTraceResponse(BaseModel):
    request_id: str
    session_id: str
    user_id: Optional[str] = None
    source: str = "api"
    labels: List[str] = Field(default_factory=list)
    attributes: Dict[str, Any] = Field(default_factory=dict)


class PhaseTraceResponse(BaseModel):
    phase: str
    status: str = "ok"
    details: Dict[str, Any] = Field(default_factory=dict)
    source_labels: List[str] = Field(default_factory=list)


class NLQueryTraceResponse(BaseModel):
    trace_id: str
    context: ContextTraceResponse
    phases: List[PhaseTraceResponse] = Field(default_factory=list)
    planner: PlannerTraceResponse
    retrieval: RetrievalTraceResponse = Field(default_factory=RetrievalTraceResponse)
    subtasks: List[SubtaskTraceResponse] = Field(default_factory=list)
    orchestration: OrchestrationTraceResponse
    repair: RepairTraceResponse
    execution: ExecutionTraceResponse
    native: NativeTraceResponse = Field(default_factory=NativeTraceResponse)


class NLQueryResponse(BaseModel):
    success: bool = True
    schema_version: str = Field(default="nlq.v1")
    query: str
    intent: str
    table_names: List[str] = Field(default_factory=list)
    sql: str
    data: List[Dict[str, Any]] = Field(default_factory=list)
    count: int = 0
    history_id: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
    trace: Optional[NLQueryTraceResponse] = None


# ============ API 路由 ============

@app.get("/")
async def root():
    """健康检查"""
    return {
        "service": "Doris API Gateway",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/api/health")
async def health_check():
    """检查 Doris 连接状态"""
    try:
        result = doris_client.execute_query("SELECT 1 AS health")
        return {
            "success": True,
            "doris_connected": True,
            "message": "Doris connection OK"
        }
    except Exception as e:
        return {
            "success": False,
            "doris_connected": False,
            "error": str(e)
        }


@app.get("/api/config")
async def get_client_config():
    """
    返回前端初始化所需的公开配置（无需认证）。
    仅在同一内网/Docker 网络内可访问，不对公网暴露。
    """
    api_key = os.getenv("SMATRIX_API_KEY", "")
    return {
        "api_key": api_key,
        "has_api_key": bool(api_key),
    }


@app.post("/api/execute")
async def execute_action(req: ExecuteRequest):
    """
    统一执行接口
    
    支持的 action:
    - query: 普通查询
    - sentiment: 情感分析
    - classify: 文本分类
    - extract: 信息提取
    - stats: 统计分析
    - similarity: 语义相似度
    - translate: 文本翻译
    - summarize: 文本摘要
    - mask: 敏感信息脱敏
    - fixgrammar: 语法纠错
    - generate: 内容生成
    - filter: 布尔过滤
    """
    try:
        # 合并参数
        params = req.params or {}
        if req.table:
            params['table'] = req.table
        if req.column:
            params['column'] = req.column
        
        # 执行操作
        result = await action_handler.execute_async(req.action, params)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


@app.get("/api/tables")
async def list_tables():
    """获取所有表"""
    try:
        tables = doris_client.get_tables()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/query/catalog")
async def get_query_catalog():
    """获取面向业务语义的数据查询目录"""
    try:
        tables = await datasource_handler.list_query_catalog()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables/{table_name}/schema")
async def get_table_schema(table_name: str):
    """获取表结构"""
    try:
        schema = doris_client.get_table_schema(table_name)
        return {
            "success": True,
            "table": table_name,
            "schema": schema
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/llm/config")
async def create_llm_config(req: LLMConfigRequest):
    """创建 LLM 配置"""
    try:
        _log_llm_config_action("create", req)
        properties = _build_llm_resource_properties(req, include_api_key=True, include_type=True)
        properties_str = ',\n    '.join(properties)

        sql = f"""
        CREATE RESOURCE '{req.resource_name}'
        PROPERTIES (
            {properties_str}
        )
        """

        doris_client.execute_update(sql)

        return {
            "success": True,
            "message": f"LLM resource '{req.resource_name}' created successfully",
            "resource_name": req.resource_name,
        }

    except Exception as e:
        logger.error("LLM resource create failed: resource_name=%s error=%s", req.resource_name, _summarize_error(e))
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": _summarize_error(e),
                "message": f"LLM resource '{req.resource_name}' create failed",
                "resource_name": req.resource_name,
            }
        )


@app.put("/api/llm/config/{resource_name}")
async def update_llm_config(resource_name: str, req: LLMConfigRequest):
    """更新 LLM 配置，API key 留空时保留旧值。"""
    if not _RESOURCE_NAME_RE.match(resource_name):
        raise HTTPException(status_code=400, detail="Invalid resource_name format")
    if req.resource_name != resource_name:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": "resource_name mismatch",
                "message": "Path resource_name must match request body resource_name",
                "resource_name": resource_name,
            },
        )

    try:
        _log_llm_config_action("update", req)
        properties = _build_llm_resource_properties(req, include_api_key=bool(req.api_key), include_type=False)
        properties_str = ',\n    '.join(properties)
        sql = f"""
        ALTER RESOURCE '{resource_name}'
        PROPERTIES (
            {properties_str}
        )
        """
        doris_client.execute_update(sql)

        return {
            "success": True,
            "message": f"LLM resource '{resource_name}' updated successfully",
            "resource_name": resource_name,
        }
    except Exception as e:
        logger.error("LLM resource update failed: resource_name=%s error=%s", resource_name, _summarize_error(e))
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": _summarize_error(e),
                "message": f"LLM resource '{resource_name}' update failed",
                "resource_name": resource_name,
            },
        )


@app.get("/api/llm/config")
async def list_llm_configs():
    """获取所有 LLM 配置"""
    try:
        llm_resources = load_llm_resources()

        return {
            "success": True,
            "resources": llm_resources,
            "count": len(llm_resources)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/llm/config/{resource_name}/test")
async def test_llm_config(resource_name: str):
    """测试 LLM 配置"""
    if not _RESOURCE_NAME_RE.match(resource_name):
        raise HTTPException(status_code=400, detail="Invalid resource_name format")
    try:
        logger.info("LLM resource test requested: resource_name=%s", resource_name)
        # 使用简单的测试查询 (Doris 4.0 使用 AI_GENERATE 函数)
        sql = f"SELECT AI_GENERATE('{resource_name}', 'Hello') AS test_result"
        result = doris_client.execute_query(sql)

        return {
            "success": True,
            "message": "LLM resource is working",
            "resource_name": resource_name,
            "test_result": result[0] if result else None
        }
    except Exception as e:
        logger.warning("LLM resource test failed: resource_name=%s error=%s", resource_name, _summarize_error(e))
        return JSONResponse(
            status_code=200,
            content={
                "success": False,
                "error": _summarize_error(e),
                "message": "LLM resource connection test failed",
                "resource_name": resource_name,
            },
        )


@app.delete("/api/llm/config/{resource_name}")
async def delete_llm_config(resource_name: str):
    """删除 LLM 配置"""
    if not _RESOURCE_NAME_RE.match(resource_name):
        raise HTTPException(status_code=400, detail="Invalid resource_name format")
    try:
        sql = f"DROP RESOURCE '{resource_name}'"
        doris_client.execute_update(sql)

        return {
            "success": True,
            "message": f"LLM resource '{resource_name}' deleted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query/natural", response_model=NLQueryResponse)
async def natural_language_query(request: NLQueryRequest):
    """
    自然语言查询主链路。

    输入输出由 `NLQueryRequest` / `NLQueryResponse` 固定，
    默认返回 `nlq.v1` explainable trace，并兼容旧字段 `question` / `table_name`。
    """
    try:
        trace_id = str(uuid.uuid4())
        query = request.query
        requested_tables = list(request.table_names or [])
        warnings: List[str] = []
        kernel_default = _resolve_default_nlq_kernel()
        kernel_requested = (request.kernel or "").strip().lower()
        kernel_mode = kernel_requested or kernel_default
        request_id = (request.request_id or "").strip() or trace_id
        session_id = (request.session_id or "").strip() or f"session-{trace_id[:8]}"
        request_context = request.context or NLQueryContextEnvelope()
        context_trace = {
            "request_id": request_id,
            "session_id": session_id,
            "user_id": request_context.user_id,
            "source": request_context.source or "api",
            "labels": list(request_context.labels or []),
            "attributes": dict(request_context.attributes or {}),
        }
        phase_traces: List[Dict[str, Any]] = []
        native_fallback_reason = ""
        native_trace_payload: Dict[str, Any] = {
            "kernel": "legacy",
            "runtime_reused": False,
            "runtime_cache_key": "",
            "tools_called": [],
            "memory": {
                "example_count": 0,
                "memory_hit": False,
                "sources_attempted": [],
                "vanna_memory_hit": False,
                "vanna_memory_source": "",
                "confidence": 0.0,
                "used_as": "none",
                "rejected_reason": "",
                "candidate_count": 0,
                "candidate_examples": [],
                "chosen_candidate": {},
                "chosen_candidates": [],
                "rejected_candidates": [],
                "query_intent": "unknown",
                "candidate_intent": "unknown",
                "intent_matched": False,
                "reuse_gate_reason": "",
                "subtasks": [],
            },
            "audit_events": [],
            "fallback_reason": "",
        }

        api_config = build_api_config(
            request.resource_name,
            api_key=request.api_key,
            model=request.model,
            base_url=request.base_url,
        )
        api_key = api_config.get("api_key")
        model = api_config.get("model")
        base_url = api_config.get("base_url")
        llm_execution_mode = str(api_config.get("llm_execution_mode") or "direct_api")

        _ensure_llm_route_available(
            request_resource_name=request.resource_name,
            llm_execution_mode=llm_execution_mode,
            api_key=api_key,
            api_config=api_config,
        )

        logger.info(f"=== Natural language query: {query}")
        logger.info(
            "=== LLM route: mode=%s resource=%s model=%s base_url=%s",
            llm_execution_mode,
            api_config.get("resource_name"),
            model,
            base_url,
        )
        logger.info("=== Query context envelope: request_id=%s session_id=%s", request_id, session_id)
        phase_traces.append(
            {
                "phase": "llm_resolution",
                "status": "ok",
                "details": {
                    "llm_execution_mode": llm_execution_mode,
                    "resource_name": api_config.get("resource_name"),
                    "kernel_requested": kernel_requested or "(default)",
                    "kernel_default": kernel_default,
                    "kernel_effective": kernel_mode,
                },
                "source_labels": ["llm.config"],
            }
        )

        if kernel_mode in {"native", "auto"}:
            try:
                native_result = await run_native_query_kernel(
                    query=query,
                    requested_tables=requested_tables,
                    api_config=api_config,
                    doris_client=doris_client,
                    datasource_handler=datasource_handler,
                    request_id=request_id,
                    session_id=session_id,
                    user_id=request_context.user_id,
                    max_repair_attempts=request.max_repair_attempts,
                )
                native_trace_payload = dict(native_result.native_trace or {})
                native_trace_payload["kernel"] = "native"
                native_trace_payload["fallback_reason"] = ""
                phase_traces.extend(list(native_result.phase_traces or []))
                warnings.extend(list(native_result.warnings or []))

                history_id = native_result.history_id
                if (
                    history_id
                    and native_result.data
                    and os.getenv("ANALYST_AUTO_ANALYZE", "false").lower() == "true"
                    and analyst_agent is not None
                ):
                    auto_analysis_executor.submit(
                        _run_auto_analysis,
                        history_id,
                        request.resource_name,
                    ).add_done_callback(_log_auto_analysis_result)

                planner_candidates = [
                    PlannerCandidateResponse(
                        table_name=str(candidate.get("table_name") or ""),
                        score=int(candidate.get("score", 0)),
                        matched_terms=list(candidate.get("matched_terms") or []),
                        selected=bool(candidate.get("selected", False)),
                        rank=int(candidate.get("rank", 0)),
                    )
                    for candidate in native_result.plan.get("candidates", [])
                    if candidate.get("table_name")
                ]

                trace_payload = None
                if request.include_trace:
                    trace_payload = NLQueryTraceResponse(
                        trace_id=trace_id,
                        context=ContextTraceResponse(**context_trace),
                        phases=[PhaseTraceResponse(**phase_trace) for phase_trace in phase_traces],
                        planner=PlannerTraceResponse(
                            normalized_query=str(native_result.plan.get("normalized_question") or ""),
                            intent=str(native_result.plan.get("intent") or "list"),
                            requested_tables=requested_tables,
                            selected_tables=list(native_result.plan.get("tables") or []),
                            needs_join=bool(native_result.plan.get("needs_join", False)),
                            fallback_used=bool(native_result.plan.get("fallback_used", False)),
                            routing_reason=str(native_result.plan.get("routing_reason") or ""),
                            candidates=planner_candidates,
                        ),
                        retrieval=RetrievalTraceResponse(**native_result.retrieval_summary),
                        subtasks=[SubtaskTraceResponse(**subtask_trace) for subtask_trace in native_result.subtask_traces],
                        orchestration=OrchestrationTraceResponse(
                            strategy=str(native_result.orchestration_trace.get("strategy") or "passthrough"),
                            input_tables=list(native_result.orchestration_trace.get("input_tables") or []),
                            candidate_relationship_count=int(
                                native_result.orchestration_trace.get("candidate_relationship_count", 0)
                            ),
                            selected_relationship=native_result.orchestration_trace.get("selected_relationship"),
                        ),
                        repair=RepairTraceResponse(
                            attempted=bool(native_result.repair_trace.get("attempted", False)),
                            max_attempts=int(native_result.repair_trace.get("max_attempts", 0)),
                            attempts=[
                                RepairAttemptTraceResponse(**attempt)
                                for attempt in native_result.repair_trace.get("attempts", [])
                            ],
                        ),
                        execution=ExecutionTraceResponse(
                            row_count=len(native_result.data),
                            history_id=history_id,
                            history_status=native_result.history_status,
                            llm_execution_mode=str(api_config.get("llm_execution_mode") or ""),
                            resource_name=api_config.get("resource_name"),
                            model=str(model or ""),
                        ),
                        native=NativeTraceResponse(**native_trace_payload),
                    )

                return NLQueryResponse(
                    success=True,
                    schema_version="nlq.v1",
                    query=query,
                    intent=native_result.intent,
                    table_names=native_result.table_names,
                    sql=native_result.sql,
                    data=native_result.data,
                    count=len(native_result.data),
                    history_id=history_id,
                    warnings=warnings,
                    trace=trace_payload,
                )
            except NativeKernelExecutionError as native_error:
                fallback_detail = native_error.to_dict()
                fallback_detail["kernel_requested"] = kernel_mode
                if kernel_mode == "native":
                    raise HTTPException(status_code=500, detail=fallback_detail)
                native_fallback_reason = f"{native_error.code}: {native_error.message}"
                warnings.append(f"native kernel fallback to legacy: {native_fallback_reason}")
                tool_name = str((native_error.details or {}).get("tool_name") or "")
                native_trace_payload = {
                    "kernel": "legacy",
                    "runtime_reused": False,
                    "runtime_cache_key": "",
                    "tools_called": [
                        {
                            "tool_name": tool_name or "native_kernel",
                            "success": False,
                            "error": {
                                "code": native_error.code,
                                "message": native_error.message,
                            },
                        }
                    ],
                    "memory": {
                        "example_count": 0,
                        "memory_hit": False,
                        "sources_attempted": [],
                        "vanna_memory_hit": False,
                        "vanna_memory_source": "",
                        "confidence": 0.0,
                        "used_as": "degraded",
                        "rejected_reason": native_fallback_reason,
                        "candidate_count": 0,
                        "candidate_examples": [],
                        "chosen_candidate": {},
                        "chosen_candidates": [],
                        "rejected_candidates": [],
                        "query_intent": "unknown",
                        "candidate_intent": "unknown",
                        "intent_matched": False,
                        "reuse_gate_reason": "reuse_blocked_native_fallback",
                        "subtasks": [],
                    },
                    "audit_events": [],
                    "fallback_reason": native_fallback_reason,
                }
                phase_traces.append(
                    {
                        "phase": "native_kernel",
                        "status": "fallback",
                        "details": {
                            "kernel_requested": kernel_mode,
                            "fallback_reason": native_fallback_reason,
                        },
                        "source_labels": ["native.kernel"],
                    }
                )
            except Exception as native_error:
                if kernel_mode == "native":
                    raise HTTPException(
                        status_code=500,
                        detail={
                            "error_code": "native_kernel_failed",
                            "message": str(native_error),
                            "kernel_requested": kernel_mode,
                        },
                    )
                native_fallback_reason = f"native_kernel_failed: {str(native_error)}"
                warnings.append(f"native kernel fallback to legacy: {native_fallback_reason}")
                native_trace_payload = {
                    "kernel": "legacy",
                    "runtime_reused": False,
                    "runtime_cache_key": "",
                    "tools_called": [
                        {
                            "tool_name": "native_kernel",
                            "success": False,
                            "error": {
                                "code": "native_kernel_failed",
                                "message": str(native_error),
                            },
                        }
                    ],
                    "memory": {
                        "example_count": 0,
                        "memory_hit": False,
                        "sources_attempted": [],
                        "vanna_memory_hit": False,
                        "vanna_memory_source": "",
                        "confidence": 0.0,
                        "used_as": "degraded",
                        "rejected_reason": native_fallback_reason,
                        "candidate_count": 0,
                        "candidate_examples": [],
                        "chosen_candidate": {},
                        "chosen_candidates": [],
                        "rejected_candidates": [],
                        "query_intent": "unknown",
                        "candidate_intent": "unknown",
                        "intent_matched": False,
                        "reuse_gate_reason": "reuse_blocked_native_fallback",
                        "subtasks": [],
                    },
                    "audit_events": [],
                    "fallback_reason": native_fallback_reason,
                }
                phase_traces.append(
                    {
                        "phase": "native_kernel",
                        "status": "fallback",
                        "details": {
                            "kernel_requested": kernel_mode,
                            "fallback_reason": native_fallback_reason,
                        },
                        "source_labels": ["native.kernel"],
                    }
                )

        try:
            tables_context = await datasource_handler.list_table_registry()
        except Exception as registry_error:
            logger.warning("failed to load table registry for planner, fallback to empty context: %s", registry_error)
            tables_context = []
            warnings.append(f"table registry unavailable: {registry_error}")

        if requested_tables:
            requested_table_names = set(requested_tables)
            filtered_tables_context = [
                table
                for table in tables_context
                if table.get("table_name") in requested_table_names
            ]
            if not filtered_tables_context:
                raise HTTPException(status_code=400, detail="Selected query scope does not match any registered tables")
            tables_context = filtered_tables_context

        planner = PlannerAgent(tables_context=tables_context)
        plan = await asyncio.to_thread(planner.plan, query)
        subtasks = plan.get("subtasks") or [{"table": table, "question": query} for table in plan.get("tables", [])]
        phase_traces.append(
            {
                "phase": "planner",
                "status": "ok",
                "details": {
                    "intent": str(plan.get("intent") or "list"),
                    "selected_table_count": len(plan.get("tables") or []),
                    "needs_join": bool(plan.get("needs_join", False)),
                },
                "source_labels": ["planner.tables_context"],
            }
        )
        table_admin = TableAdminAgent(doris_client_override=doris_client)

        async def generate_subtask_sql(subtask: Dict[str, Any]) -> Optional[tuple[str, str, Dict[str, Any]]]:
            table_name = subtask.get("table")
            if not table_name:
                return None
            if hasattr(table_admin, "generate_sql_for_subtask_with_trace"):
                result = await asyncio.to_thread(
                    table_admin.generate_sql_for_subtask_with_trace,
                    subtask,
                    query,
                    api_config,
                )
                sql = (result or {}).get("sql", "")
                subtask_trace = (result or {}).get("trace", {})
            else:
                sql = await asyncio.to_thread(
                    table_admin.generate_sql_for_subtask,
                    subtask,
                    query,
                    api_config,
                )
                subtask_trace = {
                    "table_name": table_name,
                    "question": subtask.get("question") or query,
                    "strategy": "legacy_generate_sql_for_subtask",
                    "prompt_attempts": 0,
                    "metadata_available": False,
                    "schema_column_count": 0,
                    "example_count": 0,
                    "ddl_count": 0,
                    "documentation_count": 0,
                    "retrieval_source_labels": [],
                    "candidate_retrieval_source_labels": [],
                    "memory_hit": False,
                    "candidate_memory_hit": False,
                    "memory_fallback_used": False,
                    "memory_source": "",
                    "phases": ["sql_generation"],
                    "target_only": True,
                    "referenced_tables": [table_name],
                }
            return table_name, sql, subtask_trace

        sql_results = await asyncio.gather(
            *(generate_subtask_sql(subtask) for subtask in subtasks if subtask.get("table"))
        )
        sql_map: Dict[str, str] = {
            table_name: sql
            for item in sql_results
            if item is not None
            for table_name, sql, _ in [item]
        }
        subtask_traces = [
            {
                "table_name": table_name,
                "question": (trace or {}).get("question") or query,
                "strategy": (trace or {}).get("strategy") or "legacy_generate_sql_for_subtask",
                "sql": sql,
                "prompt_attempts": int((trace or {}).get("prompt_attempts", 0)),
                "metadata_available": bool((trace or {}).get("metadata_available", False)),
                "schema_column_count": int((trace or {}).get("schema_column_count", 0)),
                "example_count": int((trace or {}).get("example_count", 0)),
                "ddl_count": int((trace or {}).get("ddl_count", 0)),
                "documentation_count": int((trace or {}).get("documentation_count", 0)),
                "retrieval_source_labels": list((trace or {}).get("retrieval_source_labels") or []),
                "candidate_retrieval_source_labels": list((trace or {}).get("candidate_retrieval_source_labels") or []),
                "memory_hit": bool((trace or {}).get("memory_hit", False)),
                "candidate_memory_hit": bool((trace or {}).get("candidate_memory_hit", False)),
                "memory_fallback_used": bool((trace or {}).get("memory_fallback_used", False)),
                "memory_source": str((trace or {}).get("memory_source") or ""),
                "phases": list((trace or {}).get("phases") or []),
                "target_only": bool((trace or {}).get("target_only", True)),
                "referenced_tables": list((trace or {}).get("referenced_tables", [table_name])),
            }
            for item in sql_results
            if item is not None
            for table_name, sql, trace in [item]
        ]

        retrieval_source_labels = sorted(
            {
                source
                for trace in subtask_traces
                for source in list(trace.get("retrieval_source_labels") or [])
                if source
            }
        )
        retrieval_summary = {
            "example_count": sum(int(trace.get("example_count", 0)) for trace in subtask_traces),
            "ddl_count": sum(int(trace.get("ddl_count", 0)) for trace in subtask_traces),
            "documentation_count": sum(int(trace.get("documentation_count", 0)) for trace in subtask_traces),
            "source_labels": retrieval_source_labels,
            "memory_hit": any(bool(trace.get("memory_hit", False)) for trace in subtask_traces),
            "memory_fallback_used": any(bool(trace.get("memory_fallback_used", False)) for trace in subtask_traces),
        }
        phase_traces.extend(
            [
                {
                    "phase": "memory_retrieval",
                    "status": "ok",
                    "details": {
                        "example_count": retrieval_summary["example_count"],
                        "memory_hit": retrieval_summary["memory_hit"],
                        "memory_fallback_used": retrieval_summary["memory_fallback_used"],
                    },
                    "source_labels": [
                        source
                        for source in retrieval_source_labels
                        if source.startswith("query_history.")
                    ],
                },
                {
                    "phase": "ddl_doc_retrieval",
                    "status": "ok",
                    "details": {
                        "ddl_count": retrieval_summary["ddl_count"],
                        "documentation_count": retrieval_summary["documentation_count"],
                    },
                    "source_labels": retrieval_source_labels,
                },
                {
                    "phase": "sql_generation",
                    "status": "ok",
                    "details": {"subtask_count": len(sql_map)},
                    "source_labels": ["table_admin_agent"],
                },
            ]
        )

        if not sql_map:
            raise HTTPException(status_code=400, detail="Planner could not resolve any target tables")

        try:
            relationships = await datasource_handler.list_relationships_async(plan.get("tables"))
        except Exception as relationship_error:
            logger.warning("failed to load relationships, fallback to none: %s", relationship_error)
            relationships = []
            warnings.append(f"relationships unavailable: {relationship_error}")

        coordinator = CoordinatorAgent()
        if hasattr(coordinator, "coordinate_with_trace"):
            coordination_result = await asyncio.to_thread(
                coordinator.coordinate_with_trace,
                plan,
                sql_map,
                relationships,
            )
            generated_sql = (coordination_result or {}).get("sql", "")
            orchestration_trace = (coordination_result or {}).get("trace") or {
                "strategy": "coordinate_with_trace",
                "input_tables": list(sql_map.keys()),
                "candidate_relationship_count": len(relationships),
                "selected_relationship": None,
            }
        else:
            coordinate_signature = inspect.signature(coordinator.coordinate)
            if "relationships" in coordinate_signature.parameters:
                generated_sql = await asyncio.to_thread(
                    coordinator.coordinate,
                    plan,
                    sql_map,
                    relationships=relationships,
                )
            else:
                generated_sql = await asyncio.to_thread(coordinator.coordinate, plan, sql_map)
            orchestration_trace = {
                "strategy": "legacy_coordinate",
                "input_tables": list(sql_map.keys()),
                "candidate_relationship_count": len(relationships),
                "selected_relationship": None,
            }
        phase_traces.append(
            {
                "phase": "orchestration",
                "status": "ok",
                "details": {"strategy": str(orchestration_trace.get("strategy") or "legacy_coordinate")},
                "source_labels": ["coordinator.relationships"],
            }
        )

        history_vanna = VannaDorisOpenAI(
            doris_client=doris_client,
            api_key=api_key,
            model=model,
            base_url=base_url,
            api_config=api_config,
            config={'temperature': 0.1}
        )
        repair_agent = RepairAgent(
            doris_client=doris_client,
            api_key=api_key,
            model=model,
            base_url=base_url,
        )

        logger.info(f"=== Generated SQL: {generated_sql}")

        final_sql = generated_sql
        repair_trace: Dict[str, Any] = {
            "attempted": False,
            "max_attempts": request.max_repair_attempts,
            "attempts": [],
        }
        try:
            query_result = await doris_client.execute_query_async(final_sql)
        except Exception as execute_error:
            logger.warning("=== SQL execution failed, starting repair flow: %s", execute_error)
            if hasattr(history_vanna, "get_related_ddl"):
                ddl_list = await asyncio.to_thread(history_vanna.get_related_ddl, query)
            else:
                ddl_list = []
            last_error = execute_error
            repair_trace["attempted"] = True

            for attempt_index in range(request.max_repair_attempts):
                failed_sql = final_sql
                if hasattr(repair_agent, "repair_sql_with_trace"):
                    repair_result = await asyncio.to_thread(
                        repair_agent.repair_sql_with_trace,
                        query,
                        final_sql,
                        str(last_error),
                        ddl_list,
                        api_config=api_config,
                    )
                    repaired_sql = (repair_result or {}).get("sql", "")
                    repair_attempt_meta = (repair_result or {}).get("trace", {})
                else:
                    repaired_sql = await asyncio.to_thread(
                        repair_agent.repair_sql,
                        query,
                        final_sql,
                        str(last_error),
                        ddl_list,
                        api_config=api_config,
                    )
                    repair_attempt_meta = {}
                final_sql = repaired_sql.strip().rstrip(";")
                logger.info("=== Repaired SQL: %s", final_sql)
                attempt_trace = {
                    "attempt": attempt_index + 1,
                    "error_message": str(last_error),
                    "failed_sql": failed_sql,
                    "repaired_sql": final_sql,
                    "succeeded": False,
                    "ddl_count": int(repair_attempt_meta.get("ddl_count", len(ddl_list))),
                    "model": repair_attempt_meta.get("model", model or ""),
                    "base_url": repair_attempt_meta.get("base_url", base_url or ""),
                }
                try:
                    query_result = await doris_client.execute_query_async(final_sql)
                    attempt_trace["succeeded"] = True
                    repair_trace["attempts"].append(attempt_trace)
                    break
                except Exception as retry_error:
                    repair_trace["attempts"].append(attempt_trace)
                    last_error = retry_error
            else:
                raise last_error

        phase_traces.append(
            {
                "phase": "validation_repair",
                "status": "repaired" if repair_trace.get("attempted") else "ok",
                "details": {
                    "attempted": bool(repair_trace.get("attempted", False)),
                    "attempt_count": len(repair_trace.get("attempts", [])),
                    "max_attempts": int(repair_trace.get("max_attempts", 0)),
                },
                "source_labels": ["repair_agent"],
            }
        )

        try:
            history_result = await asyncio.to_thread(
                history_vanna.add_question_sql,
                question=query,
                sql=final_sql,
                row_count=len(query_result),
                is_empty_result=len(query_result) == 0,
            )
        except Exception as history_error:
            logger.warning("history persistence failed: %s", history_error)
            history_result = {"status": "error", "id": None}
            warnings.append(f"history persistence failed: {history_error}")

        history_id = history_result.get("id") if isinstance(history_result, dict) else None
        if (
            history_id
            and query_result
            and os.getenv("ANALYST_AUTO_ANALYZE", "false").lower() == "true"
            and analyst_agent is not None
        ):
            auto_analysis_executor.submit(
                _run_auto_analysis,
                history_id,
                request.resource_name,
            ).add_done_callback(_log_auto_analysis_result)

        logger.info(f"=== Query executed successfully, returned {len(query_result)} rows")
        phase_traces.append(
            {
                "phase": "execution",
                "status": "ok",
                "details": {
                    "row_count": len(query_result),
                    "history_status": str((history_result or {}).get("status") or ""),
                },
                "source_labels": ["doris.execution"],
            }
        )

        planner_candidates = [
            PlannerCandidateResponse(
                table_name=str(candidate.get("table_name") or ""),
                score=int(candidate.get("score", 0)),
                matched_terms=list(candidate.get("matched_terms") or []),
                selected=bool(candidate.get("selected", False)),
                rank=int(candidate.get("rank", 0)),
            )
            for candidate in plan.get("candidates", [])
            if candidate.get("table_name")
        ]

        trace_payload = None
        if request.include_trace:
            trace_payload = NLQueryTraceResponse(
                trace_id=trace_id,
                context=ContextTraceResponse(**context_trace),
                phases=[PhaseTraceResponse(**phase_trace) for phase_trace in phase_traces],
                planner=PlannerTraceResponse(
                    normalized_query=str(plan.get("normalized_question") or ""),
                    intent=str(plan.get("intent") or "list"),
                    requested_tables=requested_tables,
                    selected_tables=list(plan.get("tables") or []),
                    needs_join=bool(plan.get("needs_join", False)),
                    fallback_used=bool(plan.get("fallback_used", False)),
                    routing_reason=str(plan.get("routing_reason") or ""),
                    candidates=planner_candidates,
                ),
                retrieval=RetrievalTraceResponse(**retrieval_summary),
                subtasks=[SubtaskTraceResponse(**subtask_trace) for subtask_trace in subtask_traces],
                orchestration=OrchestrationTraceResponse(
                    strategy=str(orchestration_trace.get("strategy") or "legacy_coordinate"),
                    input_tables=list(orchestration_trace.get("input_tables") or []),
                    candidate_relationship_count=int(orchestration_trace.get("candidate_relationship_count", 0)),
                    selected_relationship=orchestration_trace.get("selected_relationship"),
                ),
                repair=RepairTraceResponse(
                    attempted=bool(repair_trace.get("attempted", False)),
                    max_attempts=int(repair_trace.get("max_attempts", 0)),
                    attempts=[RepairAttemptTraceResponse(**attempt) for attempt in repair_trace.get("attempts", [])],
                ),
                execution=ExecutionTraceResponse(
                    row_count=len(query_result),
                    history_id=history_id,
                    history_status=str((history_result or {}).get("status") or ""),
                    llm_execution_mode=str(api_config.get("llm_execution_mode") or ""),
                    resource_name=api_config.get("resource_name"),
                    model=str(model or ""),
                ),
                native=NativeTraceResponse(**native_trace_payload),
            )

        return NLQueryResponse(
            success=True,
            schema_version="nlq.v1",
            query=query,
            intent=str(plan.get("intent") or "list"),
            table_names=list(plan.get("tables") or []),
            sql=final_sql,
            data=query_result,
            count=len(query_result),
            history_id=history_id,
            warnings=warnings,
            trace=trace_payload,
        )

    except HTTPException:
        raise
    except LLMExecutionError as llm_error:
        logger.error("LLM execution failed: %s", llm_error)
        status_code = 502
        if llm_error.error_code in {
            "missing_api_key",
            "missing_resource_name",
            "invalid_resource_name",
            "invalid_llm_execution_mode",
        }:
            status_code = 400
        raise HTTPException(status_code=status_code, detail=llm_error.to_dict())
    except Exception as e:
        logger.error(f"=== Error in natural language query: {str(e)}")
        logger.error(traceback.format_exc())

        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


@app.post("/api/query/native-chat-sse")
@app.post("/api/query/native/events")
async def native_query_sse(request: NativeSSEQueryRequest):
    """
    Native kernel SSE sidecar endpoint.

    This endpoint is additive and does not replace `/api/query/natural`.
    """
    trace_id = str(uuid.uuid4())
    query = request.query
    requested_tables = list(request.table_names or [])
    request_id = (request.request_id or "").strip() or trace_id
    session_id = (request.session_id or "").strip() or f"session-{trace_id[:8]}"
    request_context = request.context or NLQueryContextEnvelope()

    kernel_mode = str(request.kernel or "native").lower()
    if kernel_mode not in {"native"}:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error_code": "unsupported_kernel",
                "message": "native SSE endpoint currently supports kernel=native only",
                "kernel_requested": kernel_mode,
            },
        )

    api_config = build_api_config(
        request.resource_name,
        api_key=request.api_key,
        model=request.model,
        base_url=request.base_url,
    )
    api_key = api_config.get("api_key")
    llm_execution_mode = str(api_config.get("llm_execution_mode") or "direct_api")
    _ensure_llm_route_available(
        request_resource_name=request.resource_name,
        llm_execution_mode=llm_execution_mode,
        api_key=api_key,
        api_config=api_config,
    )

    async def event_stream():
        start_payload = {
            "trace_id": trace_id,
            "request_id": request_id,
            "session_id": session_id,
            "query": query,
            "kernel": kernel_mode,
            "llm_execution_mode": llm_execution_mode,
            "resource_name": api_config.get("resource_name"),
            "context": {
                "user_id": request_context.user_id,
                "source": request_context.source or "api",
                "labels": list(request_context.labels or []),
                "attributes": dict(request_context.attributes or {}),
            },
        }
        yield _format_sse_event("request_start", start_payload)

        try:
            native_result = await run_native_query_kernel(
                query=query,
                requested_tables=requested_tables,
                api_config=api_config,
                doris_client=doris_client,
                datasource_handler=datasource_handler,
                request_id=request_id,
                session_id=session_id,
                user_id=request_context.user_id,
                max_repair_attempts=request.max_repair_attempts,
            )
            native_trace = dict(native_result.native_trace or {})
            audit_events = list(native_trace.get("audit_events") or [])
            tools_called = list(native_trace.get("tools_called") or [])
            emitted_tool_result = False

            for entry in audit_events:
                phase = str(entry.get("phase") or "")
                payload = dict(entry.get("payload") or {})
                if phase == "tool_invocation":
                    yield _format_sse_event(
                        "tool_invocation",
                        {
                            "trace_id": trace_id,
                            "tool_name": payload.get("tool_name"),
                            "payload": payload,
                        },
                    )
                elif phase == "tool_result":
                    emitted_tool_result = True
                    yield _format_sse_event(
                        "tool_result",
                        {
                            "trace_id": trace_id,
                            "tool_name": payload.get("tool_name"),
                            "success": bool(payload.get("success", False)),
                            "error": payload.get("error"),
                            "payload": payload,
                        },
                    )

            if not emitted_tool_result:
                for tool in tools_called:
                    yield _format_sse_event(
                        "tool_result",
                        {
                            "trace_id": trace_id,
                            "tool_name": tool.get("tool_name"),
                            "success": bool(tool.get("success", False)),
                            "error": dict(tool.get("error") or {}),
                            "payload": dict(tool),
                        },
                    )

            yield _format_sse_event(
                "sql_generated",
                {
                    "trace_id": trace_id,
                    "sql": native_result.sql,
                    "table_names": list(native_result.table_names or []),
                    "intent": native_result.intent,
                    "subtasks": list(native_result.subtask_traces or []),
                    "memory": dict(native_trace.get("memory") or {}),
                },
            )

            yield _format_sse_event(
                "execution_result",
                {
                    "trace_id": trace_id,
                    "row_count": len(native_result.data or []),
                    "history_id": native_result.history_id,
                    "history_status": native_result.history_status,
                    "warnings": list(native_result.warnings or []),
                    "data_preview": list(native_result.data or [])[:10],
                    "memory": dict(native_trace.get("memory") or {}),
                },
            )

            yield _format_sse_event(
                "done",
                {
                    "trace_id": trace_id,
                    "success": True,
                    "kernel": "native",
                    "llm_execution_mode": llm_execution_mode,
                    "resource_name": api_config.get("resource_name"),
                    "tools_called": tools_called,
                    "memory": dict(native_trace.get("memory") or {}),
                    "audit_events": audit_events,
                    "fallback_reason": str(native_trace.get("fallback_reason") or ""),
                },
            )
        except NativeKernelExecutionError as native_error:
            error_payload = native_error.to_dict()
            error_payload["kernel_requested"] = kernel_mode
            yield _format_sse_event("error", error_payload)
            yield _format_sse_event(
                "done",
                {
                    "trace_id": trace_id,
                    "success": False,
                    "kernel": kernel_mode,
                    "llm_execution_mode": llm_execution_mode,
                    "resource_name": api_config.get("resource_name"),
                },
            )
        except HTTPException as http_error:
            detail = http_error.detail if isinstance(http_error.detail, dict) else {"message": str(http_error.detail)}
            yield _format_sse_event(
                "error",
                {
                    "error_code": detail.get("error_code", "http_error"),
                    "message": str(detail.get("message") or detail.get("error") or http_error.detail),
                    "details": detail,
                },
            )
            yield _format_sse_event(
                "done",
                {
                    "trace_id": trace_id,
                    "success": False,
                    "kernel": kernel_mode,
                    "llm_execution_mode": llm_execution_mode,
                    "resource_name": api_config.get("resource_name"),
                },
            )
        except Exception as stream_error:
            yield _format_sse_event(
                "error",
                {
                    "error_code": "native_sse_failed",
                    "message": _summarize_error(stream_error),
                    "details": {"type": stream_error.__class__.__name__},
                },
            )
            yield _format_sse_event(
                "done",
                {
                    "trace_id": trace_id,
                    "success": False,
                    "kernel": kernel_mode,
                    "llm_execution_mode": llm_execution_mode,
                    "resource_name": api_config.get("resource_name"),
                },
            )

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/analysis/table/{table_name}")
async def analyze_table_endpoint(table_name: str, request: AnalysisTableRequest):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(
            agent.analyze_table,
            table_name,
            request.depth,
            request.resource_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/analysis/replay/{history_id}")
async def analyze_replay_endpoint(history_id: str, request: AnalysisReplayRequest):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(
            agent.replay_from_history,
            history_id,
            request.resource_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysis/reports")
async def analysis_reports(table_names: Optional[str] = None, limit: int = 20, offset: int = 0):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(agent.list_reports, table_names, limit, offset)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysis/reports/latest/{table_name}")
async def latest_analysis_report(table_name: str, include_reasoning: bool = False):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(agent.get_latest_report, table_name, include_reasoning)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysis/reports/{report_id}/summary")
async def analysis_report_summary(report_id: str):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(agent.get_report_summary, report_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysis/reports/{report_id}")
async def analysis_report_detail(report_id: str, include_reasoning: bool = False):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(agent.get_report, report_id, include_reasoning)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/api/analysis/reports/{report_id}")
async def analysis_report_delete(report_id: str):
    agent = _require_analyst_agent()
    try:
        return await asyncio.to_thread(agent.delete_report, report_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/analysis/schedules")
async def create_analysis_schedule(request: AnalysisScheduleCreateRequest):
    scheduler = _require_analysis_scheduler()
    try:
        return await asyncio.to_thread(
            scheduler.create_schedule,
            request.model_dump(exclude_none=True),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysis/schedules")
async def list_analysis_schedules():
    scheduler = _require_analysis_scheduler()
    try:
        return await asyncio.to_thread(scheduler.list_schedules)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.put("/api/analysis/schedules/{schedule_id}")
async def update_analysis_schedule(schedule_id: str, request: AnalysisScheduleUpdateRequest):
    scheduler = _require_analysis_scheduler()
    try:
        return await asyncio.to_thread(
            scheduler.update_schedule,
            schedule_id,
            request.model_dump(exclude_none=True),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/api/analysis/schedules/{schedule_id}")
async def delete_analysis_schedule(schedule_id: str):
    scheduler = _require_analysis_scheduler()
    try:
        return await asyncio.to_thread(scheduler.delete_schedule, schedule_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/analysis/schedules/{schedule_id}/run")
async def run_analysis_schedule(schedule_id: str):
    scheduler = _require_analysis_scheduler()
    try:
        return await asyncio.to_thread(scheduler.run_now, schedule_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/analysis/schedules/{schedule_id}/toggle")
async def toggle_analysis_schedule(schedule_id: str):
    scheduler = _require_analysis_scheduler()
    try:
        return await asyncio.to_thread(scheduler.toggle_schedule, schedule_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysis/contracts")
async def analysis_contracts():
    return {
        "success": True,
        "module": "insight",
        "contracts": _analysis_contract_blueprint(),
        "forecast_boundary": _forecast_boundary_blueprint(),
        "collaboration_boundary": _collaboration_boundary_blueprint(),
        "metric_foundation": _metric_foundation_blueprint(),
    }


@app.post("/api/analysis/forecast")
async def analysis_forecast_boundary(request: AnalysisForecastRequest):
    agent = _require_analyst_agent()
    try:
        result = await asyncio.to_thread(
            agent.forecast_metric,
            request.metric_key,
            granularity=request.granularity,
            horizon_steps=request.horizon_steps,
            start_at=request.start_at,
            end_at=request.end_at,
            filters=request.filters,
            lookback_points=request.lookback_points,
            metric_provider=datasource_handler,
        )
        result["contract"] = _forecast_boundary_blueprint()
        result["request"] = {
            "metric_key": request.metric_key,
            "granularity": request.granularity,
            "horizon_steps": request.horizon_steps,
            "horizon_unit": request.horizon_unit,
            "start_at": request.start_at,
            "end_at": request.end_at,
            "lookback_points": request.lookback_points,
            "filters": request.filters,
            "resource_name": request.resource_name,
            "external_signals": [
                {
                    "source": signal.source,
                    "signal_key": signal.signal_key,
                    "granularity": signal.granularity,
                }
                for signal in request.external_signals
            ],
        }
        return result
    except HTTPException:
        raise
    except Exception as exc:
        return {
            "success": False,
            "status": "failed",
            "contract_version": "insight.forecast.result.v1",
            "forecast_id": str(uuid.uuid4()),
            "metric_key": request.metric_key,
            "horizon": {
                "steps": request.horizon_steps,
                "unit": request.granularity,
                "granularity": request.granularity,
                "start_at": None,
                "end_at": None,
                "history_window": {
                    "start_at": request.start_at,
                    "end_at": request.end_at,
                },
            },
            "points": [],
            "assumptions": [],
            "backtest_summary": {
                "status": "unavailable",
                "holdout_points": 0,
                "train_points": 0,
                "mae": None,
                "rmse": None,
                "mape": None,
                "residual_std": None,
            },
            "model_info": {
                "name": "baseline_internal",
                "version": "baseline.internal.v1",
                "status": "failed",
                "granularity": request.granularity,
                "aggregation": None,
                "table_name": None,
                "time_column": None,
                "value_column": None,
                "training_points": 0,
                "history_points": 0,
            },
            "error": {
                "code": "forecast_internal_error",
                "message": str(exc),
                "details": {},
            },
            "contract": _forecast_boundary_blueprint(),
        }


@app.get("/api/analysis/collaboration/contracts")
async def analysis_collaboration_contracts():
    return {
        "success": True,
        "module": "insight",
        "collaboration_boundary": _collaboration_boundary_blueprint(),
    }


@app.get("/api/query/history")
async def query_history(limit: int = 100):
    """查询历史列表（只读）"""
    try:
        history = await datasource_handler.list_query_history_async(limit=limit)
        return {
            "success": True,
            "history": history,
            "count": len(history),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueryHistoryFeedbackRequest(BaseModel):
    quality_gate: int = Field(..., description="质量门状态")


class RelationshipRequest(BaseModel):
    table_a: str
    column_a: str
    table_b: str
    column_b: str
    rel_type: Optional[str] = Field(default="logical")


@app.post("/api/query/history/{query_id}/feedback")
async def query_history_feedback(query_id: str, req: QueryHistoryFeedbackRequest):
    """更新查询历史质量标记"""
    try:
        return await datasource_handler.update_query_feedback_async(query_id, req.quality_gate)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/relationships")
async def create_relationship(req: RelationshipRequest):
    """创建手工关系覆盖"""
    try:
        return await datasource_handler.create_relationship_async(
            table_a=req.table_a,
            column_a=req.column_a,
            table_b=req.table_b,
            column_b=req.column_b,
            rel_type=req.rel_type or "logical",
            confidence=1.0,
            is_manual=True,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/relationships")
async def list_relationships(tables: Optional[str] = None):
    """获取稳定关系读取面。"""
    try:
        table_names = [item.strip() for item in (tables or "").split(",") if item.strip()] or None
        relationships = await datasource_handler.list_relationship_models_async(table_names)
        return {
            "success": True,
            "relationships": relationships,
            "count": len(relationships),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/upload/preview")
async def preview_excel_file(file: UploadFile = File(...), rows: int = 10):
    """预览 Excel 文件"""
    try:
        content = await file.read()
        result = await excel_handler.preview_excel_async(content, rows)

        return {
            "success": True,
            "filename": file.filename,
            **result
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


async def _analyze_table_async(table_name: str, source_type: str):
    """异步分析表格元数据"""
    import asyncio
    await asyncio.sleep(2)  # 等待数据完全写入
    try:
        result = await metadata_analyzer.analyze_table_async(table_name, source_type)
        if result.get('success'):
            await asyncio.to_thread(metadata_analyzer.refresh_agent_assets, table_name, source_type)
            print(f"✅ 表格 '{table_name}' 元数据分析完成")
        else:
            print(f"⚠️ 表格 '{table_name}' 元数据分析失败: {result.get('error')}")
    except Exception as e:
        print(f"❌ 元数据分析异常: {e}")


@app.post("/api/upload")
async def upload_excel(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    column_mapping: Optional[str] = Form(None),
    create_table: str = Form("true"),
    import_mode: str = Form("replace"),
):
    """
    上传 Excel 文件并导入到 Doris

    Args:
        file: Excel 文件
        table_name: 目标表名
        column_mapping: 列映射 JSON 字符串 (可选)
        create_table: 如果表不存在是否创建 (字符串 "true"/"false")
        import_mode: 导入模式 replace/append
    """
    try:
        import json

        content = await file.read()

        # 解析列映射
        mapping = None
        if column_mapping:
            mapping = json.loads(column_mapping)

        # 转换 create_table 字符串为布尔值
        create_table_bool = create_table.lower() in ('true', '1', 'yes')

        result = await excel_handler.import_excel_async(
            file_content=content,
            table_name=table_name,
            column_mapping=mapping,
            create_table_if_not_exists=create_table_bool,
            import_mode=import_mode,
        )
        actual_table_name = result.get('table') or table_name
        if result.get('success'):
            await datasource_handler.finalize_table_ingestion_async(
                actual_table_name,
                'excel',
                replace_existing=bool(result.get('table_replaced')),
                clear_relationships=True,
                origin_kind='upload',
                origin_label=file.filename,
                origin_path=file.filename,
                ingest_mode=result.get('import_mode'),
                last_rows=result.get('rows_imported'),
            )

        # 自动触发元数据分析（异步，不阻塞返回）
        try:
            import asyncio
            if result.get('success'):
                asyncio.create_task(_analyze_table_async(actual_table_name, 'excel'))
        except Exception as analyze_error:
            print(f"⚠️ 元数据分析触发失败: {analyze_error}")

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


# ============ 数据源同步 API ============

class DataSourceTestRequest(BaseModel):
    """数据源连接测试请求"""
    host: str = Field(..., description="数据库主机")
    port: int = Field(..., description="数据库端口")
    user: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: Optional[str] = Field(None, description="数据库名")


class DataSourceSaveRequest(BaseModel):
    """保存数据源请求"""
    name: str = Field(..., description="数据源名称")
    host: str = Field(..., description="数据库主机")
    port: int = Field(..., description="数据库端口")
    user: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: str = Field(..., description="数据库名")


class SyncTableRequest(BaseModel):
    """同步表请求"""
    source_table: str = Field(..., description="源表名")
    target_table: Optional[str] = Field(None, description="目标表名")
    sync_strategy: str = Field(default="full", description="Sync strategy: full/incremental")
    incremental_time_field: Optional[str] = Field(None, description="Incremental strategy time field")
    incremental_start: Optional[str] = Field(None, description="Optional incremental start time")
    incremental_end: Optional[str] = Field(None, description="Optional incremental end time")

    @field_validator("sync_strategy")
    @classmethod
    def validate_sync_strategy(cls, value: str) -> str:
        normalized = str(value or "full").strip().lower()
        if normalized not in {"full", "incremental"}:
            raise ValueError("sync_strategy must be one of full, incremental")
        return normalized

    @model_validator(mode="after")
    def validate_incremental_requirements(self):
        if self.sync_strategy == "incremental" and not (self.incremental_time_field or "").strip():
            raise ValueError("incremental_time_field is required when sync_strategy=incremental")
        return self


class SyncMultipleRequest(BaseModel):
    """批量同步请求"""
    tables: List[Dict[str, Any]] = Field(..., description="要同步的表列表")


@app.post("/api/datasource/test")
async def test_datasource_connection(req: DataSourceTestRequest):
    """测试数据源连接"""
    result = await datasource_handler.test_connection(
        host=req.host,
        port=req.port,
        user=req.user,
        password=req.password,
        database=req.database
    )
    return result


@app.post("/api/datasource")
async def save_datasource(req: DataSourceSaveRequest):
    """保存数据源配置"""
    try:
        result = await datasource_handler.save_datasource(
            name=req.name,
            host=req.host,
            port=req.port,
            user=req.user,
            password=req.password,
            database=req.database
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/datasource")
async def list_datasources():
    """获取所有数据源"""
    try:
        datasources = await datasource_handler.list_datasources()
        return {
            "success": True,
            "datasources": datasources,
            "count": len(datasources)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/datasource/{ds_id}")
async def delete_datasource(ds_id: str):
    """删除数据源"""
    try:
        result = await datasource_handler.delete_datasource(ds_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/datasource/{ds_id}/tables")
async def get_datasource_tables(ds_id: str):
    """获取数据源中的表列表"""
    try:
        print(f"📋 获取数据源表列表: ds_id={ds_id}")
        ds = await datasource_handler.get_datasource(ds_id)
        print(f"📋 数据源信息: {ds}")
        if not ds:
            raise HTTPException(status_code=404, detail="数据源不存在")

        result = await datasource_handler.get_remote_tables(
            host=ds['host'],
            port=ds['port'],
            user=ds['user'],
            password=ds['password'],
            database=ds['database_name']
        )
        print(f"📋 获取表列表结果: {result}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"❌ 获取表列表异常: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/datasource/{ds_id}/sync")
async def sync_datasource_table(ds_id: str, req: SyncTableRequest):
    """同步单个表"""
    try:
        result = await datasource_handler.sync_table(
            ds_id=ds_id,
            source_table=req.source_table,
            target_table=req.target_table,
            sync_strategy=req.sync_strategy,
            incremental_time_field=req.incremental_time_field,
            incremental_start=req.incremental_start,
            incremental_end=req.incremental_end,
        )
        if not result.get('success'):
            raise HTTPException(status_code=500, detail=result.get('error'))

        # 自动触发元数据分析
        target = req.target_table or req.source_table
        try:
            import asyncio
            asyncio.create_task(_analyze_table_async(target, 'database_sync'))
        except Exception as analyze_error:
            print(f"⚠️ 元数据分析触发失败: {analyze_error}")

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/datasource/{ds_id}/sync-multiple")
async def sync_multiple_tables(ds_id: str, req: SyncMultipleRequest):
    """批量同步多个表"""
    try:
        result = await datasource_handler.sync_multiple_tables(
            ds_id=ds_id,
            tables=req.tables
        )

        # 为每个成功同步的表触发元数据分析
        if result.get('results'):
            import asyncio
            for table_result in result['results']:
                if table_result.get('success'):
                    target = table_result.get('target_table')
                    try:
                        asyncio.create_task(_analyze_table_async(target, 'database_sync'))
                    except Exception as e:
                        print(f"⚠️ 元数据分析触发失败: {e}")

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 表预览 API ============

@app.get("/api/datasource/{ds_id}/tables/{table_name}/preview")
async def preview_datasource_table(ds_id: str, table_name: str, limit: int = 100):
    """预览远程表的结构和数据"""
    try:
        ds = await datasource_handler.get_datasource(ds_id)
        if not ds:
            raise HTTPException(status_code=404, detail="数据源不存在")

        result = await datasource_handler.preview_remote_table(
            host=ds['host'],
            port=ds['port'],
            user=ds['user'],
            password=ds['password'],
            database=ds['database_name'],
            table_name=table_name,
            limit=limit
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 同步任务调度 API ============

class ScheduleSyncRequest(BaseModel):
    """定时同步请求（增强版）"""
    datasource_id: str = Field(..., description="数据源ID")
    source_table: str = Field(..., description="源表名")
    target_table: Optional[str] = Field(None, description="目标表名")
    schedule_type: str = Field(..., description="调度类型: hourly/daily/weekly/monthly")
    schedule_minute: Optional[int] = Field(0, description="分钟 (0-59)")
    schedule_hour: Optional[int] = Field(0, description="小时 (0-23)")
    schedule_day_of_week: Optional[int] = Field(1, description="周几 (1-7, 1=周一)")
    schedule_day_of_month: Optional[int] = Field(1, description="日期 (1-31)")
    enabled_for_ai: Optional[bool] = Field(True, description="是否启用AI分析")
    sync_strategy: Optional[str] = Field("full", description="Sync strategy: full/incremental")
    incremental_time_field: Optional[str] = Field(None, description="Incremental strategy time field")

    @field_validator("sync_strategy")
    @classmethod
    def validate_schedule_sync_strategy(cls, value: Optional[str]) -> Optional[str]:
        normalized = str(value or "full").strip().lower()
        if normalized not in {"full", "incremental"}:
            raise ValueError("sync_strategy must be one of full, incremental")
        return normalized

    @model_validator(mode="after")
    def validate_schedule_incremental_requirements(self):
        if self.sync_strategy == "incremental" and not (self.incremental_time_field or "").strip():
            raise ValueError("incremental_time_field is required when sync_strategy=incremental")
        return self


class UpdateSyncTaskRequest(BaseModel):
    """更新同步任务请求"""
    schedule_type: Optional[str] = Field(None, description="调度类型")
    schedule_minute: Optional[int] = Field(None, description="分钟")
    schedule_hour: Optional[int] = Field(None, description="小时")
    schedule_day_of_week: Optional[int] = Field(None, description="周几")
    schedule_day_of_month: Optional[int] = Field(None, description="日期")
    enabled_for_ai: Optional[bool] = Field(None, description="是否启用AI分析")




class UpdateTableRegistryRequest(BaseModel):
    """????????????????????????"""
    display_name: Optional[str] = Field(None, description="????????????")
    description: Optional[str] = Field(None, description="?????????")

@app.post("/api/sync/schedule")
async def create_sync_schedule(req: ScheduleSyncRequest):
    """创建定时同步任务"""
    try:
        result = await datasource_handler.save_sync_task(
            ds_id=req.datasource_id,
            source_table=req.source_table,
            target_table=req.target_table,
            schedule_type=req.schedule_type,
            schedule_minute=req.schedule_minute or 0,
            schedule_hour=req.schedule_hour or 0,
            schedule_day_of_week=req.schedule_day_of_week or 1,
            schedule_day_of_month=req.schedule_day_of_month or 1,
            enabled_for_ai=req.enabled_for_ai if req.enabled_for_ai is not None else True,
            sync_strategy=req.sync_strategy or "full",
            incremental_time_field=req.incremental_time_field,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/sync/tasks/{task_id}")
async def update_sync_task(task_id: str, req: UpdateSyncTaskRequest):
    """更新同步任务配置"""
    try:
        result = await datasource_handler.update_sync_task(
            task_id=task_id,
            schedule_type=req.schedule_type,
            schedule_minute=req.schedule_minute,
            schedule_hour=req.schedule_hour,
            schedule_day_of_week=req.schedule_day_of_week,
            schedule_day_of_month=req.schedule_day_of_month,
            enabled_for_ai=req.enabled_for_ai
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/sync/tasks/{task_id}/toggle-ai")
async def toggle_task_ai(task_id: str, enabled: bool):
    """切换同步任务的AI分析启用状态"""
    try:
        result = await datasource_handler.toggle_ai_enabled(task_id, enabled)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sync/tasks")
async def list_sync_tasks():
    """获取所有同步任务"""
    try:
        tasks = await datasource_handler.list_sync_tasks()
        return {
            "success": True,
            "tasks": tasks,
            "count": len(tasks)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sync/ai-enabled-tables")
async def get_ai_enabled_tables():
    """获取所有启用AI分析的表名"""
    try:
        tables = await datasource_handler.get_ai_enabled_tables()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/sync/tasks/{task_id}")
async def delete_sync_task(task_id: str):
    """删除同步任务"""
    try:
        result = await datasource_handler.delete_sync_task(task_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 元数据分析 API ============

@app.post("/api/tables/{table_name}/analyze")
async def analyze_table_metadata(
    table_name: str,
    source_type: str = "manual",
    resource_name: Optional[str] = None,
):
    """分析表格元数据"""
    try:
        result = await metadata_analyzer.analyze_table_async(
            table_name,
            source_type,
            resource_name=resource_name,
        )
        if not result.get('success'):
            status_code = int(result.get("status_code") or 500)
            raise HTTPException(status_code=status_code, detail=result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables/{table_name}/metadata")
async def get_table_metadata(table_name: str):
    """获取表格元数据"""
    try:
        metadata = metadata_analyzer.get_metadata(table_name)
        if not metadata:
            return {
                "success": True,
                "metadata": None,
                "message": "表格尚未分析，请先调用分析接口"
            }
        return {
            "success": True,
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agents/{table_name}")
async def get_table_agent(table_name: str):
    """获取表 Agent 配置"""
    try:
        agent = metadata_analyzer.get_agent_config(table_name)
        if not agent:
            return {
                "success": True,
                "agent": None,
                "message": "表 Agent 配置尚未生成，请先执行分析。"
            }
        return {"success": True, "agent": agent}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metadata")
async def list_all_metadata():
    """获取所有表格元数据"""
    try:
        metadata_list = metadata_analyzer.list_all_metadata()
        return {
            "success": True,
            "metadata": metadata_list,
            "count": len(metadata_list)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/foundation/tables")
async def list_foundation_tables(tables: Optional[str] = None):
    """获取稳定的数据基础层聚合视图。"""
    try:
        table_names = [item.strip() for item in (tables or "").split(",") if item.strip()] or None
        table_profiles = await datasource_handler.list_foundation_tables(table_names=table_names)
        return {
            "success": True,
            "tables": table_profiles,
            "count": len(table_profiles),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/foundation/tables/{table_name}")
async def get_foundation_table_profile(table_name: str):
    """获取单表基础层画像。"""
    try:
        table_profile = await datasource_handler.get_table_profile_async(table_name)
        if not table_profile:
            raise HTTPException(status_code=404, detail="表不存在或尚未注册")
        return {
            "success": True,
            "table": table_profile,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/api/foundation/metrics/contracts")
async def get_foundation_metric_contracts():
    """指标基础层 contract。"""
    return {
        "success": True,
        "module": "data_foundation",
        "surface": "metric_read_model",
        "contracts": _metric_foundation_blueprint(),
    }


@app.post("/api/foundation/metrics")
async def upsert_foundation_metric_definition(req: MetricDefinitionRequest):
    """创建或更新指标定义。"""
    try:
        payload = req.model_dump(exclude_none=True)
        result = await datasource_handler.upsert_metric_definition_async(payload)
        return result
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/foundation/metrics")
async def list_foundation_metric_definitions(only_forecast_ready: bool = False):
    """列出指标定义（可筛选 forecast-ready）。"""
    try:
        metrics = await datasource_handler.list_metric_definitions_async(
            only_forecast_ready=only_forecast_ready
        )
        return {
            "success": True,
            "metrics": metrics,
            "count": len(metrics),
            "only_forecast_ready": bool(only_forecast_ready),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/foundation/metrics/series")
async def get_foundation_metric_series(req: MetricSeriesRequest):
    """按指标定义读取稳定时间序列。"""
    try:
        result = await datasource_handler.get_metric_series_async(
            req.metric_key,
            start_time=req.start_time,
            end_time=req.end_time,
            grain=req.grain,
            filters=req.filters,
            limit=req.limit,
        )
        return result
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/foundation/metrics/{metric_key}")
async def get_foundation_metric_definition(metric_key: str):
    """获取单个指标定义。"""
    try:
        metric = await datasource_handler.get_metric_definition_async(metric_key)
        if not metric:
            raise HTTPException(status_code=404, detail="metric not found")
        return {"success": True, "metric": metric}
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/foundation/metrics/{metric_key}")
async def delete_foundation_metric_definition(metric_key: str):
    """删除单个指标定义。"""
    try:
        return await datasource_handler.delete_metric_definition_async(metric_key)
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/internal/metrics/contracts")
async def get_internal_metric_contracts():
    """agent-c 内部指标读取 contract。"""
    return {
        "success": True,
        "module": "data_foundation",
        "surface": "internal_metric_read",
        "contracts": _metric_foundation_blueprint(),
    }


@app.get("/api/internal/metrics")
async def list_internal_metric_definitions(only_forecast_ready: bool = True):
    """agent-c 内部指标列表读取面。"""
    try:
        metrics = await datasource_handler.list_metric_definitions_async(
            only_forecast_ready=only_forecast_ready
        )
        return {
            "success": True,
            "surface": "internal_metric_read",
            "metrics": metrics,
            "count": len(metrics),
            "only_forecast_ready": bool(only_forecast_ready),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/internal/metrics/series")
async def get_internal_metric_series(req: MetricSeriesRequest):
    """agent-c 内部指标时间序列读取面。"""
    try:
        result = await datasource_handler.get_metric_series_async(
            req.metric_key,
            start_time=req.start_time,
            end_time=req.end_time,
            grain=req.grain,
            filters=req.filters,
            limit=req.limit,
        )
        result["surface"] = "internal_metric_read"
        return result
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/internal/metrics/{metric_key}")
async def get_internal_metric_definition(metric_key: str):
    """agent-c 内部单指标读取面。"""
    try:
        metric = await datasource_handler.get_metric_definition_async(metric_key)
        if not metric:
            raise HTTPException(status_code=404, detail="metric not found")
        return {
            "success": True,
            "surface": "internal_metric_read",
            "metric": metric,
        }
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ ??????????????????API ============

@app.get("/api/table-registry")
async def list_table_registry():
    """???????????????????????????"""
    try:
        tables = await datasource_handler.list_table_registry()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/table-registry/{table_name}")
async def update_table_registry(table_name: str, req: UpdateTableRegistryRequest):
    """????????????????????????????????????"""
    try:
        result = await datasource_handler.update_table_registry(
            table_name=table_name,
            display_name=req.display_name,
            description=req.description
        )
        if not result.get('success'):
            raise HTTPException(status_code=400, detail=result.get('error'))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/table-registry/{table_name}")
async def delete_table_registry(
    table_name: str,
    drop_physical: bool = True,
    cleanup_history: bool = True,
):
    """删除已注册表及其派生资产"""
    try:
        result = await datasource_handler.delete_registered_table_async(
            table_name=table_name,
            drop_physical=drop_physical,
            cleanup_history=cleanup_history,
        )
        if not result.get('success'):
            raise HTTPException(status_code=400, detail=result.get('error'))
        return result
    except ValueError as value_error:
        raise HTTPException(status_code=400, detail=str(value_error))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/analysis")
async def analysis_websocket(ws: WebSocket):
    try:
        dispatcher = _require_analysis_dispatcher()
    except HTTPException as exc:
        await ws.accept()
        await ws.close(code=1013, reason=str(exc.detail))
        return

    expected_api_key = os.getenv("SMATRIX_API_KEY")
    provided_api_key = ws.query_params.get("api_key")
    if not expected_api_key:
        await ws.accept()
        await ws.close(code=4001, reason="SMATRIX_API_KEY is not configured")
        return
    if provided_api_key != expected_api_key:
        await ws.accept()
        await ws.close(code=4001, reason="Unauthorized")
        return

    client_id = str(uuid.uuid4())
    await dispatcher.ws_connect(ws, client_id)
    if client_id not in dispatcher.ws_connections:
        return
    heartbeat_timeout = float(os.getenv("ANALYST_WS_IDLE_TIMEOUT", "60"))
    try:
        while True:
            try:
                message = await asyncio.wait_for(ws.receive_text(), timeout=heartbeat_timeout)
            except TimeoutError:
                await ws.close(code=4008, reason="Idle timeout")
                break
            if isinstance(message, str) and message.strip().lower() == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        await dispatcher.ws_disconnect(client_id)
    except Exception:
        await dispatcher.ws_disconnect(client_id)
        raise
    else:
        await dispatcher.ws_disconnect(client_id)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        reload=True
    )
