from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional

from .client import RuntimeClient, RuntimeFailure, normalize_forecast_payload
from .config import RuntimeSettings
from .doctor import run_doctor


TOOLS = [
    {
        "name": "dc_health",
        "description": "Check DorisClaw API readiness and Doris connectivity.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "dc_doctor",
        "description": "Run runtime-layer diagnostics for env, Docker, compose, and authenticated API reachability.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "dc_query",
        "description": "Run a natural-language query through DorisClaw without reimplementing planner logic.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {"type": "string"},
                "table_names": {"type": "array", "items": {"type": "string"}},
                "resource_name": {"type": "string"},
                "model": {"type": "string"},
                "base_url": {"type": "string"},
                "api_key": {"type": "string"},
            },
            "required": ["question"],
        },
    },
    {
        "name": "dc_list_tables",
        "description": "List currently registered tables from DorisClaw.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "dc_query_catalog",
        "description": "Fetch the query catalog metadata surface exposed by DorisClaw.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "dc_table_schema",
        "description": "Fetch schema details for a specific table.",
        "inputSchema": {
            "type": "object",
            "properties": {"table_name": {"type": "string"}},
            "required": ["table_name"],
        },
    },
    {
        "name": "dc_query_history",
        "description": "Read recent query history entries.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        },
    },
    {
        "name": "dc_insight_table",
        "description": "Request an analysis/insight run for a table via the existing analysis endpoint.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "depth": {"type": "string", "enum": ["quick", "standard", "deep", "expert"]},
                "resource_name": {"type": "string"},
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "dc_insight_replay",
        "description": "Replay an existing query-history entry through the existing analysis endpoint.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "history_id": {"type": "string"},
                "resource_name": {"type": "string"},
            },
            "required": ["history_id"],
        },
    },
    {
        "name": "dc_report_list",
        "description": "List insight reports from the report contract endpoint.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_names": {"type": "array", "items": {"type": "string"}},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                "offset": {"type": "integer", "minimum": 0},
            },
        },
    },
    {
        "name": "dc_report_detail",
        "description": "Get report detail payload by report id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "report_id": {"type": "string"},
                "include_reasoning": {"type": "boolean"},
            },
            "required": ["report_id"],
        },
    },
    {
        "name": "dc_report_summary",
        "description": "Get report summary payload by report id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "report_id": {"type": "string"},
            },
            "required": ["report_id"],
        },
    },
    {
        "name": "dc_report_latest",
        "description": "Get latest report payload by table name.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "include_reasoning": {"type": "boolean"},
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "dc_forecast",
        "description": "Call the current forecast boundary endpoint (MVP contract consumption).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "metric_key": {"type": "string"},
                "table_names": {"type": "array", "items": {"type": "string"}},
                "granularity": {"type": "string", "enum": ["day", "week", "month"]},
                "horizon_steps": {"type": "integer", "minimum": 1, "maximum": 180},
                "horizon_unit": {"type": "string", "enum": ["day", "week", "month"]},
                "start_at": {"type": "string"},
                "end_at": {"type": "string"},
                "lookback_points": {"type": "integer", "minimum": 1},
                "filters": {"type": "object"},
                "resource_name": {"type": "string"},
                "external_signals": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source": {"type": "string"},
                            "signal_key": {"type": "string"},
                            "granularity": {"type": "string"},
                        },
                        "required": ["source", "signal_key"],
                    },
                },
            },
            "required": ["metric_key"],
        },
    },
]


def handle_jsonrpc_request(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}

    try:
        if request_id is None and method and method.startswith("notifications/"):
            return None

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "dorisclaw-runtime", "version": "1.0.0"},
                    "capabilities": {"tools": {}},
                },
            }

        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}

        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            result = call_tool(name, arguments)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                    "structuredContent": result,
                },
            }

        if method == "ping":
            return {"jsonrpc": "2.0", "id": request_id, "result": {"ok": True}}

        raise RuntimeFailure(code="unsupported_method", message=f"Unsupported method: {method}")
    except RuntimeFailure as exc:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32000,
                "message": exc.message,
                "data": exc.to_payload(),
            },
        }
    except Exception as exc:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": str(exc)},
        }


def call_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    settings = RuntimeSettings.load()
    client = RuntimeClient(settings)

    if name == "dc_health":
        return client.health()
    if name == "dc_doctor":
        return run_doctor(settings, client=client)
    if name == "dc_query":
        return client.query_natural(
            {
                "query": arguments["question"],
                **({
                    key: arguments[key]
                    for key in ("table_names", "resource_name", "model", "base_url", "api_key")
                    if key in arguments and arguments[key] not in (None, "", [])
                }),
            }
        )
    if name == "dc_list_tables":
        return client.list_tables()
    if name == "dc_query_catalog":
        return client.query_catalog()
    if name == "dc_table_schema":
        return client.table_schema(arguments["table_name"])
    if name == "dc_query_history":
        limit = int(arguments.get("limit", 20))
        return client.query_history(limit=limit)
    if name == "dc_insight_table":
        payload = {
            key: arguments[key]
            for key in ("depth", "resource_name")
            if key in arguments and arguments[key] not in (None, "")
        }
        return client.analyze_table(arguments["table_name"], payload)
    if name == "dc_insight_replay":
        payload = {
            key: arguments[key]
            for key in ("resource_name",)
            if key in arguments and arguments[key] not in (None, "")
        }
        return client.analyze_replay(arguments["history_id"], payload)
    if name == "dc_report_list":
        table_names = arguments.get("table_names")
        table_names_filter = None
        if isinstance(table_names, list):
            normalized = [str(item).strip() for item in table_names if str(item).strip()]
            if normalized:
                table_names_filter = ",".join(normalized)
        return client.report_list(
            table_names=table_names_filter,
            limit=int(arguments.get("limit", 20)),
            offset=int(arguments.get("offset", 0)),
        )
    if name == "dc_report_detail":
        return client.report_detail(
            arguments["report_id"],
            include_reasoning=bool(arguments.get("include_reasoning", False)),
        )
    if name == "dc_report_summary":
        return client.report_summary(arguments["report_id"])
    if name == "dc_report_latest":
        return client.report_latest(
            arguments["table_name"],
            include_reasoning=bool(arguments.get("include_reasoning", False)),
        )
    if name == "dc_forecast":
        payload = {
            key: arguments[key]
            for key in (
                "metric_key",
                "table_names",
                "granularity",
                "horizon_steps",
                "horizon_unit",
                "start_at",
                "end_at",
                "lookback_points",
                "filters",
                "resource_name",
                "external_signals",
            )
            if key in arguments and arguments[key] not in (None, "")
        }
        payload = normalize_forecast_payload(payload)
        return client.forecast(payload)

    raise RuntimeFailure(code="unknown_tool", message=f"Unknown tool: {name}")


def main() -> None:
    for raw_line in sys.stdin:
        message = raw_line.strip()
        if not message:
            continue
        response = handle_jsonrpc_request(json.loads(message))
        if response is None:
            continue
        sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
