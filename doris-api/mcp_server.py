"""Legacy-compatible MCP module that bridges to the shared runtime layer."""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional

from dc_runtime.client import RuntimeClient
from dc_runtime.config import RuntimeSettings
from dc_runtime import mcp_server as runtime_mcp_server


LEGACY_TOOL_QUERY_NATURAL = {
    "name": "query_natural",
    "description": "Run a natural-language query and return generated SQL plus result rows.",
    "inputSchema": {
        "type": "object",
        "properties": {"question": {"type": "string"}},
        "required": ["question"],
    },
}


class LegacyApiClient:
    """Thin adapter that preserves legacy method names over RuntimeClient."""

    def __init__(self, runtime_client: Optional[RuntimeClient] = None):
        self._runtime_client = runtime_client or RuntimeClient(RuntimeSettings.load())

    def query_natural(self, question: str) -> Dict[str, Any]:
        return self._runtime_client.query_natural({"query": question})


def build_api_client() -> LegacyApiClient:
    """Legacy contract: return a client exposing query_natural(question)."""
    return LegacyApiClient()


def handle_jsonrpc_request(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Legacy-compatible dispatcher with fallback to the shared runtime MCP server."""
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}

    try:
        if request_id is None and method and method.startswith("notifications/"):
            return None

        # Keep the legacy query_natural contract available for historical callers.
        if method == "tools/call" and params.get("name") == "query_natural":
            question = (params.get("arguments") or {}).get("question")
            if not isinstance(question, str) or not question.strip():
                raise ValueError("Missing required argument: question")
            result = build_api_client().query_natural(question)
            return _build_success_response(request_id, result)

        if method == "tools/list":
            response = runtime_mcp_server.handle_jsonrpc_request(payload)
            if response is None:
                return None

            return _build_legacy_tools_list_response(response)

        return runtime_mcp_server.handle_jsonrpc_request(payload)
    except Exception as error:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": str(error)},
        }


def _build_success_response(request_id: Any, result: Any) -> Dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
            "structuredContent": result,
        },
    }


def _build_legacy_tools_list_response(runtime_response: Dict[str, Any]) -> Dict[str, Any]:
    runtime_result = runtime_response.get("result")
    if not isinstance(runtime_result, dict):
        return dict(runtime_response)

    runtime_tools = runtime_result.get("tools") or []
    copied_tools = [
        dict(tool) if isinstance(tool, dict) else tool
        for tool in runtime_tools
    ]
    if not any(isinstance(tool, dict) and tool.get("name") == "query_natural" for tool in copied_tools):
        copied_tools.append(dict(LEGACY_TOOL_QUERY_NATURAL))

    legacy_result = dict(runtime_result)
    legacy_result["tools"] = copied_tools

    legacy_response = dict(runtime_response)
    legacy_response["result"] = legacy_result
    return legacy_response


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
