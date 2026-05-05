from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from .client import RuntimeClient, RuntimeFailure, normalize_forecast_payload
from .config import RuntimeSettings
from .doctor import run_doctor, run_smoke
from .mcp_server import main as run_mcp_server


PLACEHOLDER_OWNERS = {
    "context": "agent-c",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dc", description="DorisClaw CLI runtime surface")
    parser.add_argument("--json", action="store_true", default=True, help="force JSON output (default)")
    parser.add_argument("--compact", action="store_true", help="emit compact JSON")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("doctor", help="run runtime diagnostics")
    subparsers.add_parser("health", help="check API and Doris readiness")

    smoke_parser = subparsers.add_parser("smoke", help="run the runtime smoke flow")
    smoke_parser.add_argument("--compose-up", action="store_true", help="run docker compose up -d before checks")
    smoke_parser.add_argument("--skip-frontend", action="store_true", help="skip the frontend reachability check")

    query_parser = subparsers.add_parser("query", help="run a natural-language query")
    query_parser.add_argument("--question", "-q", help="natural-language question to run")
    query_parser.add_argument("--table", dest="tables", action="append", help="restrict query scope to a table")
    query_parser.add_argument("--resource-name", help="optional LLM resource name")
    query_parser.add_argument("--model", help="optional model override")
    query_parser.add_argument("--base-url", help="optional base URL override")
    query_parser.add_argument("--api-key", help="optional LLM API key override for the query endpoint")
    query_parser.add_argument("--stdin", action="store_true", help="read a question or JSON request body from stdin")
    query_parser.add_argument("--request-file", help="read a JSON request body from a file")

    insight_parser = subparsers.add_parser("insight", help="run an insight/analysis request")
    insight_group = insight_parser.add_mutually_exclusive_group()
    insight_group.add_argument("--table-name", help="table name for /api/analysis/table/{table_name}")
    insight_group.add_argument("--history-id", help="history id for /api/analysis/replay/{history_id}")
    insight_parser.add_argument("--depth", choices=["quick", "standard", "deep", "expert"], help="analysis depth")
    insight_parser.add_argument("--resource-name", help="optional LLM resource name")
    insight_parser.add_argument("--stdin", action="store_true", help="read a JSON request body from stdin")
    insight_parser.add_argument("--request-file", help="read a JSON request body from a file")

    forecast_parser = subparsers.add_parser("forecast", help="run forecast boundary request")
    forecast_parser.add_argument("--metric-key", help="internal metric key from insight output")
    forecast_parser.add_argument("--table-name", dest="table_names", action="append", help="related table name")
    forecast_parser.add_argument("--granularity", choices=["day", "week", "month"], help="forecast granularity")
    forecast_parser.add_argument("--horizon-steps", type=int, help="forecast horizon step count")
    forecast_parser.add_argument("--horizon-unit", choices=["day", "week", "month"], help="forecast horizon unit")
    forecast_parser.add_argument("--start-at", help="optional forecast window start time (ISO 8601)")
    forecast_parser.add_argument("--end-at", help="optional forecast window end time (ISO 8601)")
    forecast_parser.add_argument("--lookback-points", type=int, help="optional lookback point count")
    forecast_parser.add_argument("--filters-json", help="JSON object for forecast filters")
    forecast_parser.add_argument("--resource-name", help="optional LLM resource name")
    forecast_parser.add_argument(
        "--external-signal",
        action="append",
        dest="external_signals",
        help="external signal in `source:signal_key[:granularity]` format",
    )
    forecast_parser.add_argument("--stdin", action="store_true", help="read a JSON request body from stdin")
    forecast_parser.add_argument("--request-file", help="read a JSON request body from a file")

    report_parser = subparsers.add_parser("report", help="read insight reports")
    report_subparsers = report_parser.add_subparsers(dest="report_command", required=True)

    report_list_parser = report_subparsers.add_parser("list", help="list reports")
    report_list_parser.add_argument("--table-name", dest="table_names", action="append", help="filter by table name")
    report_list_parser.add_argument("--limit", type=int, default=20, help="max rows to return")
    report_list_parser.add_argument("--offset", type=int, default=0, help="offset for pagination")

    report_detail_parser = report_subparsers.add_parser("detail", help="get report detail")
    report_detail_parser.add_argument("--report-id", required=True, help="report identifier")
    report_detail_parser.add_argument("--include-reasoning", action="store_true", help="include reasoning trace")

    report_summary_parser = report_subparsers.add_parser("summary", help="get report summary")
    report_summary_parser.add_argument("--report-id", required=True, help="report identifier")

    report_latest_parser = report_subparsers.add_parser("latest", help="get latest report by table")
    report_latest_parser.add_argument("--table-name", required=True, help="table name")
    report_latest_parser.add_argument("--include-reasoning", action="store_true", help="include reasoning trace")

    context_parser = subparsers.add_parser("context", help="reserved context capability shell")
    context_parser.add_argument("--stdin", action="store_true", help="accept placeholder input for contract testing")
    context_parser.add_argument("--request-file", help="read placeholder input from a file")

    mcp_parser = subparsers.add_parser("mcp", help="run the MCP server")
    mcp_subparsers = mcp_parser.add_subparsers(dest="mcp_command", required=True)
    mcp_subparsers.add_parser("serve", help="start the stdio MCP server")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "mcp":
        run_mcp_server()
        return 0

    settings = RuntimeSettings.load()
    client = RuntimeClient(settings)

    try:
        if args.command == "doctor":
            payload = run_doctor(settings, client=client)
        elif args.command == "health":
            payload = client.health()
        elif args.command == "smoke":
            payload = run_smoke(settings, compose_up=args.compose_up, skip_frontend=args.skip_frontend)
        elif args.command == "query":
            payload = _run_query(client, args)
        elif args.command == "insight":
            payload = _run_insight(client, args)
        elif args.command == "report":
            payload = _run_report(client, args)
        elif args.command == "forecast":
            payload = _run_forecast(client, args)
        else:
            raise RuntimeFailure(
                code="capability_not_ready",
                message=f"`dc {args.command}` is reserved but the backend contract is not ready yet.",
                details={"owner": PLACEHOLDER_OWNERS.get(args.command), "command": args.command},
            )
    except RuntimeFailure as exc:
        emit_json({"success": False, "error": exc.to_payload()}, compact=args.compact)
        return 2 if exc.code == "capability_not_ready" else 1

    emit_json(payload, compact=args.compact)
    return 0 if payload.get("success", True) else 1


def emit_json(payload: Dict[str, Any], *, compact: bool) -> None:
    dump_args = {"ensure_ascii": False}
    if compact:
        dump_args["separators"] = (",", ":")
    else:
        dump_args["indent"] = 2
    sys.stdout.write(json.dumps(payload, **dump_args) + "\n")


def _run_query(client: RuntimeClient, args: argparse.Namespace) -> Dict[str, Any]:
    payload = _build_payload(args)
    if args.question:
        payload["query"] = args.question
    if args.tables:
        payload["table_names"] = args.tables
    for field_name in ("resource_name", "model", "base_url", "api_key"):
        value = getattr(args, field_name, None)
        if value not in (None, ""):
            payload[field_name] = value

    if "query" not in payload or not str(payload["query"]).strip():
        raise RuntimeFailure(
            code="input_error",
            message="`dc query` requires --question, --stdin, or --request-file with a `query` field.",
        )

    return client.query_natural(payload)


def _run_insight(client: RuntimeClient, args: argparse.Namespace) -> Dict[str, Any]:
    payload = _build_payload(args)
    table_name = args.table_name
    history_id = args.history_id

    if table_name is None and "table_name" in payload:
        table_name = str(payload["table_name"])
    if history_id is None and "history_id" in payload:
        history_id = str(payload["history_id"])

    request_body: Dict[str, Any] = {}
    if args.depth:
        request_body["depth"] = args.depth
    if args.resource_name:
        request_body["resource_name"] = args.resource_name
    for field_name in ("depth", "resource_name"):
        value = payload.get(field_name)
        if value not in (None, ""):
            request_body[field_name] = value

    if table_name:
        return client.analyze_table(table_name, request_body)
    if history_id:
        request_body.pop("depth", None)
        return client.analyze_replay(history_id, request_body)

    raise RuntimeFailure(
        code="input_error",
        message="`dc insight` requires --table-name, --history-id, or JSON input containing one of those fields.",
    )


def _run_report(client: RuntimeClient, args: argparse.Namespace) -> Dict[str, Any]:
    if args.report_command == "list":
        table_names = _normalize_string_list(getattr(args, "table_names", None))
        return client.report_list(
            table_names=",".join(table_names) if table_names else None,
            limit=int(args.limit),
            offset=int(args.offset),
        )
    if args.report_command == "detail":
        return client.report_detail(args.report_id, include_reasoning=bool(args.include_reasoning))
    if args.report_command == "summary":
        return client.report_summary(args.report_id)
    if args.report_command == "latest":
        return client.report_latest(args.table_name, include_reasoning=bool(args.include_reasoning))

    raise RuntimeFailure(
        code="input_error",
        message="`dc report` requires one of: list, detail, summary, latest.",
    )


def _run_forecast(client: RuntimeClient, args: argparse.Namespace) -> Dict[str, Any]:
    payload = _build_payload(args)

    metric_key = args.metric_key or payload.get("metric_key")
    if not metric_key or not str(metric_key).strip():
        raise RuntimeFailure(
            code="input_error",
            message="`dc forecast` requires metric_key (use --metric-key or JSON input).",
        )
    payload["metric_key"] = str(metric_key).strip()

    table_names = _normalize_string_list(args.table_names)
    if table_names:
        payload["table_names"] = table_names
    elif "table_names" in payload:
        payload["table_names"] = _normalize_string_list(payload.get("table_names"))

    if args.horizon_steps is not None:
        payload["horizon_steps"] = int(args.horizon_steps)
    if args.granularity:
        payload["granularity"] = args.granularity
    if args.horizon_unit:
        payload["horizon_unit"] = args.horizon_unit
    if args.start_at:
        payload["start_at"] = args.start_at
    if args.end_at:
        payload["end_at"] = args.end_at
    if args.lookback_points is not None:
        payload["lookback_points"] = int(args.lookback_points)
    if args.filters_json:
        payload["filters"] = _parse_json_object(args.filters_json, field_name="--filters-json")
    if args.resource_name:
        payload["resource_name"] = args.resource_name

    cli_external_signals = _parse_external_signals(args.external_signals or [])
    if cli_external_signals:
        payload["external_signals"] = cli_external_signals
    elif "external_signals" in payload and isinstance(payload.get("external_signals"), list):
        payload["external_signals"] = list(payload["external_signals"])

    payload = normalize_forecast_payload(payload)
    return client.forecast(payload)


def _build_payload(args: argparse.Namespace) -> Dict[str, Any]:
    raw_input = _read_optional_input(
        request_file=getattr(args, "request_file", None),
        use_stdin=bool(getattr(args, "stdin", False)),
    )
    if raw_input is None:
        return {}

    if isinstance(raw_input, dict):
        return dict(raw_input)
    if isinstance(raw_input, str):
        return {"query": raw_input}

    raise RuntimeFailure(
        code="input_error",
        message="Input must be either a JSON object or a plain text question.",
    )


def _read_optional_input(*, request_file: Optional[str], use_stdin: bool) -> Optional[Any]:
    if request_file:
        raw = Path(request_file).read_text(encoding="utf-8")
        return _parse_input(raw)
    if use_stdin:
        raw = sys.stdin.read()
        return _parse_input(raw)
    return None


def _parse_input(raw: str) -> Optional[Any]:
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped[0] in "{[":
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass
    return stripped


def _parse_json_object(raw: str, *, field_name: str) -> Dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeFailure(
            code="input_error",
            message=f"{field_name} must be valid JSON object text.",
        ) from exc

    if not isinstance(payload, dict):
        raise RuntimeFailure(
            code="input_error",
            message=f"{field_name} must decode to a JSON object.",
        )
    return payload


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def _parse_external_signals(values: list[str]) -> list[Dict[str, str]]:
    parsed: list[Dict[str, str]] = []
    for raw_value in values:
        parts = [part.strip() for part in str(raw_value).split(":", 2)]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise RuntimeFailure(
                code="input_error",
                message="`--external-signal` must use `source:signal_key[:granularity]` format.",
            )
        signal = {
            "source": parts[0],
            "signal_key": parts[1],
        }
        if len(parts) == 3 and parts[2]:
            signal["granularity"] = parts[2]
        parsed.append(signal)
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
