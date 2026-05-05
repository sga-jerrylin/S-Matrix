"""
Data analysis agent for on-demand and replay-based reporting.
"""

from __future__ import annotations

import json
import logging
from math import ceil, sqrt
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from config import (
    ANALYST_MAX_REASONING_CHARS,
    ANALYST_MAX_ROUNDS,
)
from repair_agent import RepairAgent


logger = logging.getLogger(__name__)

_NUMERIC_MARKERS = ("int", "decimal", "float", "double", "numeric", "real")
_TEMPORAL_MARKERS = ("date", "time", "year")
_DEPTH_MAX_STEPS = {"quick": 3, "standard": 6, "deep": 10}
_EXPERT_RESULT_ROW_LIMIT = 100
_EXPERT_CONTEXT_ROW_LIMIT = 20
_EXPERT_CONTEXT_COLUMN_LIMIT = 5
_EXPERT_FALLBACK_DEPTH = "deep"
_EXPERT_MAIN_SECTION_LIMIT = 3
_TEMPORAL_GRAIN_ORDER = ("day", "week", "month", "quarter", "year")
_TEMPORAL_LOOKBACK_DEFAULTS = {
    "day": 90,
    "week": 104,
    "month": 36,
    "quarter": 16,
    "year": 10,
}
_TEMPORAL_ANALYSIS_TYPES = {"trend", "seasonality", "anomaly", "comparison"}
_TEMPORAL_COMPARISON_MODES = {"wow", "mom", "qoq", "yoy", "baseline", "none"}
_INSIGHT_REPORT_CONTRACT_VERSION = "insight.report.read.v1"
_INSIGHT_REPORT_SUMMARY_CONTRACT_VERSION = "insight.report.summary.v1"
_SUMMARY_SECTION_LIMIT = 3
_FORECAST_RESULT_CONTRACT_VERSION = "insight.forecast.result.v1"
_FORECAST_MODEL_VERSION = "baseline.internal.v1"
_FORECAST_DEFAULT_CONFIDENCE = 0.8


class AnalystAgent:
    """
    Data analysis expert agent built on top of the existing Doris stack.
    """

    def __init__(self, doris_client, build_api_config_fn, metric_provider=None):
        self.db = doris_client
        self.build_api_config = build_api_config_fn
        self.metric_provider = metric_provider

    def init_tables(self) -> bool:
        """Create system tables required by the analyst workflow."""
        sql = """
        CREATE TABLE IF NOT EXISTS `_sys_analysis_reports` (
            `id` VARCHAR(64),
            `table_names` TEXT,
            `trigger_type` VARCHAR(50),
            `depth` VARCHAR(20),
            `schedule_id` VARCHAR(64),
            `history_id` VARCHAR(64),
            `summary` TEXT,
            `report_json` TEXT,
            `insight_count` INT DEFAULT "0",
            `anomaly_count` INT DEFAULT "0",
            `failed_step_count` INT DEFAULT "0",
            `status` VARCHAR(20) DEFAULT "completed",
            `error_message` TEXT,
            `duration_ms` INT DEFAULT "0",
            `created_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
        self.db.execute_update(sql)
        return True

    def analyze_table(
        self,
        table_name: str,
        depth: str = "standard",
        resource_name: Optional[str] = None,
        *,
        trigger_type: str = "table_analysis",
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        if (depth or "").strip().lower() == "expert":
            return self.analyze_table_expert(
                table_name,
                resource_name=resource_name,
                trigger_type=trigger_type,
                schedule_id=schedule_id,
                note=note,
            )

        started_at = time.time()
        raw_table_name = (table_name or "").strip()
        safe_table_name = self.db.validate_identifier(raw_table_name)
        api_config = self.build_api_config(resource_name=resource_name)
        if not api_config.get("api_key"):
            return {
                "success": False,
                "error": "No API key configured. Set DEEPSEEK_API_KEY or OPENAI_API_KEY.",
            }

        profile = self._profile_table(raw_table_name, safe_table_name=safe_table_name)
        metadata = self._get_table_metadata(raw_table_name)
        plan_steps = self._plan_analysis(profile, metadata, depth, api_config)
        step_results = self._run_plan_steps(plan_steps, api_config)

        report = self._build_report(
            table_names=self._merge_table_names([raw_table_name], step_results),
            profile=profile,
            insights=self._generate_insights(step_results, profile, api_config),
            depth=depth,
            trigger_type=trigger_type,
            step_results=step_results,
            started_at=started_at,
            schedule_id=schedule_id,
            note=note,
        )
        report = self._normalize_report_contract(report)
        self._save_report(report)
        return report

    def analyze_table_expert(
        self,
        table_name: str,
        resource_name: Optional[str] = None,
        *,
        trigger_type: str = "table_analysis",
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        started_at = time.time()
        raw_table_name = (table_name or "").strip()
        safe_table_name = self.db.validate_identifier(raw_table_name)
        executor_config = self._build_executor_config(self.build_api_config(resource_name=resource_name))
        if not executor_config.get("api_key"):
            return {
                "success": False,
                "error": "No API key configured. Set DEEPSEEK_API_KEY or OPENAI_API_KEY.",
            }

        strategist_config = self._build_strategist_config()
        if not strategist_config.get("api_key"):
            return self._fallback_from_expert(
                table_name=raw_table_name,
                resource_name=resource_name,
                trigger_type=trigger_type,
                schedule_id=schedule_id,
                note=note,
                error=RuntimeError("No strategist API key configured."),
        )
        profile = self._profile_table(raw_table_name, safe_table_name=safe_table_name)
        metadata = self._get_table_metadata(raw_table_name)
        temporal_dimensions = self._detect_temporal_dimensions(safe_table_name, profile)
        if temporal_dimensions:
            metadata = {
                **metadata,
                "temporal_dimensions": temporal_dimensions,
            }
        stats = self._compute_statistical_facts(
            safe_table_name,
            profile,
            temporal_dimensions=temporal_dimensions,
        )

        compressed_history: List[Dict[str, Any]] = []
        all_step_results: List[Dict[str, Any]] = []
        reasoning_traces: List[Dict[str, Any]] = []

        max_rounds = int(os.getenv("ANALYST_MAX_ROUNDS", str(ANALYST_MAX_ROUNDS)))
        for round_num in range(1, max_rounds + 1):
            prompt = self._build_round_prompt(round_num, profile, stats, metadata, list(compressed_history))
            try:
                strategist_output = self._call_strategist(prompt, strategist_config)
            except Exception as exc:
                return self._fallback_from_expert(
                    table_name=raw_table_name,
                    resource_name=resource_name,
                    trigger_type=trigger_type,
                    schedule_id=schedule_id,
                    note=note,
                    error=exc,
                )
            reasoning = strategist_output.get("reasoning")
            if reasoning:
                reasoning_traces.append(
                    {
                        "round": round_num,
                        "trace": self._truncate_reasoning(self._sanitize_reasoning(reasoning)),
                    }
                )

            queries = self._extract_queries_from_strategist(strategist_output, round_num)
            round_step_results: List[Dict[str, Any]] = []
            for query in queries:
                sql = self._executor_translate_to_sql(query, metadata, executor_config)
                data, final_sql, success, error = self._execute_with_repair(
                    sql,
                    query.get("query_description") or query.get("title") or f"Expert analysis round {round_num}",
                    executor_config,
                )
                capped_data = list(data or [])[:_EXPERT_RESULT_ROW_LIMIT] if success else None
                step = {
                    "round": round_num,
                    "title": query.get("title") or f"Round {round_num} analysis",
                    "question": query.get("query_description") or query.get("title") or "",
                    "sql": final_sql,
                    "success": success,
                    "error_message": error,
                    "row_count": len(capped_data or []),
                    "data": capped_data,
                }
                round_step_results.append(step)
                all_step_results.append(step)

            round_data = {
                "round": round_num,
                "strategist": strategist_output,
                "results": round_step_results,
            }
            compressed_history.append(self._compress_round_for_context(round_data))

            if not strategist_output.get("response", {}).get("continue", False):
                break

        report = self._build_expert_report(
            table_names=[raw_table_name],
            profile=profile,
            compressed_history=compressed_history,
            reasoning_traces=reasoning_traces,
            all_step_results=all_step_results,
            trigger_type=trigger_type,
            started_at=started_at,
            schedule_id=schedule_id,
            note=note,
        )
        report = self._normalize_report_contract(report)
        self._save_report(report)
        return report

    def _fallback_from_expert(
        self,
        *,
        table_name: str,
        resource_name: Optional[str],
        trigger_type: str,
        schedule_id: Optional[str],
        note: Optional[str],
        error: Exception,
    ) -> Dict[str, Any]:
        logger.warning(
            "expert strategist unavailable for %s, falling back to %s analysis: %s",
            table_name,
            _EXPERT_FALLBACK_DEPTH,
            error,
        )
        fallback_note = "Expert mode unavailable; fell back to deep analysis."
        if note:
            fallback_note = f"{note} {fallback_note}"
        return self.analyze_table(
            table_name,
            depth=_EXPERT_FALLBACK_DEPTH,
            resource_name=resource_name,
            trigger_type=trigger_type,
            schedule_id=schedule_id,
            note=fallback_note,
        )

    def replay_from_history(
        self,
        history_id: str,
        resource_name: Optional[str] = None,
        *,
        trigger_type: str = "history_replay",
        schedule_id: Optional[str] = None,
        note: str = "Replayed against current data.",
    ) -> Dict[str, Any]:
        api_config = self.build_api_config(resource_name=resource_name)
        if not api_config.get("api_key"):
            return {
                "success": False,
                "error": "No API key configured. Set DEEPSEEK_API_KEY or OPENAI_API_KEY.",
            }

        history_rows = self.db.execute_query(
            """
            SELECT `id`, `question`, `sql`, `table_names`
            FROM `_sys_query_history`
            WHERE `id` = %s
            LIMIT 1
            """,
            (history_id,),
        )
        if not history_rows:
            return {"success": False, "error": f"History record '{history_id}' not found."}

        history_row = history_rows[0]
        table_names = self._decode_table_names(history_row.get("table_names"))
        primary_table = table_names[0] if table_names else None
        started_at = time.time()
        profile = (
            self._profile_table(primary_table)
            if primary_table
            else {"row_count": 0, "sampled": False, "sample_size": 0, "columns": {}}
        )
        data, final_sql, success, error_message = self._execute_with_repair(
            history_row.get("sql") or "",
            history_row.get("question") or "Replay saved query",
            api_config,
        )
        step_results = [
            {
                "title": "Replay saved query",
                "question": history_row.get("question") or "Replay saved query",
                "sql": final_sql,
                "success": success,
                "error_message": error_message,
                "row_count": len(data or []),
                "data": data if success else None,
            }
        ]

        report = self._build_report(
            table_names=self._merge_table_names(table_names, step_results),
            profile=profile,
            insights=self._generate_insights(step_results, profile, api_config),
            depth="quick",
            trigger_type=trigger_type,
            step_results=step_results,
            started_at=started_at,
            history_id=history_id,
            schedule_id=schedule_id,
            note=note,
        )
        report = self._normalize_report_contract(report)
        self._save_report(report)
        return report

    def forecast_metric(
        self,
        metric_key: str,
        *,
        granularity: str = "day",
        horizon_steps: int = 7,
        start_at: Optional[str] = None,
        end_at: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        lookback_points: int = 180,
        metric_provider: Optional[Any] = None,
    ) -> Dict[str, Any]:
        normalized_granularity = (granularity or "day").strip().lower()
        forecast_id = str(uuid.uuid4())
        resolved_provider = metric_provider or self.metric_provider
        payload = self._build_forecast_payload_shell(
            forecast_id=forecast_id,
            metric_key=metric_key,
            granularity=normalized_granularity,
            horizon_steps=horizon_steps,
        )

        try:
            if horizon_steps <= 0:
                return self._build_forecast_error(
                    payload,
                    code="invalid_horizon",
                    message="horizon_steps must be greater than 0.",
                )
            if normalized_granularity not in {"day", "week", "month"}:
                return self._build_forecast_error(
                    payload,
                    code="invalid_granularity",
                    message="granularity must be one of day, week, month.",
                )

            window_start, window_end = self._resolve_forecast_window(
                start_at=start_at,
                end_at=end_at,
                granularity=normalized_granularity,
                lookback_points=lookback_points,
            )
            payload["horizon"]["history_window"] = {
                "start_at": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_at": window_end.strftime("%Y-%m-%d %H:%M:%S"),
            }

            registered_metric = self._get_metric_definition_from_provider(resolved_provider, metric_key)
            metric_source = "registered_metric"
            metric_spec: Dict[str, Any]

            if registered_metric:
                metric_spec = self._build_metric_spec_from_metric_definition(registered_metric)
                availability = registered_metric.get("availability") or self._evaluate_metric_availability_from_provider(
                    resolved_provider,
                    registered_metric,
                )
                if availability and not availability.get("forecast_ready"):
                    return self._build_forecast_error(
                        payload,
                        code="metric_not_forecast_ready",
                        message="metric is not forecast-ready according to metric foundation",
                        details={
                            "metric_source": metric_source,
                            "blocking_reasons": availability.get("blocking_reasons") or [],
                        },
                    )
                series_payload = self._get_metric_series_from_provider(
                    resolved_provider,
                    metric_key=metric_key,
                    start_time=window_start,
                    end_time=window_end,
                    granularity=normalized_granularity,
                    filters=filters,
                    limit=max(int(lookback_points) * 2, int(horizon_steps) * 4, 200),
                )
                series = self._build_series_from_metric_surface_points(
                    series_payload.get("points") or [],
                    granularity=normalized_granularity,
                    aggregation=metric_spec["aggregation"],
                )
            else:
                metric_source = "legacy_expression_compat"
                metric_spec = self._parse_metric_key(metric_key)
                compat_metric = self._build_compat_metric_definition(
                    metric_key=metric_key,
                    metric_spec=metric_spec,
                    granularity=normalized_granularity,
                    filters=filters,
                )
                availability = self._evaluate_metric_availability_from_provider(resolved_provider, compat_metric)
                if availability and not availability.get("forecast_ready"):
                    return self._build_forecast_error(
                        payload,
                        code="metric_not_forecast_ready",
                        message="metric is not forecast-ready according to metric foundation",
                        details={
                            "metric_source": metric_source,
                            "blocking_reasons": availability.get("blocking_reasons") or [],
                        },
                    )
                raw_rows = self._query_metric_rows(
                    metric_spec,
                    start_at=window_start,
                    end_at=window_end,
                    filters=filters,
                )
                series = self._build_metric_series(
                    raw_rows,
                    granularity=normalized_granularity,
                    aggregation=metric_spec["aggregation"],
                    value_column=metric_spec["value_column"],
                )

            payload["model_info"]["aggregation"] = metric_spec["aggregation"]
            payload["model_info"]["table_name"] = metric_spec["table_name"]
            payload["model_info"]["time_column"] = metric_spec["time_column"]
            payload["model_info"]["value_column"] = metric_spec["value_column"]
            payload["model_info"]["metric_source"] = metric_source

            if len(series) < 3:
                return self._build_forecast_error(
                    payload,
                    code="insufficient_history",
                    message="At least 3 historical points are required for baseline forecast.",
                    details={"history_points": len(series)},
                )

            if lookback_points > 0 and len(series) > lookback_points:
                series = series[-lookback_points:]

            history_values = [float(item["value"]) for item in series]
            history_dates = [item["bucket"] for item in series]
            holdout_points = min(max(1, horizon_steps), max(1, len(history_values) // 3))
            if len(history_values) - holdout_points < 2:
                holdout_points = 1
            if len(history_values) - holdout_points < 2:
                return self._build_forecast_error(
                    payload,
                    code="insufficient_training_points",
                    message="Not enough training points after holdout split.",
                    details={"history_points": len(history_values), "holdout_points": holdout_points},
                )

            train_values = history_values[:-holdout_points]
            test_values = history_values[-holdout_points:]
            backtest_predictions, backtest_model = self._baseline_forecast_values(
                train_values,
                holdout_points,
                granularity=normalized_granularity,
                aggregation=metric_spec["aggregation"],
            )
            backtest_summary = self._compute_backtest_summary(
                actual_values=test_values,
                predicted_values=backtest_predictions,
                train_points=len(train_values),
                holdout_points=holdout_points,
            )

            forecast_values, forecast_model = self._baseline_forecast_values(
                history_values,
                horizon_steps,
                granularity=normalized_granularity,
                aggregation=metric_spec["aggregation"],
            )
            residual_std = float(backtest_summary.get("residual_std") or 0.0)
            if residual_std <= 0:
                baseline_scale = abs(sum(history_values) / len(history_values)) if history_values else 1.0
                residual_std = max(1e-6, baseline_scale * 0.05)

            last_bucket = history_dates[-1]
            forecast_points = []
            for index, value in enumerate(forecast_values, start=1):
                point_date = self._add_granularity(last_bucket, normalized_granularity, index)
                interval_width = 1.28155 * residual_std * sqrt(index)
                lower = value - interval_width
                upper = value + interval_width
                if metric_spec["aggregation"] in {"count", "count_distinct", "sum"}:
                    lower = max(0.0, lower)
                    upper = max(lower, upper)
                forecast_points.append(
                    {
                        "ts": point_date.isoformat(),
                        "value": round(float(value), 6),
                        "lower": round(float(lower), 6),
                        "upper": round(float(upper), 6),
                        "confidence": _FORECAST_DEFAULT_CONFIDENCE,
                    }
                )

            payload["success"] = True
            payload["status"] = "completed"
            payload["points"] = forecast_points
            payload["assumptions"] = [
                f"Baseline model uses {forecast_model} over internal metric history only.",
                "External signals are not used in this MVP.",
                f"Granularity is fixed at {normalized_granularity}.",
                f"Metric source: {metric_source}.",
            ]
            payload["backtest_summary"] = backtest_summary
            payload["model_info"].update(
                {
                    "name": forecast_model,
                    "version": _FORECAST_MODEL_VERSION,
                    "granularity": normalized_granularity,
                    "training_points": len(history_values),
                    "history_points": len(history_values),
                    "status": "ready",
                    "backtest_model": backtest_model,
                }
            )
            payload["horizon"]["start_at"] = forecast_points[0]["ts"]
            payload["horizon"]["end_at"] = forecast_points[-1]["ts"]
            payload["history"] = {
                "points": len(history_values),
                "start_at": history_dates[0].isoformat(),
                "end_at": history_dates[-1].isoformat(),
                "last_value": round(float(history_values[-1]), 6),
            }
            return payload
        except ValueError as exc:
            return self._build_forecast_error(
                payload,
                code="invalid_input",
                message=str(exc),
            )
        except Exception as exc:
            logger.exception("forecast metric failed: %s", exc)
            return self._build_forecast_error(
                payload,
                code="forecast_failed",
                message=str(exc),
            )

    def _build_forecast_payload_shell(
        self,
        *,
        forecast_id: str,
        metric_key: str,
        granularity: str,
        horizon_steps: int,
    ) -> Dict[str, Any]:
        return {
            "success": False,
            "status": "failed",
            "contract_version": _FORECAST_RESULT_CONTRACT_VERSION,
            "forecast_id": forecast_id,
            "metric_key": metric_key,
            "horizon": {
                "steps": int(horizon_steps),
                "unit": granularity,
                "granularity": granularity,
                "start_at": None,
                "end_at": None,
                "history_window": {
                    "start_at": None,
                    "end_at": None,
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
                "version": _FORECAST_MODEL_VERSION,
                "status": "failed",
                "granularity": granularity,
                "aggregation": None,
                "table_name": None,
                "time_column": None,
                "value_column": None,
                "training_points": 0,
                "history_points": 0,
            },
        }

    def _build_forecast_error(
        self,
        payload: Dict[str, Any],
        *,
        code: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        error_payload = dict(payload)
        error_payload["success"] = False
        error_payload["status"] = "failed"
        error_payload["points"] = []
        error_payload["error"] = {
            "code": code,
            "message": message,
            "details": details or {},
        }
        error_payload.setdefault("backtest_summary", {})
        error_payload["backtest_summary"]["status"] = "unavailable"
        error_payload.setdefault("model_info", {})
        error_payload["model_info"]["status"] = "failed"
        return error_payload

    def _get_metric_definition_from_provider(self, provider: Any, metric_key: str) -> Optional[Dict[str, Any]]:
        if provider is None:
            return None
        getter = getattr(provider, "get_metric_definition", None)
        if not callable(getter):
            return None
        return getter(metric_key)

    def _get_metric_series_from_provider(
        self,
        provider: Any,
        *,
        metric_key: str,
        start_time: datetime,
        end_time: datetime,
        granularity: str,
        filters: Optional[Dict[str, Any]],
        limit: int,
    ) -> Dict[str, Any]:
        getter = getattr(provider, "get_metric_series", None) if provider is not None else None
        if not callable(getter):
            raise ValueError("metric provider does not support metric series reads")
        return getter(
            metric_key,
            start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"),
            grain=granularity,
            filters=filters or {},
            limit=max(1, min(int(limit), 20000)),
        )

    def _evaluate_metric_availability_from_provider(
        self,
        provider: Any,
        metric_definition: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if provider is None:
            return None
        evaluator = getattr(provider, "evaluate_metric_availability", None)
        if not callable(evaluator):
            return None
        return evaluator(metric_definition)

    def _build_metric_spec_from_metric_definition(self, metric_definition: Dict[str, Any]) -> Dict[str, str]:
        aggregation = str(metric_definition.get("aggregation") or "").strip().lower()
        if aggregation not in {"sum", "avg", "min", "max", "count", "count_distinct"}:
            raise ValueError(f"Unsupported metric aggregation for forecast: '{aggregation}'")
        table_name = str(metric_definition.get("table_name") or "").strip()
        time_column = str(metric_definition.get("time_field") or "").strip()
        value_column = str(metric_definition.get("value_field") or "").strip()
        if not table_name or not time_column:
            raise ValueError("Metric definition must include table_name and time_field")
        if aggregation in {"sum", "avg", "min", "max", "count_distinct"} and not value_column:
            raise ValueError(f"Metric definition requires value_field for aggregation '{aggregation}'")
        if aggregation == "count":
            value_column = value_column or "*"
        return {
            "table_name": table_name,
            "aggregation": aggregation,
            "time_column": time_column,
            "value_column": value_column or "*",
        }

    def _build_compat_metric_definition(
        self,
        *,
        metric_key: str,
        metric_spec: Dict[str, str],
        granularity: str,
        filters: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        filter_dimensions = sorted([str(key) for key in (filters or {}).keys() if str(key).strip()])
        value_field = metric_spec["value_column"] if metric_spec["value_column"] != "*" else ""
        return {
            "metric_key": metric_key,
            "display_name": metric_key,
            "description": "legacy metric_key compatibility",
            "table_name": metric_spec["table_name"],
            "time_field": metric_spec["time_column"],
            "value_field": value_field,
            "aggregation_expression": "",
            "aggregation": metric_spec["aggregation"],
            "default_grain": granularity,
            "dimensions": filter_dimensions,
        }

    def _build_series_from_metric_surface_points(
        self,
        points: Sequence[Dict[str, Any]],
        *,
        granularity: str,
        aggregation: str,
    ) -> List[Dict[str, Any]]:
        grouped: Dict[date, List[float]] = {}
        for point in points:
            dt_value = self._coerce_datetime_value(point.get("ts"))
            if dt_value is None:
                continue
            numeric = self._coerce_float(point.get("value"))
            if numeric is None:
                continue
            bucket = self._bucket_datetime(dt_value, granularity)
            grouped.setdefault(bucket, []).append(float(numeric))

        series: List[Dict[str, Any]] = []
        for bucket in sorted(grouped.keys()):
            values = grouped[bucket]
            if not values:
                continue
            if aggregation == "sum":
                metric_value = sum(values)
            elif aggregation == "avg":
                metric_value = sum(values) / len(values)
            elif aggregation == "min":
                metric_value = min(values)
            elif aggregation == "max":
                metric_value = max(values)
            else:
                metric_value = sum(values)
            series.append(
                {
                    "bucket": bucket,
                    "ts": bucket.isoformat(),
                    "value": float(metric_value),
                }
            )
        return series

    def _parse_metric_key(self, metric_key: str) -> Dict[str, str]:
        pattern = (
            r"^\s*([\w\-\u4e00-\u9fa5]+)\."
            r"(sum|avg|min|max|count)\("
            r"(\*|[\w\-\u4e00-\u9fa5]+)\)"
            r"@([\w\-\u4e00-\u9fa5]+)\s*$"
        )
        match = re.match(pattern, str(metric_key or ""), flags=re.IGNORECASE)
        if not match:
            raise ValueError(
                "metric_key must be a registered metric id from metric foundation, "
                "or match legacy '<table>.<agg>(<value_column>|*)@<time_column>' "
                "(for example 'orders.sum(amount)@order_date')."
            )
        table_name, aggregation, value_column, time_column = match.groups()
        aggregation = aggregation.lower()
        if aggregation != "count" and value_column == "*":
            raise ValueError("Only count aggregation can use '*'.")
        return {
            "table_name": table_name,
            "aggregation": aggregation,
            "value_column": value_column,
            "time_column": time_column,
        }

    def _resolve_forecast_window(
        self,
        *,
        start_at: Optional[str],
        end_at: Optional[str],
        granularity: str,
        lookback_points: int,
    ) -> Tuple[datetime, datetime]:
        parsed_start = self._parse_optional_datetime(start_at)
        parsed_end = self._parse_optional_datetime(end_at)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        if parsed_end is None:
            parsed_end = now_utc
        if parsed_start is None:
            points = max(lookback_points, 30)
            if granularity == "week":
                parsed_start = parsed_end - timedelta(days=7 * points)
            elif granularity == "month":
                parsed_start = parsed_end - timedelta(days=31 * points)
            else:
                parsed_start = parsed_end - timedelta(days=points)
        if parsed_start > parsed_end:
            raise ValueError("start_at must be earlier than or equal to end_at.")
        return parsed_start, parsed_end

    def _parse_optional_datetime(self, value: Optional[str]) -> Optional[datetime]:
        if value in (None, ""):
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    def _query_metric_rows(
        self,
        metric_spec: Dict[str, str],
        *,
        start_at: datetime,
        end_at: datetime,
        filters: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        safe_table = self.db.validate_identifier(metric_spec["table_name"])
        safe_time_column = self.db.validate_identifier(metric_spec["time_column"])
        aggregation = metric_spec["aggregation"]
        value_column = metric_spec["value_column"]
        if aggregation == "count" and value_column == "*":
            value_expr = "1"
        else:
            value_expr = self.db.validate_identifier(value_column)

        where_clauses = [f"{safe_time_column} IS NOT NULL", f"{safe_time_column} >= %s", f"{safe_time_column} <= %s"]
        params: List[Any] = [
            start_at.strftime("%Y-%m-%d %H:%M:%S"),
            end_at.strftime("%Y-%m-%d %H:%M:%S"),
        ]
        for key, value in sorted((filters or {}).items()):
            safe_key = self.db.validate_identifier(str(key))
            if isinstance(value, list):
                if not value:
                    continue
                placeholders = ", ".join(["%s"] * len(value))
                where_clauses.append(f"{safe_key} IN ({placeholders})")
                params.extend(value)
                continue
            if value is None:
                where_clauses.append(f"{safe_key} IS NULL")
                continue
            where_clauses.append(f"{safe_key} = %s")
            params.append(value)

        sql = (
            f"SELECT {safe_time_column} AS ts, {value_expr} AS metric_value "
            f"FROM {safe_table} "
            f"WHERE {' AND '.join(where_clauses)} "
            f"ORDER BY {safe_time_column} ASC"
        )
        return self.db.execute_query(sql, tuple(params))

    def _build_metric_series(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        granularity: str,
        aggregation: str,
        value_column: str,
    ) -> List[Dict[str, Any]]:
        grouped: Dict[date, List[float]] = {}
        for row in rows:
            dt_value = self._coerce_datetime_value(row.get("ts"))
            if dt_value is None:
                continue
            bucket = self._bucket_datetime(dt_value, granularity)
            value = row.get("metric_value")
            if aggregation == "count":
                if value_column == "*":
                    numeric = 1.0
                else:
                    numeric = 1.0 if value is not None else 0.0
            else:
                numeric = self._coerce_float(value)
                if numeric is None:
                    continue
            grouped.setdefault(bucket, []).append(float(numeric))

        series: List[Dict[str, Any]] = []
        for bucket in sorted(grouped.keys()):
            values = grouped[bucket]
            if not values:
                continue
            if aggregation == "sum":
                metric_value = sum(values)
            elif aggregation == "avg":
                metric_value = sum(values) / len(values)
            elif aggregation == "min":
                metric_value = min(values)
            elif aggregation == "max":
                metric_value = max(values)
            else:
                metric_value = float(len(values))
            series.append(
                {
                    "bucket": bucket,
                    "ts": bucket.isoformat(),
                    "value": float(metric_value),
                }
            )
        return series

    def _coerce_datetime_value(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        text = str(value).strip()
        if not text:
            return None
        try:
            normalized = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            return None

    def _coerce_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _bucket_datetime(self, value: datetime, granularity: str) -> date:
        current_date = value.date()
        if granularity == "week":
            return current_date - timedelta(days=current_date.weekday())
        if granularity == "month":
            return current_date.replace(day=1)
        return current_date

    def _add_granularity(self, value: date, granularity: str, steps: int) -> date:
        if granularity == "week":
            return value + timedelta(days=7 * steps)
        if granularity == "month":
            year = value.year
            month = value.month
            total_month = (month - 1) + steps
            year += total_month // 12
            month = (total_month % 12) + 1
            return date(year, month, 1)
        return value + timedelta(days=steps)

    def _baseline_forecast_values(
        self,
        history_values: Sequence[float],
        steps: int,
        *,
        granularity: str,
        aggregation: str,
    ) -> Tuple[List[float], str]:
        if steps <= 0:
            return [], "baseline_internal"
        seasonal_map = {"day": 7, "week": 4, "month": 3}
        seasonal_period = seasonal_map.get(granularity, 7)
        history = [float(value) for value in history_values]
        predictions: List[float] = []

        if len(history) >= seasonal_period * 2:
            extended = list(history)
            for _ in range(steps):
                source_index = len(extended) - seasonal_period
                predicted = float(extended[source_index]) if source_index >= 0 else float(extended[-1])
                predictions.append(predicted)
                extended.append(predicted)
            model_name = f"seasonal_naive_p{seasonal_period}"
        else:
            window = min(max(2, seasonal_period), len(history))
            window_values = history[-window:]
            baseline = sum(window_values) / len(window_values)
            slope = 0.0
            if len(window_values) > 1:
                slope = (window_values[-1] - window_values[0]) / float(len(window_values) - 1)
            for step in range(1, steps + 1):
                predictions.append(float(baseline + slope * step))
            model_name = f"moving_average_trend_w{window}"

        if aggregation in {"count", "count_distinct", "sum"}:
            predictions = [max(0.0, float(item)) for item in predictions]
        return predictions, model_name

    def _compute_backtest_summary(
        self,
        *,
        actual_values: Sequence[float],
        predicted_values: Sequence[float],
        train_points: int,
        holdout_points: int,
    ) -> Dict[str, Any]:
        pairs = list(zip(actual_values, predicted_values))
        if not pairs:
            return {
                "status": "unavailable",
                "holdout_points": 0,
                "train_points": train_points,
                "mae": None,
                "rmse": None,
                "mape": None,
                "residual_std": None,
            }
        errors = [actual - predicted for actual, predicted in pairs]
        absolute_errors = [abs(error) for error in errors]
        mae = sum(absolute_errors) / len(absolute_errors)
        rmse = sqrt(sum((error * error) for error in errors) / len(errors))
        valid_pct_errors = [
            abs((actual - predicted) / actual)
            for actual, predicted in pairs
            if actual not in (0, 0.0)
        ]
        mape = (sum(valid_pct_errors) / len(valid_pct_errors) * 100.0) if valid_pct_errors else None
        residual_mean = sum(errors) / len(errors)
        residual_var = sum((error - residual_mean) ** 2 for error in errors) / len(errors)
        residual_std = sqrt(residual_var)
        return {
            "status": "ok",
            "holdout_points": holdout_points,
            "train_points": train_points,
            "mae": round(float(mae), 6),
            "rmse": round(float(rmse), 6),
            "mape": round(float(mape), 6) if mape is not None else None,
            "residual_std": round(float(residual_std), 6),
        }

    def list_reports(
        self,
        table_name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        sql = """
        SELECT `id`, `table_names`, `trigger_type`, `depth`, `schedule_id`, `history_id`,
               `summary`, `insight_count`, `anomaly_count`, `failed_step_count`, `status`,
               `error_message`, `duration_ms`, `created_at`, `report_json`
        FROM `_sys_analysis_reports`
        """
        params: List[Any] = []
        if table_name:
            sql += " WHERE FIND_IN_SET(%s, `table_names`)"
            params.append(table_name)
        sql += " ORDER BY `created_at` DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        rows = self.db.execute_query(sql, tuple(params))
        reports = [
            self._build_report_summary_payload(
                self._normalize_report_contract(dict(row or {}), summary_only=True),
                include_identity=True,
            )
            for row in rows
        ]
        return {
            "success": True,
            "contract_version": _INSIGHT_REPORT_SUMMARY_CONTRACT_VERSION,
            "reports": reports,
            "count": len(reports),
            "limit": limit,
            "offset": offset,
        }

    def get_report(self, report_id: str, include_reasoning: bool = False) -> Dict[str, Any]:
        rows = self.db.execute_query(
            "SELECT `report_json` FROM `_sys_analysis_reports` WHERE `id` = %s LIMIT 1",
            (report_id,),
        )
        if not rows:
            return {"success": False, "error": f"Report '{report_id}' not found."}
        payload = rows[0].get("report_json") or "{}"
        report = json.loads(payload) if isinstance(payload, str) else payload
        return self._filter_report_reasoning(report, include_reasoning=include_reasoning)

    def get_report_summary(self, report_id: str) -> Dict[str, Any]:
        report = self.get_report(report_id, include_reasoning=False)
        if not report.get("success"):
            return report
        normalized = self._normalize_report_contract(report, summary_only=True)
        summary = self._build_report_summary_payload(normalized, include_identity=True)
        summary["success"] = bool(report.get("success", True))
        return summary

    def delete_report(self, report_id: str) -> Dict[str, Any]:
        self.db.execute_update("DELETE FROM `_sys_analysis_reports` WHERE `id` = %s", (report_id,))
        return {"success": True, "deleted": True, "id": report_id}

    def get_latest_report(self, table_name: str, include_reasoning: bool = False) -> Dict[str, Any]:
        rows = self.db.execute_query(
            """
            SELECT `report_json`
            FROM `_sys_analysis_reports`
            WHERE FIND_IN_SET(%s, `table_names`)
            ORDER BY `created_at` DESC
            LIMIT 1
            """,
            (table_name,),
        )
        if not rows:
            return {"success": False, "error": f"No report found for '{table_name}'."}
        payload = rows[0].get("report_json") or "{}"
        report = json.loads(payload) if isinstance(payload, str) else payload
        return self._filter_report_reasoning(report, include_reasoning=include_reasoning)

    def _filter_report_reasoning(
        self,
        report: Dict[str, Any],
        *,
        include_reasoning: bool = False,
    ) -> Dict[str, Any]:
        payload = self._normalize_report_contract(dict(report or {}))
        if not include_reasoning:
            payload.pop("reasoning_traces", None)
        return payload

    def _normalize_report_contract(
        self,
        report: Optional[Dict[str, Any]],
        *,
        summary_only: bool = False,
    ) -> Dict[str, Any]:
        payload = self._merge_embedded_report_payload(dict(report or {}))
        payload = self._hydrate_expert_sections(payload)

        insights = self._normalize_report_items(payload.get("insights"), default_prefix="Insight")
        top_insights = self._normalize_report_items(
            payload.get("top_insights"),
            default_prefix="Top insight",
        ) or insights[:_EXPERT_MAIN_SECTION_LIMIT]
        anomalies = self._normalize_report_items(payload.get("anomalies"), default_prefix="Anomaly")
        recommendations = self._normalize_report_text_list(payload.get("recommendations"))
        action_items = self._normalize_report_action_items(payload.get("action_items"), recommendations)
        if not recommendations and action_items:
            recommendations = [self._action_item_as_text(item) for item in action_items]

        if summary_only:
            insights = insights[:_SUMMARY_SECTION_LIMIT]
            top_insights = top_insights[:_SUMMARY_SECTION_LIMIT]
            anomalies = anomalies[:_SUMMARY_SECTION_LIMIT]
            recommendations = recommendations[:_SUMMARY_SECTION_LIMIT]
            action_items = action_items[:_SUMMARY_SECTION_LIMIT]

        summary = self._normalize_report_summary(
            payload.get("summary"),
            top_insights,
            recommendations,
        )
        insight_count = self._coerce_non_negative_int(payload.get("insight_count"), len(insights))
        anomaly_count = self._coerce_non_negative_int(payload.get("anomaly_count"), len(anomalies))

        payload["summary"] = summary
        payload["insights"] = insights
        payload["top_insights"] = top_insights
        payload["anomalies"] = anomalies
        payload["recommendations"] = recommendations
        payload["action_items"] = action_items
        payload["insight_count"] = insight_count
        payload["anomaly_count"] = anomaly_count
        payload["tables"] = self._decode_table_names(payload.get("table_names"))
        if not payload.get("table_names") and payload.get("tables"):
            payload["table_names"] = ",".join(payload["tables"])
        payload["contract_version"] = _INSIGHT_REPORT_CONTRACT_VERSION
        payload["summary_contract_version"] = _INSIGHT_REPORT_SUMMARY_CONTRACT_VERSION
        payload["report_summary"] = self._build_report_summary_payload(payload, include_identity=False)

        if (payload.get("depth") or "").strip().lower() == "expert":
            payload["executive_summary"] = payload.get("executive_summary") or summary

        return payload

    def _build_report_summary_payload(
        self,
        payload: Dict[str, Any],
        *,
        include_identity: bool,
    ) -> Dict[str, Any]:
        summary_payload = {
            "contract_version": _INSIGHT_REPORT_SUMMARY_CONTRACT_VERSION,
            "summary": payload.get("summary") or "",
            "insights": list(payload.get("insights") or [])[:_SUMMARY_SECTION_LIMIT],
            "top_insights": list(payload.get("top_insights") or [])[:_SUMMARY_SECTION_LIMIT],
            "anomalies": list(payload.get("anomalies") or [])[:_SUMMARY_SECTION_LIMIT],
            "recommendations": list(payload.get("recommendations") or [])[:_SUMMARY_SECTION_LIMIT],
            "action_items": list(payload.get("action_items") or [])[:_SUMMARY_SECTION_LIMIT],
            "insight_count": self._coerce_non_negative_int(payload.get("insight_count")),
            "anomaly_count": self._coerce_non_negative_int(payload.get("anomaly_count")),
        }
        if include_identity:
            for key in (
                "id",
                "table_names",
                "tables",
                "trigger_type",
                "depth",
                "schedule_id",
                "history_id",
                "status",
                "failed_step_count",
                "error_message",
                "duration_ms",
                "created_at",
                "note",
            ):
                summary_payload[key] = payload.get(key)
        return summary_payload

    def _merge_embedded_report_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        report_json = payload.get("report_json")
        embedded: Dict[str, Any] = {}
        if isinstance(report_json, dict):
            embedded = dict(report_json)
        elif isinstance(report_json, str):
            parsed = self._parse_json_from_text(report_json)
            if isinstance(parsed, dict):
                embedded = parsed

        if not embedded:
            return payload

        merged = dict(embedded)
        merged.update(payload)
        merged.pop("report_json", None)
        return merged

    def _normalize_report_items(
        self,
        items: Any,
        *,
        default_prefix: str,
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for index, item in enumerate(self._as_list(items), start=1):
            payload = item if isinstance(item, dict) else {"detail": item}
            title = (
                payload.get("title")
                or payload.get("name")
                or payload.get("headline")
                or payload.get("category")
                or f"{default_prefix} {index}"
            )
            detail = (
                payload.get("detail")
                or payload.get("description")
                or payload.get("message")
                or payload.get("summary")
                or title
            )
            normalized_item = dict(payload)
            normalized_item["title"] = str(title).strip() or f"{default_prefix} {index}"
            normalized_item["detail"] = str(detail).strip() or normalized_item["title"]
            normalized.append(normalized_item)
        return normalized

    def _normalize_report_text_list(self, items: Any) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        for item in self._as_list(items):
            text = ""
            if isinstance(item, dict):
                title = (
                    item.get("title")
                    or item.get("name")
                    or item.get("headline")
                    or item.get("category")
                )
                detail = (
                    item.get("detail")
                    or item.get("description")
                    or item.get("message")
                    or item.get("summary")
                )
                if title and detail and str(title).strip() != str(detail).strip():
                    text = f"{str(title).strip()}: {str(detail).strip()}"
                else:
                    text = str(detail or title or "").strip()
            else:
                text = str(item or "").strip()

            if text and text not in seen:
                seen.add(text)
                normalized.append(text)
        return normalized

    def _normalize_report_action_items(
        self,
        items: Any,
        recommendations: Sequence[str],
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []

        for index, item in enumerate(self._as_list(items), start=1):
            payload = item if isinstance(item, dict) else {"detail": item}
            title = (
                payload.get("title")
                or payload.get("name")
                or payload.get("headline")
                or payload.get("category")
                or f"Action item {index}"
            )
            detail = (
                payload.get("detail")
                or payload.get("description")
                or payload.get("message")
                or payload.get("summary")
                or payload.get("action")
                or title
            )
            normalized_item = dict(payload)
            normalized_item["title"] = str(title).strip() or f"Action item {index}"
            normalized_item["detail"] = str(detail).strip() or normalized_item["title"]
            normalized.append(normalized_item)

        if not normalized:
            for index, recommendation in enumerate(recommendations, start=1):
                text = str(recommendation or "").strip()
                if not text:
                    continue
                normalized.append(
                    {
                        "title": f"Action item {index}",
                        "detail": text,
                    }
                )

        return normalized

    def _normalize_report_summary(
        self,
        summary: Any,
        top_insights: Sequence[Dict[str, Any]],
        recommendations: Sequence[str],
    ) -> str:
        text = str(summary or "").strip()
        if text:
            return text

        if top_insights:
            primary = top_insights[0]
            title = str(primary.get("title") or "").strip()
            detail = str(primary.get("detail") or "").strip()
            if title and detail and title != detail:
                return f"{title}: {detail}"
            if title or detail:
                return title or detail

        if recommendations:
            recommendation = str(recommendations[0] or "").strip()
            if recommendation:
                return f"Analysis completed. Priority action: {recommendation}"

        return "Analysis completed."

    def _coerce_non_negative_int(self, value: Any, fallback: int = 0) -> int:
        for candidate in (value, fallback):
            try:
                number = int(candidate)
                if number >= 0:
                    return number
            except (TypeError, ValueError):
                continue
        return 0

    def _as_list(self, value: Any) -> List[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if value is None:
            return []
        return [value]

    def _hydrate_expert_sections(self, report: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(report or {})
        if (payload.get("depth") or "").strip().lower() != "expert":
            return payload

        raw_findings = [
            self._coerce_expert_item(item)
            for item in (
                payload.get("findings")
                or payload.get("insights")
                or payload.get("top_insights")
                or []
            )
        ]
        insights = self._normalize_expert_findings(raw_findings)
        recommendations = self._normalize_expert_text_list(payload.get("recommendations") or [])
        action_items = self._normalize_expert_action_items(
            payload.get("action_items") or payload.get("recommendations") or [],
            raw_findings,
        )
        if not recommendations and action_items:
            recommendations = [self._action_item_as_text(item) for item in action_items]

        executive_summary = self._normalize_expert_summary(
            payload.get("executive_summary") or payload.get("summary"),
            insights[:_EXPERT_MAIN_SECTION_LIMIT],
            recommendations[:_EXPERT_MAIN_SECTION_LIMIT],
        )

        payload["summary"] = executive_summary
        payload["executive_summary"] = executive_summary
        payload["insights"] = insights
        payload["top_insights"] = insights[:_EXPERT_MAIN_SECTION_LIMIT]
        payload["recommendations"] = recommendations
        payload["action_items"] = action_items[:_EXPERT_MAIN_SECTION_LIMIT]
        return payload

    def _profile_table(
        self,
        table_name: str,
        safe_table_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        raw_name = (table_name or "").strip()
        safe_name = safe_table_name or self.db.validate_identifier(raw_name)
        row_count_rows = self.db.execute_query(f"SELECT COUNT(*) AS row_count FROM {safe_name}")
        row_count = int((row_count_rows[0] or {}).get("row_count", 0)) if row_count_rows else 0
        sampled = row_count > 100000
        sample_size = max(int(row_count * 0.1), 1) if sampled and row_count else row_count
        source_sql = f"{safe_name} TABLESAMPLE(10 PERCENT)" if sampled else safe_name
        fallback_source_sql = (
            f"(SELECT * FROM {safe_name} ORDER BY RAND() LIMIT 50000) AS sampled_source"
            if sampled
            else None
        )
        schema = self.db.get_table_schema(raw_name)

        columns: Dict[str, Dict[str, Any]] = {}
        for column in schema:
            field_name = column.get("Field")
            if not field_name:
                continue
            field_type = column.get("Type") or ""
            safe_field = self.db.validate_identifier(field_name)
            lowered_type = field_type.lower()

            if self._is_numeric_type(lowered_type):
                stat_rows, source_sql = self._execute_profile_query(
                    f"""
                    SELECT MIN({safe_field}) AS min_value,
                           MAX({safe_field}) AS max_value,
                           AVG({safe_field}) AS avg_value,
                           STDDEV({safe_field}) AS stddev_value,
                           SUM(CASE WHEN {safe_field} IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM {source_sql}
                    """,
                    source_sql,
                    fallback_source_sql,
                )
                stat_row = stat_rows[0] if stat_rows else {}
                columns[field_name] = {
                    "type": field_type,
                    "null_rate": self._null_rate(stat_row.get("null_count"), sample_size or row_count),
                    "min": stat_row.get("min_value"),
                    "max": stat_row.get("max_value"),
                    "avg": stat_row.get("avg_value"),
                    "stddev": stat_row.get("stddev_value"),
                }
                continue

            if self._is_temporal_type(lowered_type):
                stat_rows, source_sql = self._execute_profile_query(
                    f"""
                    SELECT MIN({safe_field}) AS min_value,
                           MAX({safe_field}) AS max_value,
                           SUM(CASE WHEN {safe_field} IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM {source_sql}
                    """,
                    source_sql,
                    fallback_source_sql,
                )
                stat_row = stat_rows[0] if stat_rows else {}
                columns[field_name] = {
                    "type": field_type,
                    "null_rate": self._null_rate(stat_row.get("null_count"), sample_size or row_count),
                    "min": stat_row.get("min_value"),
                    "max": stat_row.get("max_value"),
                }
                continue

            stat_rows, source_sql = self._execute_profile_query(
                f"""
                SELECT COUNT(DISTINCT {safe_field}) AS unique_count,
                       SUM(CASE WHEN {safe_field} IS NULL THEN 1 ELSE 0 END) AS null_count
                FROM {source_sql}
                """,
                source_sql,
                fallback_source_sql,
            )
            stat_row = stat_rows[0] if stat_rows else {}
            top_rows, source_sql = self._execute_profile_query(
                f"""
                SELECT {safe_field} AS value, COUNT(*) AS count
                FROM {source_sql}
                WHERE {safe_field} IS NOT NULL
                GROUP BY {safe_field}
                ORDER BY count DESC
                LIMIT 10
                """,
                source_sql,
                fallback_source_sql,
            )
            columns[field_name] = {
                "type": field_type,
                "null_rate": self._null_rate(stat_row.get("null_count"), sample_size or row_count),
                "unique_count": stat_row.get("unique_count"),
                "top_values": [
                    {"value": row.get("value"), "count": row.get("count")}
                    for row in top_rows
                ],
            }

        return {
            "row_count": row_count,
            "sampled": sampled,
            "sample_size": sample_size,
            "columns": columns,
        }

    def _get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        rows = self.db.execute_query(
            """
            SELECT r.table_name, r.display_name, r.description, m.description AS auto_description, m.columns_info
            FROM `_sys_table_registry` r
            LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
            WHERE r.table_name = %s
            LIMIT 1
            """,
            (table_name,),
        )
        if not rows:
            return {"table_name": table_name}
        row = rows[0]
        description = row.get("description") or row.get("auto_description")
        columns_info = row.get("columns_info")
        try:
            parsed_columns = json.loads(columns_info) if isinstance(columns_info, str) and columns_info else columns_info
        except Exception:
            parsed_columns = columns_info
        return {
            "table_name": table_name,
            "display_name": row.get("display_name"),
            "description": description,
            "columns_info": parsed_columns or {},
        }

    def _plan_analysis(
        self,
        profile: Dict[str, Any],
        metadata: Dict[str, Any],
        depth: str,
        api_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        prompt = (
            "Design a compact Apache Doris analysis plan.\n"
            f"Depth: {depth}\n"
            f"Metadata: {json.dumps(metadata, ensure_ascii=False, default=str)}\n"
            f"Profile: {json.dumps(profile, ensure_ascii=False, default=str)}\n\n"
            "Return JSON with a top-level `steps` array. Each step must include "
            "`title`, `question`, and `sql`. Return only JSON."
        )
        try:
            payload = self._call_json_completion(
                system_prompt="You are a business analyst who plans Apache Doris SQL analysis steps.",
                user_prompt=prompt,
                api_config=api_config,
            )
        except Exception as exc:
            logger.warning("analysis planner failed, using fallback plan: %s", exc)
            payload = {}
        steps = payload.get("steps") if isinstance(payload, dict) else payload
        if isinstance(steps, list) and steps:
            return steps[: self._get_max_steps(depth)]

        return [
            {
                "title": "Table overview",
                "question": "How many rows exist in this table?",
                "sql": f"SELECT COUNT(*) AS total_rows FROM {self.db.validate_identifier(metadata.get('table_name') or '')}",
            }
        ]

    def _build_strategist_config(self) -> Dict[str, Any]:
        env_model = os.getenv("ANALYST_STRATEGIST_MODEL")
        env_base_url = os.getenv("ANALYST_STRATEGIST_BASE_URL")
        env_api_key = os.getenv("ANALYST_STRATEGIST_API_KEY")

        custom_vars = {
            "model": env_model,
            "base_url": env_base_url,
            "api_key": env_api_key,
        }
        custom_count = sum(1 for value in custom_vars.values() if value)
        if custom_count == 3:
            return custom_vars
        if custom_count > 0:
            logger.warning(
                "Partial ANALYST_STRATEGIST_* config (%d/3 set). Set all three or none. Falling back to default.",
                custom_count,
            )

        return {
            "model": "deepseek-reasoner",
            "base_url": os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            "api_key": os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY"),
        }

    def _build_executor_config(self, base_api_config: Dict[str, Any]) -> Dict[str, Any]:
        return dict(base_api_config or {})

    def _call_strategist(self, prompt: str, strategist_config: Dict[str, Any]) -> Dict[str, Any]:
        import httpx as _httpx

        model_name = (strategist_config.get("model") or "").lower()
        is_reasoner = "reasoner" in model_name or re.search(r"(^|[^a-z0-9])r1([^a-z0-9]|$)", model_name) is not None
        if is_reasoner:
            payload = {
                "model": strategist_config["model"],
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 8000,
                "stream": True,
            }
            timeout = 300
        else:
            payload = {
                "model": strategist_config["model"],
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 4000,
                "stream": True,
            }
            timeout = 120

        url = f"{strategist_config['base_url'].rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {strategist_config['api_key']}",
            "Content-Type": "application/json",
        }

        # Use SSE streaming to avoid Docker TCP keepalive drops on long R1 responses
        reasoning_chunks: List[str] = []
        content_chunks: List[str] = []

        with _httpx.Client(timeout=_httpx.Timeout(float(timeout), connect=30.0)) as client:
            with client.stream("POST", url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                        delta = chunk["choices"][0].get("delta", {})
                        if delta.get("reasoning_content"):
                            reasoning_chunks.append(delta["reasoning_content"])
                        if delta.get("content"):
                            content_chunks.append(delta["content"])
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass

        content = "".join(content_chunks)
        reasoning = "".join(reasoning_chunks)
        return {
            "reasoning": reasoning,
            "response": self._parse_json_from_text(content),
        }

    def _detect_temporal_dimensions(
        self,
        safe_name: str,
        profile: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        columns = profile.get("columns") or {}
        time_columns = [
            (name, info)
            for name, info in columns.items()
            if self._is_temporal_type(str(info.get("type", "")).lower())
        ]
        detected: List[Dict[str, Any]] = []

        for column_name, column_profile in time_columns:
            safe_column = self.db.validate_identifier(column_name)
            try:
                rows = self.db.execute_query(
                    f"""
                    SELECT MIN({safe_column}) AS min_value,
                           MAX({safe_column}) AS max_value,
                           COUNT({safe_column}) AS non_null_count,
                           COUNT(DISTINCT DATE({safe_column})) AS distinct_day_count
                    FROM {safe_name}
                    WHERE {safe_column} IS NOT NULL
                    """
                )
            except Exception as exc:
                logger.debug("expert temporal detection failed for %s: %s", column_name, exc)
                continue

            row = rows[0] if rows else {}
            min_value = self._coerce_temporal_value(row.get("min_value"))
            max_value = self._coerce_temporal_value(row.get("max_value"))
            if not min_value or not max_value:
                continue

            span_days = max((max_value.date() - min_value.date()).days + 1, 1)
            distinct_day_count = int(row.get("distinct_day_count") or 0)
            non_null_count = int(row.get("non_null_count") or 0)
            density_ratio = round(min(distinct_day_count / span_days, 1.0), 4) if span_days else 0.0
            candidate_grains = self._candidate_temporal_grains(span_days, density_ratio)
            recommended_grains = self._recommended_temporal_grains(candidate_grains, span_days)
            time_window_limits = self._time_window_limits(span_days, candidate_grains)

            detected.append(
                {
                    "column": column_name,
                    "type": column_profile.get("type"),
                    "null_rate": column_profile.get("null_rate"),
                    "min_value": str(row.get("min_value")),
                    "max_value": str(row.get("max_value")),
                    "non_null_count": non_null_count,
                    "distinct_day_count": distinct_day_count,
                    "span_days": span_days,
                    "density_ratio": density_ratio,
                    "candidate_grains": candidate_grains,
                    "recommended_grains": recommended_grains,
                    "time_window_limits": time_window_limits,
                }
            )

        detected.sort(
            key=lambda item: (
                -(item.get("span_days") or 0),
                item.get("null_rate") if item.get("null_rate") is not None else 1.0,
                item.get("column") or "",
            )
        )
        return detected

    def _coerce_temporal_value(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            try:
                return datetime(value.year, value.month, value.day)
            except TypeError:
                return None
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("T", " ")
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            return None

    def _candidate_temporal_grains(self, span_days: int, density_ratio: float) -> List[str]:
        grains: List[str] = []
        if span_days >= 14 and density_ratio >= 0.35:
            grains.append("day")
        if span_days >= 28 and density_ratio >= 0.15:
            grains.append("week")
        if span_days >= 60:
            grains.append("month")
        if span_days >= 180:
            grains.append("quarter")
        if span_days >= 540:
            grains.append("year")
        if grains:
            return grains
        if span_days >= 60:
            return ["month"]
        if span_days >= 28:
            return ["week"]
        return ["day"]

    def _recommended_temporal_grains(self, candidate_grains: Sequence[str], span_days: int) -> List[str]:
        if span_days <= 45:
            preferred = ["day", "week"]
        elif span_days <= 210:
            preferred = ["week", "month"]
        elif span_days <= 900:
            preferred = ["month", "quarter"]
        else:
            preferred = ["quarter", "year"]

        recommended = [grain for grain in preferred if grain in candidate_grains]
        if recommended:
            return recommended[:2]
        return list(candidate_grains)[:2]

    def _time_window_limits(self, span_days: int, candidate_grains: Sequence[str]) -> Dict[str, int]:
        available_periods = {
            "day": span_days,
            "week": max(1, ceil(span_days / 7)),
            "month": max(1, ceil(span_days / 30)),
            "quarter": max(1, ceil(span_days / 90)),
            "year": max(1, ceil(span_days / 365)),
        }
        return {
            grain: min(_TEMPORAL_LOOKBACK_DEFAULTS[grain], available_periods[grain])
            for grain in candidate_grains
            if grain in available_periods
        }

    def _compute_statistical_facts(
        self,
        safe_name: str,
        profile: Dict[str, Any],
        temporal_dimensions: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        facts: List[Dict[str, Any]] = [
            {
                "type": "table_overview",
                "metric": "row_count",
                "value": profile.get("row_count", 0),
                "sampled": profile.get("sampled", False),
                "sample_size": profile.get("sample_size", 0),
            }
        ]
        columns = profile.get("columns") or {}
        time_cols = [
            name
            for name, info in columns.items()
            if self._is_temporal_type(str(info.get("type", "")).lower())
        ]
        numeric_cols = [
            name
            for name, info in columns.items()
            if self._is_numeric_type(str(info.get("type", "")).lower())
        ]

        for column_name, column_profile in columns.items():
            base_fact = {
                "type": "column_profile",
                "column": column_name,
                "null_rate": column_profile.get("null_rate"),
            }
            if any(key in column_profile for key in ("avg", "stddev", "min", "max")):
                facts.append(
                    {
                        **base_fact,
                        "min": column_profile.get("min"),
                        "max": column_profile.get("max"),
                        "avg": column_profile.get("avg"),
                        "stddev": column_profile.get("stddev"),
                    }
                )
            elif any(key in column_profile for key in ("unique_count", "top_values")):
                facts.append(
                    {
                        **base_fact,
                        "unique_count": column_profile.get("unique_count"),
                        "top_values": (column_profile.get("top_values") or [])[:5],
                    }
                )

        for dimension in temporal_dimensions or []:
            facts.append(
                {
                    "type": "temporal_dimension",
                    "column": dimension.get("column"),
                    "null_rate": dimension.get("null_rate"),
                    "span_days": dimension.get("span_days"),
                    "density_ratio": dimension.get("density_ratio"),
                    "candidate_grains": list(dimension.get("candidate_grains") or []),
                    "recommended_grains": list(dimension.get("recommended_grains") or []),
                    "time_window_limits": dict(dimension.get("time_window_limits") or {}),
                }
            )

        for numeric_column in numeric_cols[:5]:
            safe_numeric_column = self.db.validate_identifier(numeric_column)
            try:
                concentration_rows = self.db.execute_query(
                    f"""
                    SELECT ROUND(
                        100.0 * SUM(CASE WHEN decile = 1 THEN metric_value ELSE 0 END) / NULLIF(SUM(metric_value), 0),
                        2
                    ) AS top_share_pct
                    FROM (
                        SELECT {safe_numeric_column} AS metric_value,
                               NTILE(10) OVER (ORDER BY {safe_numeric_column} DESC) AS decile
                        FROM {safe_name}
                        WHERE {safe_numeric_column} IS NOT NULL
                    ) ranked_values
                    """
                )
                top_share_pct = (concentration_rows[0] or {}).get("top_share_pct") if concentration_rows else None
                if top_share_pct is not None:
                    facts.append(
                        {
                            "type": "concentration",
                            "column": numeric_column,
                            "top_percent": 10,
                            "top_share_pct": top_share_pct,
                        }
                    )
            except Exception as exc:
                logger.debug("expert concentration precompute failed for %s: %s", numeric_column, exc)

            column_profile = columns.get(numeric_column) or {}
            avg_value = column_profile.get("avg")
            stddev_value = column_profile.get("stddev")
            try:
                avg_float = float(avg_value)
                stddev_float = float(stddev_value)
            except (TypeError, ValueError):
                avg_float = None
                stddev_float = None

            if avg_float is not None and stddev_float not in (None, 0.0):
                lower_bound = avg_float - (2 * stddev_float)
                upper_bound = avg_float + (2 * stddev_float)
                try:
                    outlier_rows = self.db.execute_query(
                        f"""
                        SELECT COUNT(*) AS outlier_count
                        FROM {safe_name}
                        WHERE {safe_numeric_column} IS NOT NULL
                          AND ({safe_numeric_column} < %s OR {safe_numeric_column} > %s)
                        """,
                        (lower_bound, upper_bound),
                    )
                    outlier_count = (outlier_rows[0] or {}).get("outlier_count") if outlier_rows else 0
                except Exception as exc:
                    logger.debug("expert outlier precompute failed for %s: %s", numeric_column, exc)
                    outlier_count = None

                facts.append(
                    {
                        "type": "outlier",
                        "column": numeric_column,
                        "lower_bound": round(lower_bound, 4),
                        "upper_bound": round(upper_bound, 4),
                        "outlier_count": outlier_count,
                    }
                )

        if len(numeric_cols) >= 2:
            pair_candidates: List[Tuple[str, str]] = []
            limited_numeric = numeric_cols[:3]
            for left_index, left_column in enumerate(limited_numeric):
                for right_column in limited_numeric[left_index + 1 :]:
                    pair_candidates.append((left_column, right_column))

            for left_column, right_column in pair_candidates:
                safe_left = self.db.validate_identifier(left_column)
                safe_right = self.db.validate_identifier(right_column)
                try:
                    correlation_rows = self.db.execute_query(
                        f"""
                        SELECT CORR({safe_left}, {safe_right}) AS correlation_value
                        FROM {safe_name}
                        WHERE {safe_left} IS NOT NULL
                          AND {safe_right} IS NOT NULL
                        """
                    )
                    correlation_value = (correlation_rows[0] or {}).get("correlation_value") if correlation_rows else None
                    if correlation_value is not None:
                        facts.append(
                            {
                                "type": "correlation",
                                "columns": [left_column, right_column],
                                "value": correlation_value,
                            }
                        )
                except Exception as exc:
                    logger.debug("expert correlation precompute failed for %s/%s: %s", left_column, right_column, exc)

        return facts

    def _build_round_prompt(
        self,
        round_num: int,
        profile: Dict[str, Any],
        stats: Sequence[Dict[str, Any]],
        metadata: Dict[str, Any],
        compressed_history: Sequence[Dict[str, Any]],
    ) -> str:
        preamble = (
            "You are a Senior Data Scientist conducting a rigorous analysis.\n"
            "Methodology: descriptive -> diagnostic -> predictive.\n"
            "Quantify every claim, compare against baselines, flag outliers beyond 2 stddev, "
            "distinguish correlation from causation, and end with actionable recommendations.\n"
            "Write in Chinese for business context and English for technical terms.\n"
            "Return only JSON."
        )
        context = json.dumps(list(compressed_history), ensure_ascii=False, default=str)
        profile_json = json.dumps(profile, ensure_ascii=False, default=str)
        stats_json = json.dumps(list(stats), ensure_ascii=False, default=str)
        metadata_json = json.dumps(metadata, ensure_ascii=False, default=str)
        base_context = (
            f"Metadata: {metadata_json}\n"
            f"Profile: {profile_json}\n"
            f"Statistics: {stats_json}"
        )

        if round_num == 1:
            return (
                f"{preamble}\n\n"
                f"Data context:\n{base_context}\n\n"
                "Generate 3-5 analytical hypotheses ordered by business value.\n"
                "Each item must include `id`, `title`, `methodology`, `query_description`, "
                "`confirm_condition`, and `refute_condition`.\n"
                "If `temporal_dimension` facts exist in Statistics, choose at most 1-2 temporal analysis plans overall.\n"
                "For any temporal hypothesis, include a `time_plan` object with:\n"
                "- `time_column`\n"
                "- `grain`\n"
                "- `analysis_type` (trend/seasonality/anomaly/comparison)\n"
                "- `comparison_mode` (wow/mom/qoq/yoy/baseline/none)\n"
                "- `lookback_periods`\n"
                "- optional `metric_column` and `aggregation`\n"
                "Only choose `time_column` from the listed temporal candidates, only choose `grain` from that column's "
                "`candidate_grains`, and keep `lookback_periods` within the listed `time_window_limits`.\n"
                "Return JSON with `hypotheses` and `continue`."
            )
        if round_num == 2:
            return (
                f"{preamble}\n\n"
                f"Original data context:\n{base_context}\n\n"
                f"Prior context: {context}\n\n"
                "Critically evaluate the prior findings.\n"
                "If you propose temporal follow-ups, include a `time_plan` object using only the listed temporal candidates "
                "and their allowed grains/window limits.\n"
                "Return JSON with `assessments`, `follow_ups`, and `continue`."
            )
        return (
            f"{preamble}\n\n"
            f"Original data context:\n{base_context}\n\n"
            f"Full compressed analysis history: {context}\n\n"
            "Synthesize the full analysis into a final verdict.\n"
            "Return JSON with:\n"
            "- `summary`: a concise Chinese executive summary for business readers, 2-3 sentences, "
            "no methodology narration, no raw JSON\n"
            "- `findings`: an array of objects with `title`, `detail`, optional `severity`, "
            "`quantification`, and optional `recommendation`, sorted by business impact\n"
            "- `recommendations`: at most 3 action items, each as string or object with `title`, `detail`, optional `urgency`\n"
            "- `anomalies`, `root_causes`, `limitations`, `confidence_overall`, and `continue`."
        )

    def _extract_queries_from_strategist(
        self,
        strategist_output: Dict[str, Any],
        round_num: int,
    ) -> List[Dict[str, Any]]:
        response = strategist_output.get("response") or {}
        if response.get("hypotheses"):
            return [
                {
                    "id": item.get("id"),
                    "title": item.get("title") or f"Hypothesis {index + 1}",
                    "query_description": item.get("query_description") or item.get("title") or "",
                    "time_plan": item.get("time_plan"),
                }
                for index, item in enumerate(response.get("hypotheses") or [])
                if item.get("query_description") or item.get("title")
            ]
        if response.get("follow_ups"):
            return [
                {
                    "id": item.get("id"),
                    "title": item.get("reason") or f"Follow-up {index + 1}",
                    "query_description": item.get("query_description") or item.get("reason") or "",
                    "time_plan": item.get("time_plan"),
                }
                for index, item in enumerate(response.get("follow_ups") or [])
                if item.get("query_description") or item.get("reason")
            ]
        return []

    def _executor_translate_to_sql(
        self,
        query: Dict[str, Any],
        metadata: Dict[str, Any],
        api_config: Dict[str, Any],
    ) -> str:
        table_name = metadata.get("table_name") or ""
        safe_table_name = self.db.validate_identifier(table_name) if table_name else ""
        selected_time_plan = self._normalize_time_plan(query.get("time_plan"), metadata)
        temporal_dimensions = metadata.get("temporal_dimensions") or []
        temporal_context_lines: List[str] = []
        if temporal_dimensions:
            temporal_context_lines.append(
                "Temporal candidates: "
                + json.dumps(
                    [
                        {
                            "column": item.get("column"),
                            "candidate_grains": item.get("candidate_grains"),
                            "recommended_grains": item.get("recommended_grains"),
                            "time_window_limits": item.get("time_window_limits"),
                        }
                        for item in temporal_dimensions
                    ],
                    ensure_ascii=False,
                    default=str,
                )
            )
        if selected_time_plan:
            temporal_context_lines.append(
                "Selected time plan: " + json.dumps(selected_time_plan, ensure_ascii=False, default=str)
            )
        temporal_context = "\n".join(temporal_context_lines)
        temporal_rules = [
            "- Only use candidate time columns listed in Temporal candidates",
        ]
        if selected_time_plan:
            temporal_rules.extend(
                [
                    f"- You must use `{selected_time_plan['time_column']}` as the only time dimension",
                    f"- You must aggregate at `{selected_time_plan['grain']}` grain",
                    f"- Restrict the query to the most recent {selected_time_plan['lookback_periods']} {selected_time_plan['grain']} periods",
                ]
            )
        prompt = (
            "You are a SQL engineer for Apache Doris. Translate analytical queries to SQL.\n"
            f"Table: {table_name}\n"
            f"Metadata: {json.dumps(metadata, ensure_ascii=False, default=str)}\n"
            f"{temporal_context}\n"
            f"Query request: {query.get('query_description') or query.get('title') or ''}\n\n"
            "Rules:\n"
            "- Use backtick quoting for all identifiers\n"
            "- Doris syntax (not MySQL-specific features)\n"
            "- Return at most 100 rows (expert mode focuses on patterns, not raw data)\n"
            "- Always alias computed columns for clarity\n"
            f"{chr(10).join(temporal_rules)}\n"
            "Return only the SQL query."
        )
        try:
            sql = self._call_chat_completion(
                system_prompt="You translate natural-language analytical requests into Apache Doris SQL.",
                user_prompt=prompt,
                api_config=api_config,
            )
            cleaned = self._strip_markdown_fences(sql).strip().rstrip(";")
        except Exception as exc:
            logger.warning("expert executor translation failed, using fallback SQL: %s", exc)
            cleaned = ""

        if not cleaned.lower().startswith("select"):
            if selected_time_plan:
                return self._build_temporal_fallback_sql(metadata, selected_time_plan)
            return f"SELECT COUNT(*) AS total_rows FROM {safe_table_name}"
        if selected_time_plan and self._sql_violates_time_plan(cleaned, metadata, selected_time_plan):
            return self._build_temporal_fallback_sql(metadata, selected_time_plan)
        if " limit " not in cleaned.lower():
            cleaned = f"{cleaned} LIMIT {_EXPERT_RESULT_ROW_LIMIT}"
        return cleaned

    def _normalize_time_plan(
        self,
        raw_plan: Any,
        metadata: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(raw_plan, dict):
            return None
        candidates = {
            item.get("column"): item
            for item in (metadata.get("temporal_dimensions") or [])
            if item.get("column")
        }
        time_column = raw_plan.get("time_column")
        candidate = candidates.get(time_column)
        if not candidate:
            return None

        allowed_grains = list(candidate.get("candidate_grains") or [])
        if not allowed_grains:
            return None
        grain = raw_plan.get("grain")
        if grain not in allowed_grains:
            grain = (candidate.get("recommended_grains") or allowed_grains)[0]

        limits = candidate.get("time_window_limits") or {}
        max_lookback = int(limits.get(grain) or _TEMPORAL_LOOKBACK_DEFAULTS.get(grain, 12))
        raw_lookback = raw_plan.get("lookback_periods")
        try:
            lookback_periods = int(raw_lookback) if raw_lookback is not None else max_lookback
        except (TypeError, ValueError):
            lookback_periods = max_lookback
        lookback_periods = max(1, min(lookback_periods, max_lookback))

        analysis_type = str(raw_plan.get("analysis_type") or "trend").lower()
        if analysis_type not in _TEMPORAL_ANALYSIS_TYPES:
            analysis_type = "trend"
        comparison_mode = str(raw_plan.get("comparison_mode") or "none").lower()
        if comparison_mode not in _TEMPORAL_COMPARISON_MODES:
            comparison_mode = "none"

        normalized = {
            "time_column": time_column,
            "grain": grain,
            "analysis_type": analysis_type,
            "comparison_mode": comparison_mode,
            "lookback_periods": lookback_periods,
        }
        metric_column = raw_plan.get("metric_column")
        if metric_column:
            normalized["metric_column"] = str(metric_column)
        aggregation = str(raw_plan.get("aggregation") or "").lower()
        if aggregation in {"sum", "avg", "count_distinct"}:
            normalized["aggregation"] = aggregation
        return normalized

    def _sql_violates_time_plan(
        self,
        sql: str,
        metadata: Dict[str, Any],
        time_plan: Dict[str, Any],
    ) -> bool:
        selected_column = time_plan.get("time_column")
        if not selected_column:
            return False
        temporal_columns = [
            item.get("column")
            for item in (metadata.get("temporal_dimensions") or [])
            if item.get("column")
        ]
        if not self._sql_mentions_identifier(sql, selected_column):
            return True
        for column in temporal_columns:
            if column != selected_column and self._sql_mentions_identifier(sql, column):
                return True
        return False

    def _sql_mentions_identifier(self, sql: str, identifier: str) -> bool:
        return re.search(rf"(?<![A-Za-z0-9_])`?{re.escape(identifier)}`?(?![A-Za-z0-9_])", sql or "") is not None

    def _build_temporal_fallback_sql(
        self,
        metadata: Dict[str, Any],
        time_plan: Dict[str, Any],
    ) -> str:
        table_name = metadata.get("table_name") or ""
        safe_table_name = self.db.validate_identifier(table_name)
        safe_time_column = self.db.validate_identifier(time_plan["time_column"])
        period_expression = self._temporal_period_expression(safe_time_column, time_plan["grain"])
        cutoff_expression = self._temporal_cutoff_expression(time_plan["grain"], time_plan["lookback_periods"])

        metric_expression = "COUNT(*)"
        metric_column = time_plan.get("metric_column")
        if metric_column:
            safe_metric = self.db.validate_identifier(str(metric_column))
            aggregation = time_plan.get("aggregation")
            if aggregation == "avg":
                metric_expression = f"AVG({safe_metric})"
            elif aggregation == "count_distinct":
                metric_expression = f"COUNT(DISTINCT {safe_metric})"
            else:
                metric_expression = f"SUM({safe_metric})"

        return (
            f"SELECT {period_expression} AS period, {metric_expression} AS metric_value "
            f"FROM {safe_table_name} "
            f"WHERE {safe_time_column} IS NOT NULL "
            f"AND {safe_time_column} >= {cutoff_expression} "
            f"GROUP BY {period_expression} "
            f"ORDER BY period DESC "
            f"LIMIT {_EXPERT_RESULT_ROW_LIMIT}"
        )

    def _temporal_period_expression(self, safe_time_column: str, grain: str) -> str:
        if grain == "day":
            return f"DATE_FORMAT({safe_time_column}, '%Y-%m-%d')"
        if grain == "week":
            return (
                f"CONCAT(CAST(YEAR({safe_time_column}) AS STRING), '-W', "
                f"LPAD(CAST(WEEKOFYEAR({safe_time_column}) AS STRING), 2, '0'))"
            )
        if grain == "month":
            return f"DATE_FORMAT({safe_time_column}, '%Y-%m')"
        if grain == "quarter":
            return (
                f"CONCAT(CAST(YEAR({safe_time_column}) AS STRING), '-Q', "
                f"CAST(QUARTER({safe_time_column}) AS STRING))"
            )
        return f"CAST(YEAR({safe_time_column}) AS STRING)"

    def _temporal_cutoff_expression(self, grain: str, lookback_periods: int) -> str:
        if grain == "day":
            return f"DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_periods} DAY)"
        if grain == "week":
            return f"DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_periods} WEEK)"
        if grain == "month":
            return f"DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_periods} MONTH)"
        if grain == "quarter":
            return f"DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_periods * 3} MONTH)"
        return f"DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_periods} YEAR)"

    def _compress_round_for_context(self, round_data: Dict[str, Any]) -> Dict[str, Any]:
        compressed = {
            "round": round_data["round"],
            "strategist_output": round_data["strategist"]["response"],
        }
        compressed_results: List[Dict[str, Any]] = []
        for step in round_data.get("results", []):
            truncated_step = {
                "title": step.get("title"),
                "success": step.get("success"),
                "row_count": step.get("row_count"),
                "error_message": step.get("error_message"),
            }
            data = list(step.get("data") or [])[:_EXPERT_CONTEXT_ROW_LIMIT]
            if data:
                if len(data[0]) > _EXPERT_CONTEXT_COLUMN_LIMIT:
                    key_columns = list(data[0].keys())[:_EXPERT_CONTEXT_COLUMN_LIMIT]
                    data = [{key: row.get(key) for key in key_columns} for row in data]
                truncated_step["data_sample"] = data
                if len(step.get("data") or []) > _EXPERT_CONTEXT_ROW_LIMIT:
                    truncated_step["data_note"] = (
                        f"Showing {_EXPERT_CONTEXT_ROW_LIMIT} of {len(step.get('data') or [])} rows"
                    )
            compressed_results.append(truncated_step)
        compressed["results"] = compressed_results
        return compressed

    def _sanitize_reasoning(self, text: str) -> str:
        lines = []
        for line in (text or "").splitlines():
            lowered = line.lower()
            if "api_key" in lowered or "authorization:" in lowered or "system prompt" in lowered:
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    def _truncate_reasoning(self, text: str) -> str:
        max_chars = int(os.getenv("ANALYST_MAX_REASONING_CHARS", str(ANALYST_MAX_REASONING_CHARS)))
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + f"\n\n[truncated, {len(text) - max_chars} chars omitted]"

    def _build_expert_report(
        self,
        *,
        table_names: Sequence[str],
        profile: Dict[str, Any],
        compressed_history: Sequence[Dict[str, Any]],
        reasoning_traces: Sequence[Dict[str, Any]],
        all_step_results: Sequence[Dict[str, Any]],
        trigger_type: str,
        started_at: float,
        history_id: Optional[str] = None,
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        final_output = (compressed_history[-1] or {}).get("strategist_output") if compressed_history else {}
        raw_findings = [self._coerce_expert_item(item) for item in (final_output.get("findings") or [])]
        findings = self._normalize_expert_findings(raw_findings)
        anomalies = self._normalize_expert_findings(final_output.get("anomalies") or [], default_prefix="异常")
        recommendations = self._normalize_expert_text_list(final_output.get("recommendations") or [])
        action_items = self._normalize_expert_action_items(final_output.get("recommendations") or [], raw_findings)
        if not recommendations and action_items:
            recommendations = [self._action_item_as_text(item) for item in action_items]
        limitations = self._normalize_expert_text_list(final_output.get("limitations") or [])
        root_causes = self._normalize_expert_text_list(final_output.get("root_causes") or [])
        failed_step_count = sum(1 for step in all_step_results if not step.get("success"))
        if failed_step_count == len(all_step_results) and all_step_results:
            status = "failed"
        elif failed_step_count:
            status = "partial"
        else:
            status = "completed"

        report = {
            "success": True,
            "id": str(uuid.uuid4()),
            "table_names": ",".join([name for name in table_names if name]),
            "trigger_type": trigger_type,
            "depth": "expert",
            "schedule_id": schedule_id,
            "history_id": history_id,
            "summary": self._normalize_expert_summary(
                final_output.get("summary"),
                findings[:_EXPERT_MAIN_SECTION_LIMIT],
                recommendations[:_EXPERT_MAIN_SECTION_LIMIT],
            ),
            "profile": profile,
            "insights": findings,
            "top_insights": findings[:_EXPERT_MAIN_SECTION_LIMIT],
            "anomalies": anomalies,
            "recommendations": recommendations,
            "action_items": action_items[:_EXPERT_MAIN_SECTION_LIMIT],
            "limitations": limitations,
            "root_causes": root_causes,
            "conversation_chain": list(compressed_history),
            "reasoning_traces": list(reasoning_traces),
            "evidence_chains": self._build_evidence_chains(raw_findings, compressed_history),
            "confidence_ratings": {"overall": final_output.get("confidence_overall")},
            "steps": list(all_step_results),
            "insight_count": len(findings),
            "anomaly_count": len(anomalies),
            "failed_step_count": failed_step_count,
            "status": status,
            "error_message": None,
            "duration_ms": int((time.time() - started_at) * 1000),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        report["executive_summary"] = report["summary"]
        if note:
            report["note"] = note
        return report

    def _build_evidence_chains(
        self,
        findings: Sequence[Dict[str, Any]],
        compressed_history: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not findings:
            return []
        hypotheses = []
        assessments = []
        follow_ups = []
        for item in compressed_history:
            output = item.get("strategist_output") or {}
            hypotheses.extend(output.get("hypotheses") or [])
            assessments.extend(output.get("assessments") or [])
            follow_ups.extend(output.get("follow_ups") or [])

        evidence_chains: List[Dict[str, Any]] = []
        for finding in findings:
            finding_payload = finding if isinstance(finding, dict) else {"title": str(finding)}
            match_text = self._matching_text(finding_payload)
            evidence_ids = self._extract_evidence_ids(finding_payload)

            matched_hypotheses = [
                item
                for item in hypotheses
                if self._matches_evidence(item, evidence_ids, match_text, id_keys=("id",), text_keys=("title", "query_description"))
            ]
            matched_hypothesis_ids = {
                item.get("id")
                for item in matched_hypotheses
                if item.get("id")
            }
            matched_assessments = [
                item
                for item in assessments
                if item.get("hypothesis_id") in matched_hypothesis_ids
                or self._matches_evidence(item, evidence_ids, match_text, id_keys=("hypothesis_id",), text_keys=("verdict",))
            ]
            matched_follow_ups = [
                item
                for item in follow_ups
                if item.get("hypothesis_id") in matched_hypothesis_ids
                or self._matches_evidence(
                    item,
                    evidence_ids,
                    match_text,
                    id_keys=("hypothesis_id", "id"),
                    text_keys=("reason", "query_description"),
                )
            ]

            evidence_chains.append(
                {
                    "finding": (
                        finding_payload.get("title")
                        or finding_payload.get("category")
                        or finding_payload.get("headline")
                        or finding_payload.get("description")
                        or finding_payload
                    ),
                    "detail": self._compose_expert_detail(finding_payload),
                    "hypotheses": matched_hypotheses,
                    "assessments": matched_assessments,
                    "follow_ups": matched_follow_ups,
                }
            )
        return evidence_chains

    def _coerce_expert_item(self, value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.startswith("{") and cleaned.endswith("}"):
                parsed = self._parse_json_from_text(cleaned)
                if parsed:
                    return parsed
            return {"detail": value}
        if value is None:
            return {}
        return {"detail": str(value)}

    def _normalize_expert_findings(
        self,
        items: Sequence[Any],
        *,
        default_prefix: str = "洞察",
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for index, item in enumerate(items):
            payload = self._coerce_expert_item(item)
            title = (
                payload.get("title")
                or payload.get("category")
                or payload.get("headline")
                or payload.get("theme")
                or f"{default_prefix} {index + 1}"
            )
            detail = self._compose_expert_detail(payload, include_recommendation=False) or str(title)
            normalized_item = {
                "title": str(title),
                "detail": detail,
            }
            if payload.get("severity"):
                normalized_item["severity"] = payload.get("severity")
            normalized.append(normalized_item)
        return normalized

    def _normalize_expert_text_list(self, items: Sequence[Any]) -> List[str]:
        normalized: List[str] = []
        for item in items:
            payload = self._coerce_expert_item(item)
            title = payload.get("title") or payload.get("category") or payload.get("headline")
            detail = self._compose_expert_detail(payload)
            if title and detail:
                normalized.append(f"{title}：{detail}")
            elif title:
                normalized.append(str(title))
            elif detail:
                normalized.append(detail)
        return normalized

    def _normalize_expert_action_items(
        self,
        items: Sequence[Any],
        findings: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        seen: set[Tuple[str, str]] = set()

        def append_item(title: Optional[str], detail: Optional[str], urgency: Optional[Any] = None) -> None:
            clean_title = str(title or f"动作建议 {len(normalized) + 1}").strip()
            clean_detail = str(detail or "").strip() or clean_title
            marker = (clean_title, clean_detail)
            if marker in seen:
                return
            seen.add(marker)
            item = {"title": clean_title, "detail": clean_detail}
            if urgency:
                item["urgency"] = urgency
            normalized.append(item)

        for item in items:
            payload = self._coerce_expert_item(item)
            title = payload.get("title") or payload.get("category") or payload.get("headline") or payload.get("action")
            detail = self._compose_expert_action_detail(payload)
            append_item(title, detail, payload.get("urgency"))
            if len(normalized) >= _EXPERT_MAIN_SECTION_LIMIT:
                return normalized

        for finding in findings:
            recommendation = finding.get("recommendation")
            if not recommendation:
                continue
            title = finding.get("title") or finding.get("category") or finding.get("headline")
            detail = str(recommendation).strip()
            implication = finding.get("implication") or finding.get("business_impact")
            if implication:
                detail = f"{detail}；预期影响：{str(implication).strip()}"
            append_item(title, detail)
            if len(normalized) >= _EXPERT_MAIN_SECTION_LIMIT:
                return normalized

        return normalized

    def _action_item_as_text(self, item: Dict[str, Any]) -> str:
        title = str(item.get("title") or "").strip()
        detail = str(item.get("detail") or "").strip()
        if title and detail and title != detail:
            return f"{title}：{detail}"
        return detail or title

    def _compose_expert_detail(self, payload: Dict[str, Any], *, include_recommendation: bool = True) -> str:
        parts: List[str] = []
        keys = [
            "detail",
            "description",
            "quantification",
            "business_impact",
            "evidence",
            "implication",
        ]
        if include_recommendation:
            keys.append("recommendation")
        for key in keys:
            value = payload.get(key)
            if value:
                text = str(value).strip()
                if text and text not in parts:
                    parts.append(text)
        return "；".join(parts)

    def _compose_expert_action_detail(self, payload: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in (
            "detail",
            "action",
            "description",
            "recommendation",
            "expected_outcome",
            "business_impact",
            "owner_hint",
            "urgency",
        ):
            value = payload.get(key)
            if value:
                text = str(value).strip()
                if text and text not in parts:
                    parts.append(text)
        return "；".join(parts)

    def _normalize_expert_summary(
        self,
        summary: Any,
        insights: Sequence[Dict[str, Any]],
        recommendations: Sequence[str],
    ) -> str:
        text = str(summary or "").strip()
        if text:
            cleaned_sentences = []
            for sentence in re.split(r"[。！？!?]", text):
                normalized = sentence.strip()
                lowered = normalized.lower()
                if not normalized:
                    continue
                if "descriptive -> diagnostic -> predictive" in lowered:
                    continue
                if "methodology" in lowered or "方法论" in normalized:
                    continue
                cleaned_sentences.append(normalized)
            if cleaned_sentences:
                candidate = "；".join(cleaned_sentences[:2]).strip("； ")
                if candidate:
                    return self._shorten_expert_text(candidate + "。", 120)

        if insights:
            parts = []
            for insight in insights[:2]:
                title = insight.get("title") or "关键洞察"
                detail = self._shorten_expert_text(insight.get("detail") or "", 36)
                parts.append(f"{title}：{detail}" if detail else str(title))
            return "；".join(parts) + "。"

        if recommendations:
            return self._shorten_expert_text(f"本次 expert 分析已完成，建议优先执行：{recommendations[0]}", 120)

        return "本次 expert 分析已完成，请优先查看下方关键洞察与建议。"

    def _shorten_expert_text(self, text: str, max_chars: int) -> str:
        normalized = re.sub(r"\s+", " ", (text or "").strip())
        if len(normalized) <= max_chars:
            return normalized
        return normalized[: max_chars - 1].rstrip("；，, ") + "…"

    def _extract_evidence_ids(self, finding: Dict[str, Any]) -> set[str]:
        values: set[str] = set()
        for key in ("hypothesis_id", "assessment_id", "follow_up_id"):
            value = finding.get(key)
            if value:
                values.add(str(value).lower())
        for key in ("hypothesis_ids", "evidence_ids", "supporting_ids"):
            for value in finding.get(key) or []:
                values.add(str(value).lower())
        return values

    def _matching_text(self, payload: Any) -> str:
        return json.dumps(payload or {}, ensure_ascii=False, default=str).lower()

    def _matches_evidence(
        self,
        item: Dict[str, Any],
        evidence_ids: set[str],
        match_text: str,
        *,
        id_keys: Sequence[str],
        text_keys: Sequence[str],
    ) -> bool:
        for key in id_keys:
            value = item.get(key)
            if value and str(value).lower() in evidence_ids:
                return True
        for key in text_keys:
            value = item.get(key)
            if value and str(value).lower() in match_text:
                return True
        return False

    def _execute_with_repair(
        self,
        sql: str,
        question_context: str,
        api_config: Dict[str, Any],
    ) -> Tuple[Optional[List[Dict[str, Any]]], str, bool, Optional[str]]:
        repair_agent = RepairAgent(
            doris_client=self.db,
            api_key=api_config.get("api_key"),
            model=api_config.get("model"),
            base_url=api_config.get("base_url"),
        )

        final_sql = (sql or "").strip().rstrip(";")
        last_error: Optional[Exception] = None
        try:
            result = self.db.execute_query(final_sql)
            return result, final_sql, True, None
        except Exception as exec_error:
            last_error = exec_error

        for _ in range(2):
            repaired_sql = repair_agent.repair_sql(
                question_context,
                final_sql,
                str(last_error),
                [],
                api_config=api_config,
            )
            final_sql = (repaired_sql or "").strip().rstrip(";")
            try:
                result = self.db.execute_query(final_sql)
                return result, final_sql, True, None
            except Exception as retry_error:
                last_error = retry_error

        return None, final_sql, False, str(last_error) if last_error else "unknown execution error"

    def _generate_insights(
        self,
        step_results: Sequence[Dict[str, Any]],
        profile: Dict[str, Any],
        api_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        prompt = (
            "Interpret the analysis results and return structured JSON.\n"
            f"Profile: {json.dumps(profile, ensure_ascii=False, default=str)}\n"
            f"Steps: {json.dumps(list(step_results), ensure_ascii=False, default=str)}\n\n"
            "Return JSON with `summary`, `insights`, `anomalies`, and `recommendations`. "
            "Each insight or anomaly item should include `title` and `detail`."
        )
        try:
            payload = self._call_json_completion(
                system_prompt="You explain business insights from Apache Doris query results.",
                user_prompt=prompt,
                api_config=api_config,
            )
        except Exception as exc:
            logger.warning("insight generation failed, using fallback summary: %s", exc)
            return {
                "summary": "Analysis completed with limited automated insights.",
                "insights": [],
                "anomalies": [],
                "recommendations": [],
            }
        if isinstance(payload, dict):
            return {
                "summary": payload.get("summary", ""),
                "insights": payload.get("insights") or [],
                "anomalies": payload.get("anomalies") or [],
                "recommendations": payload.get("recommendations") or [],
            }

        return {
            "summary": "Analysis completed.",
            "insights": [],
            "anomalies": [],
            "recommendations": [],
        }

    def _execute_profile_query(
        self,
        query_sql: str,
        source_sql: str,
        fallback_source_sql: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], str]:
        try:
            return self.db.execute_query(query_sql), source_sql
        except Exception as exc:
            if fallback_source_sql and "TABLESAMPLE" in source_sql:
                logger.warning("TABLESAMPLE failed, falling back to RAND() sample: %s", exc)
                fallback_query = query_sql.replace(source_sql, fallback_source_sql, 1)
                return self.db.execute_query(fallback_query), fallback_source_sql
            raise

    def _build_report(
        self,
        *,
        table_names: List[str],
        profile: Dict[str, Any],
        insights: Dict[str, Any],
        depth: str,
        trigger_type: str,
        step_results: Sequence[Dict[str, Any]],
        started_at: float,
        history_id: Optional[str] = None,
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        failed_step_count = sum(1 for step in step_results if not step.get("success"))
        error_messages = [step.get("error_message") for step in step_results if step.get("error_message")]
        if failed_step_count == len(step_results) and step_results:
            status = "failed"
        elif failed_step_count:
            status = "partial"
        else:
            status = "completed"

        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        report = {
            "success": True,
            "id": str(uuid.uuid4()),
            "table_names": ",".join(table_names),
            "trigger_type": trigger_type,
            "depth": depth,
            "schedule_id": schedule_id,
            "history_id": history_id,
            "summary": insights.get("summary", ""),
            "profile": profile,
            "insights": insights.get("insights") or [],
            "anomalies": insights.get("anomalies") or [],
            "recommendations": insights.get("recommendations") or [],
            "steps": list(step_results),
            "insight_count": len(insights.get("insights") or []),
            "anomaly_count": len(insights.get("anomalies") or []),
            "failed_step_count": failed_step_count,
            "status": status,
            "error_message": "; ".join(error_messages) if error_messages else None,
            "duration_ms": int((time.time() - started_at) * 1000),
            "created_at": created_at,
        }
        if note:
            report["note"] = note
        return report

    def _save_report(self, report: Dict[str, Any]) -> None:
        self.db.execute_update(
            """
            INSERT INTO `_sys_analysis_reports`
            (`id`, `table_names`, `trigger_type`, `depth`, `schedule_id`, `history_id`,
             `summary`, `report_json`, `insight_count`, `anomaly_count`,
             `failed_step_count`, `status`, `error_message`, `duration_ms`, `created_at`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                report.get("id"),
                report.get("table_names"),
                report.get("trigger_type"),
                report.get("depth"),
                report.get("schedule_id"),
                report.get("history_id"),
                report.get("summary"),
                json.dumps(report, ensure_ascii=False, default=str),
                report.get("insight_count", 0),
                report.get("anomaly_count", 0),
                report.get("failed_step_count", 0),
                report.get("status"),
                report.get("error_message"),
                report.get("duration_ms", 0),
                report.get("created_at"),
            ),
        )

    def _run_plan_steps(
        self,
        plan_steps: Sequence[Dict[str, Any]],
        api_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        step_results: List[Dict[str, Any]] = []
        for step in plan_steps:
            sql = step.get("sql") or ""
            question_context = step.get("question") or step.get("title") or "Analysis step"
            data, final_sql, success, error_message = self._execute_with_repair(
                sql,
                question_context,
                api_config,
            )
            step_results.append(
                {
                    "title": step.get("title") or "Analysis step",
                    "question": question_context,
                    "sql": final_sql,
                    "success": success,
                    "error_message": error_message,
                    "row_count": len(data or []),
                    "data": data if success else None,
                }
            )
        return step_results

    def _call_json_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        api_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        content = self._call_chat_completion(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_config=api_config,
        )
        return self._parse_json_from_text(content)

    def _call_chat_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        api_config: Dict[str, Any],
    ) -> str:
        url = f"{api_config['base_url'].rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_config['api_key']}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": api_config["model"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 3000,
        }
        import httpx as _httpx

        payload["stream"] = True
        content_chunks: List[str] = []

        with _httpx.Client(timeout=_httpx.Timeout(120.0, connect=30.0)) as client:
            with client.stream("POST", url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                        delta = chunk["choices"][0].get("delta", {})
                        if delta.get("content"):
                            content_chunks.append(delta["content"])
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass

        return "".join(content_chunks)

    def _merge_table_names(
        self,
        explicit_tables: Sequence[str],
        step_results: Sequence[Dict[str, Any]],
    ) -> List[str]:
        merged = {table for table in explicit_tables if table}
        for step in step_results:
            merged.update(self._extract_table_names(step.get("sql") or ""))
        return sorted(merged)

    def _extract_table_names(self, sql: str) -> List[str]:
        return sorted(
            {
                match.group(1)
                for match in re.finditer(
                    r"(?:FROM|JOIN)\s+`?([A-Za-z0-9_\-\u4e00-\u9fa5]+)`?",
                    sql or "",
                    flags=re.IGNORECASE,
                )
                if match.group(1)
            }
        )

    def _decode_table_names(self, value: Optional[str]) -> List[str]:
        if not value:
            return []
        return [name.strip() for name in str(value).split(",") if name.strip()]

    def _get_max_steps(self, depth: str) -> int:
        return _DEPTH_MAX_STEPS.get((depth or "").lower(), _DEPTH_MAX_STEPS["standard"])

    def _is_numeric_type(self, column_type: str) -> bool:
        lowered = (column_type or "").lower()
        return any(marker in lowered for marker in _NUMERIC_MARKERS)

    def _is_temporal_type(self, column_type: str) -> bool:
        lowered = (column_type or "").lower()
        return any(marker in lowered for marker in _TEMPORAL_MARKERS)

    def _null_rate(self, null_count: Any, total_count: int) -> float:
        if not total_count:
            return 0.0
        try:
            return float(null_count or 0) / float(total_count)
        except Exception:
            return 0.0

    def _strip_markdown_fences(self, value: str) -> str:
        cleaned = (value or "").strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()

    def _parse_json_from_text(self, value: str) -> Dict[str, Any]:
        cleaned = self._strip_markdown_fences(value)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(cleaned[start : end + 1])
                except json.JSONDecodeError:
                    pass
            logger.warning("analyst agent received non-JSON response: %s", cleaned)
            return {}
