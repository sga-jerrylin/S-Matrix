# Vanna Native Runtime (Phase 1 Backbone)

`vanna_native_runtime` is the single import boundary for Vanna 2 native APIs in DorisClaw query intelligence.

## Purpose

1. Centralize all `vanna.*` imports and signature adaptation.
2. Provide a deterministic runtime probe for version/module/signature diagnostics.
3. Bootstrap minimal native kernel components without touching `/api/query/natural`.

## Components

- `imports.py`
  - `load_native_imports()`: loads `Agent`, `ToolRegistry`, `RequestContext`, `DemoAgentMemory`, `MockLlmService`, `VannaFastAPIServer`, `LegacyVannaAdapter`.
  - `resolve_legacy_vanna_base()`: resolves `VannaBase` via `vanna.legacy.base` first, then `vanna.base`.
- `probe.py`
  - `probe_vanna_native_runtime()`: gathers distribution/module versions, module availability, key signatures, and compatibility notes.
- `backbone.py`
  - `build_native_runtime_backbone()`: assembles `Agent`, `ToolRegistry`, `AgentMemory`, `UserResolver`, `RequestContext` factory, and SSE sidecar app factory.
- `dc_tools.py`
  - `register_dc_query_tools()`: registers DC query-intelligence tools into Vanna `ToolRegistry`.
  - `DCToolRuntimeAdapter`: bridges existing planner/retrieval/sql/repair/execution capabilities.
- `query_kernel.py`
  - `run_native_query_kernel()`: orchestrates planner/retrieval/sql/validation/repair/execution via ToolRegistry.
  - `NativeKernelExecutionError`: structured native-kernel failure contract for selector fallback.
- `models.py`
  - probe model, import bundle model, audit bridge, and backbone model.

## Phase-1 Constraints

- No integration with production NLQ execution path yet.
- No response schema changes.
- Native runtime is bootstrapped and testable in isolation.

## Next Phase Hooks

- Kernel selector integration (`native|legacy|auto`) is consumed by `/api/query/natural` in API layer.
- Trace bridge maps native audit/tool events to `nlq.v1 + trace.native` composite trace.
