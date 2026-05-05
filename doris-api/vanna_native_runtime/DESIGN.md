# B-P1 Phase1 Design: Vanna Native Runtime Backbone

## Design Goal

Provide a stable native runtime boundary for Vanna 2 so that future upgrades only modify one module (`vanna_native_runtime`) instead of scattered business files.

## Scope (Phase 1)

- Included:
  - Centralized Vanna imports and compatibility path selection.
  - Runtime probe for version/module/signature diagnostics.
  - Native backbone bootstrap: Agent, ToolRegistry, AgentMemory, UserResolver, RequestContext factory, SSE sidecar app factory.
  - Query-chain-safe rollout: no `/api/query/natural` wiring yet.
- Excluded:
  - Tool mapping of planner/retrieval/repair/execution.
  - Kernel selector and automatic fallback in API.
  - Native memory migration of query history.

## Scope (Phase 2 - Tools Mapping)

- Included:
  - `dc_catalog_retrieval`
  - `dc_memory_retrieval`
  - `dc_ddl_doc_retrieval`
  - `dc_sql_generation`
  - `dc_sql_validation`
  - `dc_sql_repair`
  - `dc_sql_execution`
  - tool-level access groups and audit bridge events.
- Excluded:
  - no switch-over of `/api/query/natural` to native kernel yet.
  - no deletion of legacy fallback.

## Scope (Phase 3 - Kernel Selector)

- Included:
  - Native query kernel orchestration (`run_native_query_kernel`) based on Phase2 tools.
  - `/api/query/natural` kernel selector support: `native|legacy|auto`.
  - `auto` mode fallback from native failure to legacy with explicit `trace.native.fallback_reason`.
  - `trace.native` contract fields:
    - `kernel`
    - `tools_called`
    - `memory`
    - `audit_events`
    - `fallback_reason`
- Excluded:
  - no removal of legacy path.
  - no mandatory frontend switch to native.

## Architecture

```text
business code (main/table_admin/planner/repair/...)
            |
            | imports only from
            v
     vanna_native_runtime
       |- imports.py   (all vanna.* imports)
       |- probe.py     (version/module/signature probe)
       |- backbone.py  (Agent/ToolRegistry/Memory/RequestContext bootstrap)
       |- models.py    (probe/backbone contracts + audit bridge)
```

## Compatibility Decisions

1. Legacy base resolution order:
   - `vanna.legacy.base.VannaBase`
   - fallback `vanna.base.VannaBase`
2. Native imports:
   - top-level exports are preferred when present (`vanna.Agent`, `vanna.ToolRegistry`),
   - domain fallback paths are used when top-level exports drift.

## Runtime Probe Contract

Probe output includes:
- Python version.
- `vanna` distribution version and module version.
- Module availability for key paths.
- Key constructor/signature strings:
  - `Agent.__init__`
  - `Agent.send_message`
  - `ToolRegistry.register_local_tool`
  - `VannaFastAPIServer.__init__`
  - `VannaFastAPIServer.create_app`
- Notes (including incompatibility hints).

## Backbone Contract

`build_native_runtime_backbone()` returns:
- `agent`
- `tool_registry`
- `agent_memory`
- `user_resolver`
- `llm_service`
- `audit_bridge` (phase-1 minimal event buffer)
- methods:
  - `new_request_context(...)`
  - `create_sse_sidecar_app(...)`

## Risk Controls

1. Mainline isolation:
   - no endpoint wiring changes in phase 1.
2. Compatibility retention:
   - `vanna_compat.py` now delegates base import resolution to runtime boundary.
3. Regression guard:
   - dedicated runtime tests + existing query intelligence contract suite + golden suite.
