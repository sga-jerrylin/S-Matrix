# Vanna Native Tools Schema (Phase 2)

This file defines the stable input/output contracts for DC query tools registered into Vanna2 `ToolRegistry`.

## Common Output Envelope

All tools return a structured JSON envelope through `ToolResult.result_for_llm` and `ToolResult.metadata`:

```json
{
  "tool_name": "dc_sql_generation",
  "success": true,
  "data": {},
  "error": null
}
```

Notes:
- `success=false` means tool execution failed but still returned structured output (no raw exception raised to caller).
- `error` object format:

```json
{
  "code": "sql_generation_failed",
  "message": "..."
}
```

## Tool Schemas

### 1) `dc_catalog_retrieval`

Input:
- `table_names: string[]` (optional scope)
- `include_table_registry: bool` (default `true`)
- `include_foundation_tables: bool` (default `true`)
- `include_query_catalog: bool` (default `true`)

Output `data`:
- `table_registry: object[]`
- `foundation_tables: object[]`
- `query_catalog: object[]`
- `trace.table_registry_count: int`
- `trace.foundation_table_count: int`
- `trace.query_catalog_count: int`
- `trace.source_labels: string[]`
- `trace.warnings: string[]`

### 2) `dc_memory_retrieval`

Input:
- `question: string`
- `limit: int` (`1..20`, default `5`)
- `api_config: object` (optional LLM runtime config)

Output `data`:
- `examples: object[]`
- `trace.memory_hit: bool`
- `trace.source_labels: string[]`
- `trace.sources_attempted: string[]`
- `trace.fallback_used: bool`
- `trace.errors: object[]`

### 3) `dc_ddl_doc_retrieval`

Input:
- `question: string`
- `api_config: object` (optional LLM runtime config)

Output `data`:
- `ddl: string[]`
- `documentation: string[]`
- `trace.ddl_count: int`
- `trace.documentation_count: int`
- `trace.source_labels: string[]`
- `trace.ddl_cache_hit: bool`

### 4) `dc_sql_generation`

Input:
- `question: string`
- `table_name: string | null`
- `subtask: object` (optional subtask payload)
- `api_config: object`

Output `data`:
- `sql: string`
- `trace: object`

### 5) `dc_sql_validation`

Input:
- `sql: string`

Output `data`:
- `validation.is_valid: bool`
- `validation.errors: string[]`
- `validation.warnings: string[]`
- `validation.normalized_sql: string`
- `validation.read_only: bool`

### 6) `dc_sql_repair`

Input:
- `question: string`
- `failed_sql: string`
- `error_message: string`
- `ddl_list: string[]`
- `api_config: object`

Output `data`:
- `sql: string`
- `trace: object`

### 7) `dc_sql_execution`

Input:
- `sql: string`
- `params: any[]`

Output `data`:
- `rows: object[]`
- `row_count: int`
- `execution_ms: int`

## Access Groups

Default `access_groups`:
- `dc_catalog_retrieval`: `user`, `admin`
- `dc_memory_retrieval`: `user`, `admin`
- `dc_ddl_doc_retrieval`: `user`, `admin`
- `dc_sql_generation`: `user`, `admin`
- `dc_sql_validation`: `user`, `admin`
- `dc_sql_repair`: `admin`
- `dc_sql_execution`: `user`, `admin`

## Audit Bridge Event Contract

Each tool call records to `NativeAuditTraceBridge.events`:
- `tool_invocation`
- `tool_result` (success/failure payload)
- `tool_error` (unexpected exception path)

Event payload includes:
- `tool_name`
- `context.request_id`
- `context.conversation_id`
- `context.user_id`
- `context.groups`
