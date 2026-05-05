# B-P1-ORDER-20260503-VANNA2-PHASE1-RUNTIME-BACKBONE 交付说明

## 1. 改动文件清单

- `doris-api/vanna_native_runtime/__init__.py`
- `doris-api/vanna_native_runtime/errors.py`
- `doris-api/vanna_native_runtime/models.py`
- `doris-api/vanna_native_runtime/imports.py`
- `doris-api/vanna_native_runtime/probe.py`
- `doris-api/vanna_native_runtime/backbone.py`
- `doris-api/vanna_native_runtime/README.md`
- `doris-api/vanna_native_runtime/DESIGN.md`
- `doris-api/vanna_compat.py`
- `doris-api/tests/test_vanna_native_runtime.py`

## 2. Phase1 交付内容

### 2.1 vanna_native_runtime 骨架

已新增统一 runtime 边界，集中封装：

1. Vanna 2 native import（`Agent`、`ToolRegistry`、`RequestContext`、`DemoAgentMemory`、`MockLlmService`、`VannaFastAPIServer`、`LegacyVannaAdapter`）。
2. legacy base 兼容解析（`vanna.legacy.base` 优先，`vanna.base` 回退）。
3. 运行时探针（版本/模块/签名）。
4. native backbone 初始化（Agent/ToolRegistry/Memory/UserResolver/RequestContext factory/SSE sidecar app factory）。

### 2.2 业务导入收口

`vanna_compat.py` 已改为通过 `vanna_native_runtime.resolve_legacy_vanna_base()` 获取 `VannaBase`，不再直接散落导入 `vanna.legacy.base`/`vanna.base`。

### 2.3 设计文档

已新增：

- `doris-api/vanna_native_runtime/README.md`
- `doris-api/vanna_native_runtime/DESIGN.md`

文档覆盖：
- 模块职责与边界；
- Phase1 范围；
- probe contract；
- backbone contract；
- 风险控制。

## 3. 测试与回归

1. Phase1 新增测试：
   - `python -m pytest -q doris-api/tests/test_vanna_native_runtime.py doris-api/tests/test_vanna_compat.py`
   - 结果：`6 passed`

2. 查询智能核心回归：
   - `python -m pytest -q doris-api/tests/test_query_intelligence_contract.py doris-api/tests/test_table_admin_agent.py doris-api/tests/test_phase5_vector_search.py doris-api/tests/test_vanna_compat.py`
   - 结果：`26 passed`

3. golden 回归：
   - `python doris-api/tests/run_golden.py --base-url http://localhost:38018 --api-key <runtime-key> --resource-name openrutor --timeout 180 --min-pass-rate 1.0 --min-passed 10`
   - 结果：`10/10 passed`

## 4. 验收结论（Phase1）

已满足本子单目标：

1. `vanna_native_runtime` 骨架完成。
2. 版本/模块/签名探针可执行。
3. Agent/ToolRegistry/Memory/RequestContext 初始化可执行并通过测试。
4. 未接入 `/api/query/natural` 主链路，未破坏现有响应契约。

## 5. 已知风险与下一步

1. 当前仅完成 runtime backbone，尚未做 tool mapping（Phase2）。
2. `probe` 会报告 vanna 分发版本与 `vanna.__version__` 不一致（已纳入 notes，后续继续跟踪）。
3. 下一步建议进入 Phase2：将 planner/retrieval/sql validation/repair/execution 映射为 native tools，并补权限与审计测试。
