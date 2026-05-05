# B-SPIKE-20260503-VANNA2-NATIVE-001 交付说明

## 1. 改动文件清单

- `doris-api/experimental/vanna2_native_spike/README.md`
- `doris-api/experimental/vanna2_native_spike/probe_vanna2_native.py`
- `doris-api/experimental/vanna2_native_spike/poc_native_chat_server.py`
- `doris-api/experimental/vanna2_native_spike/poc_registry_and_legacy_adapter.py`
- `project-work/agents/agent-b-query-intelligence/reviews/passed/B-review-20260503-VANNA2-NATIVE-001-迁移评估与POC.md`

说明：本次仅新增 `experimental/spike` 与评估文档，未替换 `/api/query/natural` 主链路，未删除 `vanna_compat.py`。

## 2. Vanna 2 原生能力调研结论

### 2.1 已验证可用能力（本地实测）

1. `Agent + ToolRegistry` 可运行，`send_message(request_context, message)` 正常。
2. `servers.fastapi` 可运行，`/api/vanna/v2/chat_poll` 与 `/api/vanna/v2/chat_sse` 可通。
3. `ToolRegistry` 支持 `access_groups` 权限校验与审计钩子。
4. `legacy` 路径存在，`LegacyVannaAdapter` 可桥接 legacy `run_sql / add_question_sql / get_similar_question_sql`。

### 2.2 发现的工程风险（迁移前必须正视）

1. `pip show vanna` 为 `2.0.2`，但 `vanna.__version__` 为 `0.1.0`（版本元数据不一致）。
2. `vanna.servers.flask` 依赖 `flask_cors`，当前环境缺少该依赖。
3. `vanna.server.fastapi` 路径不存在，实际路径为 `vanna.servers.fastapi`。
4. 包内 `examples` 与当前 `Agent` 构造签名存在漂移（示例与实装不完全一致）。
5. `web_components` Python 包本地几乎为空，UI 主要通过 server 模板引用 CDN 脚本加载。

## 3. Legacy 与 Native 差异表

| 维度 | 当前 DorisClaw Legacy 兼容链路 | Vanna 2 Native |
|---|---|---|
| 主入口 | `/api/query/natural` 自研编排 | `Agent.send_message` + server chat endpoint |
| 编排方式 | planner/retrieval/repair 显式阶段 | LLM + tool loop（`max_tool_iterations`） |
| 记忆能力 | Doris `_sys_query_history` + 文档/DDL 检索 | `AgentMemory` 抽象（默认 demo memory 非向量） |
| 权限模型 | 当前主要在业务层控制 | `ToolRegistry` 原生 group access |
| 审计上下文 | 自研 trace（nlq.v1） | `RequestContext` + audit logger + UI features |
| 流式输出 | 现有 API 结果为主（trace+sql） | 原生 SSE/WS/poll 三种 chat 通道 |
| 与现有 NLQ 契约关系 | 已稳定、已通过 golden 10/10 | 需要新增契约映射层，不能直接替换 |

## 4. 最小 POC 说明

1. `probe_vanna2_native.py`
   - 探测关键模块、关键签名、版本一致性。
   - 输出确认：`legacy/base`、`legacy/adapter`、`servers.fastapi` 可用；`server.fastapi` 不可用。
2. `poc_native_chat_server.py`
   - 自定义 `UserResolver` + `MockLlmService` + `DemoAgentMemory` 组装 `Agent`。
   - 本地 `TestClient` 验证 chat poll 和 SSE，均返回 200，且 SSE `[DONE]` 结束信号正常。
3. `poc_registry_and_legacy_adapter.py`
   - 验证 `ToolRegistry` 权限拒绝/放行语义。
   - 验证 `LegacyVannaAdapter` 自动注册工具并可执行 memory search。

## 5. 是否建议迁移

结论：**建议“局部迁移”**，不建议立即“分阶段全量替换主链路”。

原因：

1. 现有 NLQ 主链路已满足可用性与 golden 10/10，直接切换 native chat 主栈风险高。
2. Vanna 2 native 的可用价值在 `ToolRegistry 权限模型 + 审计上下文 + 流式协议`，可按“旁路能力”先引入。
3. 当前 vanna 包存在版本/示例漂移，先做受控小步迁移更稳。

## 6. 建议后续正式工单（2-3 个）

1. **B-P2-ORDER：NLQ ToolRegistry 映射层接入（不替换 `/api/query/natural`）**
   - 将 planner/retrieval/repair 以内部 tool schema 映射，保留现有响应契约。
2. **B-P2-ORDER：Trace v2 契约增强**
   - 增加 request_context/user/tool_access/audit 字段映射，保持向后兼容 `nlq.v1`。
3. **B-P3-ORDER：Native SSE Sidecar POC API**
   - 新增隔离端点（例如 `/api/query/native-chat-sse`）做工作台灰度，不改现有查询页面默认路径。

## 7. 回归命令与结果

1. 查询智能回归：
   - `python -m pytest -q doris-api/tests/test_query_intelligence_contract.py doris-api/tests/test_table_admin_agent.py doris-api/tests/test_phase5_vector_search.py doris-api/tests/test_vanna_compat.py`
   - 结果：`26 passed, 1 warning`
2. golden 10 条（真实服务）：
   - 调用 `golden_runner.run_cases()` 对 `tests/golden_queries.json` 执行，资源名 `openrutor`
   - 结果：`total=10, passed=10, failed=0, pass_rate=1.0`

## 8. 风险清单

1. **查询稳定性风险**：直接切换到 native chat 协议会改变当前 NLQ API 契约与错误语义。
2. **性能风险**：native tool loop 若无约束，复杂问题可能引入额外迭代开销。
3. **trace 兼容风险**：native 事件流与当前 `trace.phases` 结构并非一一对应。
4. **权限审计风险**：若同时保留双路径（legacy + native），需避免审计字段不一致。
5. **前端适配风险**：`<vanna-chat>` 当前依赖 CDN 组件形态，和现有工作台契约差异大。
6. **依赖冲突风险**：flask server 额外依赖与 vanna 包内版本元数据漂移，可能影响 CI 可重复性。

## 9. 额外发现（本单不改）

- 当前运行实例存在无鉴权 `/api/config` 可读取运行时 `api_key` 的行为，这与整体 API Key 防护目标不一致。  
- 该问题属于安全与网关契约范围，建议由 owner A 单开安全工单处理。
