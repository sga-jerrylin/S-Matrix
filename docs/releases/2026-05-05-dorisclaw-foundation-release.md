# DorisClaw Foundation Release - 2026-05-05

本次 release 标记 DorisClaw/DC 的基础能力完成：数据查询、数据挖掘、数据预测三条主线具备可联调、可回滚、可继续行业化扩展的底座。

## 核心完成项

- 数据查询：Vanna 2 native runtime、ToolRegistry、native memory、native query kernel、SSE 事件流已接入，`/api/query/natural` 默认切到 `auto`，native 失败可回退 legacy。
- 数据挖掘：数据基础层、表注册、metadata 读面、关系读面、洞察报告读模型和 metadata fallback 链路已收口。
- 数据预测：metric foundation、forecast-ready 校验、day/week/month 时间序列读取、forecast MVP contract 与 baseline 执行链路已具备。
- Doris 运行时：本地 Doris 升级到 4.0.5，单 backend 拓扑、named volume 存储、init/update/smoke 门禁、系统表写探针已稳定。
- LLM 能力：Doris LLM resource 支持 OpenRouter/DeepSeek 路由，NLQ/metadata 可复用已配置 resource，不再强依赖外传 API key。
- CLI/MCP：`dc` runtime、report/forecast 命令面、MCP tools、legacy MCP shim 已接入统一后端契约。
- 前端工作台：上传、查询、洞察、预测主路径完成基础收口，LLM 配置支持编辑、保存并测试、错误保留与脱敏。

## 验收口径

- `dc smoke`：通过，包含 FE/BE/API/Frontend、canonical backend、系统表存在与系统表写入。
- NLQ golden：默认、legacy、native、auto 四组均通过。
- Vanna 2 切流：默认 `DC_NLQ_DEFAULT_KERNEL=auto`，回滚方式为 `DC_NLQ_DEFAULT_KERNEL=legacy` 后重启 `smatrix-api`。
- 真实业务表：已同步表注册读面保持可用，`_sys_table_registry` 验收值为 30。
- AI_GENERATE 残留：切流验收时为 0。

## 后续方向

- 餐饮娱乐行业分支：围绕门店、订单、会员、库存、支付退款构建行业查询、洞察和预测模板。
- 安全专项：收口 `/api/config` 鉴权和敏感配置脱敏。
- 产品专项：把 native query trace、memory 命中、SSE 事件流转成前端可解释体验。
