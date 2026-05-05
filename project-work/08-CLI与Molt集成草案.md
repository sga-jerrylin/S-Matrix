# CLI 与 Molt 集成草案

## 1. 总原则

`DorisClaw` 需要被定义成：

- CLI-first
- JSON-first
- MCP-compatible

原因：

- `SGA-Molt` 需要通过 skills 稳定调用
- 其他 SGA 项目已经明显在走 CLI/contract-first 路线
- 页面不应成为唯一能力入口

## 2. 对外能力面

我建议第一版只暴露少量稳定命令，不要把底层能力全摊开。

推荐命令：

- `dc doctor`
- `dc health`
- `dc query`
- `dc insight`
- `dc forecast`
- `dc context`
- `dc report`
- `dc mcp serve`

## 3. 命令职责

### `dc doctor`

用途：

- 检查本地运行环境
- 检查 API、数据库、配置、依赖状态

### `dc health`

用途：

- 返回当前服务和依赖健康情况

### `dc query`

用途：

- 执行结构化查询或自然语言查询

输入示例：

- 问题文本
- 目标表
- 可选资源名

输出重点：

- SQL
- 结果摘要
- 行数
- 关键元数据

### `dc insight`

用途：

- 对内部数据做深挖
- 可选接入外部协同上下文

输出重点：

- 异常点
- 根因候选
- 维度贡献
- 外部证据引用

### `dc forecast`

用途：

- 对内部业务指标做预测

输出重点：

- 预测值
- 置信区间
- 驱动因素
- 可选情景模拟结果

### `dc context`

用途：

- 向 `SGA-Web`、`SGA-EastFactory` 发起外部上下文补充请求

输出重点：

- 外部证据摘要
- 外部趋势结果
- 来源引用

### `dc report`

用途：

- 导出查询、洞察、预测的统一报告

### `dc mcp serve`

用途：

- 启动面向 `SGA-Molt` 或其他 agent runtime 的 MCP 服务

## 4. Molt 侧的 skill 调用建议

`SGA-Molt` 不应直接调用 `DorisClaw` 内部模块。

更合理的方式是：

- 调 CLI
- 或调 MCP 工具

这样做的好处：

- 契约稳定
- 更容易回归测试
- 更容易在本地和远程环境统一行为

## 5. 推荐的工具契约

如果未来向 `Molt` 暴露技能，我建议对外只保留少量公共工具：

- `dc_health`
- `dc_query`
- `dc_insight`
- `dc_forecast`
- `dc_context`
- `dc_report`

复杂路由留在 `DorisClaw` 内部完成。

## 6. 输出规范

### 默认输出

- 机器可读 JSON

### 推荐支持

- `--json`
- `--stdin`
- `--stdout`
- `--context-file`
- `--timeout`
- 稳定错误码

### 结果要求

- 可供技能直接消费
- 可供日志持久化
- 可供报告系统回放

## 7. 与 SGA-Web 的协同方式

`dc context` 或 `dc insight` 内部可以按需调用：

- 搜索
- 抓取
- 文章富化
- 公众号相关内容获取

但对 `Molt` 来说，这仍然是 `DorisClaw` 的一部分能力，而不是要求上游自行拼装。

## 8. 与 EastFactory 的协同方式

`dc forecast` 内部可以按需调用 `EastFactory`：

- 获取外部趋势预测
- 作为业务预测的外生变量
- 或作为解释层附加说明

但这也不应让上游直接感知内部复杂度。

## 9. 第一版 CLI 路线建议

我建议第一版只先做：

1. `dc health`
2. `dc query`
3. `dc insight`
4. `dc forecast`
5. `dc mcp serve`

只要这五个命令稳定，`DorisClaw` 就已经具备接入 `SGA-Molt` 的基本条件。

## 10. 当前建议

CLI 不是网页的附属品，而应该是 `DorisClaw` 的正式产品接口之一。

如果这条线定不下来，后续 skills 集成会越来越重。
