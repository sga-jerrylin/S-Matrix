# agent-e CLI / MCP / 运行时层

- 模块 owner：`agent-e`
- 当前批次：`Batch 1`
- 责任范围：CLI-first 能力输出、MCP server、启动脚本、健康检查、Docker、集成契约

主要关注文件：

- `doris-api/mcp_server.py`
- `mcp_config.json`
- `scripts/*`
- 根目录 `docker-compose.yml`
- `init.ps1`
- `init.sh`
- `update.ps1`
- `update.sh`
- 未来新增的 `dc` CLI 入口相关文件

本目录用于保存：

- `orders/`：发给 `agent-e` 的正式指令单
- `tickets/`：`agent-e` 的正式工单
- `reviews/passed/`：通过审核记录
- `reviews/rework/`：返工单和驳回记录
