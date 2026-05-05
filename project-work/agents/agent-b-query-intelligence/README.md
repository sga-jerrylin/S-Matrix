# agent-b 查询智能层

- 模块 owner：`agent-b`
- 当前批次：`Batch 1`
- 责任范围：自然语言查询、SQL 生成与修复、检索记忆、查询编排

主要关注文件：

- `doris-api/vanna_doris.py`
- `doris-api/planner_agent.py`
- `doris-api/table_admin_agent.py`
- `doris-api/coordinator_agent.py`
- `doris-api/repair_agent.py`
- `doris-api/embedding.py`
- `doris-api/main.py` 中与 `/api/query/*` 相关接口

本目录用于保存：

- `orders/`：发给 `agent-b` 的正式指令单
- `tickets/`：`agent-b` 的正式工单
- `reviews/passed/`：通过审核记录
- `reviews/rework/`：返工单和驳回记录
