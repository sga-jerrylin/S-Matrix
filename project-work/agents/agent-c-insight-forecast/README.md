# agent-c 洞察与预测层

- 模块 owner：`agent-c`
- 当前批次：`Batch 2`
- 责任范围：数据深挖、分析报告、预测抽象层、外部协同接口

主要关注文件：

- `doris-api/analyst_agent.py`
- `doris-api/analysis_scheduler.py`
- `doris-api/analysis_dispatcher.py`
- `doris-api/app_scheduler.py`
- `doris-api/main.py` 中与 `/api/analysis/*` 相关接口

本目录用于保存：

- `orders/`：发给 `agent-c` 的正式指令单
- `tickets/`：`agent-c` 的正式工单
- `reviews/passed/`：通过审核记录
- `reviews/rework/`：返工单和驳回记录
