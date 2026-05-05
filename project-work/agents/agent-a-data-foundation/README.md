# agent-a 数据基础层

- 模块 owner：`agent-a`
- 当前批次：`Batch 1`
- 责任范围：数据接入、表注册、元数据、关系、数据源同步、Doris 基础访问能力

主要关注文件：

- `doris-api/db.py`
- `doris-api/upload_handler.py`
- `doris-api/datasource_handler.py`
- `doris-api/metadata_analyzer.py`
- `doris-api/handlers.py` 中与数据接入相关部分
- `doris-api/main.py` 中与上传、表管理、数据源相关接口

本目录用于保存：

- `orders/`：发给 `agent-a` 的正式指令单
- `tickets/`：`agent-a` 的正式工单
- `reviews/passed/`：通过审核记录
- `reviews/rework/`：返工单和驳回记录
