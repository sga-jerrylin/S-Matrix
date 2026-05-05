# E-order-20260419-2250-004-Doris后端去重与单Backend收敛指令单

## 1. 指令主题

修复本地 Docker 联调环境中 Doris FE 元数据与 BE 注册漂移问题，彻底去除重复 backend，只保留一个 canonical backend：`smatrix-be:9050`。

## 2. 背景与问题定义

当前环境在保存真实数据源时失败，错误已定位为 Doris 系统表写入失败，而非前端表单或外部数据源凭证错误。

已确认的根因：

1. FE 当前注册了两个 backend，分别是 `192.168.100.3:9050` 与 `smatrix-be:9050`。
2. `docker-compose.yml` 曾使用 `BE_ADDR=192.168.100.3:9050`，而初始化脚本使用 `ALTER SYSTEM ADD BACKEND 'smatrix-be:9050'`，导致同一 BE 以两种身份被 FE 记录。
3. 系统表 tablet/replica 已出现 bad replica / no alive replica，进而导致 `_sys_datasources` 等系统表无法写入。

结论：这是 Doris 本地环境元数据污染问题，必须做“单 backend 收敛”，不能继续容忍双 backend 并存。

## 3. 本次目标

本轮只做运行时与环境层修复，不改业务 schema，不改查询/洞察/预测逻辑。

目标如下：

1. Docker 本地 Doris 环境最终只保留一个 backend。
2. 该 backend 的唯一合法地址固定为 `smatrix-be:9050`。
3. FE 元数据、初始化脚本、docker 配置、健康检查脚本保持一致，不再出现 IP/hostname 双注册。
4. 保存数据源、系统表写入、后续同步链路恢复可用。

## 4. 负责范围

agent-E 负责以下模块：

1. `docker-compose.yml`
2. `init.sh`
3. `init.ps1`
4. `update.sh`
5. `update.ps1`
6. `scripts/smoke_docker.sh`
7. 其他与 Docker / runtime / 本地 Doris 初始化直接相关的脚本

如需改动 README，可只补充与本问题直接相关的恢复说明。

## 5. 必做事项

1. 固化 backend canonical address：
   - 全部脚本与 compose 配置统一使用 `smatrix-be:9050`
   - 不再使用固定 IP 注册 backend

2. 增加环境自检与 fail-fast：
   - 若 `SHOW BACKENDS;` 检测到多于一个 backend，初始化流程必须明确失败
   - 若检测到唯一 backend 不是 `smatrix-be`，也必须明确失败
   - 错误提示中要直接给出恢复动作，不允许静默继续

3. 给出并实现本地恢复路径：
   - 在“当前仅为本地联调环境”的前提下，允许清理本地 Doris 元数据/volume 并重建
   - 该恢复路径必须是可重复执行的
   - 恢复后必须验证 FE/BE 拓扑、系统表写能力、健康状态

4. 做一次端到端最小联调：
   - `SHOW BACKENDS;` 最终只返回 1 行 backend
   - backend host 为 `smatrix-be`
   - FE healthy、BE alive
   - 至少验证一次数据源保存成功

## 6. 明确禁止

1. 不允许保留两个 backend 再靠“绕过报错”继续开发。
2. 不允许在业务代码里吞掉 Doris 写入异常来伪装成功。
3. 不允许扩散到查询智能、洞察、预测、前端业务逻辑。
4. 不允许引入新的 IP-based backend 注册逻辑。

## 7. 授权说明

本单明确授权 agent-E 在“仅限本地 Docker 联调环境”的边界内进行以下操作：

1. 清理本地 Doris FE/BE 元数据或相关 Docker volume
2. 重建 Docker 容器与本地 Doris 拓扑
3. 调整初始化/更新/冒烟脚本
4. 为提升执行效率，可使用 sub-agent 协助做脚本校验、日志核查、回归验证

前提：

1. 不得删除用户外部真实数据源中的业务数据
2. 不得越权修改与本问题无关的业务模块

## 8. 交付物要求

必须提交以下内容：

1. 改动文件清单
2. 根因说明
3. 恢复步骤与是否需要 reset
4. 回归命令与结果
5. `SHOW BACKENDS;` 修复前后对比
6. 数据源保存链路验证结果
7. 风险与剩余事项

## 9. 验收标准

满足以下全部条件才算通过：

1. `SHOW BACKENDS;` 仅 1 条 backend 记录
2. backend host 为 `smatrix-be`
3. 不存在 duplicate backend / stale backend / bad replica 导致的系统表写入失败
4. 页面“保存数据源”可成功
5. 相关初始化脚本对重复 backend 有明确 fail-fast 保护
6. 本次变更未破坏现有 `dc` runtime / MCP / smoke 流程

## 10. 返回格式

请按以下格式回复，不要写散文：

1. 改动文件清单
2. 修复说明
3. 恢复步骤
4. 回归命令与结果
5. 风险说明

