# E-order-20260422-1252-005-Doris本地存储坏盘与Backend存活门禁返工指令单

## 1. 指令主题

对本地 Docker 联调环境做二次返工，彻底解决 Doris BE `broken disk` / `Alive=false` / `dc smoke` 误判通过问题。

本次不接受继续在当前坏盘目录上打补丁。要一次把本地 Doris 存储策略和运行时门禁收口。

## 2. 当前已确认事实

动态验收结果如下：

1. `SHOW BACKENDS;` 只剩 1 行 backend，host 为 `smatrix-be:9050`
2. 但该 backend 当前 `Alive=false`
3. `smatrix-be` 容器持续重启
4. `dc smoke` 未通过
5. `smatrix-be` 日志已明确报错：
   - `a broken disk is found /opt/apache-doris/be/storage`
6. `smatrix-fe` 已报：
   - `No backend available as scan node`

结论：

当前不是 duplicate backend 问题，而是 **本地 Doris BE 存储目录已进入坏盘状态**，导致单 backend 虽然只剩一条，但实际上不可用。

## 3. 本次目标

本次要同时解决两个根问题：

1. **BE 存储目录稳定性**
   - 本地 Docker Doris 必须从 `broken disk` 状态恢复
   - `smatrix-be` 不再重启
   - backend 必须 `Alive=true`

2. **运行时门禁真实性**
   - `dc smoke` / 运行时检查不能只看“只有一条 backend + host/port 对”
   - 必须把 backend `Alive=true`、BE 可调度、系统表可写纳入验收

## 4. 我对问题的判断

当前最可能的问题不是业务代码，而是本地存储策略本身不稳：

1. 现有 `./data/be/storage -> /opt/apache-doris/be/storage` 的本地持久化目录已经被污染或损坏
2. 在 Windows Docker Desktop 本地联调场景下，继续复用当前宿主机目录，收益很低，风险很高
3. 之前解决了“重复 backend”，但没有解决“唯一 backend 其实死着”的问题
4. 当前 `doctor.py` 的 `canonical_backend` 只校验行数和 host/port，没有校验 `Alive=true`

结论：

这次不要继续围绕旧目录修修补补，而是直接做**确定性重建方案**。

## 5. 必做事项

### A. 先收口本地 Doris 存储策略

必须在以下两种方案中选一种，并给出明确理由：

1. **推荐方案：改为 Docker named volume**
   - 不再把 Doris FE/BE 核心运行数据直接绑到当前 Windows 工作区目录
   - 至少 BE storage 要脱离 `./data/be/storage`
   - 如有必要，FE metadata 一并切到 Docker named volume

2. **备选方案：改到 WSL / Linux 文件系统路径**
   - 如果你坚持保留 bind mount，则必须证明该路径不是当前 broken disk 根因
   - 并给出为什么新的路径比当前工作区 bind mount 更稳定

禁止事项：

1. 不允许继续默认使用当前已经报 `broken disk` 的旧目录而不做存储策略调整
2. 不允许只靠 `-Reset` 清空旧目录然后假设问题消失
3. 如果 reset 后仍复现 broken disk，必须立即切存储方案，不能再拖

### B. 重建本地 Doris 状态

你有权直接做本地 destructive reset，但范围只限本地 Docker Doris 联调环境：

1. 清理容器
2. 清理 Doris 相关 volume / metadata / storage
3. 重建 FE/BE
4. 重建 canonical backend

目标是得到一个干净、可重复初始化、可稳定启动的单 backend 本地 Doris。

### C. 补强运行时门禁

必须补以下门禁：

1. `dc smoke` 中的 backend 检查，不能只看：
   - backend 仅 1 条
   - host = `smatrix-be`
   - port = `9050`

2. 必须至少新增以下校验：
   - backend `Alive=true`
   - backend 不处于 `Decommissioned=true`
   - FE 查询不再报 `No backend available as scan node`

3. 如果 backend 存在但不 alive，`dc smoke` 必须 fail

### D. 补强初始化与更新流程

`init` / `update` 必须做到：

1. 若本地 Doris 存储损坏，不能继续拉起 API / Frontend 伪装成功
2. 若 backend 不 alive，流程必须明确失败
3. 错误提示必须直接给恢复动作

## 6. 范围边界

本次允许改动：

1. `docker-compose.yml`
2. `init.ps1`
3. `init.sh`
4. `update.ps1`
5. `update.sh`
6. `scripts/smoke_docker.sh`
7. `doris-api/dc_runtime/doctor.py`
8. README 中与本问题直接相关的恢复说明

本次不允许改动：

1. 查询智能业务逻辑
2. 洞察/预测逻辑
3. 前端业务逻辑
4. CLI/MCP 业务契约

## 7. 授权说明

本单明确授权：

1. 你可以删除本地 Doris FE/BE 的元数据和存储数据
2. 你可以删除并重建本地 Docker volume
3. 你可以改变本地 Doris 的存储挂载策略
4. 你可以为执行效率使用 sub-agent 做日志核查、脚本验证、回归验证

前提：

1. 仅限本地联调环境
2. 不得删除外部真实业务数据库数据
3. 不得越权改业务模块

## 8. 验收标准

必须全部满足才算通过：

1. `docker compose ps` 中 `smatrix-fe / smatrix-be / smatrix-api / smatrix-frontend` 全部 healthy
2. `SHOW BACKENDS;` 只返回 1 行 backend
3. 该 backend 为 `smatrix-be:9050`
4. 该 backend `Alive=true`
5. `smatrix-be` 不再重启
6. `python doris-api/dc.py smoke` 全量通过
7. `_sys_datasources` 写探针通过
8. 实际保存一次数据源成功
9. 删除该测试数据源后环境保持干净

## 9. 返回格式

必须按以下格式返回：

1. 改动文件清单
2. 根因说明
3. 采用的存储策略与为什么这样选
4. 恢复步骤
5. 回归命令与结果
6. 风险说明

## 10. 一句硬要求

这次不要再围绕“旧坏盘目录还能不能抢救”纠缠。

如果 reset 后仍报 `a broken disk is found /opt/apache-doris/be/storage`，就直接切换到更稳定的本地存储方案，收口为一个明确、可重复、可验收的本地 Doris 联调环境。

