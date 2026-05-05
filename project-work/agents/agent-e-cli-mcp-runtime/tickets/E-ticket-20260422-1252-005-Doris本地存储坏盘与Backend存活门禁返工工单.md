# E-ticket-20260422-1252-005-Doris本地存储坏盘与Backend存活门禁返工工单

## 工单状态

待执行

## 对应指令单

`E-order-20260422-1252-005-Doris本地存储坏盘与Backend存活门禁返工指令单.md`

## 当前问题

当前本地环境虽已收敛为单 backend，但 backend 实际不可用：

1. `SHOW BACKENDS;` = 1 行
2. host = `smatrix-be:9050`
3. 但 `Alive=false`
4. `smatrix-be` 容器重启
5. `dc smoke` 未通过
6. `smatrix-be` 日志报 `a broken disk is found /opt/apache-doris/be/storage`

## 本单目标

1. 修复 Doris BE broken disk
2. 保证 backend `Alive=true`
3. 保证 `dc smoke` 对 backend 不 alive 会明确失败
4. 保证本地数据源保存链路恢复可用

## 执行原则

1. 不再继续围绕旧坏盘目录打补丁
2. 必要时直接切 Doris 本地存储策略
3. 必要时直接 reset / rebuild 本地 Doris 状态
4. 必须给出可重复恢复路径

## 验收口径

1. 所有容器 healthy
2. backend 唯一且 `Alive=true`
3. `dc smoke` 通过
4. `_sys_datasources` 可写
5. 实际保存数据源成功

