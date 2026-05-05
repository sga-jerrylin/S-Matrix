# E-ticket-20260419-2250-004-Doris后端去重与单Backend收敛工单

## 工单状态

待执行

## 对应指令单

`E-order-20260419-2250-004-Doris后端去重与单Backend收敛指令单.md`

## 工单目标

彻底修复本地 Doris Docker 环境的 duplicate backend 问题，统一保留唯一 backend `smatrix-be:9050`，恢复系统表写入与数据源保存能力。

## 已知根因

1. 当前 FE 元数据中同时存在 `192.168.100.3:9050` 与 `smatrix-be:9050`
2. compose 与 init 脚本使用了不一致的 backend 地址
3. tablet/replica 健康受污染，导致 `_sys_datasources` 写入失败

## 执行要求

1. 必须收敛为单 backend
2. 必须保留 `smatrix-be:9050` 作为唯一 canonical backend
3. 必须提供本地 reset/rebuild 的安全操作说明
4. 必须完成一轮真实保存数据源验证

## 禁止事项

1. 不得保留双 backend
2. 不得通过吞错或前端绕过伪装修复
3. 不得越权修改业务模块

## 验收口径

1. `SHOW BACKENDS;` = 1 行
2. backend host = `smatrix-be`
3. 保存数据源成功
4. docker/init/update/smoke 脚本一致
5. 相关验证记录完整

