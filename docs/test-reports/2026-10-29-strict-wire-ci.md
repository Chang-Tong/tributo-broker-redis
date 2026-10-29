# Redis / Tributo 反馈的严格协议 CI 验收

日期：2026-10-29。契约 revision **2**，规范仍锁定 protocol-v2-2606 的四份原文及 SHA256，Core 基线 `1eaae848ef7f6acbd600c87771f91f9291f00b3e`。本报告更新同日 revision 1 的门禁结果。

## 行为

后端或运行时生成不合规消息，或缺少实际输入/回包证据，候选检查必须非零退出并输出 JUnit failure。必须检查实际 Redis entry 的 `stream_key`、`entry_id`、字符串 `fields` 和原始 payload；只检查一份重建 DTO 不够。

- backend/runtime 均强制每个请求的 `wire_file` 和闭合 trace；不再跳过 backend 反馈。
- 实际 task entry 必须与声明请求等价，外层身份一致，信封恰为身份字段与 payload 两项。
- 对已知生产者事件严格校验字段、类型、错误码、指标/进度和终态。消费者的未知错误码降级不能使生产者错误在 CI 中通过；`trace` CLI 默认严格，容错回放需显式 `--consumer`。
- 训练结果必须关联原请求的租户/模型/版本/算法/任务类型/feature_id，存储位置不得超出分配范围；允许合法 Bundle 子目录与共享存储类型归一化。后端投影必须与实际收到的 COMPLETED 完全对应。
- backend/runtime 的 candidate_artifact 缺失时，workflow 直接失败；仅 portable 自检无需组件证据。CI 缺 redis-server 时失败，不能跳过。
- 失败报告保存规则码和字段路径；实际断言样例：`SCHEMA_REQUIRED`，`$.traces[0][5].training_result`。不回显整份请求或凭据。
- Broker 准入同步拒绝重复 JSON key、NaN、Infinity、溢出为非有限值的数字，返回 INVALID_PAYLOAD、ACK，且不提交 Tributo/Ray。

## 实测

| 检查 | 结果 |
| --- | --- |
| Broker 合并回归 | **721 passed**，进程退出 0 |
| 契约与 Broker 对照（包含在合并回归中） | **210 passed** |
| 真实 Redis（包含在合并回归中） | **79 passed** |
| 独立 Wheel 环境，仅 jsonschema/pytest，无 Core/Ray/Redis/KnoVa | **141 passed**，与契约测试重叠 |
| KnoVa 安装最终新 Wheel 的回归 | **132 passed** |
| 最终打包 CLI self-test | **67/67** |
| 实际 workflow 输入检查脚本 | **7/7**，验证缺证据和错误门禁配置确实失败 |
| Ruff / format / Mypy / suppression / diff check / actionlint | 通过 |
| 干净基线重放补丁与离线 Wheel 重建 | 逐字节一致，48 个包内文件与源码相同，vendor/lock 同步 |

真实 Redis 79 条分为：6 条正常生命周期；39 条协议请求反例；16 条传输/原始 JSON 反例；14 条实际 Redis 捕获后启动检查器子进程的 CI 证明；4 条当前 Core 公共结果对象经过实际 Broker 适配器、Reporter 和 Redis 的边界测试。

14 条检查器进程证明包含 **12 种故意制造的违规，均退出 1**，以及正常消息和合法未知可选字段两种正例，均退出 0。违规包括额外信封字段、坏 JSON、身份错位、缺时间戳、缺训练结果、错误模型、错误码拼写错误、重复终态、零起始轮次和不一致的推理行数。预期失败的子进程是反例断言的一部分，整体 pytest 应通过。

4 条 Core 边界测试使用真实 `AlgorithmRunResult` / `InferenceResult` 类型和实际适配/序列化代码；替代模型计算和 I/O。分别验证训练与推理正常映射能通过、映射后故意损坏能被发现。这不计作真实 Ray 集群 ML 验收。

测试早期曾有一次 macOS 原生库在断言结束后退出 134，该次未计作成功；本表列出的最终运行均退出 0。没有用跳过或忽略失败处理该问题。

## 证据

- [合并回归 JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/strict-wire-regression.xml)
- [真实 Redis JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/strict-wire-redis.xml)
- [独立环境 JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/strict-wire-portable.xml)
- [KnoVa JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/strict-wire-knova.xml)
- [真实 Redis 的缺训练结果反例报告](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/redis-fault-injections/feedback_missing_result/ci-result.json)
- [该反例的原始 Redis 反馈](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/redis-fault-injections/feedback_missing_result/training-trace.json)
- [workflow 缺证据检查](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/workflow-input-gate.json)
- [补丁与包核验](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/strict-wire-package.json)

CI 将 JUnit 及实际 Redis 捕获上传到 artifacts，其中 fault-injections 内的失败 JUnit 是刻意反例，顶层 redis-contract.xml 才是整体测试结果。原始本地证据目录被 gitignore。

最终 Broker 0.3.0 Wheel SHA256：`2953d77288c1a1ee09f43d16368ff34a154fb9880a6576fcee05aa07f8839148`。源码补丁独立应用于 `e2973b2a7409fc72807bbec23388d13966c5c319`，已同步 KnoVa vendor、README 与 uv.lock。

## 接入与限制

[协议与 CI 接入说明](../protocol-conformance.md) 已更新 revision 2 的证据格式。组件生产 job 必须上传自己的真实 Redis 捕获；检查器无法证明人工提供的文件确实来自那个组件。后端/Core/Ray 仓库接入后应将对应 job 设为必需检查。

本次未推送代码、修改仓库保护规则或触发 GitHub CI，也未重跑完整 Ray/ClickHouse/MinIO 集群。当前规范覆盖清单中的能力缺口仍存在，完整协议发布门禁继续失败；这里完成的是现有契约覆盖范围内的消息与反馈硬门禁，不能据此宣称整份协议及所有算法执行已完整实现。
