# Broker 协议契约与 CI 基线验收（2026-10-29）

结论：**当前不能宣称完整兼容 external v2 协议。** 已交付可复用的协议契约、独立检查器、Broker 对照测试、真实 Redis 门禁以及后端/Core/Ray 的 CI 接入入口。完整能力缺口保留为显式失败项。

## 本次交付

- 规范锁：内部 Tributo `358ffc407439657131be5b71212fc8afa6973a73` 下 protocol-v2-2606 的四份文件及 SHA256；Core 基线仍为 `1eaae848ef7f6acbd600c87771f91f9291f00b3e`。
- 可导出的 Draft 2020-12 Schema、独立的 63 个请求正反例、消息/事件/结果样例、26 项需求的状态与规范章节。契约为人工整理的规则子集，尚未穷尽规范与算法附录。
- 消息信封、Stream 身份和顺序、已知事件、未知事件/错误码消费、重复投递、终态互斥、指标首轮和进度、完整产物投影、结果列/标签/过滤的语义检查。
- `tributo-broker-contract` 支持 export / self-test / validate / trace / projection / candidate / verify-spec / coverage。安装独立检查器不需要导入或安装 Core/Ray/Redis/KnoVa。
- `.github/workflows/protocol-contracts.yml` 是轻量可复用门禁；Provider `ci.yml` 支持调用方传入候选 Core SHA。后端实际请求证据与真实 Ray 证据分级检查，缺少实时 Ray 地址不能通过 runtime 门禁。
- 修复 Broker 本身的协议偏差：允许 `params: null`；拒绝未绑定的 SQL 参数；校验产物清单、相对路径/哈希/受控格式；准入时校验结果过滤标签；训练/推理执行端同步处理 null params。没有恢复旧 Core API。

## 实测结果

| 检查 | 结果 | 证据范围 |
| --- | --- | --- |
| Broker 合并回归 | **628 passed**，0 skipped | V2 322；契约 188；generic 单元与 transport 107；Redis 8；Wheel 3 |
| 独立安装契约测试 | **119 passed** | Python 3.12 的新环境，仅装 Wheel/jsonschema/pytest，确认 Core/Ray/Redis/KnoVa 均不存在；此数与 628 中的契约测试重叠 |
| 打包后 CLI self-test | **67/67** | 63 请求向量 + 2 trace + 投影 + 结果；明确输出 full_protocol_compatible=false |
| KnoVa 回归 | **132 passed** | 安装包含准入修复和契约代码的新 Broker Wheel；KnoVa 保持调用 Broker 的薄层 |
| 历史真实集群载荷复核 | **63 份记录，429 条事件，无 Schema 违规** | 2026-10-27 的 54 案例与当前 Core 9 案例；仅历史 payload 结构复核，无原始 Stream ID/实时任务验证，不计作本轮重跑 Ray |
| Ruff / format / suppression | 通过 | src、tests、scripts |
| Mypy | 通过 | generic 25 个、完整 provider 34 个源文件；使用 follow-untyped-imports 检查无 py.typed 的算法依赖，无新增源码忽略注释 |
| Actions 静态检查 | 通过 | actionlint 1.7.7 检查两个 workflow；未在 GitHub 执行 |
| 完整协议发布门禁 | **预期拒绝（退出 1）** | coverage --require-full；没有修改成虚假的完整兼容 |
| 缺少实时 Ray 的 runtime 证据 | **预期拒绝（退出 1）** | 即使有 golden JSON，也不能自动通过真实运行门禁 |
| Wheel 与补丁 | 通过 | 从干净基线应用补丁、离线构建后逐字节一致；48 个包内文件与源码一致 |

Broker 合并回归命令：

```sh
PYTHONPATH=src:../tributo/src ../tributo-knova/.venv/bin/python -m pytest \
  tests/contracts tests/aimodel_v2 tests/unit tests/test_driver_transport.py \
  tests/integration/test_aimodel_v2_redis.py \
  tests/integration/test_broker_redis_wheel.py -q
```

Redis 八条使用隔离的真实 redis-server；六条验证成功/失败/取消的完整信封及互斥，另两条验证不兼容主版本在提交 Ray 前失败、ACK 并发出结构化失败。Ray 和模型执行在这八条里使用替身；不能把它们计入真实 Ray 执行验收。

本轮没有重新运行完整 Ray/ClickHouse/MinIO 容器矩阵，没有部署或替换旧镜像。模型数值、实际仓库写入和故障可靠性继续需要集群 E2E；candidate 检查的是调用方提供的事实及实时 Job 身份/状态，不替代这些断言。

## 包与原始证据

最终 Broker 0.3.0 Wheel SHA256：`366e2ca07e374b3dd6514972bbf7d8ad722c0a1ccbbce2ece497465363b37d68`。KnoVa vendor Wheel、README、完整源码补丁及 uv.lock 已同步；`uv lock --check --offline` 通过。源码补丁单独应用在 `e2973b2a7409fc72807bbec23388d13966c5c319`，不叠加旧补丁。最后的打包调整仅提高开发依赖 mypy 的版本下限；132 项 KnoVa 回归覆盖的运行源码与最终包一致。

- [Broker JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/pytest.xml)
- [独立安装 JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/portable.xml)
- [KnoVa JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/knova.xml)
- [打包后自检 JSON](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/self-test.json)
- [历史载荷复核 JSON](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/historical-payload-audit.json)
- [补丁与 Wheel 核验](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/protocol-20261029/package-verification.json)

原始证据目录被 gitignore；可提交的规范锁、测试、覆盖声明和本报告均位于正式源码目录。

## 后续必须补齐

任意 SQL/多表 JOIN、更多数据源、拟合预处理回放、时序透视、分层/时间/CV 切分、更多附录算法/产物链，以及持久化准入与终态可靠投递仍未完整实现。TreeSHAP 为可选扩展，不能填补基本协议缺口。

后端先导出自己实际生成的消息并验证自己的消费者；Core 使用候选 SHA 调用 Provider CI；Ray 集群流水线再提供真实请求、Redis entry、结果行与实时 Job。接入格式及 workflow 示例见 [协议与 CI 接入说明](../protocol-conformance.md)。尚未修改其他组件仓库的 workflow、推送代码、配置必需检查或触发 GitHub CI。
