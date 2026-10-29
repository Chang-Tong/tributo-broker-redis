# 当前 Core 兼容迁移：Broker / KnoVa 0.3 验收

日期：2026-10-27。基线为 `/Users/dongair/project/05-pr/tributo` 当前工作分支及远端 `origin/codex/clickhouse-write-binding` 相同提交 `1eaae848ef7f6acbd600c87771f91f9291f00b3e`；已 fetch 核实，没有用旧 Core 回避兼容问题。

## 结论

**此前 generic v1 的 8 项旧 trainer 接口不兼容已消除；V1/V2 统一以当前 Core 为基线，兼容迁移可以验收。旧训练配置与旧结果接口已丢弃。**

这份结论替代前一份 0.2 报告中的“当前 Core generic v1 阻断”。完整内部能力对等仍按原迁移计划推进，本次没有实现计划之外的 SQL、预处理、时序等功能。

## 变更

- 删除对 `tributo.training.xgboost_trainer`、`XGBoostTrainingConfig` 和 `run_training_result_with_config` 的依赖；不添加旧模块或回退分支。
- generic 训练改用公共 `IngestionRequest`、`WorkerResources` 和官方 Boosting 执行入口，消费公共 `AlgorithmRunResult`。
- `spec.config` 明确包含 ingestion、feature_names、label_name、worker_count、model、training、resources、storage_path、bundle_uri。单 Worker 为 1，分布式至少 2。
- 旧 config.data / config.ray / config.output、val_size / test_size 训练配置在 admission 返回 INVALID_TRAINING_REQUEST；新格式示例见 [training.current.example.json](../training.current.example.json)。
- 成功结果使用 result.execution.status、metrics/outputs、run_id/plan_id、Worker 证据和 Bundle 回执；不调用已失效的旧结果 model_dump 接口。
- Broker/KnoVa 升为 0.3.0，体现训练配置变更。CI 两个 job、KnoVa pyproject/uv.lock、vendor Core Wheel 都指向当前 Core；connector 升为未修改的 ray-clickhouse 1.0。
- generic 训练通过 `xgboost` extra 显式安装算法执行依赖。CI 使用已验证的 Boosting 补丁 Wheel，避免依赖未验证的同版本代码。
- 离线升级镜像替换 Core、Broker、connector、Boosting、KnoVa 五个包，先清理旧包及 metadata，再核对安装目录和 Ray 分发目录；不继续继承旧 Core。

## 实际验收

| 项目 | 结果 |
| --- | --- |
| 当前 Core：generic、V2、transport、真实 Redis、Wheel 合并回归 | **438 通过，0 失败/跳过** |
| 其中 generic 单测及 transport | 107 项，包含旧格式拒绝与公共结果序列化 |
| 其中 V2 | 322 项；另有 6 项真实 Redis 与 3 项 Wheel 测试 |
| KnoVa 当前 Core 回归 | **132 通过** |
| generic 真实 Redis → Ray | **3/3 通过**：单 Worker/双 Worker 训练、Bundle 推理、训练/推理失败、排队/运行中取消 |
| V2 真实 Redis → Ray → ClickHouse/NAS/S3 兼容服务 | **9/9 通过**：二分类/多分类/回归的训练、推理、精确 SHAP |
| 静态检查 | Ruff、Mypy（29 个文件）、禁止类型抑制及 diff 检查通过 |
| 离线升级镜像 | `--network=none` 构建成功；Core 与三类扩展的 Consumer/Ray 源码校验通过 |
| Wheel / source patch / lock | 23 个 Python 文件一致；从 e2973b2 干净归档重放 v0.3 补丁，离线构建字节一致；锁文件摘要一致 |

双 Worker 的真实回执包含两个不同 Ray 节点、相同模型状态摘要、合计 8 行训练数据；单 Worker 为一个节点、8 行数据。generic 的真实测试使用当前 Core 与算法包，没有加载旧 Core 内置 XGBoost，也没有旧 trainer 插件导入警告作为回退。

V2 的 9 个 profile Python 文件与上一轮 0.2 完整验收版本逐字节一致。本轮执行上述 9 条集成回归；上一轮 54 条业务、11 条故障恢复、8 条 Sentinel 的完整记录保留为历史证据，没有把这些数字描述为本轮重新执行。

最终 Broker Wheel SHA-256：`6ecd12a5e4db500bfb5775c900039ccb4249693fed82e512ee67cc6092825c5e`。

最终离线升级镜像：`sha256:b582a0d528b24ed498afc405e7d506ac46f2ea4bea6a74c4f816f658dc0b5f8d`，本地标签 `tributo-knova-current-core:20261027`。镜像内 Core 版本为 1.0.0（以提交区分源码），KnoVa/Broker 为 0.3.0，ray-clickhouse 为 1.0；旧 trainer 模块不存在。镜像已构建验证，尚未发布或部署到原有服务。

## 证据与使用

- [合并回归 JUnit](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/current-core-20261027/regression.xml)
- [generic 真实测试](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/current-core-20261027/generic/pytest.log) 与 [Worker 证据](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/current-core-20261027/generic/worker-audit.json)
- [V2 九条真实结果](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/current-core-20261027/v2/matrix-results.json)
- [Wheel 与锁文件校验](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/current-core-20261027/package-verification.json)
- [离线镜像构建记录](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/current-core-20261027/knova-hotfix-build.log)

Broker 源码、示例和 CI 使用同一当前 Core 基线；KnoVa 的 `uv sync --frozen` / Docker 构建使用已更新的锁与 Wheels。已有 generic 任务生产者需按照新示例更新配置，再切换到 0.3；旧配置不会被静默翻译。验收使用一次性容器，原有 devbox 服务不做部署变更。

测试项目已清理，原有 26 个容器 ID/运行状态未变。
