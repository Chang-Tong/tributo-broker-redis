# external v2 协议迁入 Broker：完整测试与验收结论

> 这是 0.2 阶段的历史验收报告。后续 0.3 已迁移到当前 Core 并解决下文记录的 8 项 generic 不兼容；当前结论见 [当前 Core 兼容验收](2026-10-27-current-core-compatibility.md)。原始测试结果保留。

测试日期：2026-10-27（Asia/Shanghai）。范围是本次协议所有权迁移、KnoVa 薄包装与相关回归，不代表完整能力对等计划已经完成。

## 结论

**本次“Broker 承接 v2 实现、KnoVa 保留部署与兼容入口”的步骤可以验收。当前 Core 与 generic v1 仍有 8 项不兼容，不能据此宣布全部配置可直接发布。**

在没有安装 `tributo_knova` 包的 Linux Ray 镜像中，当前 Core 的 v2 业务矩阵最终 **54/54** 通过；故障与恢复 **11/11** 通过；真实 Sentinel **8/8** 通过。最终 Broker Wheel 也通过了 KnoVa 原锁定依赖组合的训练、普通推理、精确 SHAP 三条烟测。

完整开源能力仍需按迁移计划继续完成参数化多表查询、预处理、时序、多源及其他算法路由等工作。本报告没有把这些未完成能力算作通过。

## 测试结果

不同 Core 组合的测试有重叠，以下数字不能简单相加。

| 测试 | 实际结果 | 说明 |
| --- | --- | --- |
| V2 回归，当前 Core 源码 | 322 通过，0 失败/跳过 | 协议、映射、Driver、事件、原生预测/SHAP、边界与恢复 |
| 最终 Wheel，KnoVa 锁定 Core | 331 通过，0 失败/跳过 | 322 回归 + 6 真实 Redis/替代 Ray 测试 + 3 Wheel 发现/隔离测试 |
| KnoVa | 132 通过，0 失败/跳过 | 兼容入口、CLI、部署及现有算法产物元数据 |
| 当前 Core 相关公共契约 | 130 通过 | Broker/CLI、读写 Binding、Inference contracts |
| generic v1 原 Core 基线 | 98 通过 | `tests/unit` 与 Driver transport |
| generic v1 当前 Core | **90 通过、8 失败** | 旧 `tributo.training.xgboost_trainer` 已移除 |
| V2 真实业务矩阵 | **54/54 最终通过** | 完整执行首轮后修复问题，定向重跑失败项；保留全部首轮证据 |
| V2 真实故障/恢复 | **11/11 最终通过** | 含真实 Consumer SIGKILL、重投递、取消、运行中超时 |
| Sentinel | **8/8 通过** | 真实 Redis 主从/Sentinel，含主节点切换 |
| generic v1 真实 Ray | 3 个测试函数最终通过 | 首轮 2 通过，排队取消断言修订后定向复测 1 通过；覆盖四条成功链路、两条失败链路及两种取消 |
| KnoVa 原依赖组合真实 Ray | **3/3 通过** | 最终 Broker Wheel，经 KnoVa 薄包装入口：训练、推理、精确 SHAP |
| 静态检查 | 通过 | Broker/KnoVa Ruff；Broker Mypy；禁止类型抑制检查 |
| 打包 | 通过 | 22 个 Python 文件与 Wheel 一致，补丁重放字节一致，lock/hash 一致 |

## 明确验证过的依赖组合

| 用途 | Core | ClickHouse connector | 入口 |
| --- | --- | --- | --- |
| V2 主矩阵/故障测试 | 开源 Core `1eaae84` | PyPI `ray-clickhouse==1.0` | `tributo-redis`，`aimodel-v2/2.0`，不安装 KnoVa |
| KnoVa 兼容组合 | Core `440da0422a3a9c5b041b6ecdbb2e40c00bf43cd90` | 现有本地修改版 `ray-clickhouse==0.1.0` Wheel | 精简后的 `tributo-knova==0.2.0` → Broker |
| generic v1 兼容基线 | Core `8a3bb4dfecd7fd6574d764b45d5f28c1ca73ed66` | 内置 CSV/Parquet 路径 | Broker generic v1，内置 XGBoost/ONNX |

V2 算法使用现有 `tributo-algorithms-boosting==0.1.0` 补丁 Wheel，SHA-256 为 `c8bbed6fd0c9f828b37de498113f578f93c4ccb2ba5811ff80ee8f4424d4e922`。这些 Core 的发行版本都可能显示为 1.0.0，必须同时记录提交和 Wheel 摘要。

主机为 macOS arm64；真实执行使用 Linux arm64、Python 3.13、Ray 2.55.1、ClickHouse 26.3.17、Redis 8、RustFS S3 兼容服务。Ray head 为 6 GiB、0 CPU，两台 worker 各 4 GiB、2 CPU；主矩阵不占用现有服务端口。

主矩阵最终修复所用镜像：`sha256:d98761db91b57701d897c2918c0e8e9cabe9ca3e279e038fd9101f50f1ba0fed`。KnoVa 兼容烟测镜像：`sha256:7d4c44a51d6fd400331a2b3f2c1a25a616f7a0b441c7c6382680afcfecbb11e1`。

## 54 项业务矩阵的验收内容

- 二分类、多分类、回归；24/1000/300000 行训练，每行 116 个特征。
- 0/7/1000/100000 行推理；输入为零行时失败，结果被过滤为零行时成功。
- NAS/NFS 与 RustFS/S3 产物；逐文件核对大小、SHA-256、manifest 与 Bundle 的投影。
- 训练/验证/测试行数、评估指标、混淆矩阵、标签与特征顺序。
- ONNX 预测逐行对照实际源表 JOIN 后的原生 XGBoost 预测；不是只检查事件或抽样几行。
- 精确/近似 SHAP、正类 0/1、字符串标签、阈值 0/1、INCLUDE/EXCLUDE，以及全部结果被排除。
- 空输入、过滤后为空、单行不足以划分训练集、回归非法结果过滤及相关错误码。

累计训练 **906,072 行**；逐行对照预测 **319,477 行**；逐行对照 SHAP **13,063 行**。SHAP 与原生选定类别 margin 的最大加总误差为 **2.2526364773511887e-07**。

54 个任务均恰好一个终态；41 个预期成功对应 Ray SUCCEEDED，13 个预期业务失败对应 Ray FAILED，没有状态不一致。对事件和 Ray 日志扫描四个一次性测试凭据的明文字面量，命中数为 0；这项检查的范围是本次 fixtures 的凭据。

## 11 项故障与恢复

1. 错误 JSON 拒绝，未启动 Ray 作业。
2. 不支持的协议主版本拒绝。
3. 缺失版本拒绝。
4. 排队取消优先于错误载荷解析，按 cancel key 的 EXISTS 判断。
5. Consumer 重启后回收 PEL 并 ACK。
6. 2.9 请求与未知字段容忍、大于单环境变量常见上限的 Driver 输入传输、完成后迟到取消不覆盖终态。
7. 训练产出 METRICS 后 SIGKILL 本次 Consumer；新 Consumer 恢复原作业，重复投递不重提，取消停止该作业。
8. 训练已产出 METRICS 后触发 60 秒预算，得到 TRAINING_TIMEOUT，Ray 作业停止。
9. 推理 STARTED 后取消，得到 CANCELLED，Ray 作业停止。
10. 模型哈希损坏得到 MODEL_ARTIFACT_CORRUPTED。
11. 目标结果表不存在得到 RESULT_WRITE_FAILED。

每个提交的控制任务也核对最终 Ray 状态与业务终态。超时 fixture 首次因默认 `max_epochs=1000` 提前完成，后来明确提高该请求的 max_epochs 才验证真实运行中超时；首次失败记录保留。

## 测试中发现并修复的问题

| 问题 | 修复及验证 |
| --- | --- |
| 旧 ClickHouse 读 Binding ID 不适用于当前 Core | 使用公共 DataGateway 的唯一匹配路由；分别验证当前 Core 和旧锁定组合 |
| `xgboost_reg` 未开启 SHAP，回归缺失解释字段 | 纳入原生 XGBoost 回归分支；NAS/S3 回归 SHAP 真实逐行对照通过 |
| 全部结果被过滤时 Ray 丢失空数据集 schema，导致写入失败 | 对明确配置的过滤后零行返回零行回执，保留源输入为空的 EMPTY_INPUT 行为 |
| 空训练集返回 `schema=None`，错误码和安全摘要在训练/Driver 间丢失 | 显式空集异常与受信错误类型投影；三种任务均输出 EMPTY_DATASET，并增加训练层和 Driver 层断言 |

还修正了验收 fixture 的完整 feature 定义、回归算法标识和错误码预期；Ray 完成状态等待由 30 秒改为 120 秒，仍要求最终状态一致；generic 排队取消使用外层 ID 的断言与解析前取消规则对齐。没有以接受错误状态代替修复。

macOS 的 ONNX Runtime 1.28 曾在断言全部通过后，由遥测 HTTP 工作线程触发 SIGABRT（退出 134）。崩溃栈已记录；回归 fixture 调用公开 API 关闭遥测后，322 项测试与进程退出均正常。模型推理和数值断言保留，Linux 真执行另行验收。

## 尚未通过的发布门槛

**generic v1 + 当前 Core 的 8 项失败仍存在。** 原 Core 基线的成功不能替代当前 Core 的兼容性证明。失败均可在 `operations.py` 的旧 trainer 导入触发，generic Driver 也仍使用旧训练接口。

下一步先将 generic 映射及 Driver 迁到现有公共训练接口，要求同一个当前 Core 同时通过 V1/V2 的单测和真实训练/推理，再发布锁定依赖组合。

本次 generic 测试复用了缓存 V2 镜像，其中遗留的 Boosting validator 在旧 Core 基线下存在可选插件导入日志；Core 跳过该插件，成功路径使用内置 XGBoost/ONNX。此结果覆盖这些具体功能，不能作为全插件兼容证明。

此外，标准 `tributo-runtime-full` 镜像不存在，因此没有把 `scripts/run_it.sh` 的标准镜像 attestation 门槛算作通过。本次使用明确记录了 Dockerfile、镜像 ID、依赖 Wheel 的功能验收镜像。未验证 amd64、GPU/NCCL、72 小时稳定性、ClickHouse 23.3、真实 AWS 服务，也未实施生产部署或发布。

## 可复现交付与证据

最终 Broker Wheel：`tributo_broker_redis-0.2.0-py3-none-any.whl`。

SHA-256：`4ffdcda8f587572baae073cd91f8df1f623b62ebb3e17e4d6229f6d6366be3e2`。

- 22 个包内 Python 文件逐字节匹配源码。
- 从 Broker `e2973b2a7409fc72807bbec23388d13966c5c319` 干净归档应用 `vendor/patches/broker-v0.2.0.patch`，离线重建 Wheel 字节一致。
- KnoVa vendor Wheel、uv.lock 和摘要表一致。
- 主矩阵 `ray-clickhouse==1.0` Wheel SHA-256：`5ac258dd425ea9fa879b3429c57755f7948c1541bda5fc6f57f2c9605e3c63cb`。
- 旧连接器和算法 Wheel 的既有本地补丁/溯源限制仍以 KnoVa `vendor/wheels/README.md` 为准。

证据目录：[完整本地验收目录](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027)。

- [汇总 JSON](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/final-summary.json)
- [54 项结果](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/latest-fixed/matrix-results.json) 与 [终态/数值审计](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/latest-fixed/matrix-audit.json)
- [故障/恢复结果](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/latest-fixed/controls/results.json)
- [KnoVa 兼容烟测](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/pinned-final/matrix-results.json)
- [generic 当前 Core 失败日志](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/generic-current-unit.log)
- [最终 Wheel 与补丁重放](/Users/dongair/project/05-pr/tributo-broker-redis/tests/integration/.runtime/full-20261027/package-verification.json)

矩阵首轮为 46/54，通过修复后定向复测收敛到 54/54；中间失败记录位于 `before-final-failures`、`before-empty-training-fix`、`before-empty-driver-fix`，未改写成首轮全通过。generic 也保留首轮 XML/日志与取消定向复测结果。

隔离资源使用项目名 `tributo-v2-full-9c117177` 与 `tributo-generic-full-9c117177`；测试结束清理这两个项目，原有 devbox 服务的 ID/运行状态另行核对并写入 cleanup-verification.json。验收产物与镜像保留供复现。
