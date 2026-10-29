# External v2 协议契约与 CI 接入

当前结论：**Broker 仅支持协议的一个执行子集，尚未完整兼容。** 本目录的测试为后端、Broker、Tributo Core/Ray 提供共同基准；测试绿色表示该门禁所声明的范围通过，不表示整份协议实现完成。

## 基准与兼容范围

规范源为内部 Tributo `external/ai-model-design-docs/训练推理引擎任务描述协议/protocol-v2-2606`，源提交 `358ffc407439657131be5b71212fc8afa6973a73`。四份 Markdown 的 SHA256 位于包内 `contracts/aimodel_v2/data/spec-lock.json`。契约 revision 为 2，协议版本为 2.0；同主版本未知字段及未知事件按规范容忍，未知错误码由消费者降级处理。生产者已知事件的结构另做严格检查。

这是依据原文整理的结构与跨字段规则子集，并非从 Broker 的 DTO 自动生成。JSON Schema 不代替跨字段、生命周期和执行测试，也尚未穷尽附录全部算法参数条件。`coverage.json` 的 26 项需求逐项标记规范章节、执行状态和测试入口。新增能力时同时补正例、反例、真实执行证据并更新清单，不能仅修改状态。

| 范围 | 当前能力或缺口 |
| --- | --- |
| 消息与事件 | 版本、双字段信封、三处身份对齐、六类已知事件、重复投递、Stream 数值顺序、进度及互斥终态的契约检查 |
| 已验证执行子集 | ClickHouse；XGBoost 二分类、多分类、回归；ONNX/UBJ；NAS/MinIO；原始标签、结果过滤；可选 TreeSHAP 扩展，详见此前验收报告 |
| 数据处理缺口 | 任意 SQL/多表 JOIN、更多数据源、时序三查询透视、拟合型预处理与推理回放、分层/时间/CV 切分 |
| 算法与产物缺口 | 附录列举的其他算法、其他产物格式/处理链；AUTO 回退 MANUAL 不代表已实现搜索 |
| 可靠性缺口 | 崩溃后的持久化准入/终态台账、完整进度节奏保证；现有原子终态互斥不等于可靠投递 |
| 本契约不证明 | 模型数值正确、仓库实际写入/事务、后端持久化和 UI 消费行为，需要对应组件自己的 E2E |

`tributo-broker-contract coverage --require-full` 在当前版本**应以 1 退出**。普通 PR 运行已实现范围的门禁；宣称全协议兼容的发布必须通过完整门禁并补齐上述执行证据。不能通过删掉规范需求使发布变绿。

## 分层门禁

| 门禁 | 检查对象 | 依赖与边界 |
| --- | --- | --- |
| portable | Schema、63 个正反向请求向量、事件序列、产物投影、结果语义、CLI | 不导入 Core/Ray/Redis/KnoVa；输出 JUnit、JSON 和可移植契约包 |
| Broker conformance | 上述同一批独立向量对照真实 Broker parser，以及真实 Reporter 产出 | 需要当前 Core；并非真实执行 |
| Redis integration | 真实 XADD/XREAD/ACK、事件字段、终态互斥、非法请求不提交 Ray | 本地独立 Redis；Ray 与模型执行明确使用替身 |
| backend candidate | 后端实际 Redis 请求信封、全部事件回包、原样产物回传 | 至少两类请求的 wire_file、全部闭合 trace 及关联投影；必须由后端测试导出，不能复制 golden 文件作为实现证据 |
| runtime candidate | 每个实际请求的 Redis 事件、每个推理的结果行，实时查询每个 Ray Job | 要求可达 Ray Dashboard，核对任务状态、Broker 入口及元数据、写入行数；JSON 文件不能替代实际运行和仓库断言 |

`test_runtime_probe...` 等单元测试中的 HTTP 替身只验证检查器，不计作真实 Ray 通过。当前 CI 的 `aimodel-v2` job 也不是完整容器矩阵；已有真实集群测试及报告必须按代码版本另行保留。

## 本地运行与导出

已安装运行环境时使用 `pip install '.[contracts]'`。只运行独立检查器的环境，可以安装 `jsonschema>=4.23,<5` 后用 `pip install --no-deps <broker.whl>` 安装已审计 Wheel；这不构成 Broker 运行环境。

```sh
tributo-broker-contract self-test --junit artifacts/protocol.xml --report artifacts/protocol.json
tributo-broker-contract export --output artifacts/contract-bundle
tributo-broker-contract validate --kind training-request --input actual-training.json
tributo-broker-contract trace --operation inference --input actual-inference-trace.json
tributo-broker-contract projection --completed actual-completed.json --inference actual-inference.json
tributo-broker-contract verify-spec --root /path/to/protocol-v2-2606
```

导出包含 5 份入口 Schema、公共定义、63 个请求变体、训练与推理消息/事件/结果样例、覆盖清单和规范锁。Java/TypeScript 后端可注册全部 `$id`，离线解析 `$ref`，使用 Draft 2020-12 验证器。跨字段语义以 CLI 为共享基准。样例采用不透明特征别名，禁止从别名推断字段身份。

后端还应将 golden 的 COMPLETED/FAILED/CANCELLED、未知事件/错误码、重复投递等喂给**自己的真实事件消费者**，断言状态持久化、终态幂等和原样产物投影。共享检查器不能证明另一端消费者已正确实现。

## 组件实际输出格式

生产者 CI 导出 `candidate/`，路径相对此目录，禁止越界读取。`candidate.json` 最小示例：

```json
{
  "contract_revision": 2,
  "producer": {"component": "backend", "revision": "实际 Git SHA"},
  "requests": [
    {"operation": "training", "file": "training.json", "wire_file": "training-wire.json", "outer_identity": "train-actual-1"},
    {"operation": "inference", "file": "inference.json", "wire_file": "inference-wire.json", "outer_identity": "infer-actual-1"}
  ],
  "projections": [
    {"completed": "completed.json", "inference": "inference.json"}
  ],
  "traces": [
    {"identity": "train-actual-1", "file": "training-trace.json", "submission_id": "actual-ray-train"},
    {"identity": "infer-actual-1", "file": "inference-trace.json", "submission_id": "actual-ray-infer"}
  ],
  "result_sets": [{"identity": "infer-actual-1", "file": "rows.json"}]
}
```

`completed.json` 是实际训练 COMPLETED **payload**，投影必须关联本批请求并保留模型/租户身份。backend 和 runtime 门禁都必须包含所有请求对应的 `wire_file` 和闭合 `traces`；缺少实际请求或回包证据立即失败。backend 不需要 result_sets 或实时 Ray。runtime 还必须包含所有推理对应的仓库查询结果（即使为 `[]`），并提供实时 Ray 地址；Ray 任务记录需在门禁结束前保留。

wire_file 使用与 trace 相同的数组格式，保存实际任务 Stream entry（任务 Stream key 不带任务 ID 后缀）；其中 payload 与 file 中的 JSON 必须完全等价，外层身份必须一致。

Trace 格式记录实际 Stream key、原始 entry ID、字符串字段；payload 保持原始 JSON 字符串，不能压平：

```json
[
  {
    "stream_key": "training:events:train-actual-1",
    "entry_id": "1760000000000-0",
    "fields": {
      "job_id": "train-actual-1",
      "payload": "{\"protocol_version\":\"2.0\",\"job_id\":\"train-actual-1\",\"event_type\":\"STARTED\",\"timestamp\":1760000000000}"
    }
  }
]
```

此片段仅展示信封，缺少终态时闭合 trace 门禁会失败。错误输出只含规则码及字段路径，不回显请求/SQL/凭据。生产者导出的实际证据也应使用隔离测试数据和凭据引用。

```sh
tributo-broker-contract candidate --directory candidate --level backend --report artifacts/backend.json --junit artifacts/backend.xml
tributo-broker-contract candidate --directory candidate --level runtime --ray-url http://isolated-ray:8265 --report artifacts/runtime.json --junit artifacts/runtime.xml
```

## GitHub Actions 接入

Broker 已提供 `.github/workflows/protocol-contracts.yml`（轻量、可复用）和 `ci.yml`（真实 Broker parser/Reporter、Core API、Redis）。以下占位 ref 在提交本次改动后换成**包含本契约的固定提交 SHA**，禁止使用旧版 tag。

纯契约自检的 candidate_level 默认为 portable。明确选择 backend/runtime 时，candidate_artifact 必填；没有证据不能退回 portable 变绿。

后端生产 job 先运行自身序列化/消费者测试，并用 `actions/upload-artifact@v4` 上传实际输出目录，名称 `backend-protocol-evidence`。随后：

```yaml
jobs:
  # backend-tests: 自己的构建、DTO/事件消费测试、导出并上传实际证据
  protocol:
    needs: backend-tests
    uses: Chang-Tong/tributo-broker-redis/.github/workflows/protocol-contracts.yml@BROKER_CONTRACT_SHA
    with:
      broker_repository: Chang-Tong/tributo-broker-redis
      broker_ref: BROKER_CONTRACT_SHA
      candidate_artifact: backend-protocol-evidence
      candidate_level: backend
```

跨仓复用时必须同时指定 Broker repository/ref；否则 checkout 默认指向调用仓库。构建、上传和复用需处于**同一次 workflow run**。私有仓库需允许调用方复用和读取，本地代码尚未替仓库配置权限。

Tributo Core PR 用自己的提交验证 Provider：

```yaml
jobs:
  broker-compatibility:
    uses: Chang-Tong/tributo-broker-redis/.github/workflows/ci.yml@BROKER_CONTRACT_SHA
    with:
      broker_repository: Chang-Tong/tributo-broker-redis
      broker_ref: BROKER_CONTRACT_SHA
      core_repository: ${{ github.repository }}
      core_ref: ${{ github.event.pull_request.head.sha || github.sha }}
```

Ray 集群 CI 应先部署隔离的 Redis/ClickHouse/对象存储/Ray，使用候选 Core/Ray 版本运行训练→产物→推理并断言真实仓库行、数值、控制与故障场景，再上传同格式 evidence。复用 `protocol-contracts.yml` 时设置 `candidate_level: runtime`、`ray_dashboard_url` 及能访问该集群的 `runner_labels_json`（JSON 字符串或数组）。两个 job 的网络可达性和集群存活由调用方负责。只调用 portable workflow 不能称为 Ray 执行兼容。

可选 `spec_repository/spec_ref/spec_directory` 会校验四份原始规范哈希；spec_ref 必须明确给出。一般 PR 固定规范快照，规范升级 PR 才更新快照、用例和覆盖声明，避免远端文档静默变化。

## 接入顺序

1. 本次：提交 Broker 契约、准入修复、portable/Redis 门禁及固定规范快照。
2. 后端接入：真实 DTO 输出、事件消费幂等、产物原样投影；将 reusable workflow 设为必需检查。
3. Core 接入：候选 Core SHA 调用 Provider CI，失效 API 直接删除，不增加旧 API 兼容壳。
4. Ray 接入：隔离集群流水线产出真实证据；先覆盖已支持执行子集，再按 coverage 清单逐项扩展。更换 Ray/Core/算法 Wheel 后重跑该矩阵。
5. 补齐缺口后：附录条件、数值与可靠性 E2E 全部通过，才开放 full-protocol 发布门禁。

当前没有修改后端仓库、Core 仓库的 workflow，也没有推送或在 GitHub 上执行工作流；这里交付的是可提交、可复用的门禁入口与接入约定。

## CI 必须拦截的消息偏差（契约 revision 2）

候选门禁检查实际 Redis task entry，而不只检查重建 DTO；检查两端所有已知事件，而不把消费者对未知错误码的降级行为当作生产者合格。`trace` CLI 默认是严格生产者检查，消费者容错回放需显式 `--consumer`。合法的未知可选字段与未来事件仍按规范容忍。

backend/runtime 还会检查 COMPLETED 与原请求的租户、模型、版本、算法、任务类型、feature_id 是否一致，并校验产物位置位于请求分配的存储范围内（允许合法子目录及共享存储别名归一化），投影的 COMPLETED 必须真实出现在反馈序列中。缺少 trace、缺少终态、先终态再写消息、缺字段、错误类型、坏 JSON、重复 key、非有限数值、信封压平等均导致非零退出。失败报告输出 `code`、`path`，并生成 JUnit failure；不回显整个 payload。

Provider CI 强制真实 redis-server，缺依赖会失败。`test_aimodel_v2_redis.py` 包括真实 Redis 错误注入并启动 CLI 子进程验证退出码，以及当前 Core `AlgorithmRunResult` / `InferenceResult` 经实际 Broker 适配与 Reporter 序列化后的检查。它们替代计算/模型运行，验证的是接口与序列化边界，不计作真实集群 ML 验收。

接入实际流水线时，务必从 Redis 读取原始 entry（保留 entry_id、fields 和 payload 字符串），从仓库查询实际结果行。不要对捕获结果“修好字段”后再验证；缺证据直接失败。候选 artifact 的所有权及其生成真实性由生产该 artifact 的 CI job 保证。下游需将这个 job 设置为必需检查，才会阻止合并。

## Provider CI 的开发验收要求

GitHub runner 使用 Ubuntu / Python 3.12，安装固定 Core SHA、固定算法 Wheel 和 Broker 对应 extras；本地已有环境不能替代这次全新安装。开发工具版本在 pyproject.toml 固定为 Ruff 0.17.3、Mypy 2.4.1。`conftest` 显式归为本项目导入，避免不同 Ruff 版本的自动识别造成排序差异。

unit job 依次要求 Ruff format、Ruff lint、Mypy、禁止源码忽略注释、generic 单元/transport 测试、Wheel 入口发现和 Wheel 构建全部通过。前一步失败会使后续步骤跳过，跳过不能算作通过。

aimodel-v2 job 另要求完整 provider 类型检查、V2/契约回归和强制真实 Redis 门禁通过；Protocol contracts workflow 要求无 Core/Ray/Redis 环境中的独立检查器通过并导出证据。远端通过后再引用具体 run，不把本地静态检查描述为远端 CI 已成功。
