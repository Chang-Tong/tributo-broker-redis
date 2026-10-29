# Broker 开发与交付要求

适用范围：本仓库所有代码、测试、文档和 CI 修改。

## 必须通过的 CI 检查

修改后必须完成与变更相关的检查。交付或推送代码前，以下七项必须通过：

1. **格式**：`ruff format --check src tests scripts`。
2. **Lint**：`ruff check src tests scripts`。
3. **类型检查**：按 `.github/workflows/ci.yml` 的依赖组合执行 Mypy；涉及 V2 时还要通过完整 provider 类型检查。
4. **禁止忽略注释**：运行 `./scripts/check_suppressions.sh`。禁止用 `# type: ignore`、`# mypy: ignore`、`# pyright: ignore` 或 `# noqa` 绕过失败；不要通过删除检查、删断言或放宽规则使 CI 变绿。
5. **单元测试**：`python -m pytest -q tests/unit tests/test_driver_transport.py`。修改 V2 或契约代码时，还必须运行 `tests/aimodel_v2`、`tests/contracts` 和相关 Redis 集成测试。
6. **Wheel 入口发现**：`python -m pytest -q tests/integration/test_broker_redis_wheel.py -m integration`。
7. **Wheel 构建**：CI 执行 `python -m pip wheel --no-deps --wheel-dir dist .`；本地可用等价的 `uv build --wheel` 验证同一 pyproject.toml 构建。

前置步骤失败导致后续检查被跳过时，后续检查不能算作通过。命令输出测试断言通过但进程退出非零，也不能算作通过。发现 CI 失败，应阅读该提交的实际失败日志，修复问题后验证新的运行结果。

## 环境与工具一致性

- 远端 Provider CI 使用 Ubuntu / Python 3.12。依赖安装、Core 提交、算法 Wheel 来源及 job 划分，以 `.github/workflows/ci.yml` 为准。
- 开发工具版本由 `pyproject.toml` 的 dev extra 固定：Ruff `0.17.3`、Mypy `2.4.1`。执行前核对实际版本，不能只依赖本机已有工具。
- `conftest` 和 `tributo_broker_redis` 显式作为本项目导入，遵守 pyproject.toml 的 isort 配置；不要依赖不同环境的自动识别。
- Generic 类型检查：`mypy src tests/unit --exclude 'src/tributo_broker_redis/profiles/' --follow-imports=silent --follow-untyped-imports`。
- V2 完整类型检查：`mypy src tests/unit --follow-imports=silent --follow-untyped-imports`。
- 本地检查通过不等于 GitHub CI 已通过。推送后应核对对应提交的远端结果；报告必须区分本地结果、远端通过、失败、运行中和跳过。

## 协议与真实输出的附加门禁

- `.github/workflows/protocol-contracts.yml` 的独立契约检查也必须通过。规范快照、覆盖范围和接入格式见 `docs/protocol-conformance.md`。
- Redis 测试校验实际 entry 的信封、原始 JSON、身份与反馈；CI 设置 `TRIBUTO_CONTRACT_REQUIRE_REDIS=1`，缺少 Redis 依赖必须失败，不能跳过。
- backend/runtime 候选门禁必须提供实际请求 wire_file、闭合反馈 trace 和关联投影；runtime 还需要实际结果行与实时 Ray Job 核对。缺少证据不能退回 portable 自检报成功。
- 不合规消息必须使检查进程非零退出，保存规则码、字段路径和 JUnit。反例测试应验证这个失败行为，同时保留协议允许的未知可选字段和消费者兼容行为。
- 模拟执行、历史消息回放和 Schema 检查不能替代真实 Ray/仓库 E2E。未实现的协议能力必须留在覆盖清单中，不能宣称完整兼容。
