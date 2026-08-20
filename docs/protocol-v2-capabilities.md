# Training protocol v2 capability matrix

The Provider validates the complete canonical request before submitting a Ray
Job. A syntactically valid but unsupported semantic is reported as
`FAILED(error_code=INVALID_PAYLOAD)` with the canonical field path; it is never
silently dropped or downgraded.

| Protocol area | Executed now | Rejected before Ray submission |
| --- | --- | --- |
| Algorithm | XGBoost with validated objectives and a strict, typed hyperparameter allowlist | Other algorithms, deep-learning mode, unknown parameters, conflicting aliases or task-incompatible objectives/metrics |
| Task | Binary classification, multiclass classification, regression | Clustering, time-series forecasting |
| Data source | S3, LOCAL, ClickHouse, HiveServer2 with `NONE`/`NOSASL` auth | Hive LDAP/CUSTOM/Kerberos auth, DORIS, JDBC, other source types |
| Query | Direct query / file location | Table topology, relations, time-series pivot and component queries |
| Features | Numeric/boolean regular passthrough columns; all four feature-engineering controls explicitly `NONE`/`PASSTHROUGH` | Omitted/AUTO feature engineering, string/category features, temporal roles, pivot/origin execution, label remapping and non-passthrough treatment |
| Split | RANDOM, TIME_ORDERED with `data_split.order_column`, classification stratify | Other strategies, TIME_ORDERED without an order column, cross-validation |
| Sampling | No sampling; CUSTOM_AMOUNT count-warning facts | Ratio/limit sampling, under-sampling, SMOTE |
| Tuning | MANUAL | AUTO and auto-config search semantics |
| Evaluation | Task-correct scalar metrics, enable/disable, ROC, threshold analysis, confusion matrix, feature importance | Unknown/task-incompatible metrics and correlation matrix |
| Artifact | Strict ONNX export to S3 or local storage, metrics summary | Optional/partial-success model export |
| Broker controls | Redis cancellation and rank-0 real-time phase/metrics bridge | Inline Redis credentials in execution context |

Canonical requests require non-empty `model_id`, `version_id`, `tenant_id`,
and a `feature_id` for every feature. `resource_limits.max_epochs` is an upper
bound: explicit rounds must not exceed it, while omitted rounds default to
`min(100, max_epochs)`. Conflicting `num_rounds`/`n_estimators` aliases are
rejected rather than resolved by precedence.

Evaluation requests use wire names (`f1`, `precision`, `recall`,
`average_precision`); Provider completion translates Core's internal macro
metric names back to that vocabulary. Opaque `extensions` are accepted as
Driver metadata but are stripped before constructing Ray worker environment
JSON and are never executed.

Datasource `properties` use a per-type allowlist. Inline datasource passwords,
S3 access keys, URI userinfo, connection strings and unresolved
`credential_ref` values are rejected because this release has no credential
resolver that could keep them out of Ray environment JSON.
For Hive, `datasource.properties.auth` defaults to `NONE`; `NONE` and
`NOSASL` are executed and case-normalized. LDAP/CUSTOM require credentials and
are rejected until a secret-reference resolver exists. Kerberos is not
advertised or silently downgraded.

## Legacy `training_config`

`training_config` is rejected by default. It is accepted only when the
Provider configuration explicitly sets `allow_legacy_training_config: true`.
Even then, only Core sections (`data`, `model`, `training`, `ray`, `output`,
`evaluation`) are accepted. Identity, broker/runtime controls, environment
variables and inline secret fields remain forbidden.

Canonical v2 should be used for all new integrations.
