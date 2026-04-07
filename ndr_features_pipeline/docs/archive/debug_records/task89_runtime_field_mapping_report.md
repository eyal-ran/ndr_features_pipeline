# Task 8.9 — IF training runtime field mapping report

## Scope
- Issue addressed: **1J Required-but-unused runtime fields** for IF training.
- In-scope runtime contract: `pipeline_if_training` required runtime parameters and `IFTrainingRuntimeConfig` consumption path.

## Required runtime field inventory and usage mapping

| Runtime field | Ingress surface | Functional use site(s) | Status |
|---|---|---|---|
| `ProjectName` | Training SF → SageMaker pipeline param → `run_if_training.py` | DDB job-spec lookup key, Batch Index readiness selectors, downstream orchestration payloads, report metadata | Used |
| `FeatureSpecVersion` | Same | DDB job-spec/version lookup, readiness manifests, downstream orchestration payloads | Used |
| `RunId` | Same | Report/artifact prefixes, remediation idempotency material, evaluation execution naming | Used |
| `ExecutionTsIso` | Same | Run metadata and deterministic report context | Used |
| `DppConfigTableName` | Same | Explicit DPP table selection for `if_training` JobSpec load (`load_job_spec(..., table_name=...)`) and DPP/MLP linkage loader construction | **Wired in Task 8.9** |
| `MlpConfigTableName` | Same | Explicit MLP table selection in `ProjectParametersLoader(..., mlp_table_name=...)` to resolve `ml_project_names` linkage before training execution | **Wired in Task 8.9** |
| `BatchIndexTableName` | Same | Batch Index authoritative readiness derivation (`BatchIndexLoader(table_name=...)`) in verify/plan flow | Used |
| `TrainingStartTs` | Same | Required window start bound for verify/plan/train | Used |
| `TrainingEndTs` | Same | Required window end bound for verify/plan/train | Used |
| `EvalStartTs` | Same | Legacy evaluation window fallback derivation | Used |
| `EvalEndTs` | Same | Legacy evaluation window fallback derivation | Used |
| `MlProjectName` | Same | Branch isolation, MLP linkage validation, evaluation pipeline invocation, report/artifact path partitioning | Used |

## Code changes implemented for Task 8.9
1. Added explicit DPP/MLP linkage resolution before training execution:
   - build `ProjectParametersLoader` with runtime-provided `dpp_config_table_name` and `mlp_config_table_name`;
   - load canonical `ml_project_names` for `(project_name, feature_spec_version)`;
   - fail fast if runtime `ml_project_name` is not in the linked list.
2. Rewired `DppConfigTableName` from required-only validation to functional use by passing it directly into `load_job_spec(..., table_name=runtime_config.dpp_config_table_name)`.

## Verification evidence
- Added tests that assert:
  - runtime-provided DPP/MLP table names are consumed by linkage/job-spec loaders;
  - unlinked `ml_project_name` fails fast before training executes.

