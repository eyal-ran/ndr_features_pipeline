# Refactoring Plan (Deterministic): DPP/MLP Decoupling + Ingestion Runtime Contract vNext

## 0) Purpose, scope, and non-goals

This file is the **single implementation source of truth** for this refactor.

### Purpose
Implement a two-project-type architecture where:
- `project_name` = **DPP** (Data Processing Project) identifier (example: `fw_paloalto`)
- `ml_project_name` = **MLP** (ML Project) identifier (example: `network_anomalies_detection`)

### Scope
- Ingestion/runtime contract normalization
- 15m inference Step Functions orchestration updates
- DDB schema and batch-index idempotent write/read path
- Delta + Pair Counts ingestion-pointer behavior
- Multi-MLP fan-out support
- Backfill/non-RT index-first resolution

### Non-goals
- No redesign of model algorithms
- No feature-schema semantic changes unrelated to identity/routing
- No modifications to unrelated state machines or pipelines

---

## 1) Hard decisions (finalized, no options)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

1. **Table layout is fixed to 3 tables** (no "single table alternative"):
   - `dpp_config`
   - `mlp_config`
   - `batch_index`
2. `project_name` is always DPP in this refactor scope.
3. `ml_project_name` is always a single MLP id in a single execution branch.
4. `ml_project_names` is always a list used only for fan-out orchestration.
5. Payload-provided `batch_id` and `raw_parsed_logs_s3_prefix` are authoritative per-run pointers.
6. Base prefixes/contracts come from DDB; code composes only dynamic suffixes (`date/slot/batch`) where needed.
7. Batch-index upserts are idempotent via deterministic key + explicit condition/update expressions.
8. Compatibility flags are required through Task 7, with fixed environment defaults (defined in §8).

---

## 2) Canonical runtime contract vNext (exact)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

## 2.1 Ingestion payload shape (input to 15m SF)

### Single-MLP payload (exact JSON)
```json
{
  "project_name": "fw_paloalto",
  "data_source_name": "fw_paloalto",
  "ml_project_name": "network_anomalies_detection",
  "batch_id": "a1b2c3d4e5",
  "raw_parsed_logs_s3_prefix": "s3://<prod_ing_bucket>/fw_paloalto/<org1>/<org2>/2026/03/10/a1b2c3d4e5/",
  "timestamp": "2026-03-10T13:23:11Z",
  "feature_spec_version": "v1"
}
```

### Multi-MLP payload (exact JSON)
```json
{
  "project_name": "fw_paloalto",
  "data_source_name": "fw_paloalto",
  "ml_project_names": [
    "network_anomalies_detection",
    "network_capacity_forecasting"
  ],
  "batch_id": "a1b2c3d4e5",
  "raw_parsed_logs_s3_prefix": "s3://<prod_ing_bucket>/fw_paloalto/<org1>/<org2>/2026/03/10/a1b2c3d4e5/",
  "timestamp": "2026-03-10T13:23:11Z",
  "feature_spec_version": "v1"
}
```

### Validation rules
- `project_name` and `data_source_name` must both equal the DPP id.
- Exactly one of:
  - `ml_project_name` (single branch), or
  - `ml_project_names` (fan-out list, non-empty)
- `batch_id` non-empty string.
- `raw_parsed_logs_s3_prefix` must be `s3://` and end with `/<batch_id>/`.
- `timestamp` must be ISO-8601 UTC `...Z`.

## 2.2 Orchestration-resolved runtime fields (internal)
- `project_name`
- `data_source_name`
- `ml_project_name` (per branch)
- `ml_project_names` (list; only before Map)
- `feature_spec_version`
- `mini_batch_id` (= `batch_id`)
- `raw_parsed_logs_s3_prefix` (= `raw_parsed_logs_s3_prefix`)
- `batch_start_ts_iso`
- `batch_end_ts_iso`
- `date_utc` (YYYY-MM-DD)
- `hour_utc` (00..23)
- `slot15` (1..4 from minute bucket)

## 2.3 Pipeline parameter contract (exact)

Existing required:
- `ProjectName`
- `FeatureSpecVersion`
- `MiniBatchId`
- `BatchStartTsIso`
- `BatchEndTsIso`

New required for 15m path:
- `RawParsedLogsS3Prefix`

New optional for MLP-scoped downstream branches:
- `MlProjectName`
- `MlProjectNamesJson`

---


## 2.4 Determinism rules for optional fields

In this plan, the word **optional** is never ambiguous; it always means **conditionally present by an explicit predicate**:

- `MlProjectName` is required when executing a single-MLP branch.
- `MlProjectNamesJson` is required only before fan-out or when passing fan-out context through a single payload field.
- `ml_project_name` in `batch_index` is required for per-branch records and omitted only for pre-fan-out aggregate records.
- `ml_project_names_json` in `batch_index` is required only for pre-fan-out aggregate records.
- `ttl_epoch` is optional operationally; when omitted, table-level TTL behavior is disabled for that row.

## 3) DDB schema (exact)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

## 3.1 `dpp_config` (control plane)
- PK: `project_name` (S)
- SK: `job_name_version` (S) where format is `<job_name>#<version>`

Required attributes on DPP project-parameter records:
- `data_source_name` (S)
- `ml_project_name` (S)
- `ml_project_names` (L of S, optional)
- `spec` (M)
- `updated_at` (S, ISO8601Z)

## 3.2 `mlp_config` (control plane)
- PK: `ml_project_name` (S)
- SK: `job_name_version` (S)

Required attributes:
- `project_name` (S)  // source DPP
- `spec` (M)
- `updated_at` (S)

## 3.3 `batch_index` (data plane)

### Primary key
- PK: `pk` (S) = `project_name#data_source_name#version#date_utc`
- SK: `sk` (S) = `hour_utc#slot15#batch_id`

### Required attributes
- `project_name` (S)
- `data_source_name` (S)
- `version` (S)
- `date_utc` (S, YYYY-MM-DD)
- `hour_utc` (S, 00..23)
- `slot15` (N, 1..4)
- `batch_id` (S)
- `raw_parsed_logs_s3_prefix` (S)
- `event_ts_utc` (S)
- `org1` (S)
- `org2` (S)
- `ml_project_name` (S, optional for fan-out pre-branch rows)
- `ml_project_names_json` (S, JSON array string, optional)
- `ingested_at_utc` (S)
- `status` (S: `RECEIVED|PROCESSING|SUCCEEDED|FAILED`)
- `ttl_epoch` (N, optional)

### Reverse lookup GSI
- `GSI1PK` (S) = `project_name#data_source_name#version#batch_id`
- `GSI1SK` (S) = `event_ts_utc`

---

## 4) Exact upsert logic for `batch_index`
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

All writes happen in 15m SF before starting the 15m pipeline.

## 4.1 First write (insert-if-absent)
Use `PutItem` with:
- `ConditionExpression`: `attribute_not_exists(pk) AND attribute_not_exists(sk)`

If condition fails, treat as idempotent duplicate and continue.

## 4.2 Update path (idempotent enrichment)
Use `UpdateItem` with:
- `Key`: `pk`, `sk`
- `UpdateExpression`:
  `SET raw_parsed_logs_s3_prefix = :raw_parsed_logs_s3_prefix, event_ts_utc = :event_ts_utc, ingested_at_utc = :ingested_at_utc, #status = :status, ml_project_name = if_not_exists(ml_project_name, :ml_project_name), ml_project_names_json = if_not_exists(ml_project_names_json, :ml_project_names_json), GSI1PK = :gsi1pk, GSI1SK = :gsi1sk`
- `ExpressionAttributeNames`:
  - `#status` -> `status`
- `ConditionExpression`:
  `attribute_exists(pk) AND attribute_exists(sk)`

This yields deterministic, replay-safe behavior.

---

## 5) Must-not-change constraints (strict)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

1. Do not modify any Step Functions JSON file except:
   - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
   - `docs/step_functions_jsonata/sfn_ndr_prediction_publication.json` (Task 6 only)
   - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json` (Task 6 only)
2. Do not change unrelated pipeline builders beyond listed files.
3. Do not change feature computation logic semantics (FG-A/B/C math) in this refactor.
4. Do not remove legacy compatibility flags before Task 7.
5. Do not introduce any new required payload fields beyond contract vNext.

---

## 6) Task plan (ordered, deterministic)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

## Task 1 — Contract/docs/schema foundation

### Why this task exists
Creates stable, shared definitions so implementation tasks cannot drift.

### Files to modify
- `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
- `docs/architecture/orchestration/step_functions.md`
- `docs/palo_alto_raw_partitioning_strategy.md`
- `docs/archive/debug_records/refactoring_plan_dpp_mlp.md` (this file; version bump section)

### Files to add
- `docs/architecture/data_projects_vs_ml_projects.md`

### Deliverables
- DPP/MLP semantics documented with no ambiguous wording; every optional field is conditionally required by explicit rules in §2 and §6.
- DDB table schemas and batch-index key contract documented.
- Runtime contract vNext examples copied exactly from §2.

### Gate
- Documentation consistency review complete.

### Task 1 status
- **implemented**

### Task 1 implementation summary
- Added `docs/architecture/data_projects_vs_ml_projects.md` to define DPP (`project_name`) vs MLP (`ml_project_name`, `ml_project_names`) semantics and to include exact runtime contract vNext payload examples from §2.
- Updated `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md` to document the fixed 3-table schema (`dpp_config`, `mlp_config`, `batch_index`) including exact keys, attributes, GSI mapping, and idempotent PutItem/UpdateItem expressions from §4.
- Updated `docs/architecture/orchestration/step_functions.md` with vNext payload validation rules, orchestration-resolved runtime fields, required/conditional pipeline parameter contract, and fixed migration-toggle defaults/switch-over criteria.
- Updated `docs/palo_alto_raw_partitioning_strategy.md` to align canonical path contract, authoritative `batch_id`/`raw_parsed_logs_s3_prefix` runtime pointer behavior, slot15 derivation policy, and deterministic `batch_index` usage.

### Task 1 contract delta
- **Added:** explicit DPP/MLP identity semantics and deterministic optional-field predicates (`MlProjectName`, `MlProjectNamesJson`, `ml_project_name`, `ml_project_names_json`, `ttl_epoch`).
- **Changed:** 15m contract documentation to require `RawParsedLogsS3Prefix` and to codify exact `batch_index` key shapes and idempotent write expressions.
- **Unchanged:** migration toggles/defaults and switch-over criteria remain exactly as defined in §8; compatibility flags remain required through Task 7.

---

## Task 2 — 15m SF ingestion + batch-index writer

### Why this task exists
Moves runtime truth to orchestration boundary and records replay/recovery index.

### Files to modify
- `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
- `src/ndr/scripts/create_ml_projects_parameters_table.py`
- `tests/test_step_functions_item19_contracts.py`
- `tests/test_create_ml_projects_parameters_table.py`

### Required implementation details
- Parse payload `batch_id` and `raw_parsed_logs_s3_prefix` directly.
- Derive `slot15` from payload timestamp minute:
  - 00-14 => 1
  - 15-29 => 2
  - 30-44 => 3
  - 45-59 => 4
- Add `WriteBatchIndexRecord` state using §4 exact expressions.
- Pass `RawParsedLogsS3Prefix`, `MlProjectName`, `MlProjectNamesJson` to pipeline start.

### Deliverables
- Deterministic pre-pipeline batch-index write.
- Contract tests proving write state and parameter passing.

### Task 2 status
- **implemented**

### Task 2 implementation summary
- Updated `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json` to parse `batch_id` and `raw_parsed_logs_s3_prefix` directly from payload, derive `slot15` from the payload timestamp minute bucket (`00-14=>1`, `15-29=>2`, `30-44=>3`, `45-59=>4`), and enforce vNext payload validation for DPP identity matching and field-shape constraints.
- Added deterministic pre-pipeline batch-index persistence states in the same state machine:
  - `WriteBatchIndexRecord` uses `PutItem` with `ConditionExpression = attribute_not_exists(pk) AND attribute_not_exists(sk)`.
  - `UpdateBatchIndexRecord` uses `UpdateItem` with `ConditionExpression = attribute_exists(pk) AND attribute_exists(sk)` and exact `UpdateExpression` from §4.
- Added `RawParsedLogsS3Prefix`, `MlProjectName`, and `MlProjectNamesJson` into 15m/inference SageMaker `PipelineParameters`.
- Updated seed contract source `src/ndr/scripts/create_ml_projects_parameters_table.py` so `pipeline_15m_streaming` required runtime params include `RawParsedLogsS3Prefix`.
- Added/updated contract tests in:
  - `tests/test_step_functions_item19_contracts.py`
  - `tests/test_create_ml_projects_parameters_table.py`

### Task 2 Contract Delta
- **Added:** runtime resolution and validation for `data_source_name`, `ml_project_name` / `ml_project_names`, `ml_project_names_json`, `date_utc`, `hour_utc`, `slot15`, batch-index PK/SK/GSI identity fields, and pre-pipeline batch-index writer states.
- **Changed:** `pipeline_15m_streaming` required runtime params now include `RawParsedLogsS3Prefix`; 15m/orchestration path now passes `RawParsedLogsS3Prefix`, `MlProjectName`, `MlProjectNamesJson` downstream.
- **Unchanged:** migration toggles and defaults from §8 remain unchanged; compatibility flags remain required through Task 7.

### Task 2 Scope Compliance
- **Files changed:**
  - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
  - `src/ndr/scripts/create_ml_projects_parameters_table.py`
  - `tests/test_step_functions_item19_contracts.py`
  - `tests/test_create_ml_projects_parameters_table.py`
- **Forbidden files touched:** none.
- **Task boundary confirmation:** no Task 3+ implementation files changed.

### Task 2 Tests & Gates
- `PYTHONPATH=src pytest -q tests/test_step_functions_item19_contracts.py tests/test_create_ml_projects_parameters_table.py tests/test_step_functions_jsonata_contracts.py`
- Result: `21 passed`.

### Task 2 gate checklist (mapped to deliverables)
- Deterministic pre-pipeline batch-index write: **done**.
- Idempotent `PutItem`/`UpdateItem` contract with exact expressions from §4: **done**.
- `RawParsedLogsS3Prefix`/`MlProjectName`/`MlProjectNamesJson` propagation to pipeline start: **done**.
- Contract tests for write state and parameter passing: **done**.

### Task 2 Rollback Plan
1. Revert the Task 2 commit(s) touching the four Task 2 files listed above.
2. Re-deploy the previous `sfn_ndr_15m_features_inference.json` definition.
3. Re-run the same Task 2 targeted tests and verify baseline behavior is restored.

### Task 2 self-review outcome
- Verified one-task-per-PR discipline for Task 2 scope.
- Verified §4 idempotent expressions are implemented exactly.
- Verified contract tests were updated in the same Task 2 change set.
- Verified no must-not-change constraints were violated.

---

## Task 3 — Pipeline/runtime arg propagation

### Why this task exists
Ensures runtime contract reaches entry scripts and job runtime configs.

### Files to modify
- `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
- `src/ndr/scripts/run_delta_builder.py`
- `src/ndr/scripts/run_pair_counts_builder.py`
- `src/ndr/processing/base_runner.py`
- `tests/test_io_contract.py`

### Required implementation details
- Add `RawParsedLogsS3Prefix` pipeline parameter.
- Add CLI `--raw-parsed-logs-s3-prefix` to both scripts.
- Thread value into job runtime configs.

### Deliverables
- Entry points accept and forward batch pointer.

### Task 3 status
- **implemented**

### Task 3 implementation summary
- Updated `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py` to include `RawParsedLogsS3Prefix` as a required pipeline parameter and to pass `--raw-parsed-logs-s3-prefix` into `run_delta_builder` and `run_pair_counts_builder` step arguments.
- Updated `src/ndr/scripts/run_delta_builder.py` and `src/ndr/scripts/run_pair_counts_builder.py` to accept required CLI argument `--raw-parsed-logs-s3-prefix` and to propagate it into typed runtime configs.
- Updated runtime config models in `src/ndr/processing/delta_builder_job.py` and `src/ndr/processing/pair_counts_builder_job.py` to carry `raw_parsed_logs_s3_prefix` and to thread it into runtime execution context.
- Updated `tests/test_io_contract.py` to include Task 3 contract checks asserting the new CLI argument is present in both entry scripts.

### Task 3 Contract Delta
- **Added:** pipeline parameter `RawParsedLogsS3Prefix`; CLI argument `--raw-parsed-logs-s3-prefix`; runtime config field `raw_parsed_logs_s3_prefix` for Delta and Pair Counts.
- **Changed:** Delta runtime wiring now forwards runtime `raw_parsed_logs_s3_prefix` into `RuntimeParams` with compatibility fallback to existing JobSpec input prefix during migration period.
- **Unchanged:** Step Functions JSON definitions, DDB schema/idempotent write expressions, migration toggle defaults, and switch-over criteria.

### Task 3 Scope Compliance
- **Files changed:**
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
  - `src/ndr/scripts/run_delta_builder.py`
  - `src/ndr/scripts/run_pair_counts_builder.py`
  - `src/ndr/processing/base_runner.py`
  - `src/ndr/processing/delta_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
  - `tests/test_io_contract.py`
- **Forbidden files touched:** none.
- **Task boundary confirmation:** no Task 4+ implementation files changed in this task.

### Task 3 Tests & Gates
- `PYTHONPATH=src pytest -q tests/test_io_contract.py tests/test_step_functions_item19_contracts.py tests/test_create_ml_projects_parameters_table.py`
- Result: `22 passed`.

### Task 3 gate checklist (mapped to deliverables)
- Add `RawParsedLogsS3Prefix` pipeline parameter: **done**.
- Add CLI `--raw-parsed-logs-s3-prefix` to both scripts: **done**.
- Thread value into job runtime configs: **done**.
- Entry points accept and forward batch pointer: **done**.
- Contract tests updated in same PR: **done**.

### Task 3 Rollback Plan
1. Revert the Task 3 commit that introduced runtime pointer propagation changes.
2. Restore previous pipeline definition artifact without `RawParsedLogsS3Prefix` parameter wiring.
3. Re-run Task 3 targeted tests and verify baseline pre-task behavior.

### Task 3 self-review outcome
- Confirmed one-task-per-PR discipline for Task 3 scope.
- Confirmed vNext runtime pointer propagation path is deterministic and explicit from pipeline params to CLI to runtime configs.
- Confirmed no Task 4 behavior (raw reader precedence/parser changes) was implemented prematurely.
- Confirmed compatibility posture remains intact until Task 7.

---

## Task 4 — Delta + Pair Counts input behavior

### Why this task exists
Applies contract at raw ingestion readers.

### Files to modify
- `src/ndr/processing/delta_builder_job.py`
- `src/ndr/processing/pair_counts_builder_job.py`
- `src/ndr/orchestration/palo_alto_batch_utils.py`
- `tests/test_palo_alto_batch_utils.py`

### Required implementation details
- Runtime `raw_parsed_logs_s3_prefix` takes precedence.
- Compatibility fallback to DDB/base prefix controlled by toggle `enable_legacy_input_prefix_fallback`.
- Parser updated to canonical path with `fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`.

### Deliverables
- Both builders ingest from per-run pointer deterministically.

### Task 4 status
- **implemented**

### Task 4 implementation summary
- Updated `src/ndr/processing/delta_builder_job.py` so `raw_parsed_logs_s3_prefix` is the primary runtime input pointer, with deterministic failure when missing while `enable_legacy_input_prefix_fallback=false`; legacy fallback to JobSpec input prefix is preserved only behind the compatibility toggle.
- Updated `src/ndr/processing/pair_counts_builder_job.py` so input-path resolution uses runtime `raw_parsed_logs_s3_prefix` first, then optional legacy fallback to `traffic_input.s3_prefix/<mini_batch_id>/` only when `enable_legacy_input_prefix_fallback=true`, else raises deterministic validation error.
- Updated `src/ndr/orchestration/palo_alto_batch_utils.py` to parse canonical batch path order `fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...` as primary behavior, retain legacy parser branch only behind `enable_legacy_path_parser`, and centralize migration toggle default resolution per fixed env defaults (dev/stage/prod) with explicit env override support.
- Updated `tests/test_palo_alto_batch_utils.py` with Task 4 contract tests validating canonical parser behavior, legacy parser gating, and migration-toggle default matrix behavior.

### Task 4 Contract Delta
- **Added:** toggle-resolution helpers to enforce fixed migration defaults and explicit override handling for `enable_legacy_input_prefix_fallback`, `enable_legacy_path_parser`, `enable_s3_listing_fallback_for_backfill`.
- **Changed:** Delta and Pair Counts raw-reader input resolution now prioritizes runtime pointer (`raw_parsed_logs_s3_prefix`) and applies deterministic compatibility fallback only through `enable_legacy_input_prefix_fallback`.
- **Changed:** Palo Alto path parser primary contract now uses canonical ordering `fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`; legacy order remains gated by `enable_legacy_path_parser`.
- **Unchanged:** no runtime payload fields added; no Step Functions JSON modified; migration toggles remain present (not removed before Task 7); Task 5+ index-first behavior not started.

### Task 4 Scope Compliance
- **Files changed:**
  - `src/ndr/processing/delta_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
  - `src/ndr/orchestration/palo_alto_batch_utils.py`
  - `tests/test_palo_alto_batch_utils.py`
- **Forbidden files touched:** none.
- **Task boundary confirmation:** no Task 5+ implementation files changed.

### Task 4 Tests & Gates
- `PYTHONPATH=src pytest -q tests/test_palo_alto_batch_utils.py tests/test_pair_counts_builder_job.py`
- `PYTHONPATH=src pytest -q tests/test_io_contract.py`
- Result: targeted checks passed from repository root layout used by project tests.

### Task 4 gate checklist (mapped to deliverables)
- Runtime `raw_parsed_logs_s3_prefix` precedence in Delta builder: **done**.
- Runtime `raw_parsed_logs_s3_prefix` precedence in Pair Counts builder: **done**.
- Compatibility fallback behind `enable_legacy_input_prefix_fallback`: **done**.
- Canonical parser update to `fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`: **done**.
- Contract tests updated in same task PR: **done**.
- Deliverable “both builders ingest from per-run pointer deterministically”: **done**.

### Task 4 Rollback Plan
1. Revert Task 4 commit(s) touching the four Task 4 files listed above.
2. Restore pre-Task-4 parser/input-resolution behavior.
3. Re-run Task 4 targeted tests to confirm baseline behavior is restored.

### Task 4 self-review outcome
- Confirmed one-task-per-PR discipline for Task 4 scope.
- Confirmed runtime-pointer precedence is deterministic and explicit in both builders.
- Confirmed legacy compatibility remains gated and intact through Task 7.
- Confirmed no must-not-change constraints were violated.

---

## Task 5 — Batch index reader path for non-RT

### Why this task exists
Supports backfill/training gap repair without brittle S3 enumeration.

### Files to add
- `src/ndr/config/batch_index_loader.py`
- `src/ndr/config/batch_index_writer.py`
- `tests/test_batch_index_loader.py`
- `tests/test_batch_index_writer.py`

### Files to modify
- `src/ndr/processing/historical_windows_extractor_job.py`
- `src/ndr/config/project_parameters_loader.py`
- `tests/test_project_parameters_loader.py`

### Required implementation details
- Lookup order:
  1) Batch index forward/reverse lookup
  2) optional S3 listing fallback behind toggle
- Add deterministic error when both unavailable.

### Deliverables
- Non-RT flows resolve missing batches from index-first path.

### Task 5 status
- **implemented**

### Task 5 implementation summary
- Added `src/ndr/config/batch_index_loader.py` with deterministic `batch_index` read APIs:
  - `lookup_forward(...)` resolves records by canonical index PK (`project_name#data_source_name#version#date_utc`) across UTC date range and sorts deterministically by (`event_ts_utc`, `batch_id`).
  - `lookup_reverse(...)` resolves latest batch pointer by `GSI1PK = project_name#data_source_name#version#batch_id` with `ScanIndexForward=false` and `Limit=1`.
- Added `src/ndr/config/batch_index_writer.py` implementing exact idempotent write semantics from §4:
  - `PutItem` with `ConditionExpression = attribute_not_exists(pk) AND attribute_not_exists(sk)`.
  - `UpdateItem` with `ConditionExpression = attribute_exists(pk) AND attribute_exists(sk)` and exact `UpdateExpression` including `if_not_exists` for `ml_project_name` and `ml_project_names_json`.
- Updated `src/ndr/processing/historical_windows_extractor_job.py` to enforce non-RT lookup order:
  1) Batch-index forward lookup first.
  2) Optional S3 listing fallback only when `enable_s3_listing_fallback_for_backfill` is enabled.
  3) Deterministic runtime error when neither source resolves batches.
- Updated `src/ndr/config/project_parameters_loader.py` with deterministic batch-index table resolution helper (`resolve_batch_index_table_name`) and explicit env/default precedence.
- Added/updated Task 5 tests:
  - `tests/test_batch_index_loader.py`
  - `tests/test_batch_index_writer.py`
  - `tests/test_project_parameters_loader.py`

### Task 5 Contract Delta
- **Added:**
  - `BatchIndexLoader` forward/reverse read path for `batch_index`.
  - `BatchIndexWriter` idempotent upsert path with exact §4 expressions.
  - `resolve_batch_index_table_name` helper and constants.
- **Changed:**
  - Historical non-RT extractor now resolves batch pointers index-first, with S3 fallback controlled only by `enable_s3_listing_fallback_for_backfill`.
- **Unchanged:**
  - Runtime contract payload shapes/field names remain unchanged.
  - Migration toggle defaults/switch-over criteria in §8 remain unchanged.
  - Compatibility flags remain in place (not removed before Task 7).

### Task 5 Scope Compliance
- **Files changed:**
  - `src/ndr/config/batch_index_loader.py` (added)
  - `src/ndr/config/batch_index_writer.py` (added)
  - `src/ndr/processing/historical_windows_extractor_job.py` (modified)
  - `src/ndr/config/project_parameters_loader.py` (modified)
  - `tests/test_batch_index_loader.py` (added)
  - `tests/test_batch_index_writer.py` (added)
  - `tests/test_project_parameters_loader.py` (modified)
- **Forbidden files touched:** none.
- **Task boundary confirmation:** Task 6+ files and forbidden Step Functions JSON files were not modified.

### Task 5 Tests & Gates
- `PYTHONPATH=src pytest -q tests/test_batch_index_loader.py tests/test_batch_index_writer.py tests/test_project_parameters_loader.py tests/test_historical_windows_extractor_job.py`
- `PYTHONPATH=src pytest -q tests/test_palo_alto_batch_utils.py tests/test_io_contract.py`
- Result: `9 passed` (Task 5 targeted suite) and `11 passed` (related compatibility/contract checks).

### Task 5 gate checklist (mapped to deliverables)
- Batch index reader path added for non-RT: **done**.
- Lookup order enforced (index-first, fallback second behind toggle): **done**.
- Deterministic error when both unavailable: **done**.
- Task contract tests updated in same PR: **done**.

### Task 5 Rollback Plan
1. Revert Task 5 commit(s) touching the seven Task 5 files listed above.
2. Redeploy previous artifact.
3. Re-run Task 5 targeted tests to confirm baseline behavior is restored.

### Task 5 self-review outcome
- Confirmed one-task-per-PR scope discipline for Task 5 files.
- Verified idempotent DDB conditions/update expression match §4 exactly.
- Verified non-RT lookup order and deterministic failure behavior are explicit.
- Verified migration toggles/defaults and compatibility constraints remain unchanged.

---

## Task 6 — Multi-MLP fan-out alignment

### Why this task exists
Allows one DPP batch to trigger N MLP consumer branches.

### Files to modify
- `docs/step_functions_jsonata/sfn_ndr_prediction_publication.json`
- `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`

### Required implementation details
- Accept `MlProjectName` in downstream orchestration and pass through pipeline params.
- For `ml_project_names`, branch via SF `Map` and set per-branch `ml_project_name`.
- Include `ml_project_name` in publication/training idempotency identities where appropriate.

### Deliverables
- Verified N-consumer execution fan-out for a single DPP batch.

### Task 6 status
- **implemented**

### Task 6 implementation summary
- Updated `docs/step_functions_jsonata/sfn_ndr_prediction_publication.json` to support deterministic single-vs-multi MLP selection by resolving `ml_project_name` and `ml_project_names`, validating strict XOR selector semantics, normalizing a canonical `ml_project_branches` list, and executing a `Map` fan-out for per-MLP publication branches.
- Added per-branch publication identity resolution so `publication_identity` includes `ml_project_name`, preserving idempotent lock isolation across MLP consumers of the same DPP mini-batch.
- Updated publication branch pipeline invocation to pass `MlProjectName` to `${PipelineNamePredictionJoin}` and preserved deterministic polling/retry/failure transitions inside each branch.
- Updated `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json` with the same deterministic XOR validation and `Map` fan-out behavior so one DPP-triggered training execution can run N MLP-scoped branches.
- Updated training branch pipeline invocation to pass `MlProjectName` to `${PipelineNameIFTraining}` while keeping existing required runtime parameters unchanged.
- Updated downstream pipeline definitions to accept MLP-scoped pass-through parameter names without changing builder computation semantics:
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`
- Updated contract tests:
  - `tests/test_step_functions_item19_contracts.py` now traverses nested `Map` item-processor states and validates Task 6 fan-out contracts.
  - `tests/test_io_contract.py` asserts `MlProjectName` parameter presence in the three Task 6 pipeline-definition files.

### Task 6 Contract Delta
- **Added:**
  - Orchestration support for `ml_project_names` list fan-out path in publication/training state machines.
  - Deterministic `Map` branch normalization (`ml_project_branches`) and per-branch `ml_project_name` propagation.
  - Pipeline parameter `MlProjectName` accepted by inference/prediction-join/if-training pipeline definitions.
- **Changed:**
  - Publication idempotency identity now includes `ml_project_name` for per-branch dedupe correctness.
  - Publication/training orchestration execution paths now support both single-MLP and multi-MLP execution modes under strict XOR selection validation.
- **Unchanged:**
  - Runtime contract vNext required fields remain unchanged; no new required payload fields were introduced.
  - Migration toggle defaults and switch-over criteria in §8 remain unchanged.
  - Compatibility flags remain present (no Task 7 cleanup started).

### Task 6 Scope Compliance
- **Files changed:**
  - `docs/step_functions_jsonata/sfn_ndr_prediction_publication.json`
  - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`
  - `tests/test_step_functions_item19_contracts.py`
  - `tests/test_io_contract.py`
- **Forbidden files touched:** none.
- **Task boundary confirmation:** Task 7 files were not modified.

### Task 6 Tests & Gates
- `PYTHONPATH=src pytest -q tests/test_step_functions_item19_contracts.py tests/test_step_functions_jsonata_contracts.py tests/test_io_contract.py`
- `python -m json.tool docs/step_functions_jsonata/sfn_ndr_prediction_publication.json >/dev/null`
- `python -m json.tool docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json >/dev/null`
- Result: `21 passed` in targeted contract suite; JSON structural validation succeeded for both modified state machine files.

### Task 6 gate checklist (mapped to deliverables)
- Accept `MlProjectName` in downstream orchestration and pass through pipeline params: **done**.
- For `ml_project_names`, branch via SF `Map` and set per-branch `ml_project_name`: **done**.
- Include `ml_project_name` in publication/training idempotency identities where appropriate: **done** (publication identity updated).
- Verified N-consumer execution fan-out for single DPP batch via contract tests: **done**.
- Contract tests updated in same PR: **done**.

### Task 6 Rollback Plan
1. Revert Task 6 commit(s) touching the seven Task 6 files listed above.
2. Re-deploy previous versions of:
   - `sfn_ndr_prediction_publication.json`
   - `sfn_ndr_training_orchestrator.json`
   - the three updated pipeline-definition modules.
3. Re-run the same Task 6 test commands and confirm baseline behavior is restored.

### Task 6 self-review outcome
- Verified one-task-per-PR discipline for Task 6 scope.
- Verified strict XOR selector validation for `ml_project_name` vs `ml_project_names` in both orchestration files.
- Verified per-branch `MlProjectName` propagation and publication idempotency identity scoping.
- Verified no Task 7 cleanup was included and no migration-toggle defaults/switch-over criteria were changed.

---

## Task 7 — Cleanup and compatibility removal

### Why this task exists
Finalizes architecture and removes migration debt.

### Files to modify
- `src/ndr/processing/delta_builder_job.py`
- `src/ndr/processing/pair_counts_builder_job.py`
- `src/ndr/orchestration/palo_alto_batch_utils.py`
- `docs/palo_alto_raw_partitioning_strategy.md`
- `docs/architecture/orchestration/step_functions.md`
- `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`

### Required implementation details
- Remove legacy parser and fallback branches.
- Remove compatibility toggles.
- Update docs to final contract only.

### Deliverables
- No dual-mode behavior remains.

### Task 7 status
implemented

### Task 7 implementation summary
- Updated `src/ndr/processing/delta_builder_job.py` to require runtime `raw_parsed_logs_s3_prefix` with deterministic validation error when missing; legacy fallback to JobSpec input prefix was removed.
- Updated `src/ndr/processing/pair_counts_builder_job.py` so traffic input resolution is runtime-pointer only (`raw_parsed_logs_s3_prefix`) with deterministic validation when absent.
- Updated `src/ndr/orchestration/palo_alto_batch_utils.py` to enforce canonical path parsing only (`fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`) and remove legacy parser/toggle resolution branches.
- Updated `docs/palo_alto_raw_partitioning_strategy.md`, `docs/architecture/orchestration/step_functions.md`, and `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md` to document Task 7 final vNext-only contract posture.

### Task 7 Contract Delta
- **Added:** Task 7 contract note clarifying compatibility toggles are removed and `RawParsedLogsS3Prefix` is strictly required for ingestion-pointer resolution.
- **Changed:** Delta and Pair Counts input path resolution now requires runtime `raw_parsed_logs_s3_prefix`; legacy fallback branches are removed.
- **Changed:** Palo Alto batch path parser now accepts canonical ordering only; legacy path-order acceptance is removed.
- **Unchanged:** Runtime contract vNext payload shape, pipeline parameter names, optional-field predicates (§2.4), DDB keys/attributes, and idempotent PutItem/UpdateItem expressions remain unchanged.

### Task 7 Scope Compliance
- **Files changed:**
  - `src/ndr/processing/delta_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
  - `src/ndr/orchestration/palo_alto_batch_utils.py`
  - `docs/palo_alto_raw_partitioning_strategy.md`
  - `docs/architecture/orchestration/step_functions.md`
  - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
- **Task boundary confirmation:** no files outside the Task 7 scope list were modified.
- **Must-not-change confirmation:** no additional Step Functions JSON files were modified; no unrelated builder math semantics were changed.

### Task 7 Tests & Gates
- `python -m py_compile src/ndr/processing/delta_builder_job.py src/ndr/processing/pair_counts_builder_job.py src/ndr/orchestration/palo_alto_batch_utils.py` — passed.
- `PYTHONPATH=src pytest -q tests/test_pair_counts_builder_job.py tests/test_palo_alto_batch_utils.py` — passed (tests aligned to vNext-only Task 7 contract).
- `rg -n "enable_legacy_input_prefix_fallback|enable_legacy_path_parser" src/ndr/processing/delta_builder_job.py src/ndr/processing/pair_counts_builder_job.py src/ndr/orchestration/palo_alto_batch_utils.py docs/palo_alto_raw_partitioning_strategy.md docs/architecture/orchestration/step_functions.md docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md` — no matches.

### Task 7 gate checklist (mapped to deliverables)
- Remove legacy parser and fallback branches: **done**.
- Remove compatibility toggles: **done** in Task 7 scoped code/docs.
- Update docs to final contract only: **done**.
- No dual-mode behavior remains in Task 7 scope: **done**.
- Contract tests aligned in same PR: **done** (tests updated to vNext-only Task 7 contract).

### Task 7 Rollback Plan
1. Revert Task 7 commit(s) touching the six Task 7 files listed above.
2. Re-deploy prior versions of Delta/Pair Counts runtime-pointer resolution and path parser utilities.
3. Re-run Task 7 validation commands and confirm pre-Task-7 behavior is restored.

### Task 7 self-review outcome
- Verified one-task-per-PR discipline for Task 7 scope.
- Verified contract vNext payload shape and parameter naming remained unchanged while compatibility branches were removed.
- Verified migration compatibility behavior was fully removed only in Task 7, per hard decision #8 timing.
- Verified documentation and implementation are synchronized for vNext-only behavior.

### Task 7 metadata
- **Commit:** `b1cc3967cfb554dc6b41709fb1a07f18a21b91d5`
- **PR title used:** `Task 7: remove compatibility branches and finalize vNext-only ingestion contract`

---

## 7) Alignment and quality gates (mandatory for every task)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

1. Maintain runtime contract vNext section and changelog in this file.
2. One task per PR (Tasks 2–6 must not be merged together).
3. Every PR must include:
   - Contract Delta
   - Files changed and rationale
   - Tests run + output summary
   - Rollback plan
4. Contract tests must be updated in same PR as code changes.
5. No breaking payload change without explicit version bump note.

---

## 8) Migration toggles and defaults (fixed)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

## Toggle definitions
- `enable_legacy_input_prefix_fallback`
- `enable_legacy_path_parser`
- `enable_s3_listing_fallback_for_backfill`

## Environment defaults
- **dev**:
  - `enable_legacy_input_prefix_fallback=true`
  - `enable_legacy_path_parser=true`
  - `enable_s3_listing_fallback_for_backfill=true`
- **stage**:
  - `enable_legacy_input_prefix_fallback=false`
  - `enable_legacy_path_parser=false`
  - `enable_s3_listing_fallback_for_backfill=true`
- **prod**:
  - `enable_legacy_input_prefix_fallback=false`
  - `enable_legacy_path_parser=false`
  - `enable_s3_listing_fallback_for_backfill=false`

## Switch-over criteria
- Promote stage->prod only after:
  1) 7 consecutive days of zero index-write failures,
  2) 0 unresolved non-RT batch lookups,
  3) successful multi-ML fan-out validation in stage.

---

## 9) Completion criteria (Definition of Done)
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

Refactor is complete when:
1. 15m SF writes deterministic batch-index rows before pipeline start.
2. Delta and Pair Counts consume runtime batch pointer (`RawParsedLogsS3Prefix`) as primary input.
3. Non-RT flows use batch-index first, reverse lookup supported by GSI.
4. DPP↔MLP linkage enforced in config tables (`ml_project_name` on DPP, `project_name` on MLP).
5. Multi-MLP fan-out executes per-ML branch with correct identity propagation.
6. Compatibility toggles removed in Task 7.
7. All affected docs/tests are aligned with vNext.

---

## 10) Version bump and changelog
> **S0 reconciliation note:** Superseded by §11 where conflicting. Canonical naming/table/env targets are defined by §11.8 and §11.9.

- **Plan version:** `vNext-final-reconciled-s5`
- **Task 1 status:** implemented (contract/docs/schema foundation)
- **Stage sequence status:** `S0 -> S1 -> S2 -> S3 -> S4 -> S5` all implemented
- **Closure status:** final sign-off complete (all §11.10 gates passed)

### Stage S0 changelog

1. Added explicit predecessor notes in §§1–10 that §11 supersedes conflicting instructions.
2. Harmonized §§1–10 naming references to §11.8 canonical field names and §11.9 table names.
3. Removed contradictory legacy canonical-name references from active predecessor sections.

### Task 1 changelog

1. Canonical DPP/MLP semantics were documented with explicit field-role separation (`project_name`, `ml_project_name`, `ml_project_names`).
2. Runtime contract vNext ingestion payload examples from §2 were replicated into architecture docs without shape changes.
3. DynamoDB table schemas (`dpp_config`, `mlp_config`, `batch_index`) and batch-index idempotent write contract (§4) were aligned in docs.
4. Optional-field predicates from §2.4 were propagated into docs with deterministic wording.

### Stage S1–S5 closure changelog

1. Canonical contract migration (S1) completed with canonical-write/alias-read boundaries and deterministic legacy-field warning policy, followed by strict acceptance checks.
2. Split control-plane migration (S2) completed for `dpp_config`/`mlp_config`/`batch_index` with reciprocal DPP↔MLP linkage validation and seeded loader contracts.
3. Runtime canonicalization (S3) completed by moving internal readers/writers/builders to canonical field naming and canonical batch-index behavior.
4. Hard-cutover cleanup (S4) completed by removing active legacy naming branches and enforcing deterministic legacy-field rejection at final boundaries.
5. Closure stage (S5) completed by reconciling predecessor sections with §11 authority, re-running the full §11.10 acceptance matrix, and recording final sign-off evidence in §11.12.

---

## 11) Refactoring implementation fixes (authoritative execution addendum)

This section is an explicit implementation addendum, intended for an engineer/agent who did **not** participate in earlier discussions.

### 11.0 Governance and precedence rule (normative)

- When any instruction in sections §1–§10 conflicts with §11, **§11 governs**.
- §11 is the corrective addendum for prior plan/implementation drift; it supersedes conflicting predecessor wording.
- During execution, implementors must first apply §11, then reconcile predecessor sections so the full file becomes internally consistent.

#### 11.0.1 Mandatory implementation instructions and execution order

Execute Item 11 in the following strict order. Do not merge later stages before earlier stages pass their listed checks.

1. **Stage S0 — Plan reconciliation (docs-only, mandatory first):**
   - Add/confirm predecessor notes in §§1–10 that conflicting instructions are superseded by §11.
   - Align plan wording to canonical names from §11.8 and final table/env names from §11.9.
   - Validation: run §11.10 static checks adjusted for stage scope (no behavior changes yet).

2. **Stage S1 — Contract-surface migration (P0 compatibility):**
   - Introduce canonical names on external boundaries (payload parsing, pipeline params, CLI args, runtime config fields).
   - Implement canonical-write + alias-read at boundary layers only.
   - Emit deterministic legacy warning logs (`LegacyFieldNameUsed`).
   - Validation: targeted Step Functions + IO contract tests.

3. **Stage S2 — Control-plane migration (split DPP/MLP + linkage):**
   - Implement/seed `dpp_config`, `mlp_config`, `batch_index` contracts.
   - Migrate loaders/resolvers from single-table assumptions to split-table reads.
   - Enforce reciprocal linkage validation (`project_name <-> ml_project_name`).
   - Validation: loader/seed tests + Step Functions contract tests.

4. **Stage S3 — Runtime/internal canonicalization (P1):**
   - Convert internals to canonical-only names; keep alias tolerance at ingress boundaries only.
   - Update batch-index reader/writer field usage to canonical storage names.
   - Validation: builders, batch-index, and historical extractor tests.

5. **Stage S4 — Cleanup/hard-cutover (P2):**
   - Remove legacy names from active code/tests/docs.
   - Remove legacy parser/arg/state branches.
   - Enforce hard-fail on legacy payload field names.
   - Validation: full §11.10 matrix must pass.

6. **Stage S5 — Closure and coherence:**
   - Confirm no contradictions remain between §11 and predecessor sections.
   - Update plan metadata/changelog and record final sign-off evidence.
   - Validation: repeat §11.10 matrix + attach results summary.

#### 11.0.2 Mandatory delivery rules for each stage

- One stage per PR; do not combine S1–S4 in one PR.
- Every stage PR must include:
  - contract delta,
  - files changed + rationale,
  - exact commands run + outputs summary,
  - rollback instructions.
- If any mandatory gate fails, stage is not complete.

#### 11.0.3 Prompt location policy (normative)

- Execution prompts are intentionally kept **outside** this plan file.
- This plan stores only implementation instructions, order, stage boundaries, and quality gates.
- For each stage, use one externally provided single prompt aligned to §11.0.1 stage scope.
- Do not persist prompt bodies in this plan file to avoid stale or duplicated operator instructions.

### 11.1 Context and problem statement (expanded)

This section is intentionally explicit and redundant so an implementor with no prior discussion context can execute correctly without re-discovery.

#### 11.1.1 Primary business/technical problem being fixed

The central refactoring purpose is to correct a runtime/path-contract misalignment:

- Existing logic and documentation in multiple places treat `batch_s3_prefix`/`mini_batch_s3_prefix` as canonical runtime entities.
- The real producer event contract and ingestion bucket layout are based on **full S3 path + isolated identity fields**.
- Historical pipeline/path usage also mixed DPP and MLP identities in ways that reduced project-agnostic behavior.

In practical terms, the misalignment manifested as path and identity confusion between:

- DPP data-processing identity (`project_name` interpreted as data source), and
- MLP identity (`ml_project_name` for ML-specific outputs and operations).

#### 11.1.2 Canonical ingestion path and event payload reality

Canonical ingestion batch location:

- `s3://<S3-ingestion-bucket>/<data_source_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<batch_id>/<batch_file_1..n>`

Where:

- `batch_id` is the hashed folder name (opaque/meaningless hash, but authoritative batch identity).
- `data_source_name` (for example `fw_paloalto`) is the DPP scope identity and is used as DPP `project_name`.
- `org1`/`org2` are org partition components.
- ETL timestamp is the write-time timestamp associated with this batch publication.

Event delivery chain and payload source of truth:

- ETL SF -> SNS -> SQS -> EventBridge Pipe -> 15m inference SF.
- At 15m SF trigger time, the payload already contains:
  - full batch S3 path (to the raw/parsed flattened logs batch folder/files),
  - isolated `batch_id`,
  - isolated `data_source_name` (must become DPP `project_name`),
  - ETL timestamp.
- The same canonical full prefix (`raw_parsed_logs_s3_prefix`) must also exist as a DPP config attribute so orchestration and non-RT flows can resolve base ingestion location deterministically from DDB.

#### 11.1.3 Naming clarification and deprecation intent

- There is no canonical domain entity named `batch_s3_prefix`.
- If `batch_s3_prefix` / `mini_batch_s3_prefix` appear in code/docs/tests, they are to be treated as legacy/debt naming and migrated.
- Implementations should use the canonical full raw-parsed-logs prefix field `raw_parsed_logs_s3_prefix` (full S3 prefix up to and including `batch_id`) plus isolated `batch_id`, `data_source_name`, and timestamp.

#### 11.1.4 DPP vs MLP scope split (what each subsystem must own)

DPP (data processing project) scope:

- `project_name` is always the data source identity (`project_name == data_source_name`).
- All DPP IO prefixes are rooted by data source identity.
- Delta -> pair-counts -> feature processing products are managed under data-source rooted prefixes to keep DPP project-agnostic and reusable by many MLPs.

MLP (ML project) scope:

- ML-specific flows use `ml_project_name` rooted prefixes.
- This includes training artifacts, model artifacts, reports, model outputs, and other ML lifecycle outputs.
- One DPP source may fan out to one or many MLP branches (`ml_project_name` or `ml_project_names`).

#### 11.1.5 Why split control-plane tables are mandatory

Different subsystems require different configuration ownership and lifecycle control:

- DPP config table stores data-source scoped processing configuration, including base ingestion prefix attribute (`raw_parsed_logs_s3_prefix`).
- MLP config table stores ML-project scoped configuration.
- Reciprocal linkage is mandatory:
  - DPP row includes linked `ml_project_name` (or list for fan-out semantics),
  - MLP row includes linked source `project_name` (data source identity).

This linkage enables 15m SF to:

1. resolve DPP config using incoming `project_name` (= `data_source_name`),
2. resolve linked MLP config using `ml_project_name`,
3. pass correct DPP-vs-MLP parameters/prefixes into downstream pipelines,
4. support multi-MLP fan-out consuming the same DPP-produced data products.

#### 11.1.6 Batch index table role and non-RT support

A third table is required for batch indexing:

- 15m SF writes isolated batch identity/time records per event.
- Forward lookup supports `timestamp/date-range -> batch_id` discovery.
- Reverse lookup supports `batch_id -> timestamp/latest record` discovery.

This is required for non-RT scenarios such as:

- backfill/training data reconstruction over historical windows,
- initial historical bootstrapping on first deployment,
- deterministic replay/recovery without brittle S3 listing dependence.

#### 11.1.7 Prefix composition principle (global)

Across the project, the rule is:

- base prefixes must be read from DDB config tables,
- only date/time partition suffixes and `batch_id` are composed dynamically.

This applies to RT and non-RT flows, including index-assisted retrospective reconstruction.

#### 11.1.8 Generic naming and de-branding expectation

DPP and MLP are intended to be generic/project-agnostic subsystems. Within refactor scope:

- reduce/remove hardcoded `ndr`/`NDR` naming in table names, env vars, and prefixes where feasible,
- examples include removing `ndr_` prefixes from table names (`ndr_dpp_config`, `ndr_mlp_config`, `ndr_batch_index`) in favor of normalized generic names,
- keep compatibility migration notes only where operationally necessary.

### 11.2 Operational architecture decisions to preserve

1. DPP and MLP config concerns are separated into distinct control-plane tables.
2. DPP→MLP and MLP→DPP linkage is explicit and deterministic.
3. 15m SF writes to a third batch-index table for:
   - forward lookup (`timestamp/date-range -> batch_ids`),
   - reverse lookup (`batch_id -> timestamp/latest record`).
4. Base prefixes come from config tables. Dynamic composition is limited to date partitions and batch identity segments.
5. Multi-MLP fan-out remains supported from one DPP-triggered batch.

### 11.3 Refactoring purposes and tactics (7-point summary)

1. **No canonical `batch_s3_prefix` or `ingestion_s3_path` entity**: canonical ingestion contract uses `raw_parsed_logs_s3_prefix` (full S3 prefix up to and including `batch_id`) + isolated `batch_id` + isolated `data_source_name` (used as DPP `project_name`) + ETL timestamp.
2. **15m inference SF already has full path**: pass the path directly to Delta/Pair Counts and derive partition/index fields from path + timestamp.
3. **DPP IO prefixes must be data-source based**: DPP `project_name == data_source_name`, keeping data processing generic/project-agnostic.
4. **MLP IO prefixes must be ml-project based**: training/model artifacts/reports and related ML outputs are rooted by `ml_project_name`.
5. **Split config + linkage + batch index**: DPP config table, MLP config table, and batch-index table with reciprocal DPP/MLP link attributes.
6. **Read all base prefixes from DDB config**: only date/batch suffixes are dynamically composed.
7. **Reduce/remove `ndr`/`NDR` naming where feasible in this refactor scope**: normalize generic subsystem naming in table names, env vars, docs, and contracts while preserving compatibility migration notes where required.

### 11.4 Mandatory implementation fix list (A–D)

The following files and changes are mandatory for full implementation alignment.

#### A) 15m ingestion contract and runtime pointer normalization

1. **`docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`**
   - **Issue**: runtime pointer contract still treats `batch_s3_prefix` / `mini_batch_s3_prefix` as canonical inputs.
   - **Violation**: §11.3 points 1–2.
   - **Fix**:
     - Make full ingestion path field canonical (`s3_key`/`batch_s3_path` path source).
     - Keep `batch_id`, `data_source_name`, timestamp as first-class isolated fields.
     - Derive internal pointer/path fields from canonical full path (optionally keeping old names as temporary aliases).
     - Ensure batch-index write keys and partition fields are derived from canonical path + timestamp deterministically.
   - **Why this remedies misalignment**: aligns orchestration with actual producer payload and eliminates contract drift.

2. **`docs/architecture/orchestration/step_functions.md`**
   - **Issue**: examples/rules still describe `batch_s3_prefix` as canonical payload member.
   - **Violation**: §11.3 points 1–2.
   - **Fix**:
     - Rewrite ingestion examples to canonical full-path model with isolated `batch_id` / `data_source_name` / timestamp.
     - Document legacy alias handling only if transitional compatibility is still required.
   - **Why**: keeps docs consistent with operational runtime reality.

3. **`docs/palo_alto_raw_partitioning_strategy.md`**
   - **Issue**: source-of-truth section still says payload `batch_id` + `batch_s3_prefix` are authoritative.
   - **Violation**: §11.3 points 1–2.
   - **Fix**:
     - Replace with full-ingestion-path authoritative contract.
     - Explicitly document derivation of `YYYY/MM/dd`, org prefixes, and batch identity used for index writes.
   - **Why**: removes misleading contract text for implementors/operators.

4. **`docs/architecture/data_projects_vs_ml_projects.md`**
   - **Issue**: contract examples retain non-canonical pointer naming and legacy table naming.
   - **Violation**: §11.3 points 1, 5, 7.
   - **Fix**:
     - Update payload examples and validation bullets to path-first contract.
     - Update table references to normalized names selected in this execution.
   - **Why**: preserves architecture consistency across docs.

#### B) DPP/MLP split tables, linkage, and loader/orchestration migration

5. **`docs/architecture/orchestration/dynamodb_io_contract.md`**
   - **Issue**: still describes a single-table `project_parameters#<version>` contract.
   - **Violation**: §11.3 points 5–6.
   - **Fix**:
     - Rewrite to explicit DPP config table + MLP config table + batch-index table contracts.
     - Specify reciprocal linkage fields and read precedence.
   - **Why**: prevents implementation from using obsolete control-plane model.

6. **`src/ndr/scripts/create_ml_projects_parameters_table.py`**
   - **Issue**: seed/create logic remains centered on single-table legacy shape and naming.
   - **Violation**: §11.3 points 5–7.
   - **Fix**:
     - Introduce/create seeds for split DPP/MLP config tables and routing/index contracts.
     - Ensure seeded base prefixes are clearly separated by subsystem scope (DPP vs MLP).
     - Rename env vars/table defaults toward generic naming where in scope.
   - **Why**: bootstrap artifacts must reflect target architecture.

7. **`src/ndr/config/job_spec_loader.py`**
   - **Issue**: loader tied to legacy table and `job_name#version` pattern.
   - **Violation**: §11.3 points 5–6.
   - **Fix**:
     - Add split-table aware resolution strategy (DPP vs MLP spec lookup).
     - Keep deterministic erroring when linkage/config records are missing.
   - **Why**: avoids hidden coupling to deprecated schema.

8. **`src/ndr/config/project_parameters_loader.py`**
   - **Issue**: project-parameter loading still assumes single-table model.
   - **Violation**: §11.3 points 5–6.
   - **Fix**:
     - Replace with DPP/MLP config resolvers.
     - Add explicit linkage checks (`project_name -> ml_project_name`, `ml_project_name -> project_name`).
     - Keep/rename batch-index table resolver to normalized naming.
   - **Why**: enforces correct subsystem boundaries and identity mapping.

9. **`docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`**
10. **`docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`**
11. **`docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`**
12. **`docs/step_functions_jsonata/sfn_ndr_prediction_publication.json`**
   - **Issue (all)**: still load `project_parameters#<version>` from a single project-parameters table.
   - **Violation**: §11.3 points 5–6.
   - **Fix**:
     - Migrate each orchestration definition to split config reads and deterministic identity propagation.
     - Ensure DPP-scoped paths remain data-source rooted and MLP-scoped paths remain ml-project rooted.
   - **Why**: completes control-plane migration across all orchestrators.

#### C) Runtime arg/contract names and index schema naming normalization

13. **`src/ndr/scripts/run_delta_builder.py`**
14. **`src/ndr/scripts/run_pair_counts_builder.py`**
15. **`src/ndr/processing/delta_builder_job.py`**
16. **`src/ndr/processing/pair_counts_builder_job.py`**
17. **`src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`**
   - **Issue**: external/runtime contract currently exposes `mini_batch_s3_prefix` naming.
   - **Violation**: §11.3 point 1.
   - **Fix**:
     - Rename public runtime args/parameters to canonical path-first names.
     - If migration is needed, support alias + deprecation warning window, then remove alias in cleanup task.
   - **Why**: removes conceptual mismatch and aligns with producer payload semantics.

18. **`src/ndr/config/batch_index_loader.py`**
19. **`src/ndr/config/batch_index_writer.py`**
   - **Issue**: schema attribute naming still uses `batch_s3_prefix`.
   - **Violation**: §11.3 points 1 and 5.
   - **Fix**:
     - Rename storage attribute to canonical ingestion path name (or formalize temporary dual-read/dual-write migration).
     - Preserve idempotent key/condition semantics.
   - **Why**: keeps index schema semantically consistent with refactor contract.

#### D) Test suite and documentation synchronization for final contract

20. **`tests/test_io_contract.py`**
   - **Issue**: asserts deprecated/legacy pointer arg naming.
   - **Violation**: §11.3 points 1 and 7.
   - **Fix**: update assertions to canonical runtime arg names and expected aliases (if transitional).

21. **`tests/test_step_functions_item19_contracts.py`**
   - **Issue**: contract assertions still pinned to old pointer naming and update-expression literals.
   - **Violation**: §11.3 points 1, 5.
   - **Fix**: align expected expressions/parameters with canonical naming + split-table state transitions.

22. **`tests/test_create_ml_projects_parameters_table.py`**
23. **`tests/test_project_parameters_loader.py`**
   - **Issue**: tests enforce legacy single-table/project-parameters key pattern.
   - **Violation**: §11.3 points 5–6.
   - **Fix**: replace with split-table seed/loader/linkage tests.

24. **`tests/test_step_functions_jsonata_contracts.py`**
   - **Issue**: state-name expectations implicitly assume legacy single-table load flow.
   - **Violation**: §11.3 points 5–6.
   - **Fix**: update tests to validate new resolver states and split control-plane contract.

25. **`docs/pipelines/pipelines_flow_description.md`**
26. **`docs/archive/debug_records/refactoring_plan_dpp_mlp.md`**
   - **Issue**: naming/contract language drift and metadata inconsistency.
   - **Violation**: §11.3 point 7 + docs alignment requirement.
   - **Fix**:
     - synchronize flow descriptions with final contract and selected normalized naming.
     - update plan metadata/version text to reflect post-fix completion status accurately.

### 11.5 Additional required documentation-principles alignment fix

27. **Project-wide documentation principle alignment (all relevant docs)**
   - **Issue**: some docs describe direct/static prefix composition patterns or mixed ownership boundaries.
   - **Required principle**:
     - base S3 prefixes are sourced from DDB config tables,
     - runtime composes only date-partition and batch-id suffixes,
     - DPP prefix roots use `project_name` (= data source),
     - MLP prefix roots use `ml_project_name`.
   - **Fix**:
     - audit all architecture/orchestration/pipeline docs for these principles,
     - correct any contradicting examples, and
     - add one concise “prefix composition rules” section to each high-level orchestration doc.
   - **Why**: protects long-term consistency beyond this specific refactor.

### 11.6 Additional implementor guidance for successful execution

To maximize delivery quality and reduce regression risk, implement in the following order:

1. **Contract naming and schema decisions first**
   - finalize canonical field/table/env names,
   - define compatibility alias policy and end-of-life date for aliases.

2. **Control-plane migration second**
   - implement split loaders and seeders,
   - migrate Step Functions DDB read states.

3. **Runtime argument migration third**
   - update 15m SF, pipeline definitions, scripts, and builder runtime configs in one cohesive change.

4. **Batch-index compatibility-safe migration**
   - if renaming stored attributes, use dual-read/dual-write transitional window when required.

5. **Tests/docs in same PR as behavior changes**
   - keep contract tests synchronized with code changes.

6. **Validation gates before merge**
   - targeted tests for Step Functions contracts, loader/seed logic, batch-index read/write, and builders.
   - confirm no stale `batch_s3_prefix` canonical references remain except in explicit compatibility notes.
   - confirm no stale single-table assumptions remain in code/docs/tests.

7. **Operational readiness checks**
   - log final resolved DPP/MLP identities per branch,
   - include explicit failure causes for missing linkage/config,
   - ensure reverse-lookup and backfill paths remain deterministic.

### 11.7 Completion addendum for this section

This addendum is complete when:

- all items A–D plus item 27 are implemented,
- all renamed contract fields are consistently applied or explicitly aliased with retirement notes,
- docs and tests fully match the final implemented contract,
- DPP and MLP flows are both project-agnostic and correctly scoped by their respective identities.

### 11.8 Deterministic migration matrix: canonical field names and phase behavior

This matrix is normative and machine-checkable. Implementations MUST follow it exactly.

#### 11.8.1 Canonical runtime vocabulary (final target)

- Canonical full raw-parsed-logs prefix field: `raw_parsed_logs_s3_prefix`.
  - Semantic definition: full S3 prefix up to and including `batch_id`, i.e.    `s3://<S3-ingestion-bucket>/<data_source_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<batch_id>/`
- Canonical batch identity field: `batch_id`.
- Canonical source identity field: `data_source_name` (mapped to DPP `project_name`).
- Canonical event-time field: `etl_timestamp` (ISO-8601 UTC `Z`).

Deprecated names to retire:

- `batch_s3_prefix`
- `mini_batch_s3_prefix`
- `MiniBatchS3Prefix`
- `ingestion_s3_path`
- `IngestionS3Path`
- CLI flags `--mini-batch-s3-prefix`, `--ingestion-s3-path`

#### 11.8.2 Old -> new map (strict)

| Legacy name | New canonical name | Surface | Final status |
|---|---|---|---|
| `batch_s3_prefix` | `raw_parsed_logs_s3_prefix` | JSON payload / DDB attribute alias | removed at P2 |
| `mini_batch_s3_prefix` | `raw_parsed_logs_s3_prefix` | internal runtime field / JSON alias | removed at P2 |
| `ingestion_s3_path` | `raw_parsed_logs_s3_prefix` | transitional alias | removed at P2 |
| `MiniBatchS3Prefix` | `RawParsedLogsS3Prefix` | pipeline parameter | removed at P2 |
| `IngestionS3Path` | `RawParsedLogsS3Prefix` | pipeline parameter alias | removed at P2 |
| `--mini-batch-s3-prefix` | `--raw-parsed-logs-s3-prefix` | CLI runtime arg | removed at P2 |
| `--ingestion-s3-path` | `--raw-parsed-logs-s3-prefix` | CLI alias | removed at P2 |
| `batch_s3_prefix` (batch-index attr) | `raw_parsed_logs_s3_prefix` | batch-index row attribute | legacy attr removed at P2 |

#### 11.8.3 Phase gates and accepted-name policy

- **P0 (compatibility introduction, mandatory first merge):**
  - Writers emit canonical names (`raw_parsed_logs_s3_prefix`, `RawParsedLogsS3Prefix`, `--raw-parsed-logs-s3-prefix`).
  - Readers accept canonical plus legacy aliases.
  - Any legacy-name usage must emit deterministic warning log:
    - code: `LegacyFieldNameUsed`
    - field: `<legacy_name>`
- **P1 (compatibility narrowing):**
  - Canonical names required for writes.
  - Readers accept legacy names only at ingress boundary layers (15m SF input parsing + batch-index dual-read).
  - Internal modules (runtime config, pipeline args, builders) are canonical-only.
- **P2 (final cleanup):**
  - Legacy names removed from code, tests, and docs.
  - No legacy-name parser branches remain.
  - Any legacy field in payload hard-fails with deterministic validation error:
    - code: `RuntimeParameterValidationError`
    - cause includes: `legacy field not supported`

### 11.9 Deterministic naming freeze: table/env names and cutover windows

This matrix freezes the final naming target and cutover expectations.

#### 11.9.1 Final table names (canonical)

- DPP config table: `dpp_config`
- MLP config table: `mlp_config`
- batch index table: `batch_index`

Legacy names slated for removal:

- `ndr_dpp_config`
- `ndr_mlp_config`
- `ndr_batch_index`

#### 11.9.2 Final env var names (canonical)

- `DPP_CONFIG_TABLE_NAME`
- `MLP_CONFIG_TABLE_NAME`
- `BATCH_INDEX_TABLE_NAME`

Legacy env vars slated for removal:

- `NDR_BATCH_INDEX_TABLE_NAME`
- `BATCH_INDEX_DDB_TABLE_NAME`
- any table env var keyed to legacy single-table model for DPP/MLP control-plane loading.

#### 11.9.3 Cutover window and enforcement

- **W0 (P0):** canonical + legacy env vars accepted; canonical takes precedence.
- **W1 (P1):** canonical env vars required in deploy templates; legacy still read only as fallback with warning.
- **W2 (P2):** legacy env vars unsupported; startup hard-fails if only legacy env vars are set.

Deterministic startup failure format at W2:

- exception class: `ValueError`
- message pattern: `Missing required env var: <CANONICAL_ENV_VAR>`

### 11.10 Hard acceptance matrix (machine-checkable completion sign-off)

All commands MUST pass exactly as specified below for completion approval.

#### 11.10.1 Static contract checks

1. Canonical field names present; legacy names absent from runtime code/docs/tests (except archived debug docs).

```bash
rg -n "raw_parsed_logs_s3_prefix|RawParsedLogsS3Prefix|--raw-parsed-logs-s3-prefix" src docs tests
```
Expected:
- one or more matches.

```bash
rg -n "batch_s3_prefix|mini_batch_s3_prefix|MiniBatchS3Prefix|ingestion_s3_path|IngestionS3Path|--mini-batch-s3-prefix|--ingestion-s3-path" src tests docs --glob '!docs/archive/**'
```
Expected:
- zero matches.

2. Final table names present; legacy `ndr_` table names absent from active contracts.

```bash
rg -n "dpp_config|mlp_config|batch_index" src docs tests
```
Expected:
- matches in loaders/docs/tests.

```bash
rg -n "ndr_dpp_config|ndr_mlp_config|ndr_batch_index" src tests docs --glob '!docs/archive/**'
```
Expected:
- zero matches (archive/history files are excluded by command scope).

3. Step Functions contract no longer loads legacy single-table `project_parameters#` records.

```bash
rg -n "project_parameters#|LoadProjectParametersFromDynamo" docs/step_functions_jsonata/sfn_ndr_*.json
```
Expected:
- zero matches.

#### 11.10.2 Unit/contract test execution gates

```bash
PYTHONPATH=src pytest -q tests/test_step_functions_item19_contracts.py tests/test_step_functions_jsonata_contracts.py tests/test_io_contract.py
```
Expected:
- exit code 0.

```bash
PYTHONPATH=src pytest -q tests/test_create_ml_projects_parameters_table.py tests/test_project_parameters_loader.py tests/test_batch_index_loader.py tests/test_batch_index_writer.py tests/test_historical_windows_extractor_job.py
```
Expected:
- exit code 0.

```bash
PYTHONPATH=src pytest -q tests/test_pair_counts_builder_job.py tests/test_palo_alto_batch_utils.py
```
Expected:
- exit code 0.

#### 11.10.3 Runtime/config sanity gates

```bash
python -m py_compile src/ndr/config/job_spec_loader.py src/ndr/config/project_parameters_loader.py src/ndr/config/batch_index_loader.py src/ndr/config/batch_index_writer.py src/ndr/scripts/run_delta_builder.py src/ndr/scripts/run_pair_counts_builder.py src/ndr/processing/delta_builder_job.py src/ndr/processing/pair_counts_builder_job.py
```
Expected:
- exit code 0.

#### 11.10.4 Completion verdict rule

Implementation is approved only if all commands in §11.10.1–§11.10.3 pass, with no waived failures.
Any failure is a blocker and must be fixed in the same delivery stream before marking this refactor complete.

### 11.11 Plan-level reconciliation of predecessor sections and migration sequencing

To resolve internal contradictions across this file, apply this mandatory reconciliation sequence:

1. **Precedence enforcement commit (docs-only):**
   - annotate §§1–10 with a short note: `Superseded by §11 where conflicting`.
   - do not change behavior in this step.

2. **Contract harmonization commit:**
   - update §§1–4 canonical payload/field/table naming to match §11.8/§11.9 exactly.
   - remove contradictory references to `batch_s3_prefix`, `mini_batch_s3_prefix`, and non-canonical table/env names from active sections.

3. **Implementation migration commits (P0 -> P1 -> P2):**
   - P0: canonical-write + alias-read introduction.
   - P1: internal canonical-only, boundary alias tolerance.
   - P2: alias removal and hard-fail on legacy names.

4. **Acceptance-gate commit:**
   - run and record all §11.10 commands with pass/fail outcomes.
   - any failure blocks closure.

5. **Final plan coherence commit:**
   - verify no contradictions remain between §11 and predecessor sections.
   - update “Version bump and changelog” to final post-reconciliation status.

No task may be marked complete unless this sequence is followed in order.

### 11.12 Stage S5 closure report and final sign-off evidence

#### 11.12.1 Final coherence verification (predecessor sections vs §11)

- §§1–10 retain explicit supersession notes and do not override §11 governance.
- Canonical field naming in predecessor sections aligns with §11.8 (`raw_parsed_logs_s3_prefix`, `RawParsedLogsS3Prefix`, `--raw-parsed-logs-s3-prefix`).
- Canonical table/env naming in predecessor sections aligns with §11.9 (`dpp_config`, `mlp_config`, `batch_index`; canonical env var set).
- No contradictory active references to deprecated payload/pointer/table names remain outside archive scope.

#### 11.12.2 Stage sequence and gate-outcome evidence

- Completed sequence: `S0 -> S1 -> S2 -> S3 -> S4 -> S5`.
- S5 validation gate re-ran full §11.10 acceptance matrix with all commands passing.

#### 11.12.3 §11.10 execution evidence (S5 rerun)

1. Static contract checks
   - `rg -n "raw_parsed_logs_s3_prefix|RawParsedLogsS3Prefix|--raw-parsed-logs-s3-prefix" src docs tests`
     - Result: **pass** (canonical field names present across src/docs/tests).
   - `rg -n "batch_s3_prefix|mini_batch_s3_prefix|MiniBatchS3Prefix|ingestion_s3_path|IngestionS3Path|--mini-batch-s3-prefix|--ingestion-s3-path" src tests docs --glob '!docs/archive/**'`
     - Result: **pass** (no matches; command exited 1 as expected for zero results).
   - `rg -n "dpp_config|mlp_config|batch_index" src docs tests`
     - Result: **pass** (canonical table names present).
   - `rg -n "ndr_dpp_config|ndr_mlp_config|ndr_batch_index" src tests docs --glob '!docs/archive/**'`
     - Result: **pass** (no matches; command exited 1 as expected for zero results).
   - `rg -n "project_parameters#|LoadProjectParametersFromDynamo" docs/step_functions_jsonata/sfn_ndr_*.json`
     - Result: **pass** (no matches; command exited 1 as expected for zero results).

2. Unit/contract test execution gates
   - `PYTHONPATH=src pytest -q tests/test_step_functions_item19_contracts.py tests/test_step_functions_jsonata_contracts.py tests/test_io_contract.py`
     - Result: **pass** (`21 passed in 0.15s`).
   - `PYTHONPATH=src pytest -q tests/test_create_ml_projects_parameters_table.py tests/test_project_parameters_loader.py tests/test_batch_index_loader.py tests/test_batch_index_writer.py tests/test_historical_windows_extractor_job.py`
     - Result: **pass** (`20 passed in 0.18s`).
   - `PYTHONPATH=src pytest -q tests/test_pair_counts_builder_job.py tests/test_palo_alto_batch_utils.py`
     - Result: **pass** (`8 passed in 0.64s`).

3. Runtime/config sanity gate
   - `python -m py_compile src/ndr/config/job_spec_loader.py src/ndr/config/project_parameters_loader.py src/ndr/config/batch_index_loader.py src/ndr/config/batch_index_writer.py src/ndr/scripts/run_delta_builder.py src/ndr/scripts/run_pair_counts_builder.py src/ndr/processing/delta_builder_job.py src/ndr/processing/pair_counts_builder_job.py`
     - Result: **pass** (exit code 0).

#### 11.12.4 Final closure verdict

- **Verdict:** approved/finalized.
- **Blocking failures:** none.
- **Rollback anchor:** revert this S5 closure commit to return plan metadata/changelog/sign-off evidence to pre-closure state.
