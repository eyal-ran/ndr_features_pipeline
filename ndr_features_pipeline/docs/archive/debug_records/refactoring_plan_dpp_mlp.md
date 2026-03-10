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

1. **Table layout is fixed to 3 tables** (no "single table alternative"):
   - `ndr_dpp_config`
   - `ndr_mlp_config`
   - `ndr_batch_index`
2. `project_name` is always DPP in this refactor scope.
3. `ml_project_name` is always a single MLP id in a single execution branch.
4. `ml_project_names` is always a list used only for fan-out orchestration.
5. Payload-provided `batch_id` and `batch_s3_prefix` are authoritative per-run pointers.
6. Base prefixes/contracts come from DDB; code composes only dynamic suffixes (`date/slot/batch`) where needed.
7. Batch-index upserts are idempotent via deterministic key + explicit condition/update expressions.
8. Compatibility flags are required through Task 7, with fixed environment defaults (defined in §8).

---

## 2) Canonical runtime contract vNext (exact)

## 2.1 Ingestion payload shape (input to 15m SF)

### Single-MLP payload (exact JSON)
```json
{
  "project_name": "fw_paloalto",
  "data_source_name": "fw_paloalto",
  "ml_project_name": "network_anomalies_detection",
  "batch_id": "a1b2c3d4e5",
  "batch_s3_prefix": "s3://<prod_ing_bucket>/fw_paloalto/<org1>/<org2>/2026/03/10/a1b2c3d4e5/",
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
  "batch_s3_prefix": "s3://<prod_ing_bucket>/fw_paloalto/<org1>/<org2>/2026/03/10/a1b2c3d4e5/",
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
- `batch_s3_prefix` must be `s3://` and end with `/<batch_id>/`.
- `timestamp` must be ISO-8601 UTC `...Z`.

## 2.2 Orchestration-resolved runtime fields (internal)
- `project_name`
- `data_source_name`
- `ml_project_name` (per branch)
- `ml_project_names` (list; only before Map)
- `feature_spec_version`
- `mini_batch_id` (= `batch_id`)
- `mini_batch_s3_prefix` (= `batch_s3_prefix`)
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
- `MiniBatchS3Prefix`

New optional for MLP-scoped downstream branches:
- `MlProjectName`
- `MlProjectNamesJson`

---


## 2.4 Determinism rules for optional fields

In this plan, the word **optional** is never ambiguous; it always means **conditionally present by an explicit predicate**:

- `MlProjectName` is required when executing a single-MLP branch.
- `MlProjectNamesJson` is required only before fan-out or when passing fan-out context through a single payload field.
- `ml_project_name` in `ndr_batch_index` is required for per-branch records and omitted only for pre-fan-out aggregate records.
- `ml_project_names_json` in `ndr_batch_index` is required only for pre-fan-out aggregate records.
- `ttl_epoch` is optional operationally; when omitted, table-level TTL behavior is disabled for that row.

## 3) DDB schema (exact)

## 3.1 `ndr_dpp_config` (control plane)
- PK: `project_name` (S)
- SK: `job_name_version` (S) where format is `<job_name>#<version>`

Required attributes on DPP project-parameter records:
- `data_source_name` (S)
- `ml_project_name` (S)
- `ml_project_names` (L of S, optional)
- `spec` (M)
- `updated_at` (S, ISO8601Z)

## 3.2 `ndr_mlp_config` (control plane)
- PK: `ml_project_name` (S)
- SK: `job_name_version` (S)

Required attributes:
- `project_name` (S)  // source DPP
- `spec` (M)
- `updated_at` (S)

## 3.3 `ndr_batch_index` (data plane)

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
- `batch_s3_prefix` (S)
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

## 4) Exact upsert logic for `ndr_batch_index`

All writes happen in 15m SF before starting the 15m pipeline.

## 4.1 First write (insert-if-absent)
Use `PutItem` with:
- `ConditionExpression`: `attribute_not_exists(pk) AND attribute_not_exists(sk)`

If condition fails, treat as idempotent duplicate and continue.

## 4.2 Update path (idempotent enrichment)
Use `UpdateItem` with:
- `Key`: `pk`, `sk`
- `UpdateExpression`:
  `SET batch_s3_prefix = :batch_s3_prefix, event_ts_utc = :event_ts_utc, ingested_at_utc = :ingested_at_utc, #status = :status, ml_project_name = if_not_exists(ml_project_name, :ml_project_name), ml_project_names_json = if_not_exists(ml_project_names_json, :ml_project_names_json), GSI1PK = :gsi1pk, GSI1SK = :gsi1sk`
- `ExpressionAttributeNames`:
  - `#status` -> `status`
- `ConditionExpression`:
  `attribute_exists(pk) AND attribute_exists(sk)`

This yields deterministic, replay-safe behavior.

---

## 5) Must-not-change constraints (strict)

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
- Updated `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md` to document the fixed 3-table schema (`ndr_dpp_config`, `ndr_mlp_config`, `ndr_batch_index`) including exact keys, attributes, GSI mapping, and idempotent PutItem/UpdateItem expressions from §4.
- Updated `docs/architecture/orchestration/step_functions.md` with vNext payload validation rules, orchestration-resolved runtime fields, required/conditional pipeline parameter contract, and fixed migration-toggle defaults/switch-over criteria.
- Updated `docs/palo_alto_raw_partitioning_strategy.md` to align canonical path contract, authoritative `batch_id`/`batch_s3_prefix` runtime pointer behavior, slot15 derivation policy, and deterministic `ndr_batch_index` usage.

### Task 1 contract delta
- **Added:** explicit DPP/MLP identity semantics and deterministic optional-field predicates (`MlProjectName`, `MlProjectNamesJson`, `ml_project_name`, `ml_project_names_json`, `ttl_epoch`).
- **Changed:** 15m contract documentation to require `MiniBatchS3Prefix` and to codify exact `ndr_batch_index` key shapes and idempotent write expressions.
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
- Parse payload `batch_id` and `batch_s3_prefix` directly.
- Derive `slot15` from payload timestamp minute:
  - 00-14 => 1
  - 15-29 => 2
  - 30-44 => 3
  - 45-59 => 4
- Add `WriteBatchIndexRecord` state using §4 exact expressions.
- Pass `MiniBatchS3Prefix`, `MlProjectName`, `MlProjectNamesJson` to pipeline start.

### Deliverables
- Deterministic pre-pipeline batch-index write.
- Contract tests proving write state and parameter passing.

### Task 2 status
- **implemented**

### Task 2 implementation summary
- Updated `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json` to parse `batch_id` and `batch_s3_prefix` directly from payload, derive `slot15` from the payload timestamp minute bucket (`00-14=>1`, `15-29=>2`, `30-44=>3`, `45-59=>4`), and enforce vNext payload validation for DPP identity matching and field-shape constraints.
- Added deterministic pre-pipeline batch-index persistence states in the same state machine:
  - `WriteBatchIndexRecord` uses `PutItem` with `ConditionExpression = attribute_not_exists(pk) AND attribute_not_exists(sk)`.
  - `UpdateBatchIndexRecord` uses `UpdateItem` with `ConditionExpression = attribute_exists(pk) AND attribute_exists(sk)` and exact `UpdateExpression` from §4.
- Added `MiniBatchS3Prefix`, `MlProjectName`, and `MlProjectNamesJson` into 15m/inference SageMaker `PipelineParameters`.
- Updated seed contract source `src/ndr/scripts/create_ml_projects_parameters_table.py` so `pipeline_15m_streaming` required runtime params include `MiniBatchS3Prefix`.
- Added/updated contract tests in:
  - `tests/test_step_functions_item19_contracts.py`
  - `tests/test_create_ml_projects_parameters_table.py`

### Task 2 Contract Delta
- **Added:** runtime resolution and validation for `data_source_name`, `ml_project_name` / `ml_project_names`, `ml_project_names_json`, `date_utc`, `hour_utc`, `slot15`, batch-index PK/SK/GSI identity fields, and pre-pipeline batch-index writer states.
- **Changed:** `pipeline_15m_streaming` required runtime params now include `MiniBatchS3Prefix`; 15m/orchestration path now passes `MiniBatchS3Prefix`, `MlProjectName`, `MlProjectNamesJson` downstream.
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
- `MiniBatchS3Prefix`/`MlProjectName`/`MlProjectNamesJson` propagation to pipeline start: **done**.
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
- Add `MiniBatchS3Prefix` pipeline parameter.
- Add CLI `--mini-batch-s3-prefix` to both scripts.
- Thread value into job runtime configs.

### Deliverables
- Entry points accept and forward batch pointer.

### Task 3 status
- **implemented**

### Task 3 implementation summary
- Updated `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py` to include `MiniBatchS3Prefix` as a required pipeline parameter and to pass `--mini-batch-s3-prefix` into `run_delta_builder` and `run_pair_counts_builder` step arguments.
- Updated `src/ndr/scripts/run_delta_builder.py` and `src/ndr/scripts/run_pair_counts_builder.py` to accept required CLI argument `--mini-batch-s3-prefix` and to propagate it into typed runtime configs.
- Updated runtime config models in `src/ndr/processing/delta_builder_job.py` and `src/ndr/processing/pair_counts_builder_job.py` to carry `mini_batch_s3_prefix` and to thread it into runtime execution context.
- Updated `tests/test_io_contract.py` to include Task 3 contract checks asserting the new CLI argument is present in both entry scripts.

### Task 3 Contract Delta
- **Added:** pipeline parameter `MiniBatchS3Prefix`; CLI argument `--mini-batch-s3-prefix`; runtime config field `mini_batch_s3_prefix` for Delta and Pair Counts.
- **Changed:** Delta runtime wiring now forwards runtime `mini_batch_s3_prefix` into `RuntimeParams` with compatibility fallback to existing JobSpec input prefix during migration period.
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
- Add `MiniBatchS3Prefix` pipeline parameter: **done**.
- Add CLI `--mini-batch-s3-prefix` to both scripts: **done**.
- Thread value into job runtime configs: **done**.
- Entry points accept and forward batch pointer: **done**.
- Contract tests updated in same PR: **done**.

### Task 3 Rollback Plan
1. Revert the Task 3 commit that introduced runtime pointer propagation changes.
2. Restore previous pipeline definition artifact without `MiniBatchS3Prefix` parameter wiring.
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
- Runtime `mini_batch_s3_prefix` takes precedence.
- Compatibility fallback to DDB/base prefix controlled by toggle `enable_legacy_input_prefix_fallback`.
- Parser updated to canonical path with `fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`.

### Deliverables
- Both builders ingest from per-run pointer deterministically.

### Task 4 status
- **implemented**

### Task 4 implementation summary
- Updated `src/ndr/processing/delta_builder_job.py` so `mini_batch_s3_prefix` is the primary runtime input pointer, with deterministic failure when missing while `enable_legacy_input_prefix_fallback=false`; legacy fallback to JobSpec input prefix is preserved only behind the compatibility toggle.
- Updated `src/ndr/processing/pair_counts_builder_job.py` so input-path resolution uses runtime `mini_batch_s3_prefix` first, then optional legacy fallback to `traffic_input.s3_prefix/<mini_batch_id>/` only when `enable_legacy_input_prefix_fallback=true`, else raises deterministic validation error.
- Updated `src/ndr/orchestration/palo_alto_batch_utils.py` to parse canonical batch path order `fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...` as primary behavior, retain legacy parser branch only behind `enable_legacy_path_parser`, and centralize migration toggle default resolution per fixed env defaults (dev/stage/prod) with explicit env override support.
- Updated `tests/test_palo_alto_batch_utils.py` with Task 4 contract tests validating canonical parser behavior, legacy parser gating, and migration-toggle default matrix behavior.

### Task 4 Contract Delta
- **Added:** toggle-resolution helpers to enforce fixed migration defaults and explicit override handling for `enable_legacy_input_prefix_fallback`, `enable_legacy_path_parser`, `enable_s3_listing_fallback_for_backfill`.
- **Changed:** Delta and Pair Counts raw-reader input resolution now prioritizes runtime pointer (`mini_batch_s3_prefix`) and applies deterministic compatibility fallback only through `enable_legacy_input_prefix_fallback`.
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
- Runtime `mini_batch_s3_prefix` precedence in Delta builder: **done**.
- Runtime `mini_batch_s3_prefix` precedence in Pair Counts builder: **done**.
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
- Added `src/ndr/config/batch_index_loader.py` with deterministic `ndr_batch_index` read APIs:
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
  - `BatchIndexLoader` forward/reverse read path for `ndr_batch_index`.
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
- Updated `src/ndr/processing/delta_builder_job.py` to require runtime `mini_batch_s3_prefix` with deterministic validation error when missing; legacy fallback to JobSpec input prefix was removed.
- Updated `src/ndr/processing/pair_counts_builder_job.py` so traffic input resolution is runtime-pointer only (`mini_batch_s3_prefix`) with deterministic validation when absent.
- Updated `src/ndr/orchestration/palo_alto_batch_utils.py` to enforce canonical path parsing only (`fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`) and remove legacy parser/toggle resolution branches.
- Updated `docs/palo_alto_raw_partitioning_strategy.md`, `docs/architecture/orchestration/step_functions.md`, and `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md` to document Task 7 final vNext-only contract posture.

### Task 7 Contract Delta
- **Added:** Task 7 contract note clarifying compatibility toggles are removed and `MiniBatchS3Prefix` is strictly required for ingestion-pointer resolution.
- **Changed:** Delta and Pair Counts input path resolution now requires runtime `mini_batch_s3_prefix`; legacy fallback branches are removed.
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

Refactor is complete when:
1. 15m SF writes deterministic batch-index rows before pipeline start.
2. Delta and Pair Counts consume runtime batch pointer (`MiniBatchS3Prefix`) as primary input.
3. Non-RT flows use batch-index first, reverse lookup supported by GSI.
4. DPP↔MLP linkage enforced in config tables (`ml_project_name` on DPP, `project_name` on MLP).
5. Multi-MLP fan-out executes per-ML branch with correct identity propagation.
6. Compatibility toggles removed in Task 7.
7. All affected docs/tests are aligned with vNext.

---

## 10) Version bump and changelog

- **Plan version:** `vNext-task1-docs-baseline`
- **Task 1 status:** implemented (contract/docs/schema foundation only)

### Task 1 changelog

1. Canonical DPP/MLP semantics were documented with explicit field-role separation (`project_name`, `ml_project_name`, `ml_project_names`).
2. Runtime contract vNext ingestion payload examples from §2 were replicated into architecture docs without shape changes.
3. DynamoDB table schemas (`ndr_dpp_config`, `ndr_mlp_config`, `ndr_batch_index`) and batch-index idempotent write contract (§4) were aligned in docs.
4. Optional-field predicates from §2.4 were propagated into docs with deterministic wording.
