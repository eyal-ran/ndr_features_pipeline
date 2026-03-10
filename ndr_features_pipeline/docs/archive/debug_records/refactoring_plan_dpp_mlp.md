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
