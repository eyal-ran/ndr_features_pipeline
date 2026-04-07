# DynamoDB Contract Spec: DPP/MLP Control Plane + Batch Index (Task 1 freeze)

## Scope

This spec freezes foundational contracts for:

1. `dpp_config` (DPP ownership)
2. `mlp_config` (MLP ownership)
3. `batch_index` (dual-item batch/run index)

## 1) `dpp_config`

### Keys

- PK: `project_name` (S)
- SK: `job_name_version` (S), format: `<job_name>#<feature_spec_version>`

### Mandatory ownership (frozen)

- DPP↔MLP linkage (`ml_project_names`, array-first)
- DPP S3 root ownership for shared processing domains (`delta`, `pair_counts`, `fg_a`, `fg_b`, `fg_c`, `machine_inventory`)
- DPP-owned code prefix roots and DPP step `code_metadata` roots
- DPP-owned unload source descriptors for monthly/backfill flows

## 2) `mlp_config`

### Keys

- PK: `ml_project_name` (S)
- SK: `job_name_version` (S)

### Mandatory ownership (frozen)

- MLP S3 roots for branch-scoped outputs (`predictions`, `prediction_join`, `publication`)
- MLP training/production artifact roots
- MLP model definition and evaluation policy contracts (authoritative source for training/evaluation consumers)

## 3) `batch_index`

### Primary key (exact)

- PK: `PK` (S) = `<project_name>`
- SK: `SK` (S)

### Dual-item schema (exact)

- Item A (direct): `SK = <batch_id>`
- Item B (reverse date): `SK = <YYYY/MM/dd>#<hh>#<within_hour_run_number>`

### Required Item A fields

- `PK`, `SK`, `batch_id`
- `date_partition`, `hour`, `within_hour_run_number`, `etl_ts`
- `org1`, `org2`, `raw_parsed_logs_s3_prefix`
- `ml_project_names`
- `s3_prefixes` with canonical DPP + per-MLP branches
- `rt_flow_status`, `backfill_status`, `source_mode`, `last_updated_at`

### Required Item B fields

- `PK`, `SK`, `batch_id`, `batch_lookup_sk`
- `date_partition`, `hour`, `within_hour_run_number`, `etl_ts`
- `org1`, `org2`

### Reverse lookup policy

No GSI is part of the frozen greenfield contract. Reverse/date scans are done by `begins_with(SK, '<YYYY/MM/dd>#')` under `PK=<project_name>`.

## 4) Canonical `s3_prefixes` and `code_metadata` keys

### DPP branch keys

- `s3_prefixes.dpp.delta`
- `s3_prefixes.dpp.pair_counts`
- `s3_prefixes.dpp.fg_a`
- `s3_prefixes.dpp.fg_a_subpaths.features`
- `s3_prefixes.dpp.fg_a_subpaths.metadata`
- `s3_prefixes.dpp.fg_c`
- `s3_prefixes.dpp.machine_inventory`
- `s3_prefixes.dpp.fg_b.machines_manifest`
- `s3_prefixes.dpp.fg_b.machines_unload_for_update`
- `s3_prefixes.dpp.fg_b.machines_base_stats`
- `s3_prefixes.dpp.fg_b.segment_base_stats`
- `s3_prefixes.dpp.code.<step_key>`
- `s3_prefixes.dpp.code_metadata.<step_key>`

### MLP branch keys

- `s3_prefixes.mlp.<ml_project_name>.predictions`
- `s3_prefixes.mlp.<ml_project_name>.prediction_join`
- `s3_prefixes.mlp.<ml_project_name>.publication`
- `s3_prefixes.mlp.<ml_project_name>.training_events.training_reports`
- `s3_prefixes.mlp.<ml_project_name>.training_events.training_artifacts`
- `s3_prefixes.mlp.<ml_project_name>.production_artifacts.inference_model`
- `s3_prefixes.mlp.<ml_project_name>.code.<step_key>`
- `s3_prefixes.mlp.<ml_project_name>.code_metadata.<step_key>`

### `code_metadata` required fields

- `artifact_mode`
- `artifact_build_id`
- `artifact_sha256`

