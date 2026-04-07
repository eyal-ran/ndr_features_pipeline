# Step Functions Orchestration Contract (vNext)

## Scope

This document defines the orchestration contract required for DPP/MLP decoupling.
For Task 1, this is a documentation foundation only.

## 1) Canonical ingestion payload contract (input to 15m SF)

### Single-MLP payload (exact)

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

### Multi-MLP payload (exact)

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
- `ml_project_names` is the canonical pre-branch selector and must be normalized before any MLP-specific DDB read.
- Scalar `ml_project_name` is accepted only as compatibility input and must be normalized into `ml_project_names=[ml_project_name]`.
- The normalized `ml_project_names` array must be non-empty, string-only, and de-duplicated.
- `batch_id` is non-empty.
- `raw_parsed_logs_s3_prefix` starts with `s3://` and ends with `/<batch_id>/`.
- `timestamp` is ISO-8601 UTC (`...Z`).

## 2) Orchestration-resolved runtime fields

- `project_name`
- `data_source_name`
- `ml_project_name` (per branch)
- `ml_project_names` (only pre-Map)
- `feature_spec_version`
- `mini_batch_id` (= `batch_id`)
- `raw_parsed_logs_s3_prefix` (= ingestion batch prefix)
- Legacy aliases are unsupported and must fail validation.
- `batch_start_ts_iso`
- `batch_end_ts_iso`
- `date_utc` (YYYY-MM-DD)
- `hour_utc` (00..23)
- `slot15` (1..4)

## 3) Pipeline parameter contract

### Required for 15m path

- `ProjectName`
- `FeatureSpecVersion`
- `MiniBatchId`
- `RawParsedLogsS3Prefix`
- `BatchStartTsIso`
- `BatchEndTsIso`

### Optional with explicit predicates

- `MlProjectName` is required when executing a single-MLP branch.
- No `MlProjectNamesJson` fan-out payload is passed to the 15m features pipeline; fan-out happens in RT father orchestration Map state.

### 3.1 Task-0 compatibility lock

- Auditable matrix source: `docs/archive/debug_records/task0_contract_compatibility_matrix.json`.
- For business parameters, allowed sources are only DDB contract data or flow payload values.
- Environment placeholder defaults (for example `${DefaultFeatureSpecVersion}`) are forbidden for business-parameter resolution.
- SF → pipeline parameter sets must exactly match declared pipeline parameters; undeclared/missing parameters fail validation.

## 4) Batch-index orchestration requirement

Before branch execution starts, RT orchestration prewrites canonical `batch_index` dual items using deterministic idempotent rules:

1. `PutItem` for batch lookup item (`PK=<project_name>, SK=<batch_id>`) with `attribute_not_exists(PK) AND attribute_not_exists(SK)`.
2. If duplicate, continue via deterministic `UpdateItem` on the same key with `attribute_exists(PK) AND attribute_exists(SK)`.
3. `PutItem` for reverse date lookup item (`PK=<project_name>, SK=<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) with `batch_lookup_sk=<batch_id>`.

This behavior is replay-safe and required for recovery/backfill lookup paths.

## 5) Finalized compatibility posture (Task 7)

Compatibility toggles are removed and orchestration runs in vNext-only mode:

- `RawParsedLogsS3Prefix` is required for the 15m path.
- Canonical Palo Alto path parsing (`fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`) is mandatory.
- No legacy fallback/parser/listing compatibility flags are part of the contract.

## 6) Task 7.1 frozen selective-backfill planner/manifest contract

### Canonical planner families (in deterministic order)

1. `delta`
2. `fg_a`
3. `pair_counts`
4. `fg_b_baseline`
5. `fg_c`

### Canonical range object

Each family range is represented as:

```json
{
  "start_ts": "2024-04-01T00:00:00Z",
  "end_ts": "2024-04-01T00:15:00Z"
}
```

Rules:
- UTC ISO-8601 `Z` timestamps only.
- `start_ts < end_ts`.
- overlapping ranges are normalized before execution.

### Canonical manifest schema (`backfill_manifest.v1`)

Produced by the historical windows extractor and by training-triggered backfill caller payload shaping:

```json
{
  "contract_version": "backfill_manifest.v1",
  "generated_at": "2026-04-07T12:00:00Z",
  "project_name": "fw_paloalto",
  "feature_spec_version": "v1",
  "planner_mode": "self_detect|caller_guided",
  "source": "historical_windows_extractor|if_training_remediation",
  "run_id": "optional",
  "family_plan": [
    {
      "family": "delta",
      "execute": true,
      "reason": "missing_ranges_detected",
      "ranges": [{"start_ts": "...", "end_ts": "..."}]
    }
  ],
  "map_items": [
    {
      "project_name": "fw_paloalto",
      "feature_spec_version": "v1",
      "family": "delta",
      "range_start_ts": "...",
      "range_end_ts": "..."
    }
  ]
}
```

### Selective execution policy contract

- Planner sets `execute=true` only for families that both:
  - have missing ranges, and
  - satisfy dependency safety checks.
- Dependency-safe rule (for selective recovery): `fg_c` requires `fg_a`, `pair_counts`, and `fg_b_baseline` coverage in the same request scope.
- Non-requested families are encoded with `execute=false` and explicit `reason` values (`not_requested`, `no_missing_ranges`, `dependency_missing:<family>`), so map workers can deterministically skip.
