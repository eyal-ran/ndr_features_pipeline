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
- Exactly one of `ml_project_name` or `ml_project_names` must be present.
- `ml_project_names` must be non-empty when present.
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
- Compatibility aliases accepted at ingress only: `batch_s3_prefix`, `mini_batch_s3_prefix`
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
- `MlProjectNamesJson` is required only before fan-out or when fan-out context is passed through one field.

## 4) Batch-index orchestration requirement

Before pipeline start, orchestration writes to `ndr_batch_index` using deterministic idempotent write rules:

1. `PutItem` with `attribute_not_exists(pk) AND attribute_not_exists(sk)`.
2. If duplicate, continue.
3. `UpdateItem` with deterministic `SET ... if_not_exists(...)` expression and `attribute_exists(pk) AND attribute_exists(sk)`.

This behavior is replay-safe and required for recovery/backfill lookup paths.

## 5) Finalized compatibility posture (Task 7)

Compatibility toggles are removed and orchestration runs in vNext-only mode:

- `RawParsedLogsS3Prefix` is required for the 15m path.
- Canonical Palo Alto path parsing (`fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`) is mandatory.
- No legacy fallback/parser/listing compatibility flags are part of the contract.
