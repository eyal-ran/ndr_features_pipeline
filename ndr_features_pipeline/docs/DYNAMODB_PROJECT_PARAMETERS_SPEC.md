# DynamoDB Contract Spec: DPP/MLP Control Plane + Batch Index (vNext)

## Scope

This spec defines the exact DynamoDB contracts for the DPP/MLP decoupling refactor:

1. `dpp_config` (control plane)
2. `mlp_config` (control plane)
3. `batch_index` (data plane)

Table count is fixed to these three tables in this refactor scope.

## 1) `dpp_config`

### Keys

- PK: `project_name` (S)
- SK: `job_name_version` (S), format: `<job_name>#<version>`

### Required attributes on DPP project-parameter records

- `data_source_name` (S)
- `ml_project_name` (S)
- `ml_project_names` (L of S, optional)
- `spec` (M)
- `updated_at` (S, ISO8601Z)

### Notes

- `project_name` is always the DPP id.
- `data_source_name` must match the same DPP id in vNext payload validation.

## 2) `mlp_config`

### Keys

- PK: `ml_project_name` (S)
- SK: `job_name_version` (S)

### Required attributes

- `project_name` (S) — source DPP id
- `spec` (M)
- `updated_at` (S)

## 3) `batch_index`

### Primary key (exact)

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

## 4) Idempotent batch-index write contract (exact)

All writes happen in 15m SF before starting the 15m pipeline.

### 4.1 Insert-if-absent

Use `PutItem` with:

- `ConditionExpression`: `attribute_not_exists(pk) AND attribute_not_exists(sk)`

If condition fails, treat as idempotent duplicate and continue.

### 4.2 Deterministic enrichment update

Use `UpdateItem` with:

- `Key`: `pk`, `sk`
- `UpdateExpression`:
  `SET raw_parsed_logs_s3_prefix = :raw_parsed_logs_s3_prefix, event_ts_utc = :event_ts_utc, ingested_at_utc = :ingested_at_utc, #status = :status, ml_project_name = if_not_exists(ml_project_name, :ml_project_name), ml_project_names_json = if_not_exists(ml_project_names_json, :ml_project_names_json), GSI1PK = :gsi1pk, GSI1SK = :gsi1sk`
- `ExpressionAttributeNames`:
  - `#status` -> `status`
- `ConditionExpression`:
  `attribute_exists(pk) AND attribute_exists(sk)`

## 5) Runtime contract linkage

The index schema above is bound to the runtime contract fields:

- `batch_id` -> `mini_batch_id`
- `raw_parsed_logs_s3_prefix` -> canonical runtime/pipeline surfaces
- Stage S3/P1 compatibility: legacy aliases are tolerated only at ingress/storage boundaries (`batch_s3_prefix`, `mini_batch_s3_prefix`) and canonical names are required for internal runtime writes/reads.
- `timestamp` + derived calendar components -> `date_utc`, `hour_utc`, `slot15`

Pipeline parameters for 15m path:

- Required: `ProjectName`, `FeatureSpecVersion`, `MiniBatchId`, `RawParsedLogsS3Prefix`, `BatchStartTsIso`, `BatchEndTsIso`
- Optional by predicate: `MlProjectName`, `MlProjectNamesJson`

P1 note: canonical write is `RawParsedLogsS3Prefix`; legacy aliases are read only at ingress/storage boundaries with deterministic warning logs (`LegacyFieldNameUsed`).
