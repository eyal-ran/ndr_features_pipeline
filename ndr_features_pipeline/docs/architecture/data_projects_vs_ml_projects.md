# Data Projects (DPP) vs ML Projects (MLP)

## Purpose

This document defines the canonical identity model for the DPP/MLP decoupling refactor.
It is normative for contract semantics and naming.

## Canonical identities

- `project_name` = **DPP id** (Data Processing Project).
- `ml_project_name` = **MLP id** (single ML project executed in one branch).
- `ml_project_names` = list of MLP ids, used only for fan-out orchestration before branching.
- `data_source_name` = DPP data-source id and must equal `project_name` for this scope.

## Runtime contract vNext (ingestion payload)

Exactly one payload shape must be used.

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

## Validation rules (strict)

- `project_name == data_source_name == <DPP id>`.
- Exactly one of:
  - `ml_project_name` (single branch), or
  - `ml_project_names` (non-empty list; pre-fan-out only).
- `batch_id` is non-empty.
- `raw_parsed_logs_s3_prefix` starts with `s3://` and ends with `/<batch_id>/`.
- `timestamp` is ISO-8601 UTC with `Z`.

## Conditional optional fields (deterministic)

- `MlProjectName` pipeline param is required in single-MLP branch execution.
- `MlProjectNamesJson` pipeline param is required only before fan-out or when fan-out context must be carried as one field.
- `ml_project_name` in `batch_index` is required for per-branch rows and omitted only for pre-fan-out aggregate rows.
- `ml_project_names_json` in `batch_index` is required only for pre-fan-out aggregate rows.
- `ttl_epoch` is operationally optional; if omitted, TTL behavior is disabled for that row.

## Deterministic routing model

1. Ingestion payload provides authoritative `batch_id` and canonical `raw_parsed_logs_s3_prefix`.
2. Base contracts/prefixes resolve from DynamoDB control-plane records.
3. Runtime composes only dynamic suffixes (`date/hour/slot/batch`) when needed.
4. For fan-out, one DPP payload can produce N MLP branches, each with exactly one `ml_project_name`.

## Compatibility policy

Compatibility toggles are removed. The active contract is canonical-only:

- legacy aliases are unsupported,
- canonical field names are required at ingress and runtime boundaries.
