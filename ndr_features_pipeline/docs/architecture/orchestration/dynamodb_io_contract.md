# DynamoDB IO Contract (Task 1 frozen foundations)

This document defines the frozen, code-facing control-plane/data-plane contracts used by shared loaders/builders.

## 1) Table key contracts

### DPP table (`dpp_config`)
- PK: `project_name`
- SK: `job_name_version` (`<job_name>#<feature_spec_version>`)

### MLP table (`mlp_config`)
- PK: `ml_project_name`
- SK: `job_name_version`

### Batch Index table (`batch_index`)
- PK: `PK = <project_name>`
- SK: one of:
  - `<batch_id>` (direct lookup item)
  - `<YYYY/MM/dd>#<hh>#<within_hour_run_number>` (reverse/date lookup item)

## 2) Ownership boundaries (frozen)

### DPP owns
- `ml_project_names` linkage for each `project_name`
- DPP root prefixes and DPP step code roots
- DPP unload source descriptors and data-processing runtime defaults

### MLP owns
- branch-scoped MLP roots (`predictions`, `prediction_join`, `publication`, training/production artifacts)
- model-definition and evaluation policy contracts by `ml_project_name`

### Batch Index owns
- precomputed batch-scoped resolved prefixes (`s3_prefixes`) and runtime attributes (`date_partition`, `hour`, `within_hour_run_number`, `etl_ts`, org fields)

## 3) Required Batch Index contract shape

### Item A (`SK=<batch_id>`)
Must include:
- `batch_id`, `raw_parsed_logs_s3_prefix`, `ml_project_names`
- `date_partition`, `hour`, `within_hour_run_number`, `etl_ts`
- `s3_prefixes` (`dpp` + `mlp` branch maps)
- status fields (`rt_flow_status`, `backfill_status`, `source_mode`, `last_updated_at`)

### Item B (`SK=<YYYY/MM/dd>#<hh>#<within_hour_run_number>`)
Must include:
- `batch_id`, `batch_lookup_sk`
- `date_partition`, `hour`, `within_hour_run_number`, `etl_ts`

## 4) Lookup modes expected by consumers

- **Direct lookup** (RT, inference, publication, training): `PK=project_name`, `SK=batch_id`
- **Reverse/date lookup** (backfill planners/extractors): `PK=project_name`, `begins_with(SK, '<YYYY/MM/dd>#')`
- **MLP branch lookup**: direct item + `s3_prefixes.mlp.<ml_project_name>`

## 5) Canonical code metadata

Every step metadata entry under `s3_prefixes.*.code_metadata.*` must include:
- `artifact_mode`
- `artifact_build_id`
- `artifact_sha256`

This is the reproducibility boundary for deployed single-file step artifacts.
