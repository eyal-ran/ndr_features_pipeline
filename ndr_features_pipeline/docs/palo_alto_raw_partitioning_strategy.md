# Palo Alto Ingestion-Batch Strategy for ML Orchestration

## Scope

This document is strictly scoped to the **ML process** in this repository:

- Step Functions orchestration
- Delta Builder
- Pair Counts Builder
- runtime parameter resolution for ML jobs

It does not specify non-ML data-loading internals.

## Canonical assumptions and constraints

1. The upstream batch producer emits **one event per batch** to SNS.
2. The payload contains only:
   - full S3 object path inside the batch folder,
   - payload timestamp close to batch creation timestamp.
3. `project_name`, `mini_batch_id`, and `mini_batch_s3_prefix` must be derived from S3 path segments.
4. `feature_spec_version` and other defaults are resolved from DynamoDB by `project_name`.
5. No persistent batch-catalog table is introduced at this stage.

## ML-relevant input layout

For ML processing, the expected input is flattened JSON GZIP files in batch folders:

- `s3://<ingestion_bucket>/<org1>/<org2>/<project_name>/YYYY/MM/dd/<mini_batch_id>/<file>.json.gz`

Each hashed folder (`mini_batch_id`) is treated as one mini-batch candidate.

## Current builder contracts

### Delta Builder

Delta reads from one explicit runtime prefix (`mini_batch_s3_prefix`) passed by orchestration.

### Pair Counts Builder

Pair Counts reads from:

- `<traffic_input.s3_prefix>/<mini_batch_id>/`

and expects flat traffic columns (`source_ip`, `destination_ip`, `destination_port`, `event_start`, `event_end`).

## Timestamp and window derivation policy (authoritative)

Cron rule is fixed to `8-59/15 * * * ? *`.

For both inference and non-inference:

- valid batch floor minutes per hour are `08`, `23`, `38`, `53`.
- `batch_start_ts_iso` = floor of source timestamp to the nearest prior minute in `{08,23,38,53}`.
- `batch_end_ts_iso` = the actual source timestamp.

Source timestamp by flow:

- inference: payload timestamp from SNS/SQS message.
- non-inference/backfill: S3 object `LastModified` timestamp.

## Updated orchestration plan (implementable)

### Live inference flow

1. S3 producer event -> SNS -> SQS.
2. Step Functions receives normalized message (`s3_key`, payload timestamp).
3. Step Functions derives from `s3_key`:
   - `project_name`, `org1`, `org2`, `mini_batch_id`, `mini_batch_s3_prefix`.
4. Step Functions loads project defaults from DynamoDB by `project_name`.
5. Step Functions computes start/end by cron-floor policy.
6. Step Functions acquires lock (project + feature_spec_version + window).
7. Step Functions starts 15m pipeline.

### Initial deployment / historical back-processing

A preliminary **SageMaker Pipeline ProcessingStep** must:

1. enumerate historical batch folders by date range,
2. parse `project_name` + `mini_batch_id` from path,
3. resolve `feature_spec_version` from DynamoDB by `project_name`,
4. read representative object `LastModified`,
5. compute `batch_start_ts_iso` and `batch_end_ts_iso` by cron-floor policy,
6. emit execution units consumed by existing backfill orchestration.

No additional Step Function is required; reuse existing backfill state machine.

## Idempotency and duplicate suppression

Use existing lock-table conditional write in orchestration to prevent duplicate executions for the same `(project_name, feature_spec_version, batch_start_ts_iso, batch_end_ts_iso)`.

This is required for producer retries and message redelivery handling.

## Verification gate before implementation

Before implementing this plan, run a repository cleanliness check and ensure previous changes not part of this approved plan are reverted.

## Decision summary

- Keep the implementation ML-only and orchestration-centric.
- Derive runtime identity from path + DynamoDB, with minimal payload assumptions.
- Use cron-based start-time flooring (`08/23/38/53`) and actual timestamp as end-time.
- Reuse existing Step Functions; add only the preliminary SageMaker extraction/attachment step for non-inference flows.
- Skip persistent batch-catalog persistence for now.
