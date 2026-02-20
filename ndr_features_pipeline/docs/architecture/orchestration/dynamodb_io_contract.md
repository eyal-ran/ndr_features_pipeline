# DynamoDB IO Contract: Keys, Placeholders, and Correlated S3 Prefixes

This document defines the **authoritative IO contract** for orchestration and pipeline script/data resolution.

## 1) DynamoDB primary keys

All records live in the ML projects parameters table (`ML_PROJECTS_PARAMETERS_TABLE_NAME`) with:

- **Partition key**: `project_name`
- **Sort key**: `job_name`
- **Versioned sort key format**: `<logical_job_name>#<feature_spec_version>`

Examples:
- `project_parameters#v1`
- `pipeline_15m_streaming#v1`
- `pipeline_inference_predictions#v1`
- `pipeline_prediction_feature_join#v1`
- `pipeline_training_data_verifier#v1`
- `pipeline_model_deploy#v1`
- `delta_builder#v1`

## 2) Pipeline-level script contract (required)

For each `pipeline_*` record, `spec` must contain:

- `required_runtime_params`: ordered list of expected runtime keys.
- `scripts.steps.<step_name>.code_prefix_s3`: S3 prefix for code assets.
- `scripts.steps.<step_name>.entry_script`: step entry `.py` filename.
- `scripts.steps.<step_name>.data_prefixes`: named input/output S3 prefixes consumed by that step.

## 3) Project-level defaults / validation contract

`project_parameters#<feature_spec_version>` includes:

- `spec.defaults`: runtime fallback values (non-critical defaults + placeholders).
- `spec.validation`: regex/type-oriented validation hints for runtime parameters.

Step Functions resolve incoming event values in this order:
1. explicit payload value,
2. parsed SQS/SNS message value,
3. `project_parameters.spec.defaults` from DynamoDB.

## 4) Placeholder-value guidance (seed templates)

Seed templates intentionally use placeholders and must be replaced during environment provisioning:

- `<bucket>`
- `<project_name>`
- `<feature_spec_version>`
- `<required:BatchStartTsIso>`
- `<required:BatchEndTsIso>`
- `<required:ReferenceTimeIso>`
- `<required:ReferenceMonthIso>`

## 5) Correlated S3 hierarchy (scripts + data)

Recommended canonical hierarchy:

- Scripts:
  - `s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/<pipeline_name>/<step_name>/`
- Data:
  - `s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/<domain>/<dataset>/`

Examples:
- `.../data/raw/traffic/`
- `.../data/features/delta/`
- `.../data/features/fg_a/`
- `.../data/features/fg_b/`
- `.../data/features/fg_c/`
- `.../data/inference/predictions/`
- `.../data/publication/prediction_feature_join/`
- `.../data/training/if_training/`

## 6) Step Functions integration expectations

Step Functions JSONata definitions should:

- Parse `project_name` and `feature_spec_version` from direct payload and SQS/SNS wrappers.
- Read `project_parameters#<feature_spec_version>` from DynamoDB.
- Resolve runtime parameters using the precedence described above.
- Pass resolved values to SageMaker pipelines via `PipelineParameters`.

This ensures project scoping and runtime parameter behavior are table-driven and consistent across 15m features/inference, monthly baselines, publication, backfill, and training orchestrators.
