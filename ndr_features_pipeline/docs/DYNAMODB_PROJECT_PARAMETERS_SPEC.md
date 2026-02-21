# DynamoDB ML Project Parameters Table Specification (Draft)

## Purpose
This table provides a single source of truth for **project-specific configuration** used by the NDR feature engineering pipeline. It is queried by runtime code (via `load_job_spec`) and by a generic SageMaker Pipeline entry step that resolves processing parameters.

## Table Name (example)
`ml_projects_parameters`

## Environment Variable
Runtime code should read the table name from:

- `ML_PROJECTS_PARAMETERS_TABLE_NAME`
- (legacy fallback) `JOB_SPEC_DDB_TABLE_NAME`

## Primary Keys
Use a composite key so multiple job specs and versions can exist per project.

- **Partition key (PK):** `project_name` (string)
- **Sort key (SK):** `job_name` (string)
  - The *value* of `job_name` is versioned as `job_name#feature_spec_version` using `#` as the delimiter.
  - Example: `delta_builder#v1`
  - Legacy (unversioned) items may use just `job_name` without the delimiter.

This structure allows multiple job specs per project and per feature spec version while keeping the sort key name stable (`job_name`).

## Required Attributes
Each item should include:

- `project_name` (string) — partition key.
- `job_name` (string) — sort key value, typically `<job_name>#<feature_spec_version>`.
- `spec` (map) — job-specific configuration body (see **Job-Specific Spec Payloads** below).

For strongly-typed JobSpec entries (currently `delta_builder`), the `spec` payload must include:
- `project_name` (string)
- `job_name` (string)
- `feature_spec_version` (string)

These fields are read from the `spec` payload itself by the JobSpec loader.

## Optional Attributes
- `feature_spec_version` (string) — if you want to store it redundantly at the item level (not required by loaders).
- `updated_at` (string, ISO8601) — last updated time.
- `owner` (string) — owning team/role, for auditing.
- `pipeline_defaults` (map) — default processing instance type/count, image URIs, etc.
- `feature_store` (map) — offline/online feature group names and flags.
- `tags` (map or list) — metadata for governance.

## Example Item (FG-C)
```json
{
  "project_name": "ndr-prod",
  "job_name": "fg_c_builder#v1",
  "spec": {
    "fg_a_input": {
      "s3_prefix": "s3://<features-bucket>/fg_a/"
    },
    "fg_b_input": {
      "s3_prefix": "s3://<features-bucket>/fg_b/host/"
    },
    "fg_c_output": {
      "s3_prefix": "s3://<features-bucket>/fg_c/"
    },
    "horizons": ["7d", "30d"],
    "join_keys": ["host_ip", "window_label"],
    "metrics": ["sessions_cnt_w_15m", "bytes_src_sum_w_15m"],
    "eps": 1e-6,
    "z_max": 6.0
  },
  "updated_at": "2025-01-01T00:00:00Z",
  "owner": "ndr-team"
}
```

## Job Names (Current Pipelines)
- `delta_builder`
- `fg_a_builder`
- `pair_counts_builder`
- `fg_b_builder`
- `fg_c_builder`
- `machine_inventory_unload`
- `inference_predictions`
- `if_training` (dedicated Isolation Forest training pipeline parameters)
- `project_parameters` (project-level parameters consumed across orchestration flows)
- `pipeline_backfill_historical_extractor` (pipeline-level script/runtime contract for preliminary historical window extraction)
- `pipeline_prediction_feature_join`
- `pipeline_training_data_verifier`
- `pipeline_missing_feature_creation`
- `pipeline_model_publish`
- `pipeline_model_attributes`
- `pipeline_model_deploy`

## Job-Specific Spec Payloads
Below are the *minimum* payload expectations based on runtime usage. Additional keys are allowed.

### `delta_builder` (strongly-typed JobSpec)
The `spec` payload must match the JobSpec dataclasses (see `ndr.config.job_spec_models`):
- `project_name`, `job_name`, `feature_spec_version`
- `input`: `{ s3_prefix, format, compression?, schema_projection?, field_mapping? }`
  - Recommended `field_mapping` keys for Palo Alto raw logs: `source_ip`, `destination_ip`, `source_port`, `destination_port`, `source_bytes`, `destination_bytes`, `event_start`, `event_end`.
- `dq`: `{ drop_malformed_ip?, duration_non_negative?, bytes_non_negative?, filter_null_bytes_ports?, emit_metrics? }`
- `enrichment`: `{ vdi_hostname_prefixes?, port_sets_location? }`
- `roles`: list of `{ name, host_ip, peer_ip, bytes_sent, bytes_recv, peer_port }`
- `operators`: list of `{ type, params? }`
- `output`: `{ s3_prefix, format, partition_keys, write_mode? }`

### `fg_a_builder`
- `delta_input.s3_prefix` **or** `delta_s3_prefix`
- `fg_a_output.s3_prefix` **or** `output_s3_prefix`
- Optional: `pair_context_input.s3_prefix`
- Optional: `pair_context_output.s3_prefix`
- Optional: `lookback30d.s3_prefix`
- Optional: `lookback30d.rare_thresholds`
- Optional: `high_risk_segments` (list of segment identifiers)
- Optional: `segment_mapping` (segment configuration used to enrich FG-A)

### `pair_counts_builder`
- `traffic_input.s3_prefix`
- `traffic_input.field_mapping` with required keys:
  - `source_ip`, `destination_ip`, `destination_port`, `event_start`, `event_end`
- `pair_counts_output.s3_prefix`
- Optional: `traffic_input.layout` (defaults to `batch_folder`)
- Optional: `filters` (`require_nonnull_ips`, `require_destination_port`)
- Optional: `segment_mapping`

### `fg_b_builder`
- `horizons` (defaults to `["7d", "30d"]`)
- `fg_a_input.s3_prefix`
- `fg_b_output.s3_prefix`
- Optional: `pair_counts`, `segment_mapping`, `anomaly_capping`, `baseline_metrics`,
  `baseline_required_metrics`, `support_min`, `join_keys`, `safety_gaps`,
  `ip_machine_mapping`, `non_persistent_machine_prefixes`, `enrichment`

### `fg_c_builder`
- `fg_a_input.s3_prefix`
- `fg_b_input.s3_prefix`
- `fg_c_output.s3_prefix`
- Optional: `horizons`, `join_keys`, `metrics`, `pair_counts`, `segment_mapping`,
  `eps`, `z_max`
- Optional: `pair_context_input.s3_prefix` (joinable pair context dataset)
- Optional: `transforms` (override default transform list)
- Optional: `suspicion_metrics` (override suspicion metric list)
- Optional: `rare_pair_metrics` (override rare-pair metric list)

### `machine_inventory_unload`
- `redshift`: `{ cluster_identifier, database, secret_arn, region, iam_role, db_user? }`
- `query`: `{ sql? | (schema, table), ip_column?, name_column?, active_filter?, additional_filters? }`
- `output`: `{ s3_prefix, output_format? (PARQUET), partitioning? (must include snapshot_month), ip_output_column?, name_output_column? }`

### `inference_predictions`
- `feature_inputs`: map of feature bundles (typically `fg_a`, `fg_c`) with:
  - `s3_prefix` (base prefix for the feature dataset)
  - `dataset` (dataset name used for batch output prefix; defaults to the map key)
  - `required` (optional, defaults to `true`)
- `join_keys` (defaults to `["host_ip", "window_label", "window_end_ts"]`)
- `model`: `{ endpoint_name, timeout_seconds?, max_payload_mb?, max_records_per_payload?, max_retries?, backoff_seconds?, content_type?, accept?, model_version?, model_name?, max_workers?, region? }`
- `output`: `{ s3_prefix, format? (PARQUET), partition_keys? (defaults to feature_spec_version + dt), write_mode?, dataset? }`
- Optional: `payload.feature_columns` (subset of feature columns to send to the model)
- Optional: `prediction_schema` (`score_column`, `label_column`)
- Optional: `join_output` (same shape as `output`) if you want to write prediction+feature joins.

### `if_training`
Dedicated configuration for the Isolation Forest training pipeline (FG-A + FG-C).

Recommended required keys:
- `feature_inputs.fg_a.s3_prefix`
- `feature_inputs.fg_c.s3_prefix`
- `output.artifacts_s3_prefix`
- `output.report_s3_prefix`
- `model.version`
- `window.lookback_months` (expected: `4`)
- `window.gap_months` (expected: `1`)

Recommended optional keys:
- `join_keys` (defaults to `["host_ip", "window_label", "window_end_ts"]`)
- `preprocessing`: `{ eps?, scaling_method?, outlier_method?, z_max? }`
- `feature_selection`: `{ enabled?, variance_threshold?, corr_threshold?, stability_runs?, permutation_eval? }`
- `tuning`: `{ method?, max_trials?, timeout_seconds?, search_space?, objective_weights? }`
- `experiments`: `{ experiment_name?, trial_prefix?, capture_artifacts? }`
- `deployment`: `{ deploy_on_success?, endpoint_name?, endpoint_config_name?, strategy? }`
- `output.model_image_prefix` (S3 prefix for frozen final model image/package copy)

Example (abbreviated):
```json
{
  "project_name": "ndr-prod",
  "job_name": "if_training#v1",
  "spec": {
    "feature_inputs": {
      "fg_a": {"s3_prefix": "s3://<features-bucket>/fg_a/"},
      "fg_c": {"s3_prefix": "s3://<features-bucket>/fg_c/"}
    },
    "window": {"lookback_months": 4, "gap_months": 1},
    "output": {
      "artifacts_s3_prefix": "s3://<models-bucket>/if_training/",
      "report_s3_prefix": "s3://<models-bucket>/if_training/reports/",
      "model_image_prefix": "s3://<models-bucket>/if_training/model_images/"
    },
    "deployment": {"deploy_on_success": false, "endpoint_name": "ndr-if-endpoint"}
  }
}
```

### `project_parameters`
Used by FG-B for shared project-level configuration. The item may store its payload under
`spec` or `parameters`. The minimum required key is:
- `ip_machine_mapping_s3_prefix` (unless `fg_b_builder.spec.ip_machine_mapping.s3_prefix_key` overrides it)

## Access Patterns
- **Get job spec:**
  - `GetItem` by `project_name` and `job_name` (where `job_name` includes `#feature_spec_version`).
- **List all job specs for a project:**
  - `Query` by `project_name` and `begins_with(job_name, "<job_name>#")`.
- **List all versions:**
  - `Query` by `project_name` and `begins_with(job_name, "<job_name>#")`.

## Consistency Requirements
- Updates must be **atomic per job spec** (single item update).
- Schema changes should be additive or versioned via `feature_spec_version`.

## Notes
- If feature specs are large, consider storing the `spec` as an S3 URI and keeping only a reference in DynamoDB.
- The pipeline entry step should read the required specs for all jobs in one pass.


## Additional items for org-path routing and S3 event ingestion

To support ingestion-bucket event routing where path fragments are known in advance (`org1`, `org2`, vendor prefix), keep these items in DynamoDB.

### `project_routing` item (recommended)

Store in a dedicated routing table (recommended name: `ml_projects_routing`), keyed by org convention path:

- PK: `org_key` (string) with value `<org1>#<org2>`
- Attributes:
  - `project_name` (string)
  - `ingestion_prefix` (string), e.g. `s3://<ingestion_bucket>/paloalto/<org1>/<org2>`
  - optional `feature_spec_version` default
  - optional `enabled` flag / lifecycle metadata

Example:
```json
{
  "org_key": "acme#finance",
  "project_name": "ndr-acme-prod",
  "ingestion_prefix": "s3://ndr-ingestion/paloalto/acme/finance",
  "feature_spec_version": "v1"
}
```

### `project_parameters#<feature_spec_version>` defaults additions

Under `spec.defaults`, include optional static path/runtime hints used by Step Functions when resolving messages:

- `Org1`, `Org2`
- `VendorPrefix` (e.g. `paloalto`)
- `IngestionBasePrefix`
- `MiniBatchS3Prefix` (fallback explicit batch prefix)

These defaults are fallback-only; explicit event-derived values and direct invocation parameters still take precedence.


## Provisioning automation requirements

The repository provisioning script must create both tables when requested:

- `ml_projects_parameters` (project/job specs; PK=`project_name`, SK=`job_name`)
- `ml_projects_routing` (org-path routing; PK=`org_key`)

The script should also support optional seed insertion for routing records so deployments do not rely on manual table bootstrapping.


## Palo Alto orchestration-specific defaults (ML-only)

For the Palo Alto ingestion-to-ML orchestration, keep these optional defaults under `project_parameters#<feature_spec_version>.spec.defaults`:

- `ProjectName`
- `FeatureSpecVersion`
- `MiniBatchS3Prefix` (fallback only)
- `WindowCronExpression` (expected value: `8-59/15 * * * ? *`)
- `WindowFloorMinutes` (expected list: `[8, 23, 38, 53]`)
- `HistoricalInputS3Prefix` (fallback raw prefix for historical-window extraction)
- `HistoricalWindowsOutputS3Prefix` (fallback output prefix for extractor-emitted windows)

Runtime policy:
- `batch_start_ts_iso` is derived by flooring the source timestamp to one of `08/23/38/53`.
- `batch_end_ts_iso` is the actual source timestamp (SNS payload timestamp for inference, S3 LastModified for non-inference).

Minimal producer event payload assumptions:
- full `s3_key` of one file in the mini-batch folder,
- event timestamp close to batch creation time.

All other runtime fields (`project_name`, `mini_batch_id`, `feature_spec_version`) are derived from path parsing and DynamoDB lookups.

Backfill extractor callback contract:
- Backfill Step Functions expects extractor completion payload to include `windows` compatible with map items containing at least `mini_batch_id`, `batch_start_ts_iso`, and `batch_end_ts_iso`.
