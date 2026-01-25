# DynamoDB ML Project Parameters Table Specification (Draft)

## Purpose
This table provides a single source of truth for **project-specific configuration** used by the NDR feature engineering pipeline. It is queried by runtime code (via `load_job_spec`) and by a generic SageMaker Pipeline entry step that resolves processing parameters.

## Table Name (example)
`ml_projects_parameters`

## Environment Variable
Runtime code should read the table name from:

- `ML_PROJECTS_PARAMETERS_TABLE_NAME`

## Primary Keys
Use a composite key so multiple job specs and versions can exist per project.

- **Partition key (PK):** `project_name` (string)
- **Sort key (SK):** `job_name#feature_spec_version` (string)
  - Example: `delta_builder#v1`

This structure allows multiple job specs per project and per feature spec version.

## Required Attributes
Each item should include:

- `project_name` (string) — partition key.
- `job_name` (string) — logical job identifier (e.g., `delta_builder`, `fg_a_builder`, `pair_counts_builder`, `fg_b_builder`, `fg_c_builder`).
- `feature_spec_version` (string) — feature schema/version identifier (e.g., `v1`).
- `spec` (map) — job-specific configuration body.
- `updated_at` (string, ISO8601) — last updated time.
- `owner` (string) — owning team/role, for auditing.

## Optional Attributes
- `pipeline_defaults` (map) — default processing instance type/count, image URIs, etc.
- `feature_store` (map) — offline/online feature group names and flags.
- `tags` (map or list) — metadata for governance.

## Example Item (FG-C)
```json
{
  "project_name": "ndr-prod",
  "job_name": "fg_c_builder",
  "feature_spec_version": "v1",
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

## Access Patterns
- **Get job spec:**
  - `GetItem` by `project_name` and `job_name#feature_spec_version`.
- **List all job specs for a project:**
  - `Query` by `project_name` and `begins_with(SK, "<job_name>#")`.
- **List all versions:**
  - `Query` by `project_name` and `begins_with(SK, "<job_name>#")`.

## Consistency Requirements
- Updates must be **atomic per job spec** (single item update).
- Schema changes should be additive or versioned via `feature_spec_version`.

## Notes
- If feature specs are large, consider storing the `spec` as an S3 URI and keeping only a reference in DynamoDB.
- The pipeline entry step should read the required specs for all jobs in one pass.
