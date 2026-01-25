# NDR Feature Engineering Pipeline Specification (Draft)

## Purpose
This repository defines an AWS-based feature engineering pipeline for Network Detection & Response (NDR). The pipeline generates feature groups for anomaly detection models, running **every 15 minutes for inference** and on-demand for training/refitting. The pipeline writes outputs compatible with **SageMaker Feature Store** and uses intermediate S3 datasets for delta tables, FG-A, FG-B, FG-C, and pair-counts datasets.

## Scope
The system produces the following layers:
- **Delta tables (15m slices):** Aggregated host-level traffic deltas from parsed Palo Alto JSON logs.
- **FG-A (current behavior features):** Multi-window aggregations (15m, 30m, 1h, 8h, 24h) for outbound and inbound roles plus time-context features.
- **Pair-counts (15m slices):** Pair-level counts for (src_ip, dst_ip, dst_port) to support rarity baselines.
- **FG-B (baselines):** Robust host/segment baselines (7d/30d) plus optional pair rarity baselines.
- **FG-C (correlations):** Derived correlation features comparing FG-A to FG-B baselines per horizon.

## Orchestration Overview
1. **S3 integration bucket event** triggers a **generic Step Function** (shared by multiple projects).
2. The Step Function passes a project S3 path to a **generic SageMaker Pipeline** entry step.
3. The Pipeline resolves all project-specific parameters from the **ML projects DynamoDB parameters table** (`ML_PROJECTS_PARAMETERS_TABLE_NAME`).
4. The Pipeline executes ProcessingSteps that invoke the Spark jobs:
   - Delta Builder
   - FG-A Builder
   - Pair-Counts Builder (optional but required for FG-B pair baselines)
   - FG-B Baseline Builder (scheduled or on-demand)
   - FG-C Correlation Builder

## Data Inputs
- **Raw inputs:** Parsed Palo Alto firewall logs in an S3 integration bucket, stored as `.json.gz` JSON Lines.
- **Configuration:** JobSpec and pipeline parameters stored in DynamoDB (per project and job name).

## Data Outputs
- **Delta tables:** S3 Parquet partitions by time and role.
- **FG-A:** S3 Parquet partitioned by `feature_spec_version` and `dt`, compatible with Feature Store offline ingestion.
- **Pair-counts:** S3 Parquet partitioned by `dt`, `hh`, `mm`, `feature_spec_version`.
- **FG-B:** Host/segment/pair baselines written to S3 with horizon partitions.
- **FG-C:** S3 Parquet partitioned by `feature_spec_version`, `baseline_horizon`, and `dt`.

## Runtime Parameters (Examples)
Processing jobs receive runtime parameters (via Step Functions → Pipeline → ProcessingStep), such as:
- `project_name`
- `feature_spec_version`
- `mini_batch_id`
- `batch_start_ts_iso`
- `batch_end_ts_iso`
- `reference_time_iso` (for FG-B)
- `mode` (REGULAR or BACKFILL)

## Configuration via DynamoDB
Each job looks up its JobSpec and project parameters from DynamoDB using the composite key
`project_name` + `job_name#feature_spec_version`. The JobSpec includes:
- S3 prefixes for inputs/outputs.
- Window/horizon configuration.
- Feature lists and thresholds.
- Data-quality filters.
- Feature Store metadata.

## Observability and Idempotency
- Each job is expected to log start/end status to CloudWatch and handle empty slices safely.
- Output partitions are written in **overwrite** or **append** modes depending on data semantics.
- The Step Function can re-run jobs safely for a given mini-batch by overwriting the relevant partitions.

## Notable Gaps (Current)
The implementation expects a coherent JobSpec loader and shared base runner utilities to map DynamoDB configuration into job runtime configuration and should be finalized to align runtime objects with the DynamoDB-driven configuration model.
