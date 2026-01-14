# NDR Delta Builder & Feature Engineering Pipelines

This repository contains the initial implementation for the NDR (Network Detection & Response)
feature engineering stack on AWS, focused on the Delta Builder Spark job and the scaffolding for
later feature group builders (FG-A current features, FG-B baselines, FG-C correlations).

## High-level flow

1. **Step Functions → SageMaker Pipeline**
   - Redshift parsing finishes, Step Function knows:
     - `project_name`, `mini_batch_id`, `parsed_minibatch_s3_prefix`,
       `feature_spec_version`, `run_id`, `job_spec_ddb_table_name`.
   - Step Function calls `StartPipelineExecution` on the NDR Pipeline, passing these
     as pipeline parameters.

2. **Pipeline → ProcessingStep**
   - `build_delta_builder_pipeline` wires parameters into a single `ProcessingStep`
     using `PySparkProcessor` (official SageMaker Spark Processing container).
   - SageMaker launches a Spark Processing job with the code distribution S3 URI and
     CLI arguments derived from pipeline parameters.

3. **Container startup → entrypoint**
   - The container downloads the code from `code_s3_uri`.
   - It executes: `python -m ndr.scripts.run_delta_builder ...`

4. **`run_delta_builder.py`**
   - Parses CLI arguments.
   - Creates a `SparkSession`.
   - Uses `JobSpecLoader` to load `JobSpec` from DynamoDB based on `(project_name, job_name)`.
   - Builds `RuntimeParams` with runtime values (project_name, job_name, mini_batch_s3_prefix,
     feature_spec_version, run_id, slice_start_ts, slice_end_ts).
   - Delegates to `DeltaBuilderRunner`.

5. **`DeltaBuilderRunner` (`BaseProcessingRunner.run`)**
   - `_read_input()` → uses `S3Reader` to read JSON Lines GZIP from the integration bucket
     under `mini_batch_s3_prefix`.
   - `_apply_data_quality()` → applies cleaning and data-quality rules (filter non-traffic
     records, enforce duration ≥ 0, handle bytes and port validity).
   - `_build_dataframe()` → implements the 15m delta aggregation for outbound and inbound
     roles using the operator functions in `delta_builder_operators`.
   - `_write_output()` → writes the final delta DataFrame as partitioned Parquet to S3 using
     `S3Writer` and the output spec from `JobSpec`.

6. **Delta table**
   - Output is a Parquet dataset partitioned by keys like:
     `feature_spec_version/dt/hh/mm/role` and contains per-host, per-role, per-slice deltas
     used later by FG-A/FG-B/FG-C builders.

## Project structure

See `package_structure.txt` for a summary of the on-disk layout.
