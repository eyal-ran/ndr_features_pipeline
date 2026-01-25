# Debug Records V1 - Known Bugs, Fixes, and Improvements

## 1) Missing base runner classes
**Issue:** `BaseRunner` and `BaseProcessingJobRunner` are referenced in multiple builders, but only `BaseProcessingRunner` exists.
- **Files:**
  - `src/ndr/processing/base_runner.py`
  - `src/ndr/processing/fg_a_builder_job.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/fg_c_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add `BaseRunner` and `BaseProcessingJobRunner` classes in `base_runner.py` (or refactor call sites to use `BaseProcessingRunner`). Ensure consistent constructor signatures.

## 2) Missing `load_job_spec` helper
**Issue:** Builders call `load_job_spec(...)`, but `job_spec_loader.py` only exposes `JobSpecLoader`.
- **Files:**
  - `src/ndr/config/job_spec_loader.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/fg_c_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add a `load_job_spec(project_name, job_name, feature_spec_version, table_name=None)` function to wrap `JobSpecLoader` and resolve the correct item from DynamoDB, including feature spec versioning.

## 3) Missing runtime config objects for CLI scripts
**Issue:** `run_delta_builder.py` and `run_fg_a_builder.py` import runtime config classes/functions that do not exist in the job modules.
- **Files:**
  - `src/ndr/scripts/run_delta_builder.py`
  - `src/ndr/scripts/run_fg_a_builder.py`
  - `src/ndr/processing/delta_builder_job.py`
  - `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Add `DeltaBuilderJobRuntimeConfig` + `run_delta_builder_from_runtime_config(...)` and `FGABuilderJobRuntimeConfig` + `run_fg_a_builder_from_runtime_config(...)` (or update scripts to call the actual entrypoints).

## 4) S3Writer method mismatch
**Issue:** `S3Writer` only defines `write_parquet_partitioned`, but FG-B and Pair-Counts call `write_parquet`.
- **Files:**
  - `src/ndr/io/s3_writer.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add `write_parquet` wrapper or update call sites to use `write_parquet_partitioned` with the correct parameters.

## 5) S3Reader constructor mismatch
**Issue:** `S3Reader` requires a `SparkSession`, but FG-B instantiates it with no arguments.
- **Files:**
  - `src/ndr/io/s3_reader.py`
  - `src/ndr/processing/fg_b_builder_job.py`
**Fix:** Either pass `SparkSession` into `S3Reader(...)` or provide a convenience constructor/factory.

## 6) Pipeline definition references undefined steps/variables
**Issue:** `build_15m_streaming_pipeline` uses `fg_c_step` without defining it; `build_fg_b_baseline_pipeline` references undefined variables (`mini_batch_id`, `batch_start_ts_iso`, `pair_counts_step`).
- **File:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
**Fix:** Define all steps and parameters explicitly or remove FG-C from the streaming pipeline until wiring is complete.

## 7) Delta vs FG-A schema naming mismatch
**Issue:** Delta builder emits `bytes_sent_sum`/`bytes_recv_sum` and `drop_cnt`, but FG-A expects `bytes_src_sum`/`bytes_dst_sum` and `deny_cnt` (and silently skips missing columns).
- **Files:**
  - `src/ndr/processing/delta_builder_operators.py`
  - `src/ndr/model/fg_a_schema.py`
  - `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Align column names via renaming or schema mapping so FG-A receives the intended metrics.

## 8) FG-C metrics with MAD/IQR fallback
**Issue:** When `m_mad` is null/zero, FG-C falls back to IQR for `z_mad`, but `abs_dev_over_mad` still divides by MAD only.
- **File:** `src/ndr/processing/fg_c_builder_job.py`
**Fix:** When MAD is null/zero, compute `abs_dev_over_mad` using IQR as fallback (or rename to `abs_dev_over_scale` to reflect behavior).

## 9) JobSpec versioning not enforced in loader
**Issue:** The JobSpec loader does not account for feature spec version in its lookup key.
- **File:** `src/ndr/config/job_spec_loader.py`
**Fix:** Incorporate `feature_spec_version` into the DynamoDB lookup key (e.g., `job_name#feature_spec_version`) and align the table name environment variable to `ML_PROJECTS_PARAMETERS_TABLE_NAME` across code and docs.

## 10) Feature Store ingestion performance
**Issue:** FG-A Feature Store ingestion uses `foreachPartition` with per-row `put_record`, which may be slow at scale.
- **File:** `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Consider S3-based offline ingestion or batch `put_record` with retries/backoff to improve throughput.
