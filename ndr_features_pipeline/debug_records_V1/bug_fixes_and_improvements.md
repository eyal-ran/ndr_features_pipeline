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
**Status:** Fully implemented. `BaseRunner` and `BaseProcessingJobRunner` are defined in `base_runner.py`, and builder jobs now import/use them consistently.

## 2) Missing `load_job_spec` helper
**Issue:** Builders call `load_job_spec(...)`, but `job_spec_loader.py` only exposes `JobSpecLoader`.
- **Files:**
  - `src/ndr/config/job_spec_loader.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/fg_c_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add a `load_job_spec(project_name, job_name, feature_spec_version, table_name=None)` function to wrap `JobSpecLoader` and resolve the correct item from DynamoDB, including feature spec versioning.
**Status:** Fully implemented. `load_job_spec(...)` exists and resolves items with `job_name#feature_spec_version`, and builders call it.

## 3) Missing runtime config objects for CLI scripts
**Issue:** `run_delta_builder.py` and `run_fg_a_builder.py` import runtime config classes/functions that do not exist in the job modules.
- **Files:**
  - `src/ndr/scripts/run_delta_builder.py`
  - `src/ndr/scripts/run_fg_a_builder.py`
  - `src/ndr/processing/delta_builder_job.py`
  - `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Add `DeltaBuilderJobRuntimeConfig` + `run_delta_builder_from_runtime_config(...)` and `FGABuilderJobRuntimeConfig` + `run_fg_a_builder_from_runtime_config(...)` (or update scripts to call the actual entrypoints).
**Status:** Fully implemented. Both runtime config dataclasses and helper entrypoints exist in their respective job modules, and scripts import them.

## 4) S3Writer method mismatch
**Issue:** `S3Writer` only defines `write_parquet_partitioned`, but FG-B and Pair-Counts call `write_parquet`.
- **Files:**
  - `src/ndr/io/s3_writer.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add `write_parquet` wrapper or update call sites to use `write_parquet_partitioned` with the correct parameters.
**Status:** Fully implemented. `S3Writer.write_parquet` exists and is used by FG-B outputs.

## 5) S3Reader constructor mismatch
**Issue:** `S3Reader` requires a `SparkSession`, but FG-B instantiates it with no arguments.
- **Files:**
  - `src/ndr/io/s3_reader.py`
  - `src/ndr/processing/fg_b_builder_job.py`
**Fix:** Either pass `SparkSession` into `S3Reader(...)` or provide a convenience constructor/factory.
**Status:** Fully implemented. `S3Reader` now accepts an optional SparkSession, and FG-B passes a SparkSession.

## 6) Pipeline definition references undefined steps/variables
**Issue:** `build_15m_streaming_pipeline` uses `fg_c_step` without defining it; `build_fg_b_baseline_pipeline` references undefined variables (`mini_batch_id`, `batch_start_ts_iso`, `pair_counts_step`).
- **File:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
**Fix:** Define all steps and parameters explicitly or remove FG-C from the streaming pipeline until wiring is complete.
**Status:** Fully implemented. `fg_c_step` is defined and wired, and baseline pipeline parameters are explicit with no undefined variables.

## 7) Delta vs FG-A schema naming mismatch
**Issue:** Delta builder emits `bytes_sent_sum`/`bytes_recv_sum` and `drop_cnt`, but FG-A expects `bytes_src_sum`/`bytes_dst_sum` and `deny_cnt` (and silently skips missing columns).
- **Files:**
  - `src/ndr/processing/delta_builder_operators.py`
  - `src/ndr/model/fg_a_schema.py`
  - `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Align column names via renaming or schema mapping so FG-A receives the intended metrics.
**Status:** Fully implemented. FG-A rename mapping converts `bytes_sent_sum` → `bytes_src_sum`, `bytes_recv_sum` → `bytes_dst_sum`, and `drop_cnt` → `deny_cnt`.

## 8) FG-C metrics with MAD/IQR fallback
**Issue:** When `m_mad` is null/zero, FG-C falls back to IQR for `z_mad`, but `abs_dev_over_mad` still divides by MAD only.
- **File:** `src/ndr/processing/fg_c_builder_job.py`
**Fix:** When MAD is null/zero, compute `abs_dev_over_mad` using IQR as fallback (or rename to `abs_dev_over_scale` to reflect behavior).
**Status:** Fully implemented. FG-C now uses a `scale` column that falls back to IQR for both `z_mad` and `abs_dev_over_mad`.

## 9) JobSpec versioning not enforced in loader
**Issue:** The JobSpec loader does not account for feature spec version in its lookup key.
- **File:** `src/ndr/config/job_spec_loader.py`
**Fix:** Incorporate `feature_spec_version` into the DynamoDB lookup key (e.g., `job_name#feature_spec_version`) and align the table name environment variable to `ML_PROJECTS_PARAMETERS_TABLE_NAME` across code and docs.
**Status:** Fully implemented. `JobSpecLoader` and `load_job_spec` include the version in the sort key and prefer `ML_PROJECTS_PARAMETERS_TABLE_NAME` with a legacy fallback.

## 10) Feature Store ingestion performance
**Issue:** FG-A Feature Store ingestion uses `foreachPartition` with per-row `put_record`, which may be slow at scale.
- **File:** `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Consider S3-based offline ingestion or batch `put_record` with retries/backoff to improve throughput.
**Status:** Partially implemented. FG-A currently writes Parquet outputs to S3 only and does not implement any Feature Store ingestion path (no batch `put_record`, retries/backoff, or explicit offline ingestion workflow).

## 11) Cold-start refactor (FG-B/FG-C split, segment fallback, and VDI handling)
**Issue:** Cold-start handling is implicit and host-IP centric. FG-C always joins host baselines and ignores cold-start flags, while segment baselines lack parity with host metrics and pair-count rarity stats are host-IP only.

**Refactoring plan:**
1) **Split FG-B outputs into three explicit tables (all keyed to FG-C join keys):**
   - **IP metadata + flags table** (new):
     - Keys: same join keys used by FG-C today (default `host_ip`, `window_label`; extend if JobSpec uses additional keys like `role`, `segment_id`, `time_band`).
     - Fields: existing metadata plus:
       - `is_full_history` (1 if support counts meet `support_min` for required metrics; 0 otherwise).
       - `is_non_persistent_machine` (1 if machine name has a known non-persistent VDI prefix).
       - `is_cold_start` (1 if `is_full_history == 0` **or** `is_non_persistent_machine == 1`).
   - **IP-based calculated features table** (host-level baselines; essentially current FG-B host output).
   - **Segment-based calculated features table** (segment-level baselines; enriched to match host-level schema so FG-C can compute all metrics from either source).
   - All three tables should be written under the FG-B output prefix with clear subpaths (e.g., `/fg_b/ip_metadata/`, `/fg_b/host/`, `/fg_b/segment/`) and partitioned consistently by `feature_spec_version` and `baseline_horizon`.

2) **Monthly FG-B run computes/updates flags:**
   - During the monthly FG-B pipeline run, compute:
     - `is_full_history` from slice counts in the IP-based calculated features table (avoid re-aggregating FG-A on every FG-C run).
     - `is_non_persistent_machine` by joining IP → machine name from the dimension table and matching against the known non-persistent prefixes list.
     - `is_cold_start` from the two flags above.
   - Store these flags in the IP metadata + flags table for use by FG-C.

3) **Enrich segment baselines to match host baselines:**
   - Extend segment-level FG-B computations to emit all fields that FG-C expects from host baselines (e.g., `*_median`, `*_p25`, `*_p75`, `*_p95`, `*_p99`, `*_mad`, `*_iqr`, `*_support_count`, and any other baseline-derived columns), using the same metric list as host baselines.
   - This ensures FG-C can compute the full feature set using segment baselines when `is_cold_start = 1`.

4) **Extend Pair-Counts pipeline for segment rarity:**
   - Add segment-based rarity stats in addition to host-IP pair stats.
   - Proposed approach: aggregate counts by `(segment_id, dst_ip, dst_port, horizon)` using the same horizon windows as host-IP rarity; store under a parallel output (e.g., `/pair/segment/`).

5) **FG-C read path selection based on cold-start flags:**
   - FG-C should first read the IP metadata + flags table for the batch and decide **per host**:
     - If `is_cold_start == 0`: read IP-based FG-B baselines and IP-based pair rarity stats.
     - If `is_cold_start == 1`: read segment-based FG-B baselines and segment-based pair rarity stats.
   - Implement as a conditional join or union of two sub-frames (cold-start vs. non-cold-start) to avoid reprocessing all rows twice.

6) **Clarify feature coverage for cold-start paths:**
   - Maintain a single list of “baseline-required” metrics so both host and segment baselines emit the same schema.
   - For any metric that cannot be safely computed at the segment level, explicitly define a fallback policy (e.g., nulls with downstream handling, or a safe default), and document it in FG-C.

7) **Data contract and validation:**
   - Add schema validation checks to ensure the segment-based table has parity with the host-based table before FG-C runs.
   - Add a small set of unit tests to verify:
     - `is_cold_start` derivation.
     - Correct baseline table selection in FG-C.
     - Pair rarity selection logic (IP vs. segment).
**Status:** Implemented with flaws/known gaps. The FG-B builder now writes split host/segment/ip-metadata outputs, computes `is_full_history`/`is_non_persistent_machine`/`is_cold_start`, and produces segment-level pair rarity; FG-C selects host vs. segment baselines and pair rarity based on `is_cold_start`, and validates segment schema parity. However, there are no unit tests covering cold-start derivation or selection logic, and the “baseline-required metrics” contract is enforced via runtime checks only (no test coverage).

## 12) Monthly machine inventory unload pipeline (Redshift → S3) for non-persistent detection
**Issue:** We need a monthly pipeline that unloads active, unique machines from a Redshift dimension table to an S3 prefix, only for machines not already present in that prefix. This output is used to determine `is_non_persistent_machine` via known name prefixes before the FG-B pipeline runs.

**Plan (ToDo):**
1) **Add a new JobSpec entry in DynamoDB** for `job_name = "machine_inventory_unload"` (feature spec versioned). The spec should include:
   - **Redshift Data API connection**: `cluster_identifier`, `database`, `db_user` (if needed), `secret_arn`, `region`.
   - **Query definition**: allow a full SQL query string to be provided (preferred by stakeholders), plus structured fields for `schema`, `table`, `ip_column`, `name_column`, `active_filter`, and optional `additional_filters` for validation/logging.
   - **S3 output**: `s3_prefix`, `output_format = "PARQUET"`, `partitioning = ["snapshot_month"]`.
   - **Non-persistent prefixes**: separate JobSpec entry (e.g., `job_name = "non_persistent_prefixes"`) with an array of prefixes so other jobs can reuse it.
   - Align with DynamoDB JobSpec conventions in `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md` (table name and key format).【F:docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md†L1-L70】

2) **Add a new SageMaker pipeline definition** mirroring the style in `sagemaker_pipeline_definitions_unified_with_fgc.py`:
   - A single `ProcessingStep` using `PySparkProcessor` and a new entrypoint script (e.g., `ndr.scripts.run_machine_inventory_unload`).
   - Runtime parameters: `ProjectName`, `FeatureSpecVersion`, `ReferenceMonthIso`, and processing resources (instance type/count/image URI).
   - The step should not hardcode table names; read all structural config via JobSpec (same pattern as other builders).【F:src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py†L1-L110】

3) **Implement `run_machine_inventory_unload.py`** under `src/ndr/scripts/`:
   - Parse CLI args: `project-name`, `feature-spec-version`, `reference-month-iso`.
   - Load the JobSpec using the existing loader pattern (similar to `run_delta_builder.py` / `run_fg_a_builder.py`).
   - Instantiate a new processing runner (see step 4) that performs the unload using Redshift Data API.

4) **Add a processing job runner** under `src/ndr/processing/` (e.g., `machine_inventory_unload_job.py`):
   - **Read existing IPs from S3 prefix** (parquet) into Spark and get distinct IPs. If prefix is empty, treat as empty set.
   - **Preferred approach** for ~93k IPs: use Redshift Data API with a **temporary staging table** for existing IPs, then perform an anti-join in Redshift and `UNLOAD` missing rows to S3:
     - Create temp table `tmp_existing_ips` (or a staging table under a scratch schema).
     - Bulk insert the existing IPs (via Data API batch or copying from a temporary S3 file if needed).
     - Execute `UNLOAD` on the provided SQL (or a validated, templated SQL) with `NOT EXISTS` against `tmp_existing_ips`.
     - Output to `s3_prefix/snapshot_month=YYYY-MM/` to preserve monthly partitioning and reproduce runs.
   - If Data API cannot support temp-table writes or is too slow, fall back to:
     - Read dim table via Data API query, anti-join in Spark, and write to S3 in Parquet (still partitioned by `snapshot_month`).
   - Log counts for input rows, existing IPs, output rows.

5) **Document pipeline usage** in `docs/README_PIPELINES_ADDITIONS.md` (or a new doc):
   - Describe how to register/run the monthly pipeline from Step Functions.
   - Explain ordering requirement: run before FG-B so the non-persistent flag uses latest data.
   - Include sample JobSpec JSON (similar to existing docs).

6) **Add minimal tests or validation hooks**:
   - Unit test for SQL templating / partition path computation.
   - Lightweight integration test stubs if test harness exists (otherwise, add logging/validation in runner and document expected outputs in `docs/test_run_log.md`).【F:docs/test_run_log.md†L1-L4】

**Status:** Implemented. Machine inventory unload job, CLI entrypoint, and SageMaker pipeline are in place, with required JobSpec validation (including `iam_role` and `processing_image_uri`), temp-prefix UNLOAD with verified copy + `_SUCCESS` marker, documentation updates, and unit tests for helpers.

## 13) Decoupled inference pipeline (Option B) + prediction output integration plan
**Issue:** Inference should be generic and decoupled from feature generation so multiple projects can reuse a common inference pipeline. The pipeline should read ready-made features from S3, call the deployed model endpoint, write predictions to S3, and enable easy joins with features for Redshift/Iceberg ingestion without duplicating features.

**Plan (ToDo):**
1) **Add a generic inference JobSpec** (DynamoDB):
   - `job_name = "inference_predictions"` and `feature_spec_version`.
   - Inputs: S3 prefixes for FG-A / FG-C (and any optional feature bundles), plus join keys (e.g., `host_ip`, `window_label`, `window_end_ts`) and batch window params.
   - Output: predictions S3 prefix, output format = Parquet, partition columns `feature_spec_version`, `dt = date(window_end_ts)`, optional `model_version`.
   - Model config: endpoint name, timeout, max payload size, and optional async/batch mode flag.
   - Include minimal prediction schema metadata (prediction_score, model_version, inference_ts, mini_batch_id, record_id).

2) **Create a decoupled inference SageMaker pipeline definition**:
   - New module (e.g., `sagemaker_pipeline_definitions_inference.py`) with a single ProcessingStep.
   - Runtime parameters: `ProjectName`, `FeatureSpecVersion`, `MiniBatchId`, `BatchStartTsIso`, `BatchEndTsIso`, `ProcessingImageUri`, `ProcessingInstanceType`, `ProcessingInstanceCount`.
   - The pipeline only depends on feature outputs being present; it can be triggered by an S3 event or Step Functions after the feature pipeline completes.

3) **Implement the inference processing job (OOP-based)**:
   - `InferencePredictionsJob` class with clear, testable methods: `load_features()`, `prepare_payloads()`, `invoke_model()`, `write_predictions()`.
   - Use efficient batching, retries, and concurrency controls to keep up with the 15-minute cadence.
   - Keep code generic and styled (type hints, docstrings, explicit interfaces, cohesive class responsibilities).

4) **Design output schema + join strategy (no feature duplication)**:
   - Output only predictions + join keys + lineage metadata:
     - `host_ip`, `window_label`, `window_end_ts`, `mini_batch_id`, `feature_spec_version`
     - `prediction_score`, `prediction_label` (optional), `model_version`, `model_name`, `inference_ts`, `record_id`
   - Partition by `feature_spec_version`, `dt = date(window_end_ts)`, optional `model_version`.
   - Ensure join keys match FG-A/FG-C keys so downstream jobs can join without transformation.

5) **Add a downstream join/aggregation step for Redshift/Iceberg ingestion**:
   - Provide a Spark/Glue job (or optional pipeline step) that:
     - Reads prediction outputs + feature tables from S3 by matching partitions.
     - Performs inner join on `host_ip`, `window_label`, `window_end_ts`, `feature_spec_version`.
     - Writes a unified table for COPY/Iceberg ingestion.
   - Keep join job optional so storage-efficient output-only path remains primary.

6) **Documentation + code quality guidance**:
   - Update docs to describe the decoupled inference pipeline triggers and required JobSpec parameters.
   - Add inline comments for non-obvious logic, and document failure handling, retries, and batch-size tuning.
   - Enforce style consistency (formatting/linting, cohesive module structure, minimal side effects).

**Status:** Implemented. Added a decoupled inference predictions job + pipeline definition, JobSpec parsing for feature inputs/model/output configs, optional prediction-feature join job, and documentation updates for the inference JobSpec and pipeline integration.
