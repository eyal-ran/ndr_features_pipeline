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

## 14) Always-on prediction feature join pipeline (separate from inference)
**Issue:** The optional prediction+feature join step is not wired to a ProcessingStep, so joined outputs are not guaranteed after inference writes predictions to S3.

**Plan (ToDo):**
1) **Add a new DynamoDB JobSpec entry** for `job_name = "prediction_feature_join"` (feature spec versioned).
   - Include destination routing: `destination.type` set to `"s3"` or `"redshift"`.
   - For S3: `destination.s3` payload with `s3_prefix`, `format`, `partition_keys`, `dataset`.
   - For Redshift: `destination.redshift` payload with `cluster_identifier`, `database`, `secret_arn`, `region`, `iam_role`, `schema`, `table`, optional `db_user`, `pre_sql`, `post_sql`.
   - Reuse existing `join_output` schema where possible to keep compatibility with inference outputs.

2) **Add a new SageMaker Pipeline definition** (e.g., `build_prediction_feature_join_pipeline`) with a single `ProcessingStep` that runs `ndr.scripts.run_prediction_feature_join`.
   - Pipeline parameters: `ProjectName`, `FeatureSpecVersion`, `MiniBatchId`, `BatchStartTsIso`, `BatchEndTsIso`, plus processing instance/image parameters if needed.
   - All structural configuration comes from JobSpec, not CLI flags, matching existing pipeline patterns.

3) **Extend the join job implementation** to route output based on the JobSpec destination:
   - If `destination.type == "s3"`: write the joined dataset to the provided S3 prefix/format/partitions.
   - If `destination.type == "redshift"`: write to Redshift (e.g., via Redshift Data API or staged UNLOAD/COPY flow), using the configured schema/table.

4) **Update orchestration** so the Step Function triggers this new join pipeline **after** the inference pipeline completes successfully.
   - Pass the same runtime parameters (`project_name`, `feature_spec_version`, `mini_batch_id`, `batch_start_ts_iso`, `batch_end_ts_iso`) to the join pipeline.
   - This keeps the join pipeline generic and reusable while guaranteeing joined outputs for every inference run.

5) **Documentation updates**:
   - Add `prediction_feature_join` to the JobSpec names list.
   - Document the destination routing keys and the new pipeline’s execution order relative to inference.
**Status:** Not implemented.
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

## 15) Authoritative feature catalogs + schema enforcement + safe imputation rules
**Issue:** The feature catalog and schema enforcement are incomplete. This risks silent drift and unsafe imputations, especially for pair-counts and cold-start scenarios.

**Plan (ToDo):**
1) **Authoritative feature catalogs (model inputs + dependencies)**:
   - Create a **Markdown feature catalog** (reviewable) and a **Parquet export** (ingestable) for:
     - **Model-input features** (FG-A/FG-C outputs used by the model).
     - **Non-model fields** required to build model features (intermediates/dependencies), including **FG-B outputs**, **delta tables**, and **pair-counts**.
   - Required fields (minimum):
     - `feature_id`, `feature_number`, `feature_name`, `feature_group`, `feature_formula`, `feature_description`.
     - `creator_script`, `upstream_dependencies`, `feature_spec_version`, `data_type`, `nullable`, `default_value`.
     - `downstream_consumers` (for model features: **"NDR system"**).
     - `feature_group_name_offline`, `feature_group_name_online` (if applicable), and `s3_storage_path`.
   - Store Parquet in S3 for copying into **DynamoDB**, **Redshift**, **Aurora**, or **S3 Tables**.
   - **Catalog counts (v1 expectations; generated by catalog builder and validated in CI):**
     - **Model-input features**
      - FG-A: **485** numeric features (48 metrics × 5 windows × 2 directions = 480, plus hour-of-day and 4 time flags).*
      - FG-C (default curated): **1,320** correlation features (44 metrics × 5 windows × 2 directions = 440 metrics; 3 derived features per metric: z_mad, ratio, log_ratio).*
      - FG-C (full expansion): **3,360** correlation features if using all 480 FG-A numeric metrics (7 derived features per metric).*
     - **Non-model fields**
       - Delta tables: **31 base metrics** + required keys per slice (schema contract).
      - FG-B host baselines: **4,320** stats for 480 FG-A metrics (9 stats per metric: median, p25, p75, p95, p99, mad, iqr, support_count, cold_start_flag) + metadata.
      - FG-B segment baselines: **4,320** stats for 480 FG-A metrics (same 9 stats per metric) + metadata.
       - Pair-counts: **5 fields** (`src_ip`, `dst_ip`, `dst_port`, `event_ts`, `sessions_cnt`) + partition columns.
       - Pair-rarity baselines: **7 computed fields** per pair (pair_seen_count, pair_last_seen_ts, active_days, pair_daily_avg, pair_rarity_score, is_new_pair_flag, is_rare_pair_flag) + keys/metadata.
    - *FG-C count explanation (default): 440 curated FG-A metrics (44 metrics × 5 windows × 2 directions) × 3 derived correlation features (z_mad, ratio, log_ratio) = 1,320.*
    - *FG-C count explanation (full): 480 FG-A numeric metrics (96 metrics × 5 windows) × 7 derived correlation features (diff, ratio, z_mad, abs_dev_over_mad, z_mad_clipped, z_mad_signed_pow3, log_ratio) = 3,360.*
     - *Final counts must be catalog-driven per `feature_spec_version` and `JobSpec.metrics`. These numbers are the current v1 baseline expectations based on `fg_a_schema.py`.*
2) **Schema enforcement (Delta + FG-A + FG-B + FG-C + Pair-Counts)**:
   - Define per-feature-spec schema manifests derived from the catalog, including **pair-counts fields**.
   - Implement shared enforcement helpers to validate required columns and types.
   - Enforce schemas **before each S3 write** in Delta, FG-A, FG-B, FG-C, and Pair-Counts.
3) **Imputation and fallback strategy (safe-only)**:
   - **Imputation is allowed only for these cases (finite list):**
     - **Cold-start hosts** where `is_cold_start=1` because `is_full_history=0`.
     - **Non-persistent VDI machines** where `is_cold_start=1` because `is_non_persistent_machine=1`.
     - **New/rare pairs** where host-level pair rarity baselines are missing for a (host_ip, dst_ip, dst_port) and the segment-level pair rarity baselines are available.
   - **Imputation details by case:**
     - **Cold-start / non-persistent**: if `is_cold_start=1` (set when `is_full_history=0` or `is_non_persistent_machine=1`), compute FG-C features from **segment-level baselines**; never default to zeros if segment stats exist.
     - **New/rare pairs**: use **segment-level pair rarity baselines** when host-pair baselines are missing; default zeros only for pair rarity fields when both host and segment baselines are missing.
   - For all other missing features, **treat as critical defects** (fail fast + alert), not imputation cases.
4) **Governance + automation**:
   - Add validation tooling to ensure catalogs stay synchronized with builder outputs.
   - Emit audit logs/metrics for any fallback/imputation event.

**Status:** ToDo.

## 16) Expand anomaly-focused FG-A/FG-C features + reduce FG-C dimensionality
**Issue:** The current FG-A/FG-C builders omit legacy anomaly-focused features (novelty, high-risk segment interactions, suspicion scores) and generate a wide FG-C feature set (2,660+) that may be redundant. We need to reintroduce high-signal features while constraining FG-C to a curated subset that maximizes detection quality.

**Plan (ToDo):**
1) **Add legacy anomaly features to FG-A (and required dependencies):**
   - **Novelty/rarity lookback30d features (per window + inbound variants):**
     - `new_peer_cnt_lookback30d_*`, `rare_peer_cnt_lookback30d_*`, `new_peer_ratio_lookback30d_*`.
     - `new_dst_port_cnt_lookback30d_*`, `rare_dst_port_cnt_lookback30d_*`.
     - `new_pair_cnt_lookback30d_*`, `new_src_dst_port_cnt_lookback30d_*`.
   - **High-risk segment interaction features:**
     - `high_risk_segment_sessions_cnt_*`, `high_risk_segment_unique_dsts_*`, `has_high_risk_segment_interaction_*`
     - inbound equivalents (prefixed with `in_`).
   - **Dependencies to add:**
     - **Delta tables:** add or extend per-slice fields needed to compute peer/port uniqueness and entropy, per-host peer lists, and segment assignment if missing.
     - **Reference tables:** add S3 reference for 30d lookback (per host) for peer/port/pair history; include rollup tables to avoid re-scanning raw logs per run.
     - **Segment/risk configuration:** ensure segment mapping + high-risk segment allowlist/denylist is accessible to FG-A (JobSpec + project parameters).
   - **Implementation steps:**
     - Extend `fg_a_schema.py` metric lists to include new novelty/high-risk segment metrics.
     - Extend `fg_a_builder_job.py` to:
       - Load lookback30d tables (or rolling reference table) and compute novelty deltas per window.
       - Join segment/risk metadata and compute high-risk segment interactions.
     - Add schema enforcement for new columns prior to S3 write (use item #15 tooling).

2) **Enable pair-rarity signals for FG-C (requires base-table changes):**
   - **Add dst_ip/dst_port to FG-A outputs** if required for downstream joins, or
   - **Alternative:** emit a lightweight “FG-A pair context” table keyed by host_ip + window_label + window_end_ts + dst_ip + dst_port for FG-C to join.
   - **Ensure FG-B pair rarity baselines** include host + segment baselines and are consistently partitioned for FG-C joins.
   - Update `fg_c_builder_job.py` to assert required join keys and fail fast if pair features are requested but missing.

3) **Add legacy FG-C suspicion/anomaly features (curated set):**
   - Reintroduce top legacy FG-C features with highest anomaly relevance:
     - `excess_over_p95_*`, `excess_ratio_p95_*`, `iqr_dev_*`
     - `time_band_violation_flag`
     - `anomaly_strength_core`, `beacon_suspicion_score`, `exfiltration_suspicion_score`
     - `rare_pair_flag`, `rare_pair_weighted_z_*`
   - Define formulas based on FG-B baselines + FG-A metrics; document in feature catalog.
   - Add explicit unit tests for at least one metric per new feature family.

4) **Reduce FG-C dimensionality via a curated metric list + transform subset:**
   - **Default curated metric list:** establish a vetted anomaly-focused subset as the **default behavior** (e.g., sessions/bytes totals, entropy, high-risk port activity, novelty metrics, burstiness, deny ratio).
   - **JobSpec override/augment:** if `JobSpec.metrics` is provided, **merge/augment** the default subset with those metrics (do not replace the default subset), so reduction remains the baseline unless explicitly expanded.
   - **Transform subset:** limit to 2–3 transforms per metric (e.g., `z_mad`, `ratio`, `log_ratio`) and drop redundant magnifiers unless proven useful.
   - **Config-driven:** make both lists JobSpec-configurable; emit a catalog manifest per `feature_spec_version`.
   - **Expected outcome:** reduce FG-C feature count from ~2,660 to a smaller, high-signal set (target 300–800 depending on curated metrics).

5) **Add production-grade NDR anomaly features beyond legacy (optional extensions):**
   - **Beaconing cadence:** periodicity scores over destination IP/port (e.g., burstiness + low variance inter-arrival time).
   - **Exfil indicators:** outbound bytes spikes vs baseline, high bytes asymmetry, unusual destination port risk.
   - **Lateral movement indicators:** spikes in unique internal destinations, new internal segments, admin/fileshare port usage anomalies.
   - Ensure these are only added if they can be computed from available deltas / baselines; otherwise add minimal new delta fields or rollups.

6) **Coordination & forward-compatibility safeguards:**
   - Ensure all feature-building steps (delta → FG-A → pair-counts → FG-B → FG-C) remain coordinated and pass end-to-end integration without errors after these changes.
   - Implement changes in a way that preserves compatibility with the upcoming schema enforcement and cataloging work in item #15 (no blocking schema enforcement, no breaking contracts assumed by item #15).

7) **Testing & validation:**
   - Add unit tests for:
     - Lookback30d novelty computation and missing-history handling.
     - High-risk segment feature derivation.
     - FG-C suspicion feature correctness (excess-over-p95, beacon score, etc.).
   - Add a small integration test (local Spark) to ensure FG-A → FG-B → FG-C pipeline can build with the curated metric list and new features.

**Status:** ToDo.

## 17) End-to-end IO contract hardening: scripts, data prefixes, and DynamoDB parameter model unification
**Issue:** The current orchestration + processing stack still contains mixed IO contract patterns:
- Some SageMaker pipeline steps resolve script locations from repo-local paths (`code="src/ndr/scripts/..."`), while only one path supports external code URI input.
- Several orchestration definitions still embed hard-coded runtime defaults (project name/version/timestamps) instead of fetching canonical defaults/overrides from DynamoDB.
- Data S3 prefixes are mostly JobSpec-driven, but there is no single enforced schema that guarantees every pipeline/job can resolve every required prefix/parameter from one DynamoDB source of truth.
- Step Functions currently do not read the project parameters table for pipeline-level defaults or script-prefix routing.

### Complete current-state IO map (as implemented)

#### A) SageMaker Pipeline Steps that consume `.py` scripts and where they expect to find them
1. `build_15m_streaming_pipeline` (`DeltaBuilderStep`, `FGABuilderStep`, `PairCountsBuilderStep`, `FGCCorrBuilderStep`) uses local repo paths in `ProcessingStep.code`:
   - `src/ndr/scripts/run_delta_builder.py`
   - `src/ndr/scripts/run_fg_a_builder.py`
   - `src/ndr/scripts/run_pair_counts_builder.py`
   - `src/ndr/scripts/run_fg_c_builder.py`
2. `build_fg_b_baseline_pipeline` uses `code="src/ndr/scripts/run_fg_b_builder.py"`.
3. `build_machine_inventory_unload_pipeline` uses `code="src/ndr/scripts/run_machine_inventory_unload.py"`.
4. `build_inference_predictions_pipeline` uses `code="src/ndr/scripts/run_inference_predictions.py"`.
5. `build_prediction_feature_join_pipeline` uses `code="src/ndr/scripts/run_prediction_feature_join.py"`.
6. `build_if_training_pipeline` uses `code="src/ndr/scripts/run_if_training.py"`.
7. Exception pattern: `build_delta_builder_pipeline` accepts `code_s3_uri` externally, making it the only pipeline that can point directly to S3 code location at definition time.

**Gap vs desired structure:** No common mechanism exists for all pipeline definitions to resolve script object paths from DynamoDB (e.g., per project/version/pipeline script prefix + per-step script key).

#### B) `.py` scripts / jobs that read data, and where they expect the data to be
1. **Delta builder**
   - Reads mini-batch input from `job_spec.input.s3_prefix` (via `RuntimeParams.mini_batch_s3_prefix`).
   - Reads optional enrichment file from `job_spec.enrichment.port_sets_location` (`s3://...json`).
   - Writes to JobSpec-configured output/pair-context locations.
2. **FG-A builder**
   - Reads delta data from `job_spec.delta_input.s3_prefix` (fallback: `delta_s3_prefix`).
   - Reads optional pair-context input from `job_spec.pair_context_input.s3_prefix`.
   - Reads optional lookback table from `job_spec.lookback30d.s3_prefix`.
   - Writes FG-A to `job_spec.fg_a_output.s3_prefix` (fallback: `output_s3_prefix`) and optional pair-context output prefix.
3. **Pair-counts builder**
   - Reads traffic input from `job_spec.traffic_input.s3_prefix` and appends `/mini_batch_id/` by layout convention.
   - Writes to `job_spec.pair_counts_output.s3_prefix`.
4. **FG-B baseline builder**
   - Reads FG-A from `job_spec.fg_a_input.s3_prefix`.
   - Reads optional pair-counts from `job_spec.pair_counts.s3_prefix`.
   - Reads IP↔machine mapping from `project_parameters[prefix_key]` where `prefix_key` defaults to `ip_machine_mapping_s3_prefix`.
   - Writes FG-B outputs under `job_spec.fg_b_output.s3_prefix`.
5. **FG-C builder**
   - Reads FG-A from `job_spec.fg_a_input.s3_prefix`.
   - Reads FG-B from `job_spec.fg_b_input.s3_prefix`.
   - Reads optional pair context from `job_spec.pair_context_input.s3_prefix`.
   - Writes to `job_spec.fg_c_output.s3_prefix`.
6. **Inference predictions job**
   - Reads each configured feature input from `job_spec.feature_inputs[*].s3_prefix` + dataset/batch-derived suffix.
   - Writes predictions to `job_spec.output.s3_prefix`.
7. **Prediction-feature-join job**
   - Reads predictions from inference `job_spec.output.s3_prefix`.
   - Reads features using the same inference `feature_inputs[*].s3_prefix` resolution.
   - Writes to destination `destination.s3.s3_prefix` (or `join_output.s3_prefix`) and optionally copies to Redshift.
8. **IF training job**
   - Reads FG-A/FG-C from `job_spec.feature_inputs.{fg_a|fg_c}.s3_prefix`.
   - Writes artifacts/reports to `job_spec.output.{artifacts_s3_prefix,report_s3_prefix}`.
   - Writes trained preprocessing payload back into DynamoDB inference spec.
9. **Machine inventory unload job**
   - Reads/writes inventory data under `job_spec.output.s3_prefix`.
   - Uses Redshift query/unload config from `job_spec.redshift` and `job_spec.query`.

**Gap vs desired structure:** Most data paths are JobSpec-driven already, but several key path conventions are implicit (dataset/batch suffix composition) and not centrally modeled as explicit table contracts, making cross-pipeline compatibility fragile.

#### C) DynamoDB reads/writes (Step Functions and scripts), and required keys
1. **JobSpec loader (`load_job_spec`)**
   - Table from env var `ML_PROJECTS_PARAMETERS_TABLE_NAME` (legacy fallback supported).
   - Reads item key:
     - `project_name` = runtime project
     - `job_name` = `<logical_job_name>#<feature_spec_version>`
2. **Project parameters loader (`load_project_parameters`)**
   - Reads same table/key pattern using default logical job `project_parameters`.
3. **Processing jobs using `load_job_spec`**
   - `delta_builder`, `fg_a_builder`, `pair_counts_builder`, `fg_b_builder`, `fg_c_builder`, `inference_predictions`, `prediction_feature_join`, `if_training`, `machine_inventory_unload` each resolve via the same key convention.
4. **Processing job using `load_project_parameters`**
   - FG-B reads project-level parameters and expects keys such as `ip_machine_mapping_s3_prefix` (or configured alias key).
5. **IF training job direct DynamoDB write-back**
   - Reads and updates `inference_predictions#<feature_spec_version>` item using key `{project_name, job_name}`.
6. **Step Functions DynamoDB usage**
   - Current JSONata state machines only use DynamoDB for execution lock table put/delete (`pk`, `sk`, `ttl_epoch`) and do **not** read ML parameters table for defaults/prefixes.

**Gap vs desired structure:** Step Functions are not parameter-table aware for project-level runtime defaults, script prefixes, or pipeline-specific config bootstrap. They rely on inline defaults and environment substitutions.

### Places where the structure is not yet as required
1. **Hard-coded local script locations in pipeline definitions** (all pipelines except delta-only backfill path), not resolved from DynamoDB-managed script prefix model.
2. **Hard-coded runtime fallback values in Step Functions** (`ndr-project`, `v1`, static timestamps, default target lists), not read from DynamoDB.
3. **Pipeline definition-time static defaults** in several pipelines (default `ProjectName`, `FeatureSpecVersion`, `ProcessingImageUri` literals) instead of table-driven dynamic defaults.
4. **`build_machine_inventory_unload_pipeline` loads JobSpec at pipeline construction time using function default args**, creating drift risk if the runtime project/version differs from defaults.
5. **No unified table schema contract for script objects** (e.g., step→script key map), so script artifact layout cannot be validated centrally.
6. **No explicit DynamoDB-backed parameter contract for derived S3 path composition strategy** (dataset names, batch partition policy), leaving hidden coupling across jobs.
7. **Step Functions do not fetch pipeline-specific required key sets from DynamoDB before starting pipelines**, so missing runtime args are masked by local defaults.

### Comprehensive fix plan (ToDo)
1) **Define an authoritative DynamoDB parameter model (single-table, versioned, complete):**
   - Keep PK/SK: `project_name` + `job_name#feature_spec_version`.
   - Add/standardize logical records:
     - `project_parameters#<version>` (global defaults + shared S3 prefixes + naming conventions).
     - `<pipeline_name>_pipeline#<version>` (pipeline-level script/preset/runtime requirements).
     - Existing per-job records (`delta_builder`, `fg_a_builder`, etc.) remain for job internals.
   - Add mandatory schema blocks:
     - `scripts`: `{code_root_s3_prefix, steps: {<step_name>: {script_s3_uri|script_key, module, entry_args_contract}}}`
     - `data_locations`: explicit input/output prefixes and path-composition policy.
     - `required_runtime_params`: strict list per pipeline/job.
     - `defaults`: optional default values only for non-critical params.
     - `validation`: regex/type requirements for each parameter.

2) **Create script artifact layout policy in S3 (direct file-addressable):**
   - Canonical layout:
     - `s3://<code-bucket>/projects/<project_name>/versions/<feature_spec_version>/pipelines/<pipeline_name>/scripts/<file>.py`
   - Optional shared libs under:
     - `.../shared/`
   - Require each step to reference the exact script object (or zip root with explicit script key), avoiding implicit co-location assumptions.

3) **Refactor all SageMaker pipeline builders to resolve script locations from DynamoDB:**
   - Replace hard-coded `code="src/..."` with runtime resolution from the relevant pipeline/job spec (`scripts.steps[step]`).
   - Add validation: fail pipeline build if any required script mapping is missing.
   - Remove special-case-only behavior where just one pipeline supports `code_s3_uri`; make this universal and table-driven.

4) **Refactor Step Functions to load runtime parameters from DynamoDB before any pipeline start:**
   - Add an initial resolver task (Lambda) that reads `project_parameters#<version>` and pipeline-specific record.
   - Resolution order:
     1. Explicit event payload
     2. DynamoDB defaults
     3. Hard fail (no silent literal fallback for required keys)
   - Remove inline literals like `'ndr-project'`, `'v1'`, static timestamps from state machine definitions.

5) **Enforce strict runtime contract between Step Functions and pipelines:**
   - Before `startPipelineExecution`, validate presence and type of all keys listed in `required_runtime_params`.
   - Ensure each pipeline receives project/version keys needed for downstream JobSpec lookups.
   - Add explicit failure state for missing/invalid runtime parameters.

6) **Unify data prefix resolution contracts across jobs:**
   - Keep job-level input/output prefixes in JobSpec but add centralized schema validation.
   - Externalize dataset naming/path composition (`dataset`, time partitioning, batch-id encoding) into table config to eliminate hidden conventions.
   - Add compatibility checks ensuring producer output contract == consumer input contract.

7) **Fix machine-inventory pipeline definition-time lookup bug:**
   - Remove eager `load_job_spec(...)` that depends on default args at build time.
   - Resolve image URI and other mutable params either:
     - from pipeline parameters at execution time, or
     - from a dedicated resolver step that uses actual runtime `ProjectName`/`FeatureSpecVersion`.

8) **Standardize and govern DynamoDB item lifecycle for multi-project/multi-version support:**
   - Add mandatory metadata fields: `schema_version`, `updated_at`, `owner`, `status`, `checksum`.
   - Add migration tooling for version upgrades and contract validation.
   - Add CI checks that all required logical records exist for every registered project/version.

9) **Observability and safety controls:**
   - Emit structured logs for every resolved script/data prefix and source (payload vs table default).
   - Add drift detection job to compare deployed pipeline definitions vs current DynamoDB records.
   - Add canary validation that each script S3 URI exists and is readable before execution.

10) **Rollout strategy (safe migration):**
   - Phase 1: introduce resolver + dual-read (existing + new table schema).
   - Phase 2: switch pipelines to table-driven script/data resolution.
   - Phase 3: remove hard-coded defaults/paths and enforce strict validation.
   - Phase 4: lock policy in CI/CD with break-glass override only.

11) **Publish an explicit S3 hierarchy standard (documentation deliverable):**
   - Add a dedicated architecture/operations document that defines the **recommended S3 hierarchy** under each project root prefix.
   - The standard must cover both **scripts** and **data**, for example:
     - `s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/<pipeline_name>/...`
     - `s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/<dataset_family>/<dataset_name>/...`
   - The document must include:
     - naming conventions,
     - required sub-prefixes per pipeline/job,
     - ownership/update boundaries,
     - compatibility and retention guidance,
     - examples for 15m features, monthly baselines, inference, join/publication, and training.
   - The resolver/Lambda and DynamoDB schema must reference this documented hierarchy as the authoritative contract.

12) **Minimal script consumption model (simple, DynamoDB-driven, `.py`-first):**
   - Keep the contract intentionally small. For each pipeline step, DynamoDB should provide only:
     - `code_prefix_s3`: S3 prefix where step-consumable code files are stored.
     - `entry_script`: the `.py` filename to execute for the step.
     - `data_prefixes`: named input/output S3 prefixes required by that step.
   - Recommended DynamoDB value examples (per project/version):
     - `code_prefix_s3`:
       - `s3://ndr-bucket/projects/ndr-project/versions/v1/code/pipelines/15m_features/DeltaBuilderStep/`
       - `s3://ndr-bucket/projects/ndr-project/versions/v1/code/pipelines/15m_features/FGABuilderStep/`
       - `s3://ndr-bucket/projects/ndr-project/versions/v1/code/pipelines/inference/InferencePredictionsStep/`
     - `entry_script`:
       - `run_delta_builder.py`
       - `run_fg_a_builder.py`
       - `run_inference_predictions.py`
     - `data_prefixes`:
       - `input_traffic`: `s3://ndr-bucket/projects/ndr-project/versions/v1/data/raw/traffic/`
       - `input_delta`: `s3://ndr-bucket/projects/ndr-project/versions/v1/data/features/delta/`
       - `output_fg_a`: `s3://ndr-bucket/projects/ndr-project/versions/v1/data/features/fg_a/`
       - `output_fg_c`: `s3://ndr-bucket/projects/ndr-project/versions/v1/data/features/fg_c/`
       - `output_predictions`: `s3://ndr-bucket/projects/ndr-project/versions/v1/data/inference/predictions/`
   - Add explicit **placeholder-value guidance** to Dynamo table definition and seeding logic:
     - Include template placeholders for every required IO key (for example `<bucket>`, `<project_name>`, `<feature_spec_version>`, `<pipeline_name>`, `<step_name>`).
     - Ensure seed records are generated for every active pipeline/step with placeholder `code_prefix_s3`, `entry_script`, and `data_prefixes` values following the recommended hierarchy.
     - Provide one baseline placeholder template row per job type (`project_parameters`, feature pipelines, inference, training, machine inventory, prediction join).
   - Add a validation step in table-generation code to verify completeness/alignment:
     - Generated table payload contains all required keys for Step Functions runtime params, Pipeline step script resolution, and script/job data IO prefixes.
     - Missing required keys/placeholders must fail generation with explicit diagnostics.
     - Validation report should list covered pipelines/steps/jobs and unresolved keys before deployment.
   - Remove advanced hardening fields (no hash validation, no artifact-mode complexity, no runtime-compatibility matrix in contract).
   - Use one unified storage style for scripts and dependencies:
     - **Preferred and default:** plain `.py` files under a step/pipeline code prefix.
     - Keep all required local imports alongside the entry script (same prefix tree), so the step can run without extra packaging logic.
   - Recommended code layout under project/version:
     - `.../code/pipelines/<pipeline_name>/<step_name>/entry/<entry_script>.py`
     - `.../code/pipelines/<pipeline_name>/<step_name>/libs/*.py`
     - Optional shared libs for multiple steps:
       - `.../code/shared/*.py`
   - Step-consumption flow (minimal):
     1. Resolver reads `project_name`, `feature_spec_version`, and step config from DynamoDB.
     2. It returns `code_prefix_s3`, `entry_script`, and all required `data_prefixes`.
     3. Pipeline binds `ProcessingStep.code` to the resolved script location and passes only runtime params.
     4. Job starts only after basic existence checks:
        - entry script exists under `code_prefix_s3`,
        - required data prefixes exist/are reachable.
   - This keeps implementation simple while still enforcing your core goal: all script/data prefixes come from DynamoDB and are easy for each consumer step to use.

13) **Acceptance criteria:**
   - Zero hard-coded S3 prefixes in code for scripts/data routing.
   - Zero hard-coded required runtime defaults in Step Functions.
   - Every pipeline start validated against DynamoDB-declared required params.
   - Every script/code location resolved from DynamoDB-backed S3 map.
   - Published S3 hierarchy standard exists and is referenced by resolver + table schema.
   - Every consumer step has a minimal DynamoDB script contract (`code_prefix_s3`, `entry_script`, `data_prefixes`) and passes basic existence checks.
   - Table schema proven to support multiple projects and multiple versions concurrently.
   - Dynamo table generation code includes all required IO keys with recommended placeholder values and fails fast on missing keys for Step Functions, Pipeline steps, and scripts.

**Status:** ToDo.

## 18) Palo Alto ingestion-batch orchestration hardening (ML-only, cron-window aligned)

**Issue:** The Palo Alto ingestion-triggered orchestration still needs an implementation-ready, integrated plan that satisfies the latest constraints:
- payload from the producer is limited to `s3_key` + timestamp,
- runtime parameters must be derived from path + DynamoDB,
- no new state machine for now,
- non-inference/historical extraction must be implemented as a SageMaker Pipeline ProcessingStep,
- and window derivation must follow cron `8-59/15 * * * ? *`.

### Approved implementation scope

1. **Inference path**
   - Keep `sfn_ndr_15m_features_inference.json` as the active orchestrator.
   - Parse path segments to derive `project_name`, `org1`, `org2`, `mini_batch_id`, and `mini_batch_s3_prefix`.
   - Resolve `feature_spec_version` and defaults from DynamoDB project parameters.
   - Derive time window using:
     - floor minute set `{08, 23, 38, 53}` for `batch_start_ts_iso`,
     - source payload timestamp as `batch_end_ts_iso`.
   - Preserve lock-table conditional-write idempotency behavior.

2. **Non-inference (initial deployment/backfill) path**
   - Reuse `sfn_ndr_backfill_reprocessing.json` (no new Step Function).
   - Add a preliminary SageMaker ProcessingStep that:
     - enumerates historical mini-batch folders,
     - extracts `project_name` and `mini_batch_id` from S3 paths,
     - looks up `feature_spec_version` in DynamoDB by `project_name`,
     - reads S3 `LastModified` as source timestamp,
     - computes `batch_start_ts_iso` via floor minutes `{08,23,38,53}`,
     - assigns `batch_end_ts_iso = LastModified`,
     - emits execution rows consumed by backfill map execution.

3. **Pair-Counts builder hardening (mandatory, no fallback behavior)**
   - Refactor Pair-Counts builder to rely on DynamoDB-provided field specification only (same principle used by Delta builder).
   - Remove hard-coded input field assumptions in Pair-Counts read path.
   - Move Pair-Counts to typed JobSpec loader path (`JobSpecLoader.load`) instead of plain dict retrieval, and enforce typed schema contract for required input mappings.
   - Update Pair-Counts JobSpec contract in DynamoDB specification with explicit field mapping keys for source/destination IP, destination port, and event timestamps.
   - Update supporting components as needed (loader models/spec docs/tests) so Pair-Counts can parse flattened raw-log fields smoothly via specification-driven mapping.
   - No compatibility fallback path is required; existing behavior can be replaced directly per current project constraints.

4. **DynamoDB defaults and generation script**
   - Ensure project-parameter defaults include window policy (`WindowCronExpression`, `WindowFloorMinutes`) and runtime fallbacks.
   - Update the table provisioning script to seed/maintain required defaults for this flow.
   - Seed/validate Pair-Counts field-mapping keys as part of generated defaults/templates.

5. **Documentation synchronization**
   - Update strategy and orchestration docs to match the above behavior and contracts.
   - Explicitly state that no persistent batch catalog is introduced in this phase.

### Detailed implementation checklist

- [ ] Add/confirm parser utility for project/batch extraction from `s3_key`.
- [ ] Add/confirm time-window utility implementing cron-floor minutes `{08,23,38,53}`.
- [ ] Wire inference runtime derivation to use timestamp-floor policy and actual timestamp end.
- [ ] Implement preliminary SageMaker ProcessingStep for historical folder extraction + attachment.
- [ ] Wire backfill map input contract to extractor output (`mini_batch_id`, `batch_start_ts_iso`, `batch_end_ts_iso`).
- [ ] Extend Dynamo defaults and provisioning script entries for window policy values.
- [ ] Refactor Pair-Counts to typed JobSpec loader path and specification-only input-field mapping.
- [ ] Update Pair-Counts schema contract docs and Dynamo seed/template generation for required field mappings.
- [ ] Add tests:
  - path extraction cases,
  - floor-minute window derivation correctness,
  - duplicate-lock behavior on repeated input,
  - extractor output schema/contract tests,
  - Pair-Counts field-mapping resolution and required-column enforcement via typed spec.
- [ ] Run final repository check and revert any unrelated changes before merge.

### Acceptance criteria

- One mini-batch event results in exactly one successful orchestration path unless lock detects duplicate.
- Derived windows always align to floor minutes `08/23/38/53`.
- Historical runs can process previous-month data with deterministic `mini_batch_id` + window pairs.
- Backfill orchestration consumes extractor output without introducing additional state machines.
- Pair-Counts reads source fields only via Dynamo specification (typed contract), with no hard-coded field fallback.
- Documentation, Dynamo defaults, and orchestration definitions are consistent.

**Status:** Implemented. Inference/backfill orchestration contracts now derive runtime windows from source timestamps (08/23/38/53 floor policy), backfill includes a preliminary historical extractor pipeline step whose output feeds map execution, and both Pair-Counts/Delta raw reads are specification-driven via Dynamo field mappings, with docs/defaults/tests synchronized.

## 19) Orchestration simplification plan: producer-completion payload contract, callback/lambda removal, and training data verifier
**Issue:** Current orchestration/docs still include patterns that are now out of alignment with the agreed target model (single producer completion event contract, reduced orchestration indirection, and no human-approval/lambda-based control gates).

**Plan (ToDo):**
1. **Ingestion contract and 15m Step Function parsing update**
   - Update all docs that still describe marker-file-only ingestion (`_SUCCESS`) to the agreed producer completion event contract:
     - event contains only the full S3 batch-folder path (up to and including hashed folder) and event timestamp.
   - Update `sfn_ndr_15m_features_inference.json` parsing logic to use the event payload + timestamp directly for runtime derivation (`project_name`, `feature_spec_version`, `mini_batch_id`, `batch_start_ts_iso`, `batch_end_ts_iso`) without requiring `_SUCCESS` suffix filtering.

2. **Remove callback orchestration dependence across Step Functions**
   - Replace `lambda:invoke.waitForTaskToken` callback waits with direct orchestration control returned from SageMaker pipeline execution completion.
   - If completion tokening is truly required, use a pipeline-native final step to emit a deterministic completed signal/token (no external callback lambda dependency), and consume that in orchestration.

3. **Replace `SupplementalBaselineLambda`**
   - Move supplemental baseline logic into a SageMaker Pipeline (or native Pipeline step) so monthly baseline flow remains fully pipeline-driven.

4. **Replace `PublishJoinedPredictionsLambda`**
   - Move joined-prediction publication to a SageMaker Pipeline (or native Pipeline step) with explicit destination routing and status outputs.

5. **Replace `ModelPublishLambda`**
   - Move model artifact publication/registration preparation to a SageMaker Pipeline (or native Pipeline step).

6. **Replace `ModelAttributesRegistryLambda`**
   - Move model attributes registration logic to a SageMaker Pipeline (or native Pipeline step), with idempotent writes and deterministic keys.

7. **Replace duplicate lambda-dependent publication/deployment references**
   - Consolidate duplicated model-publish lambda replacement requirements and ensure there is one authoritative pipeline-driven implementation path.

8. **Remove all human-approval waits/branches**
   - Remove wait-for-human-approval orchestration branches and associated state transitions from all workflows.

9. **Replace `ModelDeployLambdaArn`**
   - Move deployment logic to a SageMaker Pipeline (or native Pipeline step), preserving rollback and status-reporting semantics.

10. **Documentation synchronization for lambda/approval removal**
    - Update all docs to reflect the final architecture with no callback lambdas, no auxiliary lambdas for publication/deployment, and no human-approval states.

11. **Add training data verifier and auto-remediation trigger path**
    - Extend `sfn_ndr_training_orchestrator` with a pre-training verifier stage that validates feature availability/coverage for:
      - training period,
      - required historical lookback slices,
      - evaluation periods.
    - On verifier failure, trigger existing feature-creation process bounded to missing-window timestamps only, then re-run training.
    - Apply a bounded remediation loop with **maximum 2 retries** total for: (a) feature-availability verification, (b) missing-feature creation, and (c) retraining attempt; fail closed with diagnostics after retry budget is exhausted.
    - Define runtime inputs for explicit window boundaries (e.g., `training_start_ts`, `training_end_ts`, `eval_start_ts`, `eval_end_ts`, and optional `missing_windows_override`).

12. **Trigger prediction publication directly from 15m workflow**
    - Trigger `sfn_ndr_prediction_publication` directly from the 15m workflow path after inference completion, removing EventBridge dependency for this handoff.

13. **Feasibility/risk gate (mandatory before implementation approval)**
    - Produce an explicit implementation-feasibility report before execution, and flag any sub-item that is:
      - not implementable as specified,
      - operationally unsafe/undesired,
      - blocked by AWS integration limits,
      - incompatible with existing deployment/runtime contracts.
    - For each flagged item, include recommended alternative and migration/rollback approach.


14. **Concrete integration changes required to remove WaitCallback across asynchronous pipeline starts**
    - For every state machine using `arn:aws:states:::aws-sdk:sagemaker:startPipelineExecution`, replace callback waits with a polling completion pattern:
      - add `DescribePipelineExecution` polling states (`arn:aws:states:::aws-sdk:sagemaker:describePipelineExecution`),
      - add status choice branching (`Executing`/`Stopping` => wait+poll loop, `Succeeded` => continue, `Failed|Stopped` => fail path),
      - add bounded wait interval + max poll attempts/time-budget to avoid unbounded execution.
    - Apply this change in all affected orchestrators:
      - `sfn_ndr_15m_features_inference.json` (features pipeline completion and inference pipeline completion),
      - `sfn_ndr_monthly_fg_b_baselines.json` (machine inventory completion and FG-B completion),
      - `sfn_ndr_training_orchestrator.json` (training completion),
      - `sfn_ndr_backfill_reprocessing.json` (historical extractor completion and per-window backfill completion),
      - `sfn_ndr_prediction_publication.json` (prediction join completion).
    - Remove `PipelineCompletionCallbackLambdaArn` references from state-machine definitions and deployment templates once polling paths are validated.
    - Add Step Functions retry/backoff policies for transient SageMaker API failures on both `startPipelineExecution` and `describePipelineExecution` calls.
    - Update IAM policies for state-machine roles to include `sagemaker:DescribePipelineExecution` where missing.

15. **Concrete changes required for direct 15m -> prediction-publication idempotency and duplicate suppression**
    - Add deterministic execution identity fields to the publication input contract and persist them end-to-end:
      - `project_name`, `feature_spec_version`, `mini_batch_id`, `batch_start_ts_iso`, `batch_end_ts_iso`.
    - Introduce an orchestration-level publication lock (DynamoDB conditional write) keyed by the deterministic identity above.
      - On duplicate lock conflict, short-circuit publication as already-processed.
      - On failure paths, release or transition lock state according to terminal outcome policy.
    - Add sink-level dedupe keys/constraints to publication outputs (S3 manifest key and/or table primary key/merge key).
    - Ensure publication pipeline writes are idempotent (`upsert/merge` semantics, deterministic output partitioning, and no append-only duplicates for retries).
    - Add contract tests covering:
      - first-run publish success,
      - replay with same identity (suppressed),
      - partial-failure then retry (single final published record set).
    - Add operational metrics/alerts for duplicate-suppression events and lock-conflict counts.


### Item 19 implementation sequencing (for smooth delivery)
1. Update ingestion contract docs + 15m parsing contract first (item 1).
2. Implement callback-removal mechanics and IAM/polling topology updates (item 14), then remove callback lambda references (item 2).
3. Replace lambda-owned business/control steps with pipeline-native steps in this order: baseline supplements, publication, model publish/attributes/deploy (items 3–7, 9).
4. Remove human-approval branches after deployment path parity is validated (item 8).
5. Add training verifier + bounded remediation loop (item 11), then execute feasibility/risk review and rollback playbooks (item 13).
6. Enable direct 15m -> prediction-publication trigger only after idempotency/duplicate-suppression controls and tests are passing (items 12 + 15).
7. Finish with full documentation synchronization across architecture/orchestration/runtime docs (item 10).

### Item 19 definition-of-done / acceptance checklist
- All targeted Step Functions no longer depend on `lambda:invoke.waitForTaskToken` for SageMaker pipeline completion.
- No state machine references to `PipelineCompletionCallbackLambdaArn`, `SupplementalBaselineLambdaArn`, `PublishJoinedPredictionsLambdaArn`, `ModelPublishLambdaArn`, `ModelAttributesRegistryLambdaArn`, or `ModelDeployLambdaArn` remain.
- No human-approval wait states remain in any orchestration JSON.
- 15m ingestion accepts the agreed producer completion event schema (hashed-folder path + timestamp) without `_SUCCESS` marker dependency.
- Training verifier path enforces max 2 retries and emits terminal diagnostics on retry exhaustion.
- Direct 15m -> publication chain passes idempotency/dedupe contract tests and produces no duplicate published records under replay/retry scenarios.
- Operational dashboards/alerts include polling failures, retry-exhaustion events, lock conflicts, and duplicate-suppression counts.
- Docs and deployment templates are fully synchronized with the implemented architecture.

**Status:** Implemented. Step Functions now use native SageMaker polling instead of callback lambdas, lambda-owned control steps were replaced with pipeline-native stages, the 15m producer payload contract and direct publication trigger were updated, training verifier/remediation with max-2 retries was added, and docs/tests were synchronized for the new orchestration model.

## 20) Consolidate prediction publication into a single join+publish pipeline (S3 and Redshift)
**Issue:** `sfn_ndr_prediction_publication` still models a second placeholder pipeline stage (`StartPublicationPipeline`) even though the implemented join pipeline already supports writing the joined output to S3 or Redshift. The current split introduces orchestration overhead and leaves an unimplemented placeholder path.

**Decision:** Use a single SageMaker pipeline (`PipelineNamePredictionJoin`) to execute both operations end-to-end:
- perform the prediction/feature join,
- publish joined outputs to S3 **or** Redshift (including required S3 staging for Redshift loads),
- preserve publication idempotency and duplicate suppression at orchestration level.

### Implementation plan (authoritative)

1. **Step Functions simplification (`sfn_ndr_prediction_publication`)**
   - Remove the placeholder publish pipeline branch:
     - `StartPublicationPipeline`
     - `DescribePublicationPipeline`
     - `PublishPipelineStatusChoice`
     - `WaitBeforePublishDescribe`
     - `IncrementPublishPollAttempt`
   - Route join success directly from `JoinPipelineStatusChoice` to `MarkPublicationSucceeded`.
   - Keep existing lock semantics unchanged:
     - `AcquirePublicationLock` conditional write,
     - `DuplicatePublicationSuppressed` short-circuit,
     - `MarkPublicationFailed` on terminal failure,
     - `EmitPublicationEvent` after success.

2. **Single-pipeline publication behavior (`prediction_feature_join`)**
   - Keep `prediction_feature_join` as the only publication executor.
   - Ensure spec-driven destination routing remains explicit:
     - `destination.type = s3` => write joined dataset to publication S3 prefix,
     - `destination.type = redshift` => stage joined dataset to S3 and load into Redshift with deterministic/idempotent semantics.
   - Add strict schema/contract validation for required publication keys and sink-specific required fields before write/copy.

3. **DynamoDB JobSpec contract cleanup**
   - Remove `pipeline_prediction_publish` from bootstrap items and runtime-parameter maps.
   - Keep and document `pipeline_prediction_feature_join` as the sole pipeline-level publication contract.
   - Ensure seeded/default specs contain both S3 and Redshift publication examples for `prediction_feature_join` destination routing.

4. **Pipeline registration / deployment cleanup**
   - Remove deployment-time dependency on `PipelineNamePredictionPublish`.
   - Keep `PipelineNamePredictionJoin` as the only publication pipeline placeholder required by `sfn_ndr_prediction_publication`.
   - Update IAM/templates only where needed due to removed placeholder references.

5. **Documentation synchronization (required in same change set)**
   - Update flow docs to reflect one-stage publication execution:
     - `docs/pipelines/pipelines_flow_description.md`
     - `docs/architecture/orchestration/step_functions.md`
     - `docs/architecture/orchestration/dynamodb_io_contract.md`
   - Remove references that imply an implemented second publish pipeline.
   - Clarify that publication occurs within the join pipeline and is destination-driven (`s3` or `redshift`).

6. **Test plan / quality gates**
   - Update Step Functions contract tests to verify:
     - publication flow has no `StartPublicationPipeline` state,
     - join success transitions directly to success lock update,
     - lock duplicate-suppression behavior remains intact.
   - Add/extend processing tests to verify single-pipeline publication behavior:
     - S3 publication path writes expected partitioned output,
     - Redshift path performs staged-copy flow with required parameters,
     - retries/replays do not create duplicate published records.
   - Add regression assertions for docs/contracts consistency (pipeline names/placeholders and JobSpec expectations).

### Rollout, migration, and rollback
- **Rollout order:**
  1) ship join-pipeline publication validations/tests,
  2) deploy simplified `sfn_ndr_prediction_publication`,
  3) remove `PipelineNamePredictionPublish` wiring from IaC/config,
  4) clean up Dynamo seed templates.
- **Rollback:** restore prior Step Functions definition and placeholder wiring if any production dependency still expects the second pipeline state.

### Definition of done
- Publication executes fully via `PipelineNamePredictionJoin` (join + publish), with support for both S3 and Redshift destinations.
- `sfn_ndr_prediction_publication` has no `StartPublicationPipeline` branch.
- No required runtime/deployment contract depends on `PipelineNamePredictionPublish` or `pipeline_prediction_publish`.
- Lock-based idempotency and duplicate suppression behavior is preserved and tested.
- All relevant docs are synchronized and describe the single-pipeline publication architecture.

**Status:** Implemented. `sfn_ndr_prediction_publication` now completes publication after `PipelineNamePredictionJoin` succeeds (no secondary publish pipeline states), bootstrap/runtime contracts no longer seed `pipeline_prediction_publish`, and publication validation for destination routing/idempotent writes is enforced in `prediction_feature_join`.

## 21) Remove monthly supplemental baseline placeholder by hardening FG-B canonical publication and snapshot contracts
**Issue:** `sfn_ndr_monthly_fg_b_baselines` still contains `StartSupplementalBaselinePipeline` as an unimplemented placeholder. Current FG-B outputs are written under timestamped batch prefixes, while FG-C reads canonical FG-B paths (`/host`, `/segment`, `/ip_metadata`, `/pair/*`), creating integration/contract gaps and leaving unnecessary orchestration overhead.

**Goal:** Eliminate the supplemental placeholder path by making FG-B the authoritative publisher of FG-C-consumable canonical baseline artifacts with deterministic monthly overwrite semantics and exactly one active snapshot per horizon.

### Implementation plan (authoritative)

1. **Refactor FG-B write contract to canonical publish mode**
   - Update `fg_b_builder_job` output writing so canonical FG-B paths become first-class outputs:
     - `<fg_b_output_prefix>/host/`
     - `<fg_b_output_prefix>/segment/`
     - `<fg_b_output_prefix>/ip_metadata/`
     - `<fg_b_output_prefix>/pair/host/`
     - `<fg_b_output_prefix>/pair/segment/`
   - Remove reliance on timestamped-only final output locations for the monthly baseline publication path.
   - Keep optional archival/history behavior only if needed for audit/backfill, but decouple it from FG-C online baseline-read contract.

2. **Enforce deterministic overwrite semantics and “one active snapshot per horizon”**
   - For each horizon (`7d`, `30d`), publish exactly one active baseline snapshot per `feature_spec_version` in canonical paths.
   - Implement safe publish mechanics (stage/verify/promote or equivalent) so readers never observe partial canonical writes.
   - Ensure all canonical writes are idempotent under retry/re-execution and do not accumulate duplicate active partitions.
   - Emit/record publish metadata (e.g., reference time, baseline bounds, run id) to support observability and rollback.

3. **Preserve and validate FG-C compatibility contracts**
   - Verify canonical FG-B outputs preserve columns required by FG-C:
     - host/segment baseline stat families (`*_median`, `*_p25`, `*_p75`, `*_p95`, `*_p99`, `*_mad`, `*_iqr`, `*_support_count`),
     - `baseline_horizon`,
     - `ip_metadata` cold-start fields,
     - optional pair rarity schema if enabled.
   - Confirm FG-C joins continue to work for warm/cold routing and pair rarity fallback behavior.
   - If any schema/key drift is introduced, update FG-C joins/validation and corresponding docs/tests in the same change set.

4. **Step Functions simplification (`sfn_ndr_monthly_fg_b_baselines`)**
   - Remove supplemental placeholder branch states:
     - `StartSupplementalBaselinePipeline`
     - `DescribeSupplementalPipeline`
     - `SupplementalPipelineStatusChoice`
     - `WaitBeforeSupplementalDescribe`
     - `IncrementSupplementalPollAttempt`
   - Route FG-B success directly to `EmitBaselineReadyEvent`.
   - Keep machine-inventory -> FG-B ordering unchanged.
   - Retain bounded polling/retry/backoff patterns and explicit terminal failure behavior.

5. **Pipeline/runtime contract cleanup**
   - Remove `pipeline_supplemental_baseline` bootstrap/runtime parameter contract entries.
   - Remove deployment/IaC placeholder dependency on `PipelineNameSupplementalBaseline`.
   - Ensure project parameter seed payloads and validation logic are updated to reflect the simplified monthly orchestration.

6. **Data migration/backward compatibility and rollback planning**
   - Provide one-time migration guidance for environments currently containing FG-B outputs only under timestamped paths.
   - Define a cutover procedure:
     1) deploy FG-B canonical publish,
     2) run one monthly baseline cycle,
     3) verify canonical artifacts,
     4) deploy simplified Step Function.
   - Define rollback plan:
     - restore prior state machine definition,
     - re-enable placeholder wiring if emergency fallback is required,
     - preserve previous artifact locations for rapid reversion.

7. **Documentation synchronization (required in same delivery wave)**
   - Update all relevant docs to describe the post-change architecture and contracts:
     - `docs/pipelines/pipelines_flow_description.md`
     - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json` (if tracked as source-of-truth artifact)
     - `docs/architecture/orchestration/step_functions.md`
     - `docs/architecture/diagrams/pipeline_flow.md` (+ generated diagram artifact if applicable)
     - `docs/feature_builders/current_state.md`
     - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
     - any runbook/deployment docs that still mention supplemental monthly baseline pipeline placeholder wiring.
   - Explicitly document canonical FG-B publication semantics, monthly overwrite policy, and active-snapshot guarantees.

8. **Testing, checks, and verification gates (must pass before completion)**
   - **Unit/integration processing checks**
     - Add/extend FG-B tests to verify canonical-path writes and deterministic overwrite behavior.
     - Add/extend FG-C tests to verify successful reads from canonical FG-B paths and no regression in warm/cold baseline routing.
     - Add pair rarity path tests for enabled/disabled modes and fallback behavior.
   - **Orchestration contract checks**
     - Update Step Functions contract tests to assert no supplemental states remain in monthly baseline workflow.
     - Verify monthly flow still emits `NdrMonthlyBaselinesCompleted` only after FG-B success.
   - **Bootstrap/contract checks**
     - Update/create tests ensuring `pipeline_supplemental_baseline` is removed from seeded runtime contracts.
     - Verify required runtime params and code-prefix contracts remain valid for retained pipelines.
   - **End-to-end dry-run verification**
     - Validate one monthly execution end-to-end in a non-prod environment:
       - machine inventory completes,
       - FG-B publishes canonical artifacts,
       - Step Function reaches completion event,
       - FG-C batch reads canonical baselines without ambiguity.
   - **Operational readiness checks**
     - Validate logs/metrics for publish success, overwrite idempotency, and baseline availability.
     - Add/verify alerts for failed canonical publish, missing horizon artifacts, and monthly orchestration failure.

9. **Completeness matrix (required file-level coverage, including omissions)**
   - **Processing code**
     - `src/ndr/processing/fg_b_builder_job.py`: implement canonical publish paths, deterministic overwrite mechanics, snapshot metadata emission, and idempotent retry behavior.
     - `src/ndr/processing/output_paths.py`: update/extend helpers only if needed so FG-B canonical publication semantics are explicit and not coupled to timestamped batch-only paths.
     - `src/ndr/processing/fg_c_builder_job.py`: confirm read-contract compatibility remains stable (or update with explicit snapshot selectors if the final design introduces them).
   - **Pipeline definitions and registration contracts**
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`: ensure FG-B pipeline runtime contract remains aligned with revised output/publication semantics.
     - `src/ndr/scripts/create_ml_projects_parameters_table.py`: remove `pipeline_supplemental_baseline` seeds/runtime params and keep retained pipeline contracts valid.
     - Any IaC/deployment template wiring pipeline placeholders: remove `PipelineNameSupplementalBaseline` references.
   - **Orchestration definitions and tests**
     - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`: remove supplemental states and route FG-B success directly to baseline-ready event.
     - `tests/test_step_functions_item19_contracts.py` and any monthly-orchestration tests: assert supplemental states are absent and success routing is correct.
   - **Builder/contract tests**
     - `tests/test_fg_b_builder_job.py`: add canonical write + overwrite-idempotency assertions.
     - `tests/test_fg_c_builder_job.py`: verify warm/cold baseline routing and pair rarity behavior using canonical FG-B outputs.
     - `tests/test_create_ml_projects_parameters_table.py`: verify `pipeline_supplemental_baseline` removal and seed consistency.
   - **Documentation updates (must all be synchronized)**
     - `docs/pipelines/pipelines_flow_description.md`
     - `docs/architecture/orchestration/step_functions.md`
     - `docs/architecture/diagrams/pipeline_flow.md` (+ rendered diagram artifact)
     - `docs/feature_builders/current_state.md`
     - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
     - Any deployment/runbook doc that still lists supplemental placeholder wiring.
   - **Explicit omissions to avoid (must be checked before merge)**
     - Leaving stale placeholder names (`PipelineNameSupplementalBaseline`, `pipeline_supplemental_baseline`) in docs/tests/IaC.
     - Implementing canonical FG-B writes without partial-write safety/atomic publish checks.
     - Removing orchestration states without preserving equivalent failure signaling/observability.
     - Updating code without matching docs/tests and migration guidance.

10. **Execution/verification checklist (authoritative go-live gate)**
   - Run full relevant automated tests (Step Functions contract tests, FG-B/FG-C unit tests, seed/contract tests).
   - Perform non-prod monthly dry-run and collect evidence for:
     - machine inventory success,
     - FG-B canonical publish success,
     - no supplemental orchestration dependency,
     - baseline-ready event emission,
     - FG-C successfully reading canonical baselines for both horizons.
   - Validate monitoring/alerts and runbook updates are complete.
   - Record rollback drill outcome (or documented simulation) before production rollout.

### Definition of done
- Monthly state machine contains no supplemental placeholder pipeline states.
- No deployment/runtime contract requires `PipelineNameSupplementalBaseline` or `pipeline_supplemental_baseline`.
- FG-B is the single authoritative canonical publisher for FG-C-consumable baselines.
- Canonical FG-B paths expose exactly one active snapshot per horizon and are idempotent under retries.
- FG-C and pair-rarity integrations are verified to function against canonical FG-B outputs without schema/join regressions.
- All architecture/orchestration/contract documentation is synchronized with the implemented post-change state.
- End-to-end monthly baseline workflow validation passes in pre-production with explicit evidence captured.

**Status:** Planned.
