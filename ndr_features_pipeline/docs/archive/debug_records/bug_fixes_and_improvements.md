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
**Status:** Fully implemented.

## 2) Missing `load_job_spec` helper
**Issue:** Builders call `load_job_spec(...)`, but `job_spec_loader.py` only exposes `JobSpecLoader`.
- **Files:**
  - `src/ndr/config/job_spec_loader.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/fg_c_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add a `load_job_spec(project_name, job_name, feature_spec_version, table_name=None)` function to wrap `JobSpecLoader` and resolve the correct item from DynamoDB, including feature spec versioning.
**Status:** Fully implemented.

## 3) Missing runtime config objects for CLI scripts
**Issue:** `run_delta_builder.py` and `run_fg_a_builder.py` import runtime config classes/functions that do not exist in the job modules.
- **Files:**
  - `src/ndr/scripts/run_delta_builder.py`
  - `src/ndr/scripts/run_fg_a_builder.py`
  - `src/ndr/processing/delta_builder_job.py`
  - `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Add `DeltaBuilderJobRuntimeConfig` + `run_delta_builder_from_runtime_config(...)` and `FGABuilderJobRuntimeConfig` + `run_fg_a_builder_from_runtime_config(...)` (or update scripts to call the actual entrypoints).
**Status:** Fully implemented.

## 4) S3Writer method mismatch
**Issue:** `S3Writer` only defines `write_parquet_partitioned`, but FG-B and Pair-Counts call `write_parquet`.
- **Files:**
  - `src/ndr/io/s3_writer.py`
  - `src/ndr/processing/fg_b_builder_job.py`
  - `src/ndr/processing/pair_counts_builder_job.py`
**Fix:** Add `write_parquet` wrapper or update call sites to use `write_parquet_partitioned` with the correct parameters.
**Status:** Fully implemented.

## 5) S3Reader constructor mismatch
**Issue:** `S3Reader` requires a `SparkSession`, but FG-B instantiates it with no arguments.
- **Files:**
  - `src/ndr/io/s3_reader.py`
  - `src/ndr/processing/fg_b_builder_job.py`
**Fix:** Either pass `SparkSession` into `S3Reader(...)` or provide a convenience constructor/factory.
**Status:** Fully implemented.

## 6) Pipeline definition references undefined steps/variables
**Issue:** `build_15m_streaming_pipeline` uses `fg_c_step` without defining it; `build_fg_b_baseline_pipeline` references undefined variables (`mini_batch_id`, `batch_start_ts_iso`, `pair_counts_step`).
- **File:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
**Fix:** Define all steps and parameters explicitly or remove FG-C from the streaming pipeline until wiring is complete.
**Status:** Fully implemented.

## 7) Delta vs FG-A schema naming mismatch
**Issue:** Delta builder emits `bytes_sent_sum`/`bytes_recv_sum` and `drop_cnt`, but FG-A expects `bytes_src_sum`/`bytes_dst_sum` and `deny_cnt` (and silently skips missing columns).
- **Files:**
  - `src/ndr/processing/delta_builder_operators.py`
  - `src/ndr/model/fg_a_schema.py`
  - `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Align column names via renaming or schema mapping so FG-A receives the intended metrics.
**Status:** Fully implemented.

## 8) FG-C metrics with MAD/IQR fallback
**Issue:** When `m_mad` is null/zero, FG-C falls back to IQR for `z_mad`, but `abs_dev_over_mad` still divides by MAD only.
- **File:** `src/ndr/processing/fg_c_builder_job.py`
**Fix:** When MAD is null/zero, compute `abs_dev_over_mad` using IQR as fallback (or rename to `abs_dev_over_scale` to reflect behavior).
**Status:** Fully implemented.

## 9) JobSpec versioning not enforced in loader
**Issue:** The JobSpec loader does not account for feature spec version in its lookup key.
- **File:** `src/ndr/config/job_spec_loader.py`
**Fix:** Incorporate `feature_spec_version` into the DynamoDB lookup key (e.g., `job_name#feature_spec_version`) and align the table name environment variable to `ML_PROJECTS_PARAMETERS_TABLE_NAME` across code and docs.
**Status:** Fully implemented.

## 10) Feature Store ingestion performance
**Issue:** FG-A Feature Store ingestion uses `foreachPartition` with per-row `put_record`, which may be slow at scale.
- **File:** `src/ndr/processing/fg_a_builder_job.py`
**Fix:** Consider S3-based offline ingestion or batch `put_record` with retries/backoff to improve throughput.
**Status:** Fully implemented.

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
**Status:** Fully implemented.

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

**Status:** Fully implemented.

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
**Status:** Fully implemented.
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

**Status:** Fully implemented.

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

**Status:** Fully implemented.

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

**Status:** Fully implemented.

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

**Status:** Fully implemented.

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

**Status:** Fully implemented.

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

**Status:** Fully implemented.

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

**Status:** Fully implemented.

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


### Completeness verification and mandatory gap-closure checklist (required before implementation sign-off)
To ensure the plan is correct, complete, and workable end-to-end, implementation must explicitly close all items below and record evidence per item.

#### 1) Code-path coverage (all required modifications)
1. **IF training runtime/spec layer**
   - `src/ndr/processing/if_training_spec.py`: add typed fields for evaluation windows, history planner policy, remediation toggles, and evaluation output sinks.
   - `src/ndr/scripts/run_if_training.py`: add argument plumbing for multi-window evaluation payload and optional toggles.
2. **IF training execution layer**
   - `src/ndr/processing/if_training_job.py`:
     - add history planner computation + persisted artifact,
     - replace placeholder remediation with real orchestrated invocations,
     - add post-training evaluation replay orchestration,
     - add final report expansion for evaluation artifacts and provenance.
3. **Pipeline definition layer**
   - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`:
     - add parameters for evaluation windows/toggles,
     - add step sequencing dependencies reflecting planner→verify→remediate→reverify→train→evaluate→publish/attributes/deploy (or equivalent coherent order).
4. **Orchestration definitions**
   - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`:
     - add parameter resolution/validation for evaluation windows and toggles,
     - ensure remediation/evaluation orchestration branches are represented and bounded.
   - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`:
     - confirm contract compatibility with planner-produced windows payload.
   - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json` (or equivalent FG-B orchestration owner):
     - ensure callable contract supports remediation-triggered FG-B rebuild windows/reference periods.
5. **Evaluation reuse integration (must avoid duplicate logic)**
   - Confirm invocation contracts are compatible with:
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
     - `src/ndr/processing/inference_predictions_job.py`
     - `src/ndr/processing/prediction_feature_join_job.py`
6. **DynamoDB seed/contracts**
   - `src/ndr/scripts/create_ml_projects_parameters_table.py`: seed new defaults/spec fields and runtime requirements.
   - Ensure validators enforce presence/shape and fail-fast behavior.

#### 2) Documentation coverage (all required updates)
The following docs must be updated in the same delivery stream so post-implementation state is accurately documented:
- `docs/TRAINING_PROTOCOL_IF.md`
- `docs/TRAINING_PROTOCOL_IF_VERIFICATION.md`
- `docs/architecture/orchestration/step_functions.md`
- `docs/pipelines/pipelines_flow_description.md`
- `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
- `docs/architecture/orchestration/dynamodb_io_contract.md`
- relevant FG-B/FG-C references where safety-gap/horizon planner behavior is described.

#### 3) Explicit omissions that must be preserved (no unintended scope creep)
- Do **not** duplicate inference scoring/join algorithms; only orchestrate reuse.
- Do **not** remove existing fail-fast validation semantics; extend them for new params.
- Do **not** introduce mandatory rollout/canary requirements; rollout controls remain optional toggles.
- Do **not** silently change FG-A/FG-B/FG-C output schemas without synchronized contract docs/tests.

#### 4) Verification evidence matrix (required artifacts)
Implementation PR(s) must include evidence links/artifacts for:
- formula correctness tests (`W_15m`, `B_start/B_end`, `W_required`) including concrete 44-day derivation under current policy,
- remediation invocation payload correctness (15m backfill + FG-B rebuild),
- evaluation replay output manifests and joined output sink writes,
- SageMaker Experiments lineage screenshots/log excerpts/JSON artifacts,
- orchestration bounded retry/idempotency behavior under replay.

#### 5) Final implementation readiness gate (must pass)
A final coordinated review must confirm:
- all files in sections (1) and (2) were updated or explicitly marked N/A with rationale,
- tests/checks from this item all pass,
- no unresolved TODO/placeholder remains in planner/remediation/evaluation paths,
- operational runbook notes (inputs, toggles, rollback) are documented.

### Definition of done
- Monthly state machine contains no supplemental placeholder pipeline states.
- No deployment/runtime contract requires `PipelineNameSupplementalBaseline` or `pipeline_supplemental_baseline`.
- FG-B is the single authoritative canonical publisher for FG-C-consumable baselines.
- Canonical FG-B paths expose exactly one active snapshot per horizon and are idempotent under retries.
- FG-C and pair-rarity integrations are verified to function against canonical FG-B outputs without schema/join regressions.
- All architecture/orchestration/contract documentation is synchronized with the implemented post-change state.
- End-to-end monthly baseline workflow validation passes in pre-production with explicit evidence captured.

**Status:** Fully implemented.

---

## 22) Redesign `sfn_ndr_training_orchestrator` to coarse-grained mode with one fully implemented unified training pipeline (Option A)
**Issue:** The training orchestrator currently contains five placeholder/unimplemented pipeline stages (`training_data_verifier`, `missing_feature_creation`, `model_publish`, `model_attributes`, `model_deploy`) and corresponding polling branches. This creates orchestration complexity and contract drift while core behavior should be owned by one pipeline-native training lifecycle.

**Goal:** Implement Option A end-to-end: keep Step Functions coarse-grained and make a single SageMaker training pipeline fully own verifier + remediation + training + publish + attributes + deploy, with bounded retries, deterministic contracts, and production-grade observability.

### Implementation plan (authoritative)

1. **Architecture baseline and invariants (must be captured first)**
   - Establish canonical target architecture:
     - `sfn_ndr_training_orchestrator` only performs input normalization, runtime resolution, starts one training pipeline, polls status, emits success/failure.
     - `pipeline_if_training` becomes the sole implementation owner for:
       1) training/eval data verification,
       2) missing-window remediation,
       3) IF model training,
       4) model publication,
       5) model attributes registration,
       6) model deployment.
   - Preserve fail-closed behavior and bounded retry semantics currently expected by orchestration contracts.
   - Define compatibility policy for rollout:
     - backward-compatible input fields retained at Step Function boundary,
     - explicit deprecation path for orphan placeholder pipeline names and Dynamo job-spec entries.

2. **Refactor Step Functions definition to coarse-grained orchestration**
   - Update `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json` to:
     - remove verifier/remediation branches and all model lifecycle sub-pipeline branches,
     - retain a single `StartTrainingPipeline` + `DescribeTrainingPipeline` polling loop,
     - route successful completion directly to terminal success,
     - route failures to explicit terminal failure with deterministic cause/error identifiers.
   - Ensure runtime parameter resolution still supports all required fields for the unified pipeline.
   - Keep polling backoff/retry patterns aligned with existing state machine conventions.

3. **Expand unified training pipeline contract (`pipeline_if_training`)**
   - Extend required runtime parameters beyond current training-only keys to include verifier/remediation inputs:
     - `TrainingStartTs`, `TrainingEndTs`, `EvalStartTs`, `EvalEndTs`, optional `MissingWindowsOverride` (or approved equivalent).
   - Update pipeline-definition parameter objects and downstream argument wiring.
   - Add strict runtime validation for required timestamps/ranges and parameter coherence before heavy compute starts.

4. **Implement full internal step chain inside unified training pipeline**
   - Update `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py` to define a full ordered multi-step pipeline that includes:
     1. **TrainingDataVerifierStep**
        - verifies training/eval feature coverage and emits deterministic machine-readable verification outputs.
     2. **MissingFeatureCreationStep (conditional/remediation path)**
        - when verifier indicates missing windows, execute remediation using existing data creation implementation path(s) (manual/orchestrated-equivalent code), constrained to missing windows only.
     3. **Post-remediation re-verification step**
        - confirms gaps are resolved before training continues.
     4. **IFTrainingStep**
        - trains model artifacts with validated/complete features.
     5. **ModelPublishStep**
        - publishes versioned model artifact(s) and writes deterministic publication metadata.
     6. **ModelAttributesStep**
        - writes/updates model attributes/registry metadata with idempotent keys.
     7. **ModelDeployStep**
        - deploys/promotes model to serving target(s) with explicit rollout status output.
   - Ensure each step emits clear outputs/metrics needed for troubleshooting and auditability.

5. **Bounded remediation and failure semantics (inside pipeline)**
   - Recreate bounded retry/remediation safety previously represented in Step Functions (`max 2` remediation attempts or approved equivalent) within pipeline-native control logic.
   - Enforce deterministic terminal failure categories:
     - verifier failed without remediable gaps,
     - remediation exhausted,
     - training failure,
     - publication/attributes/deploy failure.
   - Ensure failure propagation to Step Functions is unambiguous (single pipeline status surface + structured diagnostics persisted to expected outputs/logs).

6. **DynamoDB JobSpec/runtime contract cleanup and migration**
   - Update `src/ndr/scripts/create_ml_projects_parameters_table.py` and related contract docs/templates:
     - move unified requirements into `pipeline_if_training` spec,
     - deprecate/remove placeholder pipeline specs:
       - `pipeline_training_data_verifier`,
       - `pipeline_missing_feature_creation`,
       - `pipeline_model_publish`,
       - `pipeline_model_attributes`,
       - `pipeline_model_deploy`.
   - Ensure seeded scripts/data-prefix metadata for the unified pipeline contains all new internal steps.
   - Provide migration notes for existing environments that still have old seeded entries.

7. **Deployment/IaC/runtime placeholder alignment**
   - Remove obsolete deployment placeholders and references for retired training sub-pipelines.
   - Retain only the unified training pipeline placeholder for training orchestration.
   - Validate IAM policies still include required actions for the now-expanded unified training pipeline behavior (training + publish + deploy related calls/resources).

8. **Code quality, reliability, and observability hardening (required)**
   - Add/standardize structured logs per internal stage with correlation identifiers (`project_name`, `feature_spec_version`, `run_id`, execution ARN/ID).
   - Add stage-level counters/timers/status artifacts to support production monitoring.
   - Apply idempotency and replay-safe behavior for publish/attributes/deploy writes.
   - Validate safe defaults and explicit input validation for all new runtime parameters.

9. **Documentation synchronization (required in same change set)**
   - Update all relevant documents to reflect post-implementation truth:
     - `docs/pipelines/pipelines_flow_description.md`
     - `docs/architecture/orchestration/step_functions.md`
     - `docs/architecture/orchestration/dynamodb_io_contract.md`
     - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
     - training runbooks/protocol docs (`docs/TRAINING_PROTOCOL_IF.md` and any training orchestration references)
     - diagrams/docs that currently imply separate verifier/remediation/model lifecycle pipeline triggers.
   - Remove stale references to unimplemented placeholder training sub-pipelines once unified path is implemented.

10. **Testing, checks, and verification gates (must pass before completion)**
   - **Step Functions contract tests**
     - update/add tests to assert training orchestrator no longer contains:
       - `StartTrainingDataVerifier` branch,
       - `StartMissingFeatureCreationPipeline` branch,
       - `StartModelPublishPipeline`, `StartModelAttributesPipeline`, `StartModelDeployPipeline` branches,
       - associated describe/wait/increment/choice states.
     - assert single `StartTrainingPipeline` success path and polling behavior remain valid.
   - **Unified training pipeline tests**
     - add/extend unit/integration tests for:
       - verifier detects missing/non-missing windows,
       - remediation called only when needed,
       - remediation bounded retry behavior,
       - training execution after successful verification,
       - publish/attributes/deploy stage sequencing and outputs,
       - idempotent re-run behavior.
   - **Contract/bootstrap tests**
     - update tests to confirm retired pipeline specs/placeholders are removed from seeds/contracts.
     - verify `pipeline_if_training` required runtime parameter list and step metadata are complete and valid.
   - **Documentation/contract consistency checks**
     - add/extend assertions or review checklist ensuring docs and JSON definitions align with implemented architecture.
   - **Pre-prod end-to-end dry run**
     - execute one full training orchestration in non-prod and capture evidence for:
       - unified pipeline launch and completion,
       - verifier + remediation behavior,
       - model publication/attributes/deploy completion,
       - expected terminal orchestration status,
       - observability signals and alerts.

11. **Rollout, migration, rollback strategy**
   - **Rollout order:**
     1) implement unified training pipeline internals and tests,
     2) update Dynamo contracts + docs,
     3) deploy updated training pipeline,
     4) deploy simplified `sfn_ndr_training_orchestrator`,
     5) remove deprecated placeholder wiring from IaC/config.
   - **Rollback plan:**
     - restore prior Step Functions definition and/or prior training pipeline version alias,
     - re-enable legacy placeholder wiring only if operationally required,
     - keep backward-compatible parameter acceptance for rapid rollback.

12. **Execution checklist (authoritative go-live gate)**
   - Run all targeted and relevant full-suite tests for orchestration, pipeline definitions, and contract generation.
   - Validate no stale placeholder references remain in code/docs/tests/deployment templates.
   - Validate pipeline-level observability and failure diagnostics in non-prod.
   - Record explicit evidence of successful non-prod run and rollback readiness.
   - Confirm docs reflect implemented state exactly (no future-tense placeholders for completed behavior).

13. **Completeness matrix (required file-level coverage, including omissions)**
   - **Orchestration definition + contracts**
     - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`: reduce to coarse-grained start/poll/finalize pattern and remove verifier/remediation/model-lifecycle branch states.
     - `tests/test_step_functions_item19_contracts.py`: replace assertions that currently require verifier/remediation states with assertions that require unified training orchestration shape and preserved polling/failure semantics.
   - **Unified training pipeline implementation**
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`: implement full multi-step chain (verifier, conditional remediation, re-verification, training, publish, attributes, deploy) and parameter/step dependency wiring.
     - `src/ndr/scripts/run_if_training.py` and `src/ndr/processing/if_training_job.py`: extend runtime/config handling so pipeline step arguments and internal stage execution are fully implemented and auditable.
     - Any new/updated step-entry scripts required by the unified chain (for verifier/remediation/publish/attributes/deploy) must be added under `src/ndr/scripts/` and covered by tests.
   - **Dynamo seed/runtime contracts**
     - `src/ndr/scripts/create_ml_projects_parameters_table.py`: migrate runtime params and seeded step metadata into `pipeline_if_training`; deprecate/remove separate training sub-pipeline specs/placeholders.
     - `tests/test_create_ml_projects_parameters_table.py`: update expected pipeline catalog and per-pipeline contract assertions for the new unified training contract.
   - **Documentation set that must be synchronized**
     - `docs/pipelines/pipelines_flow_description.md`: remove placeholder-stage narrative and describe single unified training pipeline lifecycle.
     - `docs/architecture/orchestration/step_functions.md`: update responsibilities and required runtime/IAM placeholder list to remove retired training sub-pipeline placeholders.
     - `docs/architecture/orchestration/dynamodb_io_contract.md`: update example pipeline keys/contracts to reflect unified training ownership.
     - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`: remove retired training sub-pipeline entries and document unified runtime-parameter expectations.
     - `docs/TRAINING_PROTOCOL_IF.md`: align operational training protocol with verifier/remediation/publish/deploy now being pipeline-native internal stages.
     - `docs/architecture/diagrams/pipeline_flow.md` and `docs/architecture/diagrams/orchestration_event_lifecycle.md` (+ generated artifacts if maintained): remove obsolete branch topology and reflect coarse-grained training orchestrator flow.
   - **Deployment/runtime wiring**
     - IaC/template/environment wiring that currently references `PipelineNameTrainingDataVerifier`, `PipelineNameMissingFeatureCreation`, `PipelineNameModelPublish`, `PipelineNameModelAttributes`, `PipelineNameModelDeploy`: remove/retire and keep only unified training pipeline placeholder wiring.
   - **Explicit omissions to prevent incomplete implementation**
     - Do not leave stale placeholder names in tests/docs/contracts after code migration.
     - Do not remove Step Functions branches without implementing equivalent bounded remediation/failure semantics inside unified pipeline.
     - Do not promote with docs-only updates; implementation, tests, and docs must ship together for this item to be considered complete.


### Definition of done
- `sfn_ndr_training_orchestrator` is coarse-grained and controls only unified training pipeline start/poll/finalization.
- Unified `pipeline_if_training` fully implements verifier + remediation + training + publish + attributes + deploy.
- No required runtime/deployment contract depends on retired training sub-pipeline placeholders.
- Bounded remediation safety, deterministic failure signaling, and idempotent model lifecycle side effects are implemented and validated.
- All relevant docs/diagrams/contracts are synchronized with post-implementation reality.
- Automated tests and non-prod end-to-end verification demonstrate the redesigned workflow is coordinated, robust, and production-ready.

**Status:** Fully implemented.

## 23) Priority fallback hardening plan (P0 + P1 + selective P2-D metric instrumentation)

### Objective
Eliminate high-risk fallback behavior that can make orchestrations unworkable or silently incorrect, while preserving controlled resilience where explicitly required.

### Scope (authoritative)
Implement all previously recommended changes for:
- **Priority 0:** fail-fast runtime resolution and backfill-date fallback removal.
- **Priority 1:** FG-A strict mini-batch filtering behavior.
- **Priority 2 (item D only):** emit explicit metric/tag when IF HPO fallback is used.

Do **not** implement Priority 2 item E (machine-inventory fallback changes) or item F (config-loader fallback policy changes) as part of this item.

### Implementation plan
1. **Step Functions runtime-parameter fail-fast enforcement (required)**
   - Update the following orchestration definitions to prevent required parameters from silently resolving to empty literals:
     - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
     - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
     - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
     - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json` (for consistency with shared resolution patterns)
   - Add explicit `ValidateResolvedRuntimeParams` Choice/Fail stages (or equivalent validation guard) immediately after runtime-resolution Pass states.
   - Validation must hard-fail when required keys are missing/empty (e.g., `project_name`, `feature_spec_version`, workflow-specific timestamps/identifiers/prefixes).
   - Failure causes must be deterministic and operator-readable (single-line reason per missing/invalid required input).

2. **Remove static backfill default window literals (required)**
   - In `sfn_ndr_backfill_reprocessing.json`, remove `'2025-01-01T00:00:00Z'` / `'2025-01-02T00:00:00Z'` fallback literals.
   - Require `start_ts` and `end_ts` to come from explicit invocation, parsed message, or explicitly defined project defaults.
   - Add validation constraints:
     - both values present,
     - valid timestamp format,
     - `start_ts < end_ts`.
   - Ensure invalid windows fail before launching downstream pipelines.

3. **FG-A strict mini-batch behavior (required)**
   - Update `src/ndr/processing/fg_a_builder_job.py` so that when runtime config includes `mini_batch_id`, FG-A fails fast if the delta dataset lacks the `mini_batch_id` column.
   - Remove permissive behavior that silently processes all rows under prefix when `mini_batch_id` is absent from source schema.
   - Emit structured error logs including `project_name`, `feature_spec_version`, `mini_batch_id`, and `delta_s3_prefix`.
   - Preserve backward compatibility only via explicit opt-in flag in JobSpec/runtime (if introduced), defaulting to strict mode.

4. **IF HPO fallback observability instrumentation (Priority 2 item D only)**
   - Update `src/ndr/processing/if_training_job.py` around Optuna import fallback branch:
     - emit explicit structured log field/tag indicating fallback path activation (`hpo_method=local_bayesian_fallback`),
     - emit metric/counter (or persisted training summary field) for fallback activation.
   - Ensure both success and fallback paths expose comparable telemetry fields so monitoring can reliably trend fallback frequency.
   - Do not alter search policy or fallback eligibility in this item.

5. **Operational contracts and failure signaling**
   - Ensure all new fail-fast guards propagate clear failure reasons into Step Functions terminal status and CloudWatch logs.
   - Standardize error codes/messages for parameter-validation failures to simplify alert routing and runbook triage.

6. **Documentation synchronization (required in same change set)**
   - Update docs to reflect post-change runtime-resolution and validation behavior:
     - `docs/architecture/orchestration/step_functions.md`
     - `docs/architecture/orchestration/dynamodb_io_contract.md`
     - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
     - `docs/pipelines/pipelines_flow_description.md`
   - Explicitly document:
     - precedence order for runtime values,
     - which fields are required vs optional,
     - fail-fast behavior (no silent empty-string/static-date fallbacks for required fields),
     - FG-A strict `mini_batch_id` requirement and optional compatibility mode semantics (if present).

### Testing, checks, and verification gates (must pass)
1. **Step Functions contract tests**
   - Extend/update JSONata contract tests to assert:
     - required-field validation state(s) exist,
     - required fields no longer resolve to silent empty defaults,
     - backfill workflow no longer contains static date literals,
     - failure branch behavior is deterministic for missing/invalid required runtime parameters.

2. **FG-A behavior tests**
   - Add/update tests in `tests/test_fg_a_builder_job.py` to verify:
     - missing `mini_batch_id` column with runtime mini-batch request raises failure,
     - strict-mode error message contains actionable context,
     - existing happy-path behavior remains valid when column exists.

3. **IF training fallback instrumentation tests**
   - Add/update tests to confirm fallback-activation telemetry is emitted when Optuna is unavailable.
   - Verify no regression in trial-summary fields and training report artifacts.

4. **End-to-end orchestration sanity checks (non-prod)**
   - Execute representative runs for:
     - valid inference invocation,
     - invalid inference invocation (missing required key) expecting controlled fail-fast,
     - valid backfill invocation,
     - invalid backfill window expecting pre-launch rejection,
     - IF training run with induced no-Optuna environment to confirm fallback telemetry visibility.

5. **Coordination/orchestration quality gate**
   - Confirm stage ordering remains coherent after new validation states.
   - Confirm no downstream pipeline execution is triggered after validation failure.
   - Confirm logs/metrics provide enough signal for rapid diagnosis in production.

### Completeness matrix (required file-level coverage, including omissions)
- **Step Functions definitions + contract tests**
  - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
  - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
  - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
  - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
  - `tests/test_step_functions_jsonata_contracts.py`
  - `tests/test_step_functions_item18_contracts.py`
  - `tests/test_step_functions_item19_contracts.py`
- **FG-A strict mini-batch enforcement**
  - `src/ndr/processing/fg_a_builder_job.py`
  - `tests/test_fg_a_builder_job.py`
- **IF HPO fallback telemetry (Priority 2-D only)**
  - `src/ndr/processing/if_training_job.py`
  - `tests/test_if_training_job_orchestration.py` (or nearest IF-training test module covering fallback path telemetry)
- **Documentation set that must be synchronized**
  - `docs/architecture/orchestration/step_functions.md`
  - `docs/architecture/orchestration/dynamodb_io_contract.md`
  - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
  - `docs/pipelines/pipelines_flow_description.md`
- **Explicit omissions (must stay out of this item)**
  - Do not implement machine-inventory fallback policy changes (`src/ndr/processing/machine_inventory_unload_job.py`).
  - Do not change config-loader fallback policy (`src/ndr/config/job_spec_loader.py`, `src/ndr/config/project_parameters_loader.py`) beyond documentation/validation clarity.


### Completeness verification and mandatory gap-closure checklist (required before implementation sign-off)
To ensure the plan is correct, complete, and workable end-to-end, implementation must explicitly close all items below and record evidence per item.

#### 1) Code-path coverage (all required modifications)
1. **IF training runtime/spec layer**
   - `src/ndr/processing/if_training_spec.py`: add typed fields for evaluation windows, history planner policy, remediation toggles, and evaluation output sinks.
   - `src/ndr/scripts/run_if_training.py`: add argument plumbing for multi-window evaluation payload and optional toggles.
2. **IF training execution layer**
   - `src/ndr/processing/if_training_job.py`:
     - add history planner computation + persisted artifact,
     - replace placeholder remediation with real orchestrated invocations,
     - add post-training evaluation replay orchestration,
     - add final report expansion for evaluation artifacts and provenance.
3. **Pipeline definition layer**
   - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`:
     - add parameters for evaluation windows/toggles,
     - add step sequencing dependencies reflecting planner→verify→remediate→reverify→train→evaluate→publish/attributes/deploy (or equivalent coherent order).
4. **Orchestration definitions**
   - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`:
     - add parameter resolution/validation for evaluation windows and toggles,
     - ensure remediation/evaluation orchestration branches are represented and bounded.
   - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`:
     - confirm contract compatibility with planner-produced windows payload.
   - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json` (or equivalent FG-B orchestration owner):
     - ensure callable contract supports remediation-triggered FG-B rebuild windows/reference periods.
5. **Evaluation reuse integration (must avoid duplicate logic)**
   - Confirm invocation contracts are compatible with:
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
     - `src/ndr/processing/inference_predictions_job.py`
     - `src/ndr/processing/prediction_feature_join_job.py`
6. **DynamoDB seed/contracts**
   - `src/ndr/scripts/create_ml_projects_parameters_table.py`: seed new defaults/spec fields and runtime requirements.
   - Ensure validators enforce presence/shape and fail-fast behavior.

#### 2) Documentation coverage (all required updates)
The following docs must be updated in the same delivery stream so post-implementation state is accurately documented:
- `docs/TRAINING_PROTOCOL_IF.md`
- `docs/TRAINING_PROTOCOL_IF_VERIFICATION.md`
- `docs/architecture/orchestration/step_functions.md`
- `docs/pipelines/pipelines_flow_description.md`
- `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
- `docs/architecture/orchestration/dynamodb_io_contract.md`
- relevant FG-B/FG-C references where safety-gap/horizon planner behavior is described.

#### 3) Explicit omissions that must be preserved (no unintended scope creep)
- Do **not** duplicate inference scoring/join algorithms; only orchestrate reuse.
- Do **not** remove existing fail-fast validation semantics; extend them for new params.
- Do **not** introduce mandatory rollout/canary requirements; rollout controls remain optional toggles.
- Do **not** silently change FG-A/FG-B/FG-C output schemas without synchronized contract docs/tests.

#### 4) Verification evidence matrix (required artifacts)
Implementation PR(s) must include evidence links/artifacts for:
- formula correctness tests (`W_15m`, `B_start/B_end`, `W_required`) including concrete 44-day derivation under current policy,
- remediation invocation payload correctness (15m backfill + FG-B rebuild),
- evaluation replay output manifests and joined output sink writes,
- SageMaker Experiments lineage screenshots/log excerpts/JSON artifacts,
- orchestration bounded retry/idempotency behavior under replay.

#### 5) Final implementation readiness gate (must pass)
A final coordinated review must confirm:
- all files in sections (1) and (2) were updated or explicitly marked N/A with rationale,
- tests/checks from this item all pass,
- no unresolved TODO/placeholder remains in planner/remediation/evaluation paths,
- operational runbook notes (inputs, toggles, rollback) are documented.

### Definition of done
- Required runtime parameters fail fast instead of silently defaulting to empty/static values.
- Backfill flow rejects invalid/missing windows and has no hardcoded date fallbacks.
- FG-A no longer silently processes full-prefix data when mini-batch partition key is absent.
- IF HPO fallback usage is observable via explicit metric/tag/summary signal.
- Tests and non-prod checks prove orchestration correctness, failure determinism, and operational visibility.
- Relevant documentation reflects the implemented runtime/fallback behavior exactly.

**Status:** Planned.

---

## 24) FG-B/FG-A contract alignment plan (Option A: normalize FG-A wide rows in FG-B)

### Objective
Align FG-B baseline computation with the current FG-A output shape without changing FG-A write contract, while preserving role-specific behavioral semantics (initiator/source vs receiver/target) for baseline modeling.

### Chosen design (Option A)
Implement a normalization layer inside FG-B that converts FG-A wide rows (`outbound_*` + `in_*`) into a role-explicit long representation consumed by existing FG-B baseline logic.

### Implementation plan
1. **Introduce FG-A normalization stage in FG-B (required)**
   - Add a dedicated method in `src/ndr/processing/fg_b_builder_job.py` (e.g., `_normalize_fg_a_for_baselines`) invoked immediately after FG-A read and before anomaly capping.
   - Input: current FG-A row shape.
   - Output: long-form rows with explicit columns required by FG-B grouping logic:
     - `host_ip`, `role`, `window_label`, `window_end_ts`, `segment_id`, `time_band`, baseline metric columns.

2. **Role-split mapping logic (required)**
   - For each FG-A row, emit two logical rows:
     - **outbound role row** using unprefixed feature columns,
     - **inbound role row** using `in_` prefixed feature columns remapped to canonical metric names used by FG-B.
   - Drop role-row outputs that do not have sufficient metric presence (with explicit structured warning counters).
   - Ensure remapping is deterministic and schema-version-aware.

3. **Time-band derivation in FG-B (required)**
   - Derive `time_band` in FG-B from FG-A available time context (`hour_of_day`) or from `window_end_ts` via the project’s canonical time-band policy.
   - Keep derivation logic centralized and reusable to prevent divergence across FG-B/FG-C expectations.
   - Validate that produced `time_band` values match configured/expected enumerations.

4. **Preserve existing baseline/anomaly semantics (required)**
   - Keep existing FG-B anomaly capping and baseline aggregation logic unchanged after normalization stage.
   - Ensure grouping keys continue to separate behavior by `role`, `segment_id`, `time_band`, and `window_label`.
   - Ensure publication outputs remain path/schema compatible for downstream FG-C consumption.

5. **Compatibility controls and rollout safety**
   - Add explicit layout-mode handling in FG-B JobSpec/runtime (`fg_a_layout`: `wide`, `long`, `auto`) with default `auto`.
   - `auto` behavior:
     - if `role`/`time_band` already present, consume as long input,
     - otherwise apply wide-to-long normalization path.
   - Emit clear run metadata indicating which ingestion path was used.

6. **Schema and contract validation hardening**
   - Add upfront validation that required FG-A metric families exist for at least one role path; fail fast on irrecoverable schema mismatch.
   - Add explicit diagnostics listing missing columns and expected mappings.
   - Ensure normalized schema feeds `_apply_segment_anomaly_capping` and baseline builders without implicit column assumptions.

7. **Documentation synchronization (required in same change set)**
   - Update docs to reflect the finalized FG-A→FG-B contract:
     - `docs/README_FG_A.md`
     - `docs/feature_builders/current_state.md`
     - `docs/pipelines/pipelines_flow_description.md`
     - `docs/README_FG_C.md` (if any FG-B output expectations are affected)
     - relevant feature-catalog pages for FG-B/FG-C consumption contracts.
   - Document:
     - role semantics preservation through normalization,
     - long-vs-wide acceptance behavior,
     - operational rollout flags (`fg_a_layout`) and defaults.

### Testing, checks, and verification gates (must pass)
1. **FG-B unit tests (required)**
   - Add/update `tests/test_fg_b_builder_job.py` to cover:
     - wide FG-A input normalization into outbound/inbound long rows,
     - inbound `in_` remapping correctness,
     - time-band derivation correctness,
     - `fg_a_layout=auto` path selection logic,
     - failure behavior for irrecoverable missing-column cases.

2. **Cross-builder contract tests (required)**
   - Add/extend tests ensuring FG-A-produced schema (current wide write shape) is acceptable to FG-B with Option A normalization enabled.
   - Assert resulting FG-B outputs include expected role-separated baseline records and remain consumable by FG-C join/validation logic.

3. **Regression tests for existing FG-B behavior**
   - Verify that when long-form FG-A-compatible input is provided, FG-B path remains functional and unchanged.
   - Verify anomaly-capping and support/cold-start flags remain stable for equivalent datasets pre/post normalization stage.

4. **End-to-end non-prod verification**
   - Run one full inference cadence flow (delta → FG-A → FG-B → FG-C) and capture evidence that:
     - FG-B receives current FG-A output without contract failure,
     - role-specific baselines are produced for both outbound and inbound behavior,
     - FG-C downstream joins and derived features complete successfully.

5. **Coordination/orchestration quality gate**
   - Verify updated FG-B stage order is coherent and observable.
   - Confirm no duplicated or dropped hosts due to normalization logic.
   - Confirm logs/metrics expose role-row counts, dropped-row reasons, and selected layout mode.

### Completeness matrix (required file-level coverage, including omissions)
- **FG-B implementation + contracts**
  - `src/ndr/processing/fg_b_builder_job.py` (normalization stage, layout-mode routing, schema/diagnostic guards)
  - `src/ndr/scripts/run_fg_b_builder.py` (runtime wiring for `fg_a_layout`, if introduced via args)
  - `src/ndr/config/job_spec_loader.py` and related JobSpec models if typed fields are added for `fg_a_layout`
- **FG-A compatibility surface (no write-shape change in Option A)**
  - `src/ndr/processing/fg_a_builder_job.py` (no contract-breaking shape changes; only compatibility-aware references if needed)
  - `src/ndr/catalog/schema_manifest.py` (remain aligned with current FG-A output contract)
- **Tests that must be updated/added**
  - `tests/test_fg_b_builder_job.py`
  - `tests/test_fg_c_builder_job.py` (downstream compatibility checks for FG-B outputs)
  - `tests/test_fg_a_builder_job.py` only if needed to codify unchanged FG-A output assumptions consumed by FG-B Option A
- **Documentation set that must be synchronized**
  - `docs/README_FG_A.md`
  - `docs/feature_builders/current_state.md`
  - `docs/pipelines/pipelines_flow_description.md`
  - `docs/README_FG_C.md`
  - `docs/feature_catalog/01_model_fg_a.md`
  - `docs/feature_catalog/04_fg_b_baseline_features.md`
  - `docs/feature_catalog/02_model_fg_c.md`
- **Explicit omissions (must remain true for Option A)**
  - Do not change FG-A published dataset shape to long form as part of this item.
  - Do not introduce parallel FG-A writer contracts unless separately planned and documented as migration work.


### Completeness verification and mandatory gap-closure checklist (required before implementation sign-off)
To ensure the plan is correct, complete, and workable end-to-end, implementation must explicitly close all items below and record evidence per item.

#### 1) Code-path coverage (all required modifications)
1. **IF training runtime/spec layer**
   - `src/ndr/processing/if_training_spec.py`: add typed fields for evaluation windows, history planner policy, remediation toggles, and evaluation output sinks.
   - `src/ndr/scripts/run_if_training.py`: add argument plumbing for multi-window evaluation payload and optional toggles.
2. **IF training execution layer**
   - `src/ndr/processing/if_training_job.py`:
     - add history planner computation + persisted artifact,
     - replace placeholder remediation with real orchestrated invocations,
     - add post-training evaluation replay orchestration,
     - add final report expansion for evaluation artifacts and provenance.
3. **Pipeline definition layer**
   - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`:
     - add parameters for evaluation windows/toggles,
     - add step sequencing dependencies reflecting planner→verify→remediate→reverify→train→evaluate→publish/attributes/deploy (or equivalent coherent order).
4. **Orchestration definitions**
   - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`:
     - add parameter resolution/validation for evaluation windows and toggles,
     - ensure remediation/evaluation orchestration branches are represented and bounded.
   - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`:
     - confirm contract compatibility with planner-produced windows payload.
   - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json` (or equivalent FG-B orchestration owner):
     - ensure callable contract supports remediation-triggered FG-B rebuild windows/reference periods.
5. **Evaluation reuse integration (must avoid duplicate logic)**
   - Confirm invocation contracts are compatible with:
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
     - `src/ndr/processing/inference_predictions_job.py`
     - `src/ndr/processing/prediction_feature_join_job.py`
6. **DynamoDB seed/contracts**
   - `src/ndr/scripts/create_ml_projects_parameters_table.py`: seed new defaults/spec fields and runtime requirements.
   - Ensure validators enforce presence/shape and fail-fast behavior.

#### 2) Documentation coverage (all required updates)
The following docs must be updated in the same delivery stream so post-implementation state is accurately documented:
- `docs/TRAINING_PROTOCOL_IF.md`
- `docs/TRAINING_PROTOCOL_IF_VERIFICATION.md`
- `docs/architecture/orchestration/step_functions.md`
- `docs/pipelines/pipelines_flow_description.md`
- `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
- `docs/architecture/orchestration/dynamodb_io_contract.md`
- relevant FG-B/FG-C references where safety-gap/horizon planner behavior is described.

#### 3) Explicit omissions that must be preserved (no unintended scope creep)
- Do **not** duplicate inference scoring/join algorithms; only orchestrate reuse.
- Do **not** remove existing fail-fast validation semantics; extend them for new params.
- Do **not** introduce mandatory rollout/canary requirements; rollout controls remain optional toggles.
- Do **not** silently change FG-A/FG-B/FG-C output schemas without synchronized contract docs/tests.

#### 4) Verification evidence matrix (required artifacts)
Implementation PR(s) must include evidence links/artifacts for:
- formula correctness tests (`W_15m`, `B_start/B_end`, `W_required`) including concrete 44-day derivation under current policy,
- remediation invocation payload correctness (15m backfill + FG-B rebuild),
- evaluation replay output manifests and joined output sink writes,
- SageMaker Experiments lineage screenshots/log excerpts/JSON artifacts,
- orchestration bounded retry/idempotency behavior under replay.

#### 5) Final implementation readiness gate (must pass)
A final coordinated review must confirm:
- all files in sections (1) and (2) were updated or explicitly marked N/A with rationale,
- tests/checks from this item all pass,
- no unresolved TODO/placeholder remains in planner/remediation/evaluation paths,
- operational runbook notes (inputs, toggles, rollback) are documented.

### Definition of done
- FG-B reliably consumes current FG-A wide output by normalizing to role-explicit long rows.
- Role semantics (source vs target behavior baselines) are explicitly preserved and validated.
- Existing FG-B anomaly/baseline logic operates unchanged on normalized input.
- FG-C downstream consumption remains compatible and verified.
- Documentation, tests, and non-prod verification collectively prove a coordinated, production-ready alignment.

**Status:** Planned.


---

## 25) Unified automated IF training + evaluation with history planner, remediation orchestration, and DDB-driven runtime windows

### Objective
Implement a fully automated, production-ready IF training lifecycle that:
1. derives training/evaluation windows and required history from DynamoDB,
2. computes exact historical backfill envelopes using FG-A lookbacks + FG-B horizon safety gaps,
3. performs real remediation by orchestrating existing backfill and feature/statistics creation flows,
4. trains IF, runs post-training evaluation windows, writes outputs, joins evaluation predictions with features, and optionally publishes to Redshift,
5. logs complete lineage/metrics to SageMaker Experiments,
6. keeps rollout controls optional and table-driven.

### Why this is required (current gap summary)
- Current IF remediation stage only writes status artifacts and does not create missing data.
- Training orchestrator currently launches one unified training pipeline but does not orchestrate external remediation.
- Eval timestamps are runtime-required yet not used to execute real post-training evaluation windows.
- Backfill 15m flow regenerates Delta/FG-A/PairCounts/FG-C but does not rebuild FG-B monthly/statistical baselines.

### Known current parameters that MUST be pre-integrated (not only symbolic formulas)
The first implementation must wire these as explicit defaults/known constants (while still supporting future config overrides):
- **FG-A max lookback:** `L_A = 24h` from current FG-A windows (`w_15m`, `w_30m`, `w_1h`, `w_8h`, `w_24h`).
- **FG-B baseline horizons:** `H = {7d, 30d}`.
- **FG-B safety gaps (current known policy):**
  - for `7d`: `tail_days=2`, `head_days=2`,
  - for `30d`: `tail_days=7`, `head_days=7`.
- **FG-C back-dating requirements already known in feature set:** novelty/rarity families use `lookback30d` (for example `new_peer_cnt_lookback30d`, `rare_peer_cnt_lookback30d`, `new_dst_port_cnt_lookback30d`, `new_pair_cnt_lookback30d`, `new_src_dst_port_cnt_lookback30d`).

For initial implementation, planner output must expose both:
1. **computed values from formulas**, and
2. **resolved concrete values** above, with per-run provenance (`from_job_spec` vs `from_default_constants`).

### Authoritative required-history formulas (must be implemented exactly)
Let:
- training interval be `[T_s, T_e)`,
- evaluation intervals be `{[E_{s,i}, E_{e,i})}_{i=1..N}`,
- scoring union be `U = [T_s, T_e) ∪ ⋃[E_{s,i}, E_{e,i})`,
- `U_start = min(T_s, E_{s,1..N})`, `U_end = max(T_e, E_{e,1..N})`.

#### A) 15m-chain envelope (raw/delta/FG-A/PairCounts/FG-C-current rows)
- FG-A max lookback window minutes:
  `L_A = max(FG_A_WINDOWS_MINUTES)` (currently 24h).
- Required 15m envelope:
  `W_15m = [U_start - L_A, U_end)`.

#### B) FG-B baseline envelope needed by FG-C
For each configured baseline horizon `h`:
- `d(h)` = effective horizon days (e.g., 7 or 30),
- `g_t(h)` = tail safety gap days,
- `g_h(h)` = head safety gap days.

FG-B bound per reference time `ref`:
- `baseline_end(h, ref)   = ref - g_h(h)`
- `baseline_start(h, ref) = baseline_end(h, ref) - (d(h) + g_t(h))`

Across the full union interval:
- earliest baseline input requirement:
  `B_start = U_start - max_h(d(h) + g_t(h) + g_h(h))`
- latest baseline input requirement:
  `B_end   = U_end   - min_h(g_h(h))`

#### C) Global historical creation envelope
- `W_required = [min(W_15m.start, B_start), U_end)`

#### D) Concrete envelope under current known horizons/safety gaps (must be emitted by planner)
Given current known FG-B policy:
- `d(7d)=7`, `g_t(7d)=2`, `g_h(7d)=2`  => `d+g_t+g_h = 11`
- `d(30d)=30`, `g_t(30d)=7`, `g_h(30d)=7` => `d+g_t+g_h = 44`

Therefore initial planner must compute:
- `B_start = U_start - max(11, 44) days = U_start - 44 days`
- `B_end   = U_end   - min(2, 7) days = U_end - 2 days`
- `W_15m   = [U_start - 24h, U_end)`
- `W_required = [min(U_start - 24h, U_start - 44d), U_end) = [U_start - 44d, U_end)`

Operationally, this means the first automated implementation must plan/remediate at least **44 days of backward data** before the earliest point in `(training ∪ evaluation)` under current known settings.

This formula set must drive both verification expectations and remediation task planning.

### Scope and implementation plan

#### 1) Introduce history-planner stage (new, required)
Add a deterministic planning stage before verify/remediation that:
- loads training/eval windows from DynamoDB defaults/spec,
- loads FG-B horizon + safety-gap policy from FG-B spec,
- computes `W_15m`, `B_start/B_end`, `W_required`,
- materializes required partition/window manifests:
  - missing 15m windows,
  - missing FG-B baseline windows by horizon/reference period,
- writes planning artifact under training report prefix (`run_id=.../history_planner.json`).

#### 2) Refactor runtime/contracts to be fully DDB-derived (required)
All required runtime values must be resolvable from DDB (payload overrides allowed but optional).

##### 2.1 `project_parameters#<feature_spec_version>.spec.defaults` additions
Add defaults for:
- `TrainingStartTs`
- `TrainingEndTs`
- `EvaluationWindowsJson` (JSON array of windows; preferred)
- optional fallback compatibility fields:
  - `EvalStartTs`, `EvalEndTs` (single window)
- optional automation toggles:
  - `EnableHistoryPlanner`
  - `EnableAutoRemediate15m`
  - `EnableAutoRemediateFGB`
  - `EnablePostTrainingEvaluation`
  - `EnableEvalJoinPublication`
  - `EnableEvalExperimentsLogging`

##### 2.2 `if_training#<feature_spec_version>.spec` additions
Add typed/table-driven fields:
- `evaluation.windows` (structured list)
- `history_planner` policy block:
  - `derive_from_training_and_evaluation: true`
  - `fg_a_max_lookback_minutes` (optional override; default from FG-A schema)
- `remediation` policy block:
  - `enable_backfill_15m`
  - `enable_fgb_rebuild`
  - `max_retries`
- `evaluation_output` block:
  - S3 output prefixes for predictions/joined outputs
  - optional Redshift sink settings (reuse prediction join sink model)

##### 2.3 `pipeline_if_training#<feature_spec_version>.spec`
Update `required_runtime_params` and step metadata to include:
- multi-window evaluation payload parameter (e.g., `EvaluationWindowsJson`)
- optional toggles if passed runtime-side.

##### 2.4 Validation contracts
Extend `project_parameters.spec.validation` for timestamp and evaluation-windows JSON shape.
Fail fast on invalid/missing required derived params.

#### 3) Make remediation actually generate missing data (required)

##### 3.1 Remediation orchestration wiring
Replace placeholder remediation behavior with orchestrated execution of existing flows:
1. for missing 15m windows: invoke existing backfill orchestration (`sfn_ndr_backfill_reprocessing`) with computed range/windows.
2. for missing/stale FG-B baseline coverage: invoke existing FG-B baseline pipeline/orchestrator for required reference period(s) + horizons.
3. rerun verify (`reverify`) after remediation.
4. enforce bounded retries and deterministic failure artifact on exhaustion.

##### 3.2 Sufficiency rule
Backfill15m is necessary but not sufficient; FG-B rebuild must run when baseline coverage is incomplete per planner output.

#### 4) Implement true automated post-training evaluation (required)

##### 4.1 Evaluation execution model
After training completion and artifact persistence:
- for each configured evaluation window:
  1. ensure required history/features are present (using planner/remediation outputs),
  2. run inference replay using existing inference predictions logic/pipeline,
  3. write predictions to evaluation S3 prefixes,
  4. run prediction-feature join using existing join flow,
  5. write joined outputs to S3 and optionally Redshift (existing destination model).

##### 4.2 Reuse-first rule
Do not duplicate scoring/join logic; reuse:
- `inference_predictions` job/pipeline for model scoring,
- `prediction_feature_join` job/pipeline for joined outputs and optional Redshift copy.

##### 4.3 Evaluation artifacts
Persist per-window artifacts:
- `evaluation/<window_id>/predictions_manifest.json`
- `evaluation/<window_id>/join_manifest.json`
- `evaluation/<window_id>/metrics.json`
Include these in final training report.

#### 5) SageMaker Experiments lineage expansion (required)
Extend experiments logging to register:
- one trial lineage per training run,
- one trial component per evaluation window,
- metrics/artifact URIs for planner, remediation actions, training outputs, and evaluation outputs.

#### 6) Optional rollout controls only (required)
Rollout strategy is optional, controlled by toggles. No mandatory canary sequence in DoD.
- all automation blocks can be selectively enabled/disabled via DDB config.

### Orchestration and architecture coherence requirements
- Keep Step Functions fail-fast validation for resolved params.
- Ensure orchestration remains callback-free and poll-based where already standardized.
- Ensure idempotency keys are deterministic for remediation and evaluation replay runs.
- Ensure bounded retries and explicit terminal diagnostics.

### Required documentation updates (must be in same implementation stream)
Update docs to reflect post-implementation authoritative behavior:
1. `docs/TRAINING_PROTOCOL_IF.md`
   - include history-planner formulas and remediation orchestration semantics.
2. `docs/TRAINING_PROTOCOL_IF_VERIFICATION.md`
   - update checklist and caveats for true remediation + evaluation flow.
3. `docs/architecture/orchestration/step_functions.md`
   - reflect training orchestrator decision branches and remediation/evaluation orchestration.
4. `docs/pipelines/pipelines_flow_description.md`
   - include history-planner + evaluation replay/join steps in training flow.
5. `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
   - add new defaults/spec keys and examples for training/evaluation windows + toggles.
6. `docs/architecture/orchestration/dynamodb_io_contract.md`
   - include new runtime params and validation rules.
7. Any relevant FG-B/FG-C docs impacted by horizon/safety-gap planner behavior.

### Testing, checks, and verification gates (must pass)

#### A) Unit tests (required)
1. History planner computation tests
   - verify exact formulas for `W_15m`, `B_start/B_end`, `W_required` under multiple horizon/gap configs.
   - verify union behavior across multiple evaluation windows.
2. Runtime parsing/spec tests
   - verify DDB-derived training/eval window loading and compatibility fallback (`EvalStartTs/EvalEndTs`).
3. Remediation behavior tests
   - verify missing 15m windows trigger backfill orchestrator invocation payloads.
   - verify missing FG-B coverage triggers FG-B rebuild invocations.
   - verify bounded retries and terminal failure context.
4. Evaluation automation tests
   - verify per-window replay invocation, S3 outputs, join invocation, and manifest generation.
5. Experiments logging tests
   - verify trial components for planner/remediation/evaluation with proper lineage.

#### B) Integration/contract tests (required)
1. Step Functions JSONata contract checks
   - runtime param resolution from DDB defaults,
   - validation failures for malformed timestamps/eval JSON,
   - correct parameter pass-through to pipelines.
2. DDB schema seed/validation tests
   - ensure seeded table definitions include new required params and pass contract validators.
3. Pipeline composition tests
   - ensure training flow stage order and dependencies are coherent with remediation and evaluation branches.

#### C) End-to-end non-prod verification (required)
Execute at least one non-prod scenario each:
1. clean-data scenario (no remediation needed)
2. missing 15m data scenario (backfill remediation path)
3. missing FG-B baseline scenario (FG-B remediation path)
4. mixed-missing scenario (both remediation paths)
5. multi-window evaluation scenario (≥2 historical eval windows)

For each scenario verify:
- successful completion or deterministic expected failure,
- produced artifacts/manifests/reports,
- joined evaluation outputs written to sink,
- experiments lineage completeness,
- no orchestration dead-ends/poll leaks.

#### D) Performance/reliability checks (required)
- verify retries/backoff bounded behavior,
- verify idempotent rerun safety for same `run_id` + window IDs,
- verify no duplicate publication/join writes under replay,
- verify observability: stage timers/counters/log correlation ids.

### Completeness matrix (files expected to change)
- **Core training flow**
  - `src/ndr/processing/if_training_job.py`
  - `src/ndr/processing/if_training_spec.py`
  - `src/ndr/scripts/run_if_training.py`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`
- **Orchestration definitions**
  - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
  - optionally related orchestration docs if additional branches/states are introduced.
- **DDB schema/seed contracts**
  - `src/ndr/scripts/create_ml_projects_parameters_table.py`
  - `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
  - `docs/architecture/orchestration/dynamodb_io_contract.md`
- **Evaluation reuse integration**
  - interfaces touching `inference_predictions` / `prediction_feature_join` invocation wiring.
- **Tests**
  - existing IF orchestration/spec tests + new planner/remediation/evaluation coverage.
- **Documentation**
  - training protocol, architecture/orchestration, pipeline flow, and any FG-B/FG-C contract notes impacted.


### Completeness verification and mandatory gap-closure checklist (required before implementation sign-off)
To ensure the plan is correct, complete, and workable end-to-end, implementation must explicitly close all items below and record evidence per item.

#### 1) Code-path coverage (all required modifications)
1. **IF training runtime/spec layer**
   - `src/ndr/processing/if_training_spec.py`: add typed fields for evaluation windows, history planner policy, remediation toggles, and evaluation output sinks.
   - `src/ndr/scripts/run_if_training.py`: add argument plumbing for multi-window evaluation payload and optional toggles.
2. **IF training execution layer**
   - `src/ndr/processing/if_training_job.py`:
     - add history planner computation + persisted artifact,
     - replace placeholder remediation with real orchestrated invocations,
     - add post-training evaluation replay orchestration,
     - add final report expansion for evaluation artifacts and provenance.
3. **Pipeline definition layer**
   - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`:
     - add parameters for evaluation windows/toggles,
     - add step sequencing dependencies reflecting planner→verify→remediate→reverify→train→evaluate→publish/attributes/deploy (or equivalent coherent order).
4. **Orchestration definitions**
   - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`:
     - add parameter resolution/validation for evaluation windows and toggles,
     - ensure remediation/evaluation orchestration branches are represented and bounded.
   - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`:
     - confirm contract compatibility with planner-produced windows payload.
   - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json` (or equivalent FG-B orchestration owner):
     - ensure callable contract supports remediation-triggered FG-B rebuild windows/reference periods.
5. **Evaluation reuse integration (must avoid duplicate logic)**
   - Confirm invocation contracts are compatible with:
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
     - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
     - `src/ndr/processing/inference_predictions_job.py`
     - `src/ndr/processing/prediction_feature_join_job.py`
6. **DynamoDB seed/contracts**
   - `src/ndr/scripts/create_ml_projects_parameters_table.py`: seed new defaults/spec fields and runtime requirements.
   - Ensure validators enforce presence/shape and fail-fast behavior.

#### 2) Documentation coverage (all required updates)
The following docs must be updated in the same delivery stream so post-implementation state is accurately documented:
- `docs/TRAINING_PROTOCOL_IF.md`
- `docs/TRAINING_PROTOCOL_IF_VERIFICATION.md`
- `docs/architecture/orchestration/step_functions.md`
- `docs/pipelines/pipelines_flow_description.md`
- `docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md`
- `docs/architecture/orchestration/dynamodb_io_contract.md`
- relevant FG-B/FG-C references where safety-gap/horizon planner behavior is described.

#### 3) Explicit omissions that must be preserved (no unintended scope creep)
- Do **not** duplicate inference scoring/join algorithms; only orchestrate reuse.
- Do **not** remove existing fail-fast validation semantics; extend them for new params.
- Do **not** introduce mandatory rollout/canary requirements; rollout controls remain optional toggles.
- Do **not** silently change FG-A/FG-B/FG-C output schemas without synchronized contract docs/tests.

#### 4) Verification evidence matrix (required artifacts)
Implementation PR(s) must include evidence links/artifacts for:
- formula correctness tests (`W_15m`, `B_start/B_end`, `W_required`) including concrete 44-day derivation under current policy,
- remediation invocation payload correctness (15m backfill + FG-B rebuild),
- evaluation replay output manifests and joined output sink writes,
- SageMaker Experiments lineage screenshots/log excerpts/JSON artifacts,
- orchestration bounded retry/idempotency behavior under replay.

#### 5) Final implementation readiness gate (must pass)
A final coordinated review must confirm:
- all files in sections (1) and (2) were updated or explicitly marked N/A with rationale,
- tests/checks from this item all pass,
- no unresolved TODO/placeholder remains in planner/remediation/evaluation paths,
- operational runbook notes (inputs, toggles, rollback) are documented.


### Definition of done
- Training/eval windows are derivable from DynamoDB (payload override optional).
- History planner computes and persists exact required envelopes using FG-A lookbacks + FG-B horizon safety gaps.
- Missing data remediation actually creates required data by invoking existing flows (15m backfill and FG-B rebuild as needed).
- Training runs end-to-end; configured evaluation windows execute automatically post-training.
- Evaluation predictions + joined outputs are written to configured sinks (S3, optional Redshift).
- SageMaker Experiments includes planner/remediation/training/evaluation lineage artifacts and metrics.
- Documentation and tests are updated and demonstrably consistent with implementation.

**Status:** Planned.
