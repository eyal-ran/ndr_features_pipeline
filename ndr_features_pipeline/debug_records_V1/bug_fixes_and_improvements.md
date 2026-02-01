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
