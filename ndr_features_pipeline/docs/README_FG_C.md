# FG-C Correlation Builder – Implementation Notes

This document summarizes the implementation of the FG‑C correlation feature builder
and its integration into the NDR streaming pipeline.

## Purpose

FG‑C produces **correlation features** that relate current host behaviour (FG‑A)
to longer‑term baselines (FG‑B), primarily to support isolation‑forest and other
unsupervised models. It is a purely *derived* layer:

- Input: FG‑A (current windows), FG‑B (7d/30d baselines, including MAD/IQR),
  and configuration.
- Output: z‑scores, ratios, magnifiers, and drift metrics per
  `(host_ip, window_label, window_end_ts, baseline_horizon)`.

The output is written as Parquet to S3 and is suitable for registration as an
offline feature group in SageMaker Feature Store. Output prefixes include the
batch start timestamp and mini_batch_id (e.g., `.../fg_c/ts=YYYY/MM/DD/HH/MM-batch_id=<mini_batch_id>/`).

FG-C does not ingest into Feature Store directly; ingestion is orchestrated
outside the builder job.

## Code Locations

- Core job implementation:
  - `src/ndr/processing/fg_c_builder_job.py`
- CLI entrypoint:
  - `src/ndr/scripts/run_fg_c_builder.py`
- Pipelines:
  - `src/ndr/pipeline/sagemaker_pipeline_definitions.py`
    - `build_15m_streaming_pipeline` now includes `FGCCorrBuilderStep` after `PairCountsBuilderStep`.

## Runtime Contract

`run_fg_c_builder.py` is invoked by a SageMaker ProcessingStep with:

- `--project-name`
- `--feature-spec-version`
- `--mini-batch-id`
- `--batch-start-ts-iso`
- `--batch-end-ts-iso`

All structural configuration (S3 prefixes, metric lists, horizons, thresholds)
is resolved in the job via `load_job_spec(project_name, "fg_c_builder", feature_spec_version)`.

## Main Flow in `FGCorrBuilderJob.run()`

1. Load JobSpec (from DynamoDB) for `job_name="fg_c_builder"`.
2. Build a SparkSession with UTC timezone.
3. For each configured horizon (e.g. `"7d"`, `"30d"`):
   - Read FG‑A from `fg_a_input.s3_prefix`, filter by `feature_spec_version` and
     `[batch_start_ts_iso, batch_end_ts_iso)` over `window_end_ts`.
   - Read FG‑B from `fg_b_input.s3_prefix`, filter by the same `feature_spec_version`
     and `baseline_horizon`.
   - Join FG‑A and FG‑B on `join_keys` (default `["host_ip", "window_label"]`).
   - Derive FG‑C features for each metric.
   - Add metadata (baseline_horizon, baseline_start_ts, baseline_end_ts,
     record_id, mini_batch_id, feature_spec_version).
   - Write Parquet to `fg_c_output.s3_prefix`, partitioned by
     `feature_spec_version`, `baseline_horizon`, and `dt = date(window_end_ts)`.
4. Stop Spark.

## Correlation Feature Logic

For each metric `m` in the configured metric list, FG‑C expects FG‑B to provide:

- `m_median`
- `m_mad`
- `m_iqr`

and computes:

- `diff_m = m - m_median`
- `ratio_m = m / (m_median + eps)`
- `z_mad_m = diff_m / (m_mad + eps)` with fallback to IQR if MAD is null/zero
- `abs_dev_over_mad_m = |diff_m| / (m_mad + eps)`
- `z_mad_clipped_m = clip(z_mad_m, -z_max, +z_max)`
- `z_mad_signed_pow3_m = sign(z_mad_clipped_m) * |z_mad_clipped_m|^3`
- `log_ratio_m = log(ratio_m + eps)`

`eps` and `z_max` are configurable via JobSpec (`eps` defaults to `1e-6`,
`z_max` to `6.0`).

These patterns align with the FG‑C specification for MAD‑based z‑scores and
bounded magnifiers.

## Output Schema / Idempotency

FG‑C appends:

- `baseline_horizon` (e.g. `"7d"` or `"30d"`)
- `baseline_start_ts` / `baseline_end_ts` (if present on FG‑B)
- `record_id = host_ip | window_label | window_end_ts | baseline_horizon`
- `mini_batch_id`
- `feature_spec_version`
- `dt = date(window_end_ts)`

Partitions:

- `feature_spec_version`
- `baseline_horizon`
- `dt`

The ProcessingStep can safely overwrite the partitions for the mini‑batch it is
responsible for, using the standard S3 overwrite semantics.

## Testing

A minimal unit test exists in:

- `tests/test_fg_c_builder_job.py`

It uses a local SparkSession and a synthetic single‑row DataFrame to validate
that `_compute_correlation_features` produces the expected diff, ratio, z‑score,
and magnifier values for a simple case.

Additional tests can be added to validate:

- More metrics
- Edge cases when MAD/IQR are zero or null
- Integration with real FG‑A / FG‑B sample data.
