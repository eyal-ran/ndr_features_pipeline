
# FG-B Baseline Builder

This folder contains the implementation of the FG-B builder job for the NDR pipeline.

FG-B writes baseline datasets to S3 only; ingestion into Feature Store is handled
outside the builder job.

## New files

- `src/ndr/processing/fg_b_builder_job.py`
  - Implements `FGBaselineBuilderJob` which:
    - Loads JobSpec from DynamoDB via `load_job_spec`.
    - Creates a SparkSession.
    - Reads FG-A features from S3 for 7d and 30d horizons (with safety gaps).
    - Derives `segment_id` from `host_ip` using IPv4 prefix mapping.
    - Applies segment-level anomaly capping using robust z-scores on key metrics.
    - Computes host-level and segment-level baselines (median, quantiles, MAD, IQR, support/counts, cold_start flags).
    - Optionally computes pair-level rarity baselines from pair-count datasets.
    - Writes FG-B baselines to S3 as Parquet (host, segment, pair) under a batch-scoped
      prefix derived from the reference time (e.g., `.../fg_b/ts=YYYY/MM/DD/HH/MM-batch_id=baseline/`).

- `src/ndr/scripts/run_fg_b_builder.py`
  - CLI / runtime entrypoint for the SageMaker Processing container.
  - Parses:
    - `--project-name`
    - `--feature-spec-version`
    - `--reference-time-iso`
    - `--mode` (REGULAR/BACKFILL)
  - Instantiates a `FGBaselineJobRuntimeConfig` and runs `FGBaselineBuilderJob`.

- `tests/test_fg_b_builder_job.py`
  - Unit tests for:
    - RuntimeConfig construction.
    - Horizon bounds computation with mocked JobSpec.
    - End-to-end initialization path with Spark/JobSpec/S3 IO mocked out.

## Integration into existing project structure

Place the files under the existing `ndr_pipeline` project root as follows:

```
ndr_pipeline/
  src/
    ndr/
      processing/
        fg_b_builder_job.py      # REPLACE existing stub with this implementation
      scripts/
        run_fg_b_builder.py      # NEW entrypoint
  tests/
    test_fg_b_builder_job.py     # NEW tests
```

The rest of the package (job_spec_loader, s3_reader, s3_writer, logger, etc.)
is reused as-is from the previously implemented Delta and FG-A builders.
