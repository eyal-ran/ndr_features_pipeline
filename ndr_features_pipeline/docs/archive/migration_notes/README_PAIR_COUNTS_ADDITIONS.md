
# NDR Pair-Counts Builder Additions

This directory contains the additional code needed to support pair-level counts
as part of the 15m streaming pipeline.

Pair-counts are written to S3 only; ingestion or downstream registration is
handled outside the builder job.

## New modules

- `src/ndr/processing/pair_counts_builder_job.py`
  - Spark-based Processing job that:
    - Reads parsed Palo Alto integration logs for a single 15m mini-batch
      based on `mini_batch_id`.
    - Applies basic data-quality filters (non-null IPs, valid destination_port,
      and optional `event_name == "TRAFFIC"` restriction).
    - Aggregates to pair-counts:
        (src_ip, dst_ip, dst_port, event_ts, sessions_cnt)
    - Writes Parquet partitioned by `dt`, `hh`, `mm`, `feature_spec_version`
      into a batch-scoped prefix under `pair_counts_output.s3_prefix`
      (e.g., `.../pair_counts/ts=YYYY/MM/DD/HH/MM-batch_id=<mini_batch_id>/`).

- `src/ndr/scripts/run_pair_counts_builder.py`
  - CLI / SageMaker ProcessingStep entrypoint. Accepts:
    - `--project-name`
    - `--feature-spec-version`
    - `--mini-batch-id`
    - `--batch-start-ts-iso`
    - `--batch-end-ts-iso`
  - Constructs a `PairCountsJobRuntimeConfig` and runs `PairCountsBuilderJob`.

- `src/ndr/pipeline/sagemaker_pipeline_definitions_pair_example.py`
  - Example SageMaker pipeline definition that shows how to add a
    `PairCountsBuilderStep` to the existing 15m NDR pipeline after FG-A.

- `tests/test_pair_counts_builder_job.py`
  - Basic unit tests for the PairCounts builder job wiring.

## Integration notes

1. **JobSpec (DynamoDB)**

   Add an entry for `job_name = "pair_counts_builder"` with fields:

   ```json
   {
     "traffic_input": {
       "s3_prefix": "s3://<integration-bucket>/<parsed-prefix>/",
       "layout": "batch_folder"
     },
     "pair_counts_output": {
       "s3_prefix": "s3://<ndr-features-bucket>/pair_counts/"
     },
     "filters": {
       "require_nonnull_ips": true,
       "require_destination_port": true
     }
   }
   ```

2. **Pipeline wiring**

   - Ensure your 15m pipeline includes:
     - Delta builder step
     - FG-A builder step
     - Pair-Counts builder step (this new step)
   - Configure the new ProcessingStep to invoke:
     `src/ndr/scripts/run_pair_counts_builder.py` with the same
     `project_name`, `feature_spec_version`, `mini_batch_id`,
     `batch_start_ts_iso`, `batch_end_ts_iso` you already pass to the
     Delta and FG-A steps.

3. **FG-B baseline integration**

   - The FG-B builder uses the `pair_counts_output.s3_prefix` to compute
     pair-level rarity baselines over 7d/30d horizons.
   - No Python code changes are strictly required in FG-B if you already
     have its JobSpec configured with a matching `pair_counts.s3_prefix`.
   - Make sure the partitioning scheme (`dt`, `hh`, `mm`) and field names
     (`src_ip`, `dst_ip`, `dst_port`, `event_ts`, `sessions_cnt`) stay
     consistent between Pair-Counts builder and FG-B.

## Testing

- Run the new unit tests from the project root:

  ```bash
  python -m pytest tests/test_pair_counts_builder_job.py
  ```

- For an end-to-end dry run (in a dev environment):

  ```bash
  python -m ndr.scripts.run_pair_counts_builder \
    --project-name ndr_project \
    --feature-spec-version v1 \
    --mini-batch-id batch_20251231T0000Z \
    --batch-start-ts-iso 2025-12-31T00:00:00Z \
    --batch-end-ts-iso 2025-12-31T00:15:00Z
  ```

  (Use an integration S3 prefix populated with representative parsed
  Palo Alto traffic logs.)
