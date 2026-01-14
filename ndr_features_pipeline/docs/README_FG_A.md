
# NDR FG-A Builder

This directory contains implementation artifacts for the FG-A (current
behaviour) feature builder in the NDR pipeline.

It includes:

- `fg_a_schema.py` – window configuration and naming utilities for FG-A
  features, used by the builder job.
- `fg_a_builder_job.py` – the Spark-based Processing job that:
  - reads 15-minute host-level delta tables from S3,
  - aggregates them into FG-A features for multiple windows (15m, 30m,
    1h, 8h, 24h) for both outbound and inbound roles,
  - adds time-of-day and weekday/weekend context features,
  - writes a partitioned Parquet dataset to S3,
  - optionally ingests rows into SageMaker Feature Store.
- `test_fg_a_builder_job.py` – unit tests that validate window
  aggregation logic and basic schema expectations.

These files are intended to be integrated into the existing project
structure as follows:

- `src/ndr/model/fg_a_schema.py`  ←  `fg_a_schema.py`
- `src/ndr/processing/fg_a_builder_job.py`  ←  `fg_a_builder_job.py`
- `tests/test_fg_a_builder_job.py`  ←  `test_fg_a_builder_job.py`

The FG-A builder is designed to be compatible with the previously
defined delta table schema and does not modify the delta builder
behaviour. Baseline and correlation features are handled by FG-B and
FG-C builder jobs, and may be joined to FG-A outputs in later steps of
the pipeline.
