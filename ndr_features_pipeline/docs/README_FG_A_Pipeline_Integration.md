
# NDR FG-A Pipeline Integration

This bundle contains example code to integrate the FG-A builder into your
existing SageMaker Pipeline and Processing-based workflow.

Files
-----

- `run_fg_a_builder.py`
  Entry-point script for the FG-A builder SageMaker ProcessingStep.
  Expected location in your project:
      `ndr_pipeline/src/ndr/scripts/run_fg_a_builder.py`

- `sagemaker_pipeline_definitions_fga_example.py`
  Example `build_delta_and_fg_a_pipeline(...)` function that:
    - Defines pipeline parameters for project_name, mini_batch_id, etc.
    - Creates a Delta Builder ProcessingStep.
    - Creates a FG-A Builder ProcessingStep that depends on the Delta step.
    - Wires parameters through to `run_fg_a_builder.py` via `job_arguments`.
  Intended to be merged into:
      `ndr_pipeline/src/ndr/pipeline/sagemaker_pipeline_definitions.py`
  (You can copy the function into your existing module and adjust
   image URIs, instance types, and arguments as needed.)

How Step Functions should call the pipeline
-------------------------------------------

When Step Functions starts this pipeline (using the AWS SDK or
`StartPipelineExecution` API), it should pass concrete values for the
pipeline parameters, for example:

- `ProjectName`               → your logical project name (e.g., "ndr-prod")
- `MiniBatchId`               → the mini-batch identifier from the ETL flow
- `FeatureSpecVersion`        → the FG-A spec version ("fga_v1", etc.)
- `DeltaS3Prefix`             → S3 prefix where delta tables are written
- `FgaOutputS3Prefix`         → S3 prefix where FG-A parquet should be written
- `BatchStartTsIso`           → ISO8601 batch start timestamp (e.g., 2025-12-31T00:00:00Z)

These parameter values are passed as `job_arguments` to the FG-A
ProcessingStep, where `run_fg_a_builder.py` parses them and constructs
`FGABuilderConfig` for `FGABuilderJob`.

FG-A output is written to S3 only; any Feature Store ingestion should be
performed in a separate pipeline step after processing completes.

The delta builder step remains responsible for:
  - reading parsed Palo Alto logs from the integration bucket,
  - applying cleaning and data-quality rules,
  - writing 15-minute host-level delta parquet slices to S3.

The FG-A step then:
  - reads from the delta prefix (filtered by `mini_batch_id`),
  - computes FG-A windows (15m, 30m, 1h, 8h, 24h),
  - writes FG-A parquet to S3 using:
    `.../fg_a/ts=YYYY/MM/DD/HH/MM-batch_id=<mini_batch_id>/`.
