
# NDR SageMaker Pipeline Additions

This directory includes two additional pipeline definition modules:

1. `sagemaker_pipeline_definitions_with_pairs.py`
   - Defines `build_ndr_15m_pipeline_with_pairs`, a 15m streaming pipeline
     with three ProcessingSteps:
       1. `DeltaBuilderStep`      -> `run_delta_builder.py`
       2. `FGABuilderStep`        -> `run_fg_a_builder.py`
       3. `PairCountsBuilderStep` -> `run_pair_counts_builder.py`
   - All steps share a common ScriptProcessor and take the same runtime
     parameters:
       - `ProjectName`
       - `FeatureSpecVersion`
       - `MiniBatchId`
       - `BatchStartTsIso`
       - `BatchEndTsIso`
       - `ProcessingImageUri`
       - `ProcessingInstanceType`
       - `ProcessingInstanceCount`
   - In your existing `sagemaker_pipeline_definitions.py`, you can either:
       - import and call this function (`build_ndr_15m_pipeline_with_pairs`)
         instead of your current builder, or
       - copy the relevant step and parameter wiring into your existing
         pipeline construction function.

2. `sagemaker_pipeline_definitions_fg_b.py`
   - Defines `build_fg_b_baseline_pipeline`, a coarse-grained pipeline
     dedicated to FG-B baseline computation (host/segment/pair) over 7d/30d
     horizons.
   - Single ProcessingStep:
       - `FGBaselineBuilderStep` -> `run_fg_b_builder.py`
   - Parameters:
       - `ProjectName`
       - `FeatureSpecVersion`
       - `BaselineHorizon`  ("7d", "30d", "both")
       - `BaselineMode`     ("REGULAR", "BACKFILL")
       - `BaselineEndTsIso`
       - `BackfillStartTsIso`
       - `BackfillEndTsIso`
       - `ProcessingImageUri`
       - `ProcessingInstanceType`
       - `ProcessingInstanceCount`

3. `sagemaker_pipeline_definitions_unified_with_fgc.py`
   - Defines `build_machine_inventory_unload_pipeline`, a monthly pipeline
     that unloads unique, active machine inventory records from Redshift
     to S3 before the FG-B pipeline runs.
   - Single ProcessingStep:
       - `MachineInventoryUnloadStep` -> `run_machine_inventory_unload.py`
   - Parameters:
       - `ProjectName`
       - `FeatureSpecVersion`
       - `ReferenceMonthIso`
       - `InstanceType`
       - `InstanceCount`
       - `ImageUri`

## Integration instructions

1. **15m streaming pipeline (Delta + FG-A + Pair-Counts)**

   In your existing `sagemaker_pipeline_definitions.py`, you can do:

   ```python
   from ndr.pipeline.sagemaker_pipeline_definitions_with_pairs import (
       build_ndr_15m_pipeline_with_pairs,
   )

   def build_ndr_15m_pipeline(role_arn: str, region: str, default_bucket: str):
       return build_ndr_15m_pipeline_with_pairs(
           role_arn=role_arn,
           region=region,
           default_bucket=default_bucket,
           pipeline_name="ndr-15m-pipeline",
       )
   ```

   This preserves your existing entrypoint name while delegating the
   step wiring to the new module.

2. **FG-B baseline pipeline**

   Create a new entry or command in your infrastructure code (or Step
   Function) to build and execute the FG-B pipeline:

   ```python
   from ndr.pipeline.sagemaker_pipeline_definitions_fg_b import (
       build_fg_b_baseline_pipeline,
   )

   def build_and_register_fg_b_pipeline(role_arn: str, region: str, bucket: str):
       pipeline = build_fg_b_baseline_pipeline(
           role_arn=role_arn,
           region=region,
           default_bucket=bucket,
       )
       pipeline.upsert(role_arn)
       return pipeline
   ```

   Then, from Step Functions (or a separate orchestration job), you can
   trigger:

   - Nightly 7d baselines
   - Weekly 30d baselines
   - Initial BACKFILL runs

   by calling `.start()` on the registered pipeline with the appropriate
   parameter overrides (BaselineMode, BaselineHorizon, etc.).

3. **JobSpec and DynamoDB alignment**

   Ensure that the JobSpecs for:

   - `delta_builder`
   - `fg_a_builder`
   - `pair_counts_builder`
   - `fg_b_builder`

   all share the same `project_name` and `feature_spec_version`, and that
   their S3 prefixes (integration, deltas, FG-A, pair_counts, FG-B) are
   consistent with the paths your Spark jobs write to.

   Once aligned, the 15m pipeline will continuously populate:
   - Delta tables
   - FG-A features
   - Pair-counts dataset

   and the FG-B pipeline will periodically convert these into robust
   7d/30d baselines and pair rarity measures.

4. **Machine inventory unload pipeline**

   Register and run the monthly unload pipeline before the FG-B baseline
   pipeline so cold-start detection has the latest machine inventory data:

   ```python
   from ndr.pipeline.sagemaker_pipeline_definitions_unified_with_fgc import (
       build_machine_inventory_unload_pipeline,
   )

   def build_and_register_machine_inventory_pipeline(role_arn: str, region: str, bucket: str):
       pipeline = build_machine_inventory_unload_pipeline(
           role_arn=role_arn,
           region_name=region,
           default_bucket=bucket,
           pipeline_name="ndr-machine-inventory-unload",
       )
       pipeline.upsert(role_arn)
       return pipeline
   ```

   The machine inventory JobSpec should provide the Redshift Data API
   connection, query definition, output prefix, and processing image URI. Example:

   ```json
   {
     "project_name": "ndr-prod",
     "job_name": "machine_inventory_unload",
     "feature_spec_version": "v1",
     "spec": {
       "redshift": {
         "cluster_identifier": "redshift-cluster-1",
         "database": "ndr",
         "secret_arn": "arn:aws:secretsmanager:...",
         "region": "us-east-1",
         "iam_role": "arn:aws:iam::123456789012:role/RedshiftUnloadRole"
       },
       "query": {
         "schema": "public",
         "table": "dim_machine",
         "ip_column": "ip_address",
         "name_column": "machine_name",
         "active_filter": "is_active = true",
         "additional_filters": ["environment = 'prod'"]
       },
       "output": {
         "s3_prefix": "s3://ndr-data/machine_inventory/",
         "output_format": "PARQUET",
         "partitioning": ["snapshot_month"],
         "ip_output_column": "ip_address",
         "name_output_column": "machine_name"
       },
       "processing_image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest"
     },
     "updated_at": "2025-01-01T00:00:00Z",
     "owner": "ndr-team"
   }
   ```

   If the monthly snapshot partition already exists, the unload job writes
   to a temporary sub-prefix, copies new files into the partition, writes a
   `_SUCCESS` marker, and only then deletes the temporary prefix. If copy
   verification fails, the temporary prefix is retained for inspection.

   Store non-persistent VDI prefixes in a separate JobSpec (for reuse by FG-B):

   ```json
   {
     "project_name": "ndr-prod",
     "job_name": "non_persistent_prefixes",
     "feature_spec_version": "v1",
     "spec": {
       "prefixes": ["VDI-", "NP-"]
     },
     "updated_at": "2025-01-01T00:00:00Z",
     "owner": "ndr-team"
   }
   ```
