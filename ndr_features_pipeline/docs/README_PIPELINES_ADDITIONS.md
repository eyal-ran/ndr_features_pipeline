
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
