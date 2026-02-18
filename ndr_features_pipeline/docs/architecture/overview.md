# NDR Feature Engineering Architecture Overview

## Purpose

The NDR feature engineering stack builds and publishes feature datasets for near-real-time inference and training/refitting workflows. The design is centered on Spark processing jobs executed via SageMaker Processing and orchestrated by Step Functions + SageMaker Pipelines.

## Scope

Primary data layers:
- Delta (15-minute slices)
- FG-A current behavior features
- Pair-counts
- FG-B baselines
- FG-C correlation features
- Inference prediction outputs and prediction joins
- IF training outputs

## High-level architecture

1. **Event and orchestration layer**
   - Upstream events and schedules trigger Step Functions state machines.
   - Step Functions resolve runtime parameters and invoke SageMaker pipelines.

2. **Pipeline and processing layer**
   - SageMaker pipeline steps execute entrypoint scripts in `src/ndr/scripts/`.
   - Entrypoints execute Spark jobs in `src/ndr/processing/`.

3. **Configuration layer**
   - Job specifications and project parameters are loaded from DynamoDB-backed configuration via the repository config loaders.
   - Runtime arguments carry execution-specific values (project/spec/batch/run/timestamps).

4. **Storage layer**
   - Inputs and outputs are persisted as S3 datasets (mostly Parquet outputs for processed feature layers).

## Pipeline inventory (current code)

- **Unified feature pipelines module** (`sagemaker_pipeline_definitions_unified_with_fgc.py`)
  - Delta-only pipeline (ad-hoc/backfill use)
  - 15-minute streaming feature pipeline: Delta -> FG-A -> Pair-Counts -> FG-C
  - FG-B baseline pipeline
  - Machine inventory unload pipeline
- **Inference predictions pipeline** (`sagemaker_pipeline_definitions_inference.py`)
- **IF training pipeline** (`sagemaker_pipeline_definitions_if_training.py`)
- **Prediction feature join pipeline** (`sagemaker_pipeline_definitions_prediction_feature_join.py`)
- **Backfill historical extractor pipeline** (`sagemaker_pipeline_definitions_backfill_historical_extractor.py`)

## Operational expectations

- Processing jobs should be idempotent for re-runs over the same slice/run identifiers.
- Contracts across feature layers (especially FG-A/FG-B/FG-C) should remain schema-stable unless coordinated changes are performed.
- Orchestration-level placeholders in Step Functions JSON must be substituted during deployment.

## Related docs

- Step Functions details: `docs/architecture/orchestration/step_functions.md`
- Diagrams:
  - `docs/architecture/diagrams/pipeline_flow.md` (runtime feature/data path)
  - `docs/architecture/diagrams/system_architecture.md` (full AWS architecture view)
  - `docs/architecture/diagrams/orchestration_event_lifecycle.md` (state-machine callback/event flow)
  - `docs/architecture/diagrams/stakeholder_value_flow.md` (business-oriented value stream)
- Processing + builders current state: `docs/feature_builders/current_state.md`
- Feature definitions: `docs/FEATURE_CATALOG.md` and `docs/feature_catalog/`
