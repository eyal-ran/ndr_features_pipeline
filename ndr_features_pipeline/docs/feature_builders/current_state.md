# NDR Processing Jobs and Pipelines (Current State)

This document describes the **current implementation state** of the NDR processing stack based on the code under `src/ndr/`.

## Runtime model

The system is orchestrated through Step Functions and SageMaker Pipelines. Pipeline steps invoke Python entrypoints in `src/ndr/scripts/`, which in turn execute Spark processing jobs under `src/ndr/processing/`. Runtime parameters are passed from orchestration, while structural configuration is read from JobSpec (DynamoDB-backed) and related environment configuration.

## Core 15-minute feature path

### 1) Delta Builder
- Entrypoint: `ndr.scripts.run_delta_builder`
- Job: `src/ndr/processing/delta_builder_job.py`
- Purpose: reads parsed traffic logs and writes 15-minute host-level deltas.
- Typical runtime args:
  - `--project-name`
  - `--feature-spec-version`
  - `--mini-batch-id`
  - `--batch-start-ts-iso`
  - `--batch-end-ts-iso`

### 2) FG-A Builder
- Entrypoint: `ndr.scripts.run_fg_a_builder`
- Job: `src/ndr/processing/fg_a_builder_job.py`
- Purpose: computes multi-window current-behavior features from delta slices and writes FG-A outputs.

### 3) Pair-Counts Builder
- Entrypoint: `ndr.scripts.run_pair_counts_builder`
- Job: `src/ndr/processing/pair_counts_builder_job.py`
- Purpose: computes pair-level counts used downstream for rarity and baseline features.

### 4) FG-C Correlation Builder
- Entrypoint: `ndr.scripts.run_fg_c_builder`
- Job: `src/ndr/processing/fg_c_builder_job.py`
- Purpose: computes correlation/drift features by combining current behavior (FG-A) with baseline references.

## Baseline and reference-data path

### FG-B Baseline Builder
- Entrypoint: `ndr.scripts.run_fg_b_builder`
- Job: `src/ndr/processing/fg_b_builder_job.py`
- Purpose: computes baseline outputs (host/segment/pair-oriented baseline datasets) for configured horizons.
- Publication semantics: publishes canonical FG-B artifacts directly under `fg_b_output.s3_prefix` (`/host`, `/segment`, `/ip_metadata`, `/pair/host`, `/pair/segment`) with deterministic overwrite by (`feature_spec_version`, `baseline_horizon`).
- Publication observability: emits `publication_metadata` records with baseline bounds plus deterministic `created_at`/`created_date` derived from the monthly reference time.
- Runtime args include:
  - `--project-name`
  - `--feature-spec-version`
  - `--reference-time-iso`
  - `--mode` (for example REGULAR/BACKFILL)

### Machine Inventory Unload (monthly)
- Entrypoint: `ndr.scripts.run_machine_inventory_unload`
- Job: `src/ndr/processing/machine_inventory_unload_job.py`
- Purpose: refreshes inventory data used by baseline and cold-start/non-persistent logic.

## Model-scoring and post-processing path

### Inference Predictions
- Entrypoint: `ndr.scripts.run_inference_predictions`
- Job: `src/ndr/processing/inference_predictions_job.py`
- Purpose: reads feature datasets and produces inference predictions.

### Prediction Feature Join
- Entrypoint: `ndr.scripts.run_prediction_feature_join`
- Job: `src/ndr/processing/prediction_feature_join_job.py`
- Purpose: joins prediction outputs and associated metadata/features for downstream publication.

## Training path

### IF Training
- Entrypoint: `ndr.scripts.run_if_training`
- Job: `src/ndr/processing/if_training_job.py`
- Purpose: executes IF-oriented model training workflow against configured feature inputs.

## SageMaker pipeline modules (current)

- `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
  - `build_delta_builder_pipeline`
  - `build_15m_streaming_pipeline` (Delta -> FG-A -> Pair-Counts -> FG-C)
  - `build_fg_b_baseline_pipeline`
  - `build_machine_inventory_unload_pipeline`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
  - `build_inference_predictions_pipeline`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`
  - `build_if_training_pipeline`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
  - `build_prediction_feature_join_pipeline`

## JobSpec and contract notes

- Jobs resolve structural configuration (S3 paths, thresholds, schema expectations, and similar controls) through JobSpec loaders and related config models.
- Runtime orchestration should primarily provide execution-specific parameters (project, spec version, run/batch identifiers, and timestamps), while static topology stays in JobSpec.

## Canonical references

- Architecture and system-level pipeline behavior: `docs/architecture/overview.md`
- Orchestration details and Step Functions definitions: `docs/architecture/orchestration/step_functions.md`
- Feature-level artifact catalog: `docs/FEATURE_CATALOG.md` and `docs/feature_catalog/`
