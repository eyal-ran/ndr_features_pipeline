# NDR SageMaker Pipelines Flow Description

This document groups SageMaker Pipelines by their **triggering source**, and orders each pipeline according to the order in which it is triggered by that source.

It includes:
- exact pipeline names (as used in orchestrators/placeholders),
- pipeline code locations,
- triggering source names and locations,
- all steps in each triggering source,
- and nested run-chain details (pipeline step -> run script -> processing job entrypoint).

---

## 1) Triggering source: `sfn_ndr_15m_features_inference` (15-minute features + inference)

- **Trigger source name:** 15-minute features + inference state machine.
- **Trigger source location:** `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`.
- **How it is triggered:** ingestion completion/event payload flow (batch folder path + timestamp), normalized and routed by Step Functions.

### All steps in triggering source (in declared order)
1. `NormalizeIncomingMessage` — normalize raw inbound wrapper payload.
2. `ParseIncomingProjectContext` — parse project/spec/path/timestamp hints.
3. `BatchCompletionEventChoice` — gate on valid batch-completion event.
4. `IgnoreNonBatchCompletionEvent` — terminate cleanly for non-batch events.
5. `LoadProjectRoutingFromDynamo` — resolve org-based routing metadata.
6. `LoadProjectParametersFromDynamo` — load project defaults/spec settings.
7. `ResolvePipelineRuntimeParams` — compute final runtime parameters.
8. `AcquireMiniBatchLock` — enforce idempotency lock for 15m window.
9. `DuplicateMiniBatch` — fail duplicate execution attempts.
10. `Start15mFeaturesPipeline` — **PIPELINE TRIGGER**; start `${PipelineName15m}`.
11. `Describe15mFeaturesPipeline` — poll 15m features pipeline status.
12. `FeaturesPipelineStatusChoice` — branch on features pipeline status.
13. `WaitBeforeDescribe15mFeatures` — wait between status polls.
14. `IncrementFeaturesPollAttempt` — increment features poll counter.
15. `StartInferencePipeline` — **PIPELINE TRIGGER**; start `${PipelineNameInference}`.
16. `DescribeInferencePipeline` — poll inference pipeline status.
17. `InferencePipelineStatusChoice` — branch on inference status.
18. `WaitBeforeDescribeInference` — wait between inference polls.
19. `IncrementInferencePollAttempt` — increment inference poll counter.
20. `StartPredictionPublicationWorkflow` — start nested prediction-publication state machine synchronously.
21. `ReleaseMiniBatchLockOnSuccess` — release lock on success path.
22. `ReleaseMiniBatchLockOnFailure` — release lock on failure path.
23. `WorkflowFailed` — terminal failure state.
24. `Success` — terminal success state.

### Pipeline triggered at `Start15mFeaturesPipeline`: `${PipelineName15m}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_15m_streaming_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- **Purpose:** 15-minute feature-generation chain.

#### Pipeline steps and run chain
1. `DeltaBuilderStep`
   - Pipeline step runs: `python -m ndr.scripts.run_delta_builder`
   - Script location: `src/ndr/scripts/run_delta_builder.py`
   - Script purpose: parse runtime params and delegate delta build.
   - Second-hand invoked processing entrypoint: `run_delta_builder_from_runtime_config`
   - Processing module location: `src/ndr/processing/delta_builder_job.py`
   - Processing purpose: build 15-minute delta tables from Palo Alto logs.
2. `FGABuilderStep` (depends on `DeltaBuilderStep`)
   - Runs: `python -m ndr.scripts.run_fg_a_builder`
   - Script: `src/ndr/scripts/run_fg_a_builder.py`
   - Second-hand processing entrypoint: `run_fg_a_builder_from_runtime_config`
   - Processing module: `src/ndr/processing/fg_a_builder_job.py`
   - Processing purpose: build FG-A (current-behavior) windowed features on top of deltas.
3. `PairCountsBuilderStep` (depends on `FGABuilderStep`)
   - Runs: `python -m ndr.scripts.run_pair_counts_builder`
   - Script: `src/ndr/scripts/run_pair_counts_builder.py`
   - Second-hand processing entrypoint: `run_pair_counts_builder_from_runtime_config`
   - Processing module: `src/ndr/processing/pair_counts_builder_job.py`
   - Processing purpose: build pair-count datasets for `(src_ip, dst_ip, dst_port)` over 15-minute slices.
4. `FGCCorrBuilderStep` (depends on `PairCountsBuilderStep`)
   - Runs: `python -m ndr.scripts.run_fg_c_builder`
   - Script: `src/ndr/scripts/run_fg_c_builder.py`
   - Second-hand processing entrypoint: `run_fg_c_builder_from_runtime_config`
   - Processing module: `src/ndr/processing/fg_c_builder_job.py`
   - Processing purpose: compute FG-C correlation features.

### Pipeline triggered at `StartInferencePipeline`: `${PipelineNameInference}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_inference_predictions_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`.
- **Purpose:** decoupled inference prediction generation.

#### Pipeline steps and run chain
1. `InferencePredictionsStep`
   - Runs: `python -m ndr.scripts.run_inference_predictions`
   - Script: `src/ndr/scripts/run_inference_predictions.py`
   - Second-hand processing entrypoint: `run_inference_predictions_from_runtime_config`
   - Processing module: `src/ndr/processing/inference_predictions_job.py`
   - Processing purpose: execute inference predictions for the mini-batch window.

### Nested trigger in this source
- `StartPredictionPublicationWorkflow` starts the separate triggering source `sfn_ndr_prediction_publication` synchronously.

---

## 2) Triggering source: `sfn_ndr_prediction_publication` (prediction publication)

- **Trigger source name:** prediction publication state machine.
- **Trigger source location:** `docs/step_functions_jsonata/sfn_ndr_prediction_publication.json`.
- **How it is triggered:** direct nested call from `sfn_ndr_15m_features_inference` after inference succeeds.

### All steps in triggering source (in declared order)
1. `NormalizeIncomingMessage` — normalize inbound payload shape.
2. `ParseIncomingProjectContext` — parse project/spec values.
3. `LoadProjectParametersFromDynamo` — load project defaults.
4. `ResolvePipelineRuntimeParams` — resolve publication runtime values.
5. `AcquirePublicationLock` — enforce publication idempotency lock.
6. `StartPredictionJoinPipeline` — **PIPELINE TRIGGER**; start `${PipelineNamePredictionJoin}`.
7. `DescribeJoinPipeline` — poll prediction-join pipeline status.
8. `JoinPipelineStatusChoice` — branch on join pipeline status.
9. `WaitBeforeJoinDescribe` — wait between join polls.
10. `IncrementJoinPollAttempt` — increment join poll counter.
11. `MarkPublicationSucceeded` — mark publication lock as succeeded.
12. `MarkPublicationFailed` — mark publication lock as failed.
13. `DuplicatePublicationSuppressed` — suppress duplicate publication request.
14. `EmitPublicationEvent` — emit completion event.
15. `WorkflowFailed` — terminal failure state.

### Pipeline triggered at `StartPredictionJoinPipeline`: `${PipelineNamePredictionJoin}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_prediction_feature_join_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`.
- **Purpose:** join prediction outputs with feature datasets and publish to destination (`s3` or staged `redshift`) within the same pipeline.

#### Pipeline steps and run chain
1. `PredictionFeatureJoinStep`
   - Runs: `python -m ndr.scripts.run_prediction_feature_join`
   - Script: `src/ndr/scripts/run_prediction_feature_join.py`
   - Second-hand processing entrypoint: `run_prediction_feature_join_from_runtime_config`
   - Processing module: `src/ndr/processing/prediction_feature_join_job.py`
   - Processing purpose: combine inference predictions with relevant features.

---

## 3) Triggering source: `sfn_ndr_monthly_fg_b_baselines` (monthly FG-B baseline orchestration)

- **Trigger source name:** monthly FG-B baselines state machine.
- **Trigger source location:** `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`.
- **How it is triggered:** monthly baseline workflow (schedule/domain event pattern).

### All steps in triggering source (in declared order)
1. `NormalizeIncomingMessage` — normalize inbound payload.
2. `ParseIncomingProjectContext` — parse project/spec hints.
3. `LoadProjectParametersFromDynamo` — load project defaults.
4. `ResolvePipelineRuntimeParams` — resolve monthly runtime parameters.
5. `StartMachineInventoryRefresh` — **PIPELINE TRIGGER**; start `${PipelineNameMachineInventory}`.
6. `DescribeInventoryPipeline` — poll machine-inventory pipeline status.
7. `InventoryPipelineStatusChoice` — branch on inventory status.
8. `WaitBeforeInventoryDescribe` — wait between inventory polls.
9. `IncrementInventoryPollAttempt` — increment inventory poll counter.
10. `StartFGBBaselinePipeline` — **PIPELINE TRIGGER**; start `${PipelineNameFGB}`.
11. `DescribeFGBPipeline` — poll FG-B pipeline status.
12. `FGBPipelineStatusChoice` — branch on FG-B status.
13. `WaitBeforeFGBDescribe` — wait between FG-B polls.
14. `IncrementFGBPollAttempt` — increment FG-B poll counter.
15. `EmitBaselineReadyEvent` — emit monthly-baseline completion event.
16. `WorkflowFailed` — terminal failure state.

### Pipeline triggered at `StartMachineInventoryRefresh`: `${PipelineNameMachineInventory}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_machine_inventory_unload_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- **Purpose:** monthly machine inventory refresh/unload to S3 for downstream baseline logic.

#### Pipeline steps and run chain
1. `MachineInventoryUnloadStep`
   - Runs: `python -m ndr.scripts.run_machine_inventory_unload`
   - Script: `src/ndr/scripts/run_machine_inventory_unload.py`
   - Second-hand processing entrypoint: `run_machine_inventory_unload_from_runtime_config`
   - Processing module: `src/ndr/processing/machine_inventory_unload_job.py`
   - Processing purpose: unload active machine inventory (Redshift -> S3).

### Pipeline triggered at `StartFGBBaselinePipeline`: `${PipelineNameFGB}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_fg_b_baseline_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- **Purpose:** compute FG-B host/segment/pair baselines over configured windows/horizons.
- **Publication contract:** FG-B pipeline now acts as the canonical monthly publisher for FG-C consumers by writing deterministic overwrite snapshots directly under canonical prefixes (`/host`, `/segment`, `/ip_metadata`, `/pair/host`, `/pair/segment`) partitioned by `feature_spec_version` + `baseline_horizon`.

#### Pipeline steps and run chain
1. `FGBaselineBuilderStep`
   - Runs: `python -m ndr.scripts.run_fg_b_builder`
   - Script: `src/ndr/scripts/run_fg_b_builder.py`
   - Second-hand processing entrypoint: `run_fg_b_builder_from_runtime_config`
   - Processing module: `src/ndr/processing/fg_b_builder_job.py`
   - Processing purpose: FG-B baseline build for regular/backfill modes.

---

## 4) Triggering source: `sfn_ndr_training_orchestrator` (training workflow)

- **Trigger source name:** training orchestrator state machine.
- **Trigger source location:** `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`.
- **How it is triggered:** training workflow trigger (schedule/domain event/manual orchestration input).

### All steps in triggering source (in declared order)
1. `NormalizeIncomingMessage` — normalize inbound payload.
2. `ParseIncomingProjectContext` — parse project/spec hints.
3. `LoadProjectParametersFromDynamo` — load project defaults.
4. `ResolvePipelineRuntimeParams` — resolve unified training runtime values.
5. `StartTrainingPipeline` — **PIPELINE TRIGGER**; starts `${PipelineNameIFTraining}`.
6. `DescribeTrainingPipeline` — poll unified training pipeline status.
7. `TrainingPipelineStatusChoice` — branch on pipeline status.
8. `WaitBeforeTrainingDescribe` — wait between polls.
9. `IncrementTrainingPollAttempt` — increment poll counter.
10. `WorkflowFailed` — terminal workflow failure state.
11. `Success` — terminal success state.

### Pipeline triggered at `StartTrainingPipeline`: `${PipelineNameIFTraining}`
- **Implementation state:** Implemented and authoritative owner for the full training lifecycle.
- **Implemented pipeline function:** `build_if_training_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`.
- **Purpose:** execute verifier + bounded remediation + re-verification + IF training + publish + attributes + deploy in one ordered pipeline-native chain.

#### Pipeline steps and run chain
1. `TrainingDataVerifierStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage verify`
   - Purpose: validate FG-A/FG-C coverage and persist deterministic verification status artifact.
2. `MissingFeatureCreationStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage remediate`
   - Purpose: bounded remediation (`max 2` attempts) using missing windows from verifier status or runtime override.
3. `PostRemediationVerificationStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage reverify`
   - Purpose: re-check coverage after remediation and persist re-verification status.
4. `IFTrainingStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage train`
   - Purpose: train/validate/refit, persist artifacts, write final report and SUCCESS marker.
5. `ModelPublishStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage publish`
   - Purpose: persist publication metadata derived from final report for deterministic downstream lifecycle state.
6. `ModelAttributesStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage attributes`
   - Purpose: persist model attributes payload (join/preprocessing/window metadata) with idempotent run key.
7. `ModelDeployStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage deploy`
   - Purpose: perform deployment gate evaluation + deployment call and persist rollout status artifact.

## 5) Triggering source: `sfn_ndr_backfill_reprocessing` (backfill and reprocessing)

- **Trigger source name:** backfill and reprocessing state machine.
- **Trigger source location:** `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`.
- **How it is triggered:** backfill request/workflow input over historical range.

### All steps in triggering source (in declared order)
1. `NormalizeIncomingMessage` — normalize inbound payload.
2. `ParseIncomingProjectContext` — parse project/spec context.
3. `LoadProjectParametersFromDynamo` — load project defaults.
4. `ResolvePipelineRuntimeParams` — resolve backfill runtime values.
5. `StartHistoricalWindowsExtractorPipeline` — **PIPELINE TRIGGER**; start `${PipelineNameBackfillHistoricalExtractor}`.
6. `DescribeHistoricalWindowsExtractor` — poll extractor pipeline status.
7. `ExtractorPipelineStatusChoice` — branch on extractor status.
8. `WaitBeforeExtractorDescribe` — wait between extractor polls.
9. `IncrementExtractorPollAttempt` — increment extractor poll counter.
10. `ResolveBackfillWindows` — resolve emitted windows list.
11. `RunBackfillWindows` (Map) — iterate each window and run per-item pipeline trigger.
12. `EmitBackfillReconciliationEvent` — emit backfill completion/reconciliation event.
13. `WorkflowFailed` — terminal failure state.

### Pipeline triggered at `StartHistoricalWindowsExtractorPipeline`: `${PipelineNameBackfillHistoricalExtractor}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_backfill_historical_extractor_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_backfill_historical_extractor.py`.
- **Purpose:** discover/extract backfill windows from historical mini-batch objects and emit map execution units.

#### Pipeline steps and run chain
1. `HistoricalWindowsExtractorStep`
   - Runs: `python -m ndr.scripts.run_historical_windows_extractor`
   - Script: `src/ndr/scripts/run_historical_windows_extractor.py`
   - Second-hand processing entrypoint: `HistoricalWindowsExtractorJob(runtime).run()`
   - Processing module: `src/ndr/processing/historical_windows_extractor_job.py`
   - Processing purpose: enumerate candidate objects, derive window bounds, and write backfill window manifest.

### Pipeline triggered inside `RunBackfillWindows` map at `StartBackfillPipeline`: `${PipelineNameBackfill15m}`
- **Implementation state:** Implemented by wiring this placeholder to the existing 15-minute feature pipeline (`build_15m_streaming_pipeline`) in deployment.
- **Trigger reference location:** `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`.
- **Likely implementation location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py` (`build_15m_streaming_pipeline`).
- **Purpose:** run the full 15m feature chain per extracted historical window.
- **Steps/scripts:** same as `${PipelineName15m}` under section 1.

---

## 6) Pipelines without a fixed triggering source

These are currently not tied to one canonical, always-on trigger source in Step Functions.

### Pipeline: Delta-only ad-hoc/backfill pipeline (`build_delta_builder_pipeline`)
- **Pipeline exact name at runtime:** provided externally as `pipeline_name` argument at registration/build time.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- **Purpose:** run only delta generation for ad-hoc/backfill/debug scenarios.
- **Explicit operating modes from repository context:**
  1. **CLI-oriented/ad-hoc use** (run script directly):
     - `python -m ndr.scripts.run_delta_builder --project-name ... --feature-spec-version ... --mini-batch-id ... --batch-start-ts-iso ... --batch-end-ts-iso ...`
  2. **Pipeline SDK / notebook / automation registration path**:
     - construct and register/start the SageMaker Pipeline via `build_delta_builder_pipeline(...)` and a chosen runtime `pipeline_name`.
  3. **Step Functions wiring option**:
     - can be targeted by orchestration when a dedicated backfill/delta-only path is required.

#### Pipeline steps and run chain
1. `DeltaBuilderStep`
   - Runs: `python -m ndr.scripts.run_delta_builder`
   - Script: `src/ndr/scripts/run_delta_builder.py`
   - Second-hand processing entrypoint: `run_delta_builder_from_runtime_config`
   - Processing module: `src/ndr/processing/delta_builder_job.py`
   - Processing purpose: 15-minute delta table construction.

---

## Inventory note for placeholders

The architecture inventory lists currently implemented pipeline definition modules in:
- `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`
- `src/ndr/pipeline/sagemaker_pipeline_definitions_backfill_historical_extractor.py`

Any pipeline names present only as Step Functions placeholders and not present in these modules are explicitly marked as placeholders in this document.
