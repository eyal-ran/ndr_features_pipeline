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
6. `LoadDppConfigFromDynamo -> LoadMlpConfigFromDynamo` — load project defaults/spec settings.
7. `ResolvePipelineRuntimeParams` — compute final runtime parameters.
8. `AcquireMiniBatchLock` — enforce idempotency lock for 15m window.
9. `DuplicateMiniBatch` — fail duplicate execution attempts.
10. `WriteBatchIndexBatchIdItem -> UpdateBatchIndexBatchIdItem -> WriteBatchIndexDateLookupItem` — prewrite canonical batch-index records before any branch execution.
11. `RunPerMlProjectBranch` — strict `ml_project_names` Map fan-out with explicit branch context (`ml_project_name`).
12. `Start15mFeaturesPipeline` — **PIPELINE TRIGGER (inside Map branch)**; start `${PipelineName15m}` (core phase).
13. `CheckRtArtifactReadiness` / remediation loop — enforce dependent readiness.
14. `Start15mDependentFeaturesPipeline` — start `${PipelineName15mDependent}` (FG-C dependent phase).
13. `Describe15mFeaturesPipeline` — poll 15m features pipeline status.
14. `FeaturesPipelineStatusChoice` — branch on features pipeline status.
15. `WaitBeforeDescribe15mFeatures` — wait between status polls.
16. `IncrementFeaturesPollAttempt` — increment features poll counter.
17. `StartInferencePipeline` — **PIPELINE TRIGGER (inside Map branch)**; start `${PipelineNameInference}`.
18. `DescribeInferencePipeline` — poll inference pipeline status.
19. `InferencePipelineStatusChoice` — branch on inference status.
20. `WaitBeforeDescribeInference` — wait between inference polls.
21. `IncrementInferencePollAttempt` — increment inference poll counter.
22. `StartPredictionPublicationWorkflow` — start nested prediction-publication state machine synchronously (per branch).
23. `ReleaseMiniBatchLockOnSuccess` — release lock on success path.
24. `ReleaseMiniBatchLockOnFailure` — release lock on failure path.
25. `WorkflowFailed` — terminal failure state.
26. `Success` — terminal success state.

### Pipeline triggered at `Start15mFeaturesPipeline`: `${PipelineName15m}` (core)
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_15m_streaming_pipeline` (Delta/FG-A/Pair-Counts).
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
   - Raw-input source contract: resolves `ingestion` vs `redshift_unload_fallback` using shared `RawInputResolver` and DDB-owned `project_parameters.backfill_redshift_fallback`.
   - Fail-fast contract errors: `RAW_INPUT_FALLBACK_DISABLED`, `RAW_INPUT_FALLBACK_QUERY_CONTRACT_MISSING`, `RAW_INPUT_FALLBACK_EMPTY_RESULT`.
   - Provenance persistence: writes `<pair_counts_batch_output_prefix>/_metadata/raw_input_resolution.json` including `source_mode`, `resolution_reason`, and full resolver provenance.
### Pipeline triggered at `Start15mDependentFeaturesPipeline`: `${PipelineName15mDependent}` (dependent)

- **Implemented pipeline function:** `build_15m_dependent_pipeline`.
- **Step/script:** `FGCCorrBuilderStep` → `run_fg_c_builder.py`.
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
3. `LoadDppConfigFromDynamo -> LoadMlpConfigFromDynamo` — load project defaults.
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
3. `LoadDppConfigFromDynamo -> LoadMlpConfigFromDynamo` — load project defaults.
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
   - Processing purpose: unload active machine inventory (Redshift -> S3), validate monthly snapshot manifest, and atomically advance DPP pointer `project_parameters.ip_machine_mapping_s3_prefix` to the validated `snapshot_month=YYYY-MM` root before monthly flow proceeds to FG-B.

### Pipeline triggered at `StartFGBBaselinePipeline`: `${PipelineNameFGB}`
- **Implementation state:** Implemented.
- **Implemented pipeline function:** `build_fg_b_baseline_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- **Purpose:** compute FG-B host/segment/pair baselines over configured windows/horizons.
- **Publication contract:** FG-B pipeline now acts as the canonical monthly publisher for FG-C consumers by writing deterministic overwrite snapshots directly under canonical prefixes (`/host`, `/segment`, `/ip_metadata`, `/pair/host`, `/pair/segment`) partitioned by `feature_spec_version` + `baseline_horizon` + `baseline_month` (without altering `baseline_horizon` semantics used by FG-C and existing consumers).

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
3. `LoadDppConfigFromDynamo -> LoadMlpConfigFromDynamo` — load project defaults.
4. `ResolvePipelineRuntimeParams` — resolve unified training runtime values.
5. `RunTrainingPerMlProject` — strict `ml_project_name` map fan-out for training branches.
6. `StartTrainingPipeline` — **PIPELINE TRIGGER**; starts `${PipelineNameIFTraining}` with `MlProjectName`.
7. `DescribeTrainingPipeline` — poll unified training pipeline status.
8. `TrainingPipelineStatusChoice` — branch on pipeline status.
9. `WaitBeforeTrainingDescribe` — wait between polls.
10. `IncrementTrainingPollAttempt` — increment poll counter.
11. `WorkflowFailed` — terminal workflow failure state.
12. `Success` — terminal success state.

### Pipeline triggered at `StartTrainingPipeline`: `${PipelineNameIFTraining}`
- **Implementation state:** Implemented and authoritative owner for the full training lifecycle.
- **Implemented pipeline function:** `build_if_training_pipeline`.
- **Pipeline code location:** `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`.
- **Purpose:** execute verifier + bounded remediation + re-verification + IF training + publish + attributes + deploy in one ordered pipeline-native chain.
- **Branch context contract:** each step invocation propagates `--ml-project-name` to `run_if_training` so training/evaluation handlers execute against the correct MLP branch contract.
- **Evaluation downstream contract:** post-training evaluation starts inference and prediction-join pipelines with `MlProjectName` so branch outputs stay isolated.

#### Pipeline steps and run chain
1. `TrainingDataVerifierStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage verify`
   - Purpose: validate FG-A/FG-C coverage and persist deterministic verification status artifact.
2. `MissingFeatureCreationStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage remediate`
   - Purpose: bounded remediation (`max_retries` from IF training spec) with real orchestrator execution:
     - starts Step Functions backfill for missing 15m windows,
     - starts FG-B baseline pipeline for missing reference periods.
3. `PostRemediationVerificationStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage reverify`
   - Purpose: re-check coverage after remediation and persist re-verification status.
4. `IFTrainingStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage train`
   - Purpose: train/validate/refit, persist artifacts, write final report and SUCCESS marker.
5. `ModelPublishStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage publish`
   - Purpose: execute deterministic production artifact promotion contract:
     - verifies source model tar hash from final report (`artifact_hash`) before promotion;
     - copies to immutable production key under `output.production_model_root/model_version=<version>/artifact_hash=<sha256>/model.tar.gz`;
     - writes durable production pointer metadata (current + last-known-good) to MLP control-plane DDB registry;
     - emits only production publish contract fields (`production_model_uri/hash/version`, publish timestamp, source run id, LKG pointer fields).
6. `ModelAttributesStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage attributes`
   - Purpose: persist model attributes payload (join/preprocessing/window metadata) with idempotent run key.
7. `ModelDeployStep`
   - Runs: `python -m ndr.scripts.run_if_training --stage deploy`
   - Purpose: consume production publish contract directly and deploy only validated production artifact URIs.
   - Guardrails:
     - rejects non-production URIs (outside configured `output.production_model_root`);
     - rejects mutable URIs that do not contain immutable `model_version` + `artifact_hash` path segments;
     - supports explicit last-known-good pointer deployment target (`deploy_pointer_target=last_known_good`).

### Item 26 runtime behavior (strict closure)
- All IF training stages execute through the same runtime resolver path in `run_if_training`/`if_training_job`, ensuring DDB-first target selection and dependency-readiness preflight checks before expensive train/remediate/evaluation operations.
- Remediation and evaluation artifacts now include per-branch target provenance and selected target details for auditability.
- Runtime `EvaluationWindowsJson` is validated for JSON shape, time ordering, and non-overlap before execution proceeds.
- Pipeline-definition code URI resolution uses concrete deployment contract identifiers (project/spec values), and rejects placeholder values such as `<required:...>` during pipeline build.
- IF training step code resolution enforces `scripts.steps.<Step>.code_metadata` at build time (`artifact_mode`, `artifact_build_id`, `artifact_sha256`); missing metadata fails fast with a packaging-decision-required contract error.

## 5) Triggering source: `sfn_ndr_backfill_reprocessing` (backfill and reprocessing)

- **Trigger source name:** backfill and reprocessing state machine.
- **Trigger source location:** `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`.
- **How it is triggered:** backfill request/workflow input over historical range.

### All steps in triggering source (in declared order)
1. `NormalizeIncomingMessage` — normalize inbound payload.
2. `ParseIncomingProjectContext` — parse project/spec context.
3. `LoadDppConfigFromDynamo -> LoadMlpConfigFromDynamo` — load project defaults.
4. `ResolvePipelineRuntimeParams` — resolve backfill runtime values.
5. `StartHistoricalWindowsExtractorPipeline` — **PIPELINE TRIGGER**; start `${PipelineNameBackfillHistoricalExtractor}`.
6. `DescribeHistoricalWindowsExtractor` — poll extractor pipeline status.
7. `ExtractorPipelineStatusChoice` — branch on extractor status.
8. `WaitBeforeExtractorDescribe` — wait between extractor polls.
9. `IncrementExtractorPollAttempt` — increment extractor poll counter.
10. `ReadExtractorManifest` / `ValidateExtractorManifest` — read and validate extractor `backfill_manifest.v1` from S3.
11. `ResolveBackfillWindows` / `RunBackfillWindows` (Map) — fan out only over `extractor_manifest.map_items` with deterministic family/range item shaping.
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
- **Fallback processing utility:** `src/ndr/processing/backfill_redshift_fallback.py` provides deterministic ingestion-miss Redshift UNLOAD execution with flow-specific DPP query loading, multi-range retry semantics, and local FS staging before downstream reconstruction.

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

## Orchestration/runtime hardening updates (item 23)

The current flow includes explicit runtime validation gates before launching downstream pipelines for inference, monthly baselines, backfill, and unified training.

Operational behavior:
- Missing required runtime inputs now fail before compute starts.
- Backfill windows are validated for presence, ISO-8601 UTC format, and `start_ts < end_ts` ordering.
- Invalid runtime windows are rejected pre-launch (no extractor or map-window fanout starts).
- FG-A mini-batch runs fail fast when source deltas do not include `mini_batch_id` unless compatibility mode is explicitly enabled.

IF training telemetry now explicitly records whether HPO used primary Optuna or fallback local Bayesian search (`hpo_method`, `hpo_fallback_used`, `hpo_fallback_activation_count`) so fallback frequency can be monitored over time.


- FG-B ingestion compatibility: `run_fg_b_builder` supports runtime/job-spec layout mode `fg_a_layout` (`auto` default; `wide`; `long`). Auto mode normalizes current FG-A wide outputs into role-explicit long rows and derives `time_band` before baseline computation, preserving downstream FG-C contracts.

## IF training pipeline flow additions (item 25)
Unified IF training now includes:
1. history-planner computation + artifact,
2. verification/remediation/reverification,
3. train/persist/report,
4. post-training evaluation replay for each configured evaluation window,
5. prediction-feature join/publication manifests,
6. expanded experiments lineage for planner + evaluation windows.
