# Step Functions Orchestration (Canonical)

This document captures the current Step Functions orchestration model and points to the JSON definitions tracked in the repository.

## Definition sources

JSONata definitions are stored under `docs/step_functions_jsonata/`:
- `sfn_ndr_15m_features_inference.json`
- `sfn_ndr_monthly_fg_b_baselines.json`
- `sfn_ndr_training_orchestrator.json`
- `sfn_ndr_backfill_reprocessing.json`
- `sfn_ndr_prediction_publication.json`

## State machine responsibilities

### 1) 15-minute features + inference
- Controls idempotency/locking for mini-batch processing.
- Parses producer completion payload contract (batch folder path + event timestamp).
- Starts feature and inference SageMaker pipelines.
- Uses native `describePipelineExecution` polling loops (no callback lambda).
- Triggers prediction publication state machine directly (`states:startExecution.sync:2`) and releases lock.

### 2) Monthly FG-B baseline orchestration
- Runs machine inventory refresh pipeline.
- Runs FG-B baseline pipeline.
- Emits `NdrMonthlyBaselinesCompleted` immediately after FG-B succeeds; no supplemental placeholder stage remains.
- Uses native SageMaker start + describe polling for all async stages.

### 3) Training orchestrator
- Runs a training-data verifier pre-stage before training.
- On verifier failure, runs bounded remediation (`missing feature creation`) and retries verifier with max 2 attempts.
- Runs training, model publish, model attributes registration, and deployment as pipeline-native stages.
- Uses native polling and fails closed with explicit terminal diagnostics after retry exhaustion.

### 4) Backfill and reprocessing
- Starts a preliminary historical-window extractor SageMaker pipeline.
- Polls extractor completion, resolves emitted windows, and runs feature pipelines per window with bounded parallelism.
- Uses polling for each map item pipeline execution and emits reconciliation events.

### 5) Prediction publication
- Enforces publication idempotency via deterministic identity + DynamoDB conditional lock.
- Runs a single prediction join+publish pipeline with native polling.
- Marks lock outcome (`SUCCEEDED`/`FAILED`) and suppresses duplicates on lock conflicts.

## Common orchestration pattern

Across all definitions:
- Input normalization and runtime parameter extraction are performed early in the flow.
- Pipelines are started via SageMaker APIs with runtime parameters.
- Asynchronous completion is controlled by polling `sagemaker:describePipelineExecution` with bounded waits and retry/backoff on transient API failures.

## S3 ingestion event routing contract (Palo Alto raw->ingestion)

For 15-minute inference triggering from ingestion-bucket writes, the canonical contract is:

1. Producer emits an event containing the full S3 batch-folder path (`s3_key` or `batch_s3_path`) and event timestamp.
2. Step Functions normalizes wrapper message formats and resolves path/timestamp.
3. Step Functions extracts `org1`, `org2`, date path fragments, and hashed batch folder from the path.
4. Step Functions loads routing metadata from DynamoDB (`project_routing` style item keyed by `org1#org2`) to resolve:
   - `project_name`
   - base `ingestion_prefix`
5. Step Functions loads `project_parameters#<feature_spec_version>` and resolves runtime defaults (including window policy defaults).
6. Runtime windows are derived from source timestamps using floor-minute policy `08/23/38/53` and lock keys are built from `(project_name, feature_spec_version, batch_start_ts_iso, batch_end_ts_iso)` to suppress duplicates.

## Feasibility/risk review summary (item 19 gate)

- **Polling replacement feasibility:** fully feasible with existing Step Functions AWS SDK integrations; no extra callback lambdas required.
- **Training verifier loop safety:** bounded to max 2 retries to avoid runaway retraining/remediation loops.
- **Direct 15m -> publication handoff:** implemented via synchronous nested Step Function start to remove EventBridge handoff race/duplication risk.
- **Idempotency controls:** publication lock + deterministic identity suppress duplicate publication attempts and support replay safety.
- **Migration/rollback approach:** deploy updated definitions behind versioned state machines; rollback by repointing aliases to prior definitions if pipeline-native replacements are not yet available.

## Deployment notes

Definitions contain deploy-time placeholders (for names/ARNs/resources). Replace placeholders with environment-specific values through IaC templating or deployment automation before creating/updating state machines.

### Required runtime/IAM alignment
- Add `sagemaker:DescribePipelineExecution` to all state-machine roles that call SageMaker pipelines.
- Ensure lock tables exist and state-machine roles include `dynamodb:PutItem`, `dynamodb:UpdateItem`, and `dynamodb:DeleteItem` as applicable.
- Ensure pipeline name placeholders are wired for the replacement pipeline-native stages:
  - `PipelineNamePredictionJoin`
  - `PipelineNameTrainingDataVerifier`
  - `PipelineNameMissingFeatureCreation`
  - `PipelineNameModelPublish`
  - `PipelineNameModelAttributes`
  - `PipelineNameModelDeploy`
  - `PredictionPublicationStateMachineArn`
