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
- On verifier failure, remediation stage triggers concrete orchestrations (backfill Step Functions + FG-B pipeline as needed) and retries verifier with bounded attempts from IF training spec.
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
- **Training lifecycle ownership:** Step Functions now starts one unified IF training pipeline and delegates verifier/remediation/training/publish/attributes/deploy to pipeline-native steps.
- **Direct 15m -> publication handoff:** implemented via synchronous nested Step Function start to remove EventBridge handoff race/duplication risk.
- **Idempotency controls:** publication lock + deterministic identity suppress duplicate publication attempts and support replay safety.
- **Migration/rollback approach:** deploy updated definitions behind versioned state machines; rollback by repointing aliases to prior definitions if pipeline-native replacements are not yet available.

## Deployment notes

Definitions contain deploy-time placeholders (for names/ARNs/resources). Replace placeholders with environment-specific values through IaC templating or deployment automation before creating/updating state machines.

### Required runtime/IAM alignment
- Add `sagemaker:DescribePipelineExecution` to all state-machine roles that call SageMaker pipelines.
- Ensure lock tables exist and state-machine roles include `dynamodb:PutItem`, `dynamodb:UpdateItem`, and `dynamodb:DeleteItem` as applicable.
- Ensure pipeline name placeholders are wired for current orchestrator contracts:
  - `PipelineNamePredictionJoin`
  - `PipelineNameIFTraining`
  - `PredictionPublicationStateMachineArn`

## Runtime fail-fast policy (item 23)

The four core orchestrators now include a `ValidateResolvedRuntimeParams` guard immediately after runtime resolution and before any pipeline start/lock mutation:
- `sfn_ndr_15m_features_inference.json`
- `sfn_ndr_monthly_fg_b_baselines.json`
- `sfn_ndr_backfill_reprocessing.json`
- `sfn_ndr_training_orchestrator.json`

Required values must be non-empty. Validation failures terminate with deterministic `RuntimeParameterValidationError` fail states and single-line operator-readable causes (for example: `Missing required runtime parameter: project_name`).

Backfill no longer uses static date literals. `start_ts`/`end_ts` must come from invocation payload, parsed message, or explicit Dynamo defaults, and are validated for:
- presence,
- ISO-8601 UTC format (`YYYY-MM-DDThh:mm:ssZ`),
- strict ordering (`start_ts < end_ts`).

FG-A now defaults to strict mini-batch enforcement: if runtime specifies `mini_batch_id` and the source delta data is missing the `mini_batch_id` column, processing fails fast. Compatibility mode is opt-in via `allow_missing_mini_batch_id_column=true`.

## Training orchestrator runtime expansion (item 25)
The training orchestrator now resolves and passes additional IF training runtime parameters:
- `EvaluationWindowsJson`
- `EnableHistoryPlanner`
- `EnableAutoRemediate15m`
- `EnableAutoRemediateFgb`
- `EnablePostTrainingEvaluation`
- `EnableEvalJoinPublication`
- `EnableEvalExperimentsLogging`

Validation remains fail-fast before pipeline start.
