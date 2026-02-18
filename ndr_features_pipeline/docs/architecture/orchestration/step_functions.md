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
- Starts feature pipeline execution.
- Waits for pipeline completion callback.
- Triggers inference pipeline and completion handling.

### 2) Monthly FG-B baseline orchestration
- Runs machine inventory refresh.
- Runs FG-B baseline pipeline.
- Publishes baseline completion/signaling events.

### 3) Training orchestrator
- Launches IF training pipeline.
- Handles completion and artifact publication/register workflows.
- Supports approval/deployment gating patterns.

### 4) Backfill and reprocessing
- Generates and iterates over window ranges.
- Runs feature pipelines per window with bounded parallelism.
- Emits reconciliation outcomes/events.

### 5) Prediction publication
- Starts prediction feature join pipeline.
- Waits for completion.
- Invokes downstream publication logic and completion signaling.

## Common orchestration pattern

Across all definitions:
- Input normalization and runtime parameter extraction are performed early in the flow.
- Pipelines are started via SageMaker APIs with runtime parameters.
- Callback/event-driven waits are used for dependent-stage completion.

## Deployment notes

Definitions contain deploy-time placeholders (for names/ARNs/resources). Replace placeholders with environment-specific values through IaC templating or deployment automation before creating/updating state machines.

## Related docs

- Architecture overview: `docs/architecture/overview.md`
- Flow diagram: `docs/architecture/diagrams/pipeline_flow.md`
- Pipeline implementations: `src/ndr/pipeline/`


## S3 ingestion event routing contract (Palo Alto raw->ingestion)

For 15-minute inference triggering from ingestion-bucket writes, the canonical contract is:

1. S3 ObjectCreated event (ingestion bucket) -> SNS -> SQS.
2. Step Functions normalizes message wrappers and resolves `s3_key`.
3. Step Functions extracts `org1`, `org2`, date path fragments, and batch folder from the key.
4. Step Functions loads routing metadata from DynamoDB (`project_routing` style item keyed by `org1#org2`) to resolve:
   - `project_name`
   - base `ingestion_prefix`
5. Step Functions loads `project_parameters#<feature_spec_version>` and resolves runtime defaults.
6. The state machine accepts only batch marker events (for example `.../<hashed_batch_folder>/_SUCCESS`) unless `force_process=true` is provided for controlled replay.
7. A single execution is launched per `(project_name, batch_folder)`; duplicate file events are suppressed via lock/dedupe semantics.

This pattern supports both regular inference cadence and deterministic back-processing by replaying explicit batch-folder prefixes from ingestion storage.


## Palo Alto implementation plan alignment (approved constraints)

- Use the existing `sfn_ndr_15m_features_inference.json` and `sfn_ndr_backfill_reprocessing.json` state machines; do not introduce a new state machine for this scope.
- Producer payload assumptions are minimal: full `s3_key` + payload timestamp only.
- Derive `project_name` and `mini_batch_id` from the path.
- Resolve `feature_spec_version` and policy defaults from DynamoDB (`project_parameters#<feature_spec_version>`).
- Apply cron-floor policy `8-59/15 * * * ? *` for `batch_start_ts_iso` (minutes `08/23/38/53`) and preserve the source timestamp as `batch_end_ts_iso`.
- Keep existing lock-table conditional-write behavior to suppress duplicates on retries/redelivery.
- For non-inference historical runs, add a preliminary SageMaker ProcessingStep that enumerates folders and emits `(project_name, feature_spec_version, mini_batch_id, batch_start_ts_iso, batch_end_ts_iso)` entries consumed by backfill map execution.
