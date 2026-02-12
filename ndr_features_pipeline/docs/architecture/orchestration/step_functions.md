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
