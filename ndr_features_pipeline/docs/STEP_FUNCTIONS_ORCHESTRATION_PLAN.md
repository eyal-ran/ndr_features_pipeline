# NDR Step Functions Orchestration Plan (v3, JSON + JSONata verified)

## Goal of this revision
This revision addresses two explicit requirements:
1. Verify the Step Functions creation JSON is aligned with the orchestration plan and can be created in AWS (after placeholder substitution).
2. Verify each Step Function definition covers required functionality.

All definitions remain console-importable via:
**Create state machine -> Write your workflow in code -> JSON**.

---

## Source JSON definitions (JSONata)
- `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
- `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
- `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
- `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
- `docs/step_functions_jsonata/sfn_ndr_prediction_publication.json`

All five definitions include:
- `"QueryLanguage": "JSONata"`
- JSONata expressions (`{% ... %}`)
- event-driven continuation (`waitForTaskToken`) for dependent stages

---

## Alignment and no-flow verification checklist

## A) AWS-creation readiness (definition-level)
- JSON syntax is valid for each file.
- `StartAt` targets existing states.
- Each state has legal terminal flow (`Next`, `End`, `Succeed`, or `Fail`).
- `putEvents` definitions now serialize `Detail` as a JSON string (`$string(...)`) to satisfy EventBridge API contract.
- Callback-based waits are used instead of polling loops.

## B) Runtime flow safety
- 15m orchestrator now releases lock on both success and failure paths.
- Failure path is explicit (`WorkflowFailed`) instead of implicit termination.
- Backfill map injects required per-item and global context via `ItemSelector`.

## C) Placeholder policy
Definitions intentionally keep deployment placeholders (pipeline names, table names, ARNs, bus name). Replace placeholders before deployment via IaC templating.

---

## Functional coverage matrix

### 1) `SFN-NDR-15M-FEATURES-INFERENCE`
Definition: `sfn_ndr_15m_features_inference.json`

Covered:
- mini-batch idempotency lock (DynamoDB conditional put),
- start 15m feature generation,
- event-driven completion wait,
- start inference pipeline,
- event-driven completion wait,
- inference completion event emission,
- lock cleanup on success/failure.

### 2) `SFN-NDR-MONTHLY-FG-B-BASELINES`
Definition: `sfn_ndr_monthly_fg_b_baselines.json`

Covered:
- machine inventory refresh,
- FG-B baseline execution,
- event-driven chaining,
- supplemental baseline component build (segment/cold-start/population fallback),
- baseline manifest publication event.

### 3) `SFN-NDR-TRAINING-ORCHESTRATOR`
Definition: `sfn_ndr_training_orchestrator.json`

Covered:
- training execution + event-driven completion,
- publication of approved model artifacts to canonical S3 path,
- explicit model attributes map registration,
- deployment-required branching,
- human approval gate before replacement,
- deploy-or-hold outcomes.

### 4) `SFN-NDR-BACKFILL-REPROCESSING`
Definition: `sfn_ndr_backfill_reprocessing.json`

Covered:
- window range generation,
- bounded parallel processing with Map,
- per-window feature pipeline execution,
- event-driven completion for each window,
- reconciliation summary event.

Supports either range-parameterized builders or ranged-delta-path-oriented window producer logic.

### 5) `SFN-NDR-PREDICTION-PUBLICATION`
Definition: `sfn_ndr_prediction_publication.json`

Covered:
- start prediction join pipeline,
- event-driven completion wait,
- publish joined outputs to targets (Redshift and/or Iceberg) via publisher function,
- publication completion event.

---

## Deployment notes
Before creating state machines in AWS, replace placeholders:
- `${EventBusName}`
- `${PipelineCompletionCallbackLambdaArn}`
- `${LockTableName}`
- `${PipelineName15m}` / `${PipelineNameInference}` / `${PipelineNameFGB}` / etc.
- `${ModelPublishLambdaArn}` / `${ModelAttributesRegistryLambdaArn}` / `${ModelDeployLambdaArn}` / `${HumanApprovalLambdaArn}`
- `${SupplementalBaselineLambdaArn}`
- `${ApprovedModelS3Prefix}`

After substitution, these JSON definitions are intended to create cleanly in Step Functions console and implement the planned flows without missing transitions.


---

## SNS/SQS input support and parameter extraction (placeholder logic)
All five state machines now begin with normalization and parameter-resolution states:
1. `NormalizeIncomingMessage`
   - Accepts either raw string input, SQS envelope (`Records[0].body`), SNS envelope (`Records[0].Sns.Message`), or `message_body`.
2. `ResolvePipelineRuntimeParams`
   - Extracts `project_name` from the message string using placeholder parsing (`project_name=<value>;...`).
   - Resolves the runtime parameters that must be passed by Step Functions to SageMaker pipelines (for example `FeatureSpecVersion`, batch window, `RunId`, `ReferenceTimeIso`, etc.).
   - Uses input-field override first, then placeholder message extraction, then safe defaults.

The remainder of configuration remains pipeline-resolved from DynamoDB as intended.


## Latest verification status
The current JSON definitions were re-verified for two acceptance criteria:
1. **Creation readiness / no flow gaps**
   - All 5 files parse as JSON, use `QueryLanguage=JSONata`, have valid `StartAt` targets, and all `Next`/`Default`/Choice transitions resolve to existing states.
   - Non-terminal states have `Next` or `End`, and terminal states are explicit (`Succeed`/`Fail`).
2. **Functional coverage completeness**
   - Every state machine includes message normalization + parameter resolution and passes required runtime parameters to SageMaker `StartPipelineExecution`.
   - Mission-specific responsibilities are explicitly present (locks, callbacks, supplemental monthly baselines, training publish/attributes/approval, backfill map, publication split).

These checks indicate the definitions are aligned with this plan and are expected to create in AWS Step Functions after placeholder substitution.
