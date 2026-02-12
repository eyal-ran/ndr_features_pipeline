# Orchestration and Event Lifecycle

This diagram focuses on control flow, callbacks, and event publication patterns across the five state machines.

```mermaid
sequenceDiagram
    participant EB as EventBridge / Scheduler
    participant SF15 as SFN: 15m Features+Inference
    participant DDB as DynamoDB Lock Table
    participant SMF as SageMaker Feature Pipeline
    participant SMI as SageMaker Inference Pipeline
    participant SFP as SFN: Prediction Publication
    participant SMP as SageMaker Prediction Join
    participant SFT as SFN: Training Orchestrator
    participant SMT as SageMaker IF Training
    participant L as Callback Lambda
    participant BUS as EventBridge Bus

    EB->>SF15: Trigger mini-batch event
    SF15->>DDB: PutItem lock (idempotency)
    SF15->>SMF: startPipelineExecution (15m features)
    SF15->>L: waitForTaskToken callback request
    L-->>SF15: Feature pipeline completion callback
    SF15->>SMI: startPipelineExecution (inference)
    SF15->>L: waitForTaskToken callback request
    L-->>SF15: Inference pipeline completion callback
    SF15->>BUS: PutEvents (inference complete)
    SF15->>DDB: DeleteItem lock

    BUS->>SFP: Trigger prediction-publication workflow
    SFP->>SMP: startPipelineExecution (prediction join)
    SFP->>L: waitForTaskToken callback request
    L-->>SFP: Join pipeline completion callback
    SFP->>BUS: PutEvents (publication complete)

    EB->>SFT: Trigger training workflow
    SFT->>SMT: startPipelineExecution (IF training)
    SFT->>L: waitForTaskToken callback request
    L-->>SFT: Training completion callback
    SFT->>BUS: PutEvents (training state update)
```

## Why this view helps

- Clarifies where idempotency is enforced.
- Shows asynchronous callback boundaries.
- Makes event-driven handoffs explicit for technical and non-technical stakeholders.
