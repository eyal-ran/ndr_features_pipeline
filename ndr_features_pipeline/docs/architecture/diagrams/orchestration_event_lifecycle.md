# Orchestration and Event Lifecycle

This diagram focuses on control flow and pipeline-driven completion patterns across the five state machines.

```mermaid
sequenceDiagram
    participant EB as EventBridge / Scheduler
    participant SF15 as SFN: 15m Features+Inference
    participant DDB as DynamoDB Lock Tables
    participant SMF as SageMaker Feature Pipeline
    participant SMI as SageMaker Inference Pipeline
    participant SFP as SFN: Prediction Publication
    participant SMPJ as SageMaker Prediction Join Pipeline
    participant SMPP as SageMaker Prediction Publish Pipeline
    participant SFT as SFN: Training Orchestrator
    participant SMV as SageMaker Training Verifier Pipeline
    participant SMT as SageMaker IF Training Pipeline
    participant BUS as EventBridge Bus

    EB->>SF15: Trigger mini-batch event (batch folder path + timestamp)
    SF15->>DDB: PutItem lock (idempotency)
    SF15->>SMF: startPipelineExecution (15m features)
    loop poll completion
      SF15->>SMF: describePipelineExecution
    end
    SF15->>SMI: startPipelineExecution (inference)
    loop poll completion
      SF15->>SMI: describePipelineExecution
    end
    SF15->>SFP: startExecution.sync (direct handoff)
    SFP->>DDB: PutItem publication lock (conditional)
    SFP->>SMPJ: startPipelineExecution (prediction join)
    loop poll completion
      SFP->>SMPJ: describePipelineExecution
    end
    SFP->>SMPP: startPipelineExecution (prediction publish)
    loop poll completion
      SFP->>SMPP: describePipelineExecution
    end
    SFP->>DDB: UpdateItem publication lock state
    SFP->>BUS: PutEvents (publication complete)
    SF15->>DDB: DeleteItem lock

    EB->>SFT: Trigger training workflow
    SFT->>SMV: startPipelineExecution (training verifier)
    loop poll completion
      SFT->>SMV: describePipelineExecution
    end
    SFT->>SMT: startPipelineExecution (IF training)
    loop poll completion
      SFT->>SMT: describePipelineExecution
    end
```

## Why this view helps

- Clarifies where idempotency and duplicate suppression are enforced.
- Shows callback-lambda removal and polling-based completion control.
- Makes direct 15m-to-publication handoff explicit.
