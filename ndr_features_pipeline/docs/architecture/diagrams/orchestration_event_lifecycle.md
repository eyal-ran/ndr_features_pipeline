# Orchestration and Event Lifecycle

This diagram focuses on current callback-free control flow and pipeline-driven completion patterns across all Step Functions definitions.

![Orchestration lifecycle chart](images/orchestration_event_lifecycle.svg)

```mermaid
sequenceDiagram
    participant EB as EventBridge (schedule/domain events)
    participant SF15 as SFN: 15m Features+Inference
    participant DDB as DynamoDB Lock Tables
    participant SMF as SM Pipeline: 15m Streaming
    participant SMI as SM Pipeline: Inference Predictions
    participant SFP as SFN: Prediction Publication
    participant SMPJ as SM Pipeline: Prediction Join
    participant SMPP as SM Pipeline: Prediction Publish
    participant SFM as SFN: Monthly FG-B Baselines
    participant SMMI as SM Pipeline: Machine Inventory Unload
    participant SMB as SM Pipeline: FG-B Baseline
    participant SMS as SM Pipeline: Supplemental Baseline
    participant SFT as SFN: Training Orchestrator
    participant SMV as SM Pipeline: Training Verifier
    participant SMM as SM Pipeline: Missing Feature Creation
    participant SMT as SM Pipeline: IF Training
    participant SMP as SM Pipeline: Model Publish
    participant SMA as SM Pipeline: Model Attributes
    participant SMD as SM Pipeline: Model Deploy
    participant SFB as SFN: Backfill + Reprocessing
    participant SMH as SM Pipeline: Historical Extractor

    EB->>SF15: Trigger with batch folder path + timestamp
    SF15->>DDB: PutItem processing lock (idempotency)
    SF15->>SMF: startPipelineExecution
    loop native polling
      SF15->>SMF: describePipelineExecution
    end
    SF15->>SMI: startPipelineExecution
    loop native polling
      SF15->>SMI: describePipelineExecution
    end
    SF15->>SFP: startExecution.sync (direct handoff)
    SFP->>DDB: PutItem publication lock (conditional)
    SFP->>SMPJ: startPipelineExecution
    loop native polling
      SFP->>SMPJ: describePipelineExecution
    end
    SFP->>SMPP: startPipelineExecution
    loop native polling
      SFP->>SMPP: describePipelineExecution
    end
    SFP->>DDB: UpdateItem publication outcome
    SF15->>DDB: DeleteItem processing lock

    EB->>SFM: Trigger monthly baseline workflow
    SFM->>SMMI: startPipelineExecution
    loop native polling
      SFM->>SMMI: describePipelineExecution
    end
    SFM->>SMB: startPipelineExecution
    loop native polling
      SFM->>SMB: describePipelineExecution
    end
    SFM->>SMS: startPipelineExecution
    loop native polling
      SFM->>SMS: describePipelineExecution
    end

    EB->>SFT: Trigger training workflow
    SFT->>SMV: startPipelineExecution
    loop native polling
      SFT->>SMV: describePipelineExecution
    end
    alt verifier failed and retries remaining
      SFT->>SMM: startPipelineExecution
      loop native polling
        SFT->>SMM: describePipelineExecution
      end
      SFT->>SMV: retry verifier (max 2 attempts)
    end
    SFT->>SMT: startPipelineExecution
    loop native polling
      SFT->>SMT: describePipelineExecution
    end
    SFT->>SMP: startPipelineExecution
    SFT->>SMA: startPipelineExecution
    SFT->>SMD: startPipelineExecution

    EB->>SFB: Trigger backfill request
    SFB->>SMH: startPipelineExecution
    loop native polling
      SFB->>SMH: describePipelineExecution
    end
    SFB->>SF15: Run feature flow per extracted window (bounded map)
```

## Why this view helps

- Clarifies that all orchestrators rely on direct SageMaker API polling, not callback lambdas.
- Makes lock-based idempotency boundaries explicit for 15-minute processing and prediction publication.
- Shows the bounded remediation loop in training and extractor-driven fan-out in backfill.
