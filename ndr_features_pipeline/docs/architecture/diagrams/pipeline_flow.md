# NDR Pipeline Flow Diagram

![Pipeline flow chart](images/pipeline_flow.svg)

```mermaid
flowchart LR
    subgraph Sources[Sources and Trigger Inputs]
        A1[S3 Parsed Batch Folder Event]
        A2[Scheduled Monthly Baseline Trigger]
        A3[Scheduled Training Trigger]
        A4[Backfill Request Event]
    end

    subgraph Orchestration[Step Functions State Machines]
        B1[15m Features + Inference]
        B2[Monthly FG-B Baselines]
        B3[Training Orchestrator]
        B4[Prediction Publication]
        B5[Backfill + Reprocessing]
    end

    subgraph Pipelines[SageMaker Pipelines]
        C1[15m Streaming\nDelta -> FG-A -> Pair-Counts -> FG-C]
        C2[Inference Predictions]
        C3[Machine Inventory Unload]
        C4[FG-B Baseline]
        C5[Supplemental Baseline]
        C6[Prediction Feature Join]
        C7[Prediction Publish]
        C8[Training Data Verifier]
        C9[Missing Feature Creation]
        C10[IF Training]
        C11[Model Publish]
        C12[Model Attributes]
        C13[Model Deploy]
        C14[Backfill Historical Extractor]
    end

    subgraph Data[S3 Data and Artifacts]
        D1[(Parsed Input Batches)]
        D2[(Delta)]
        D3[(FG-A)]
        D4[(Pair-Counts)]
        D5[(FG-B Baselines)]
        D6[(FG-C)]
        D7[(Predictions)]
        D8[(Joined Prediction Outputs)]
        D9[(Published Prediction Outputs)]
        D10[(Training Artifacts)]
        D11[(Machine Inventory Snapshot)]
        D12[(Backfill Time Windows)]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
    A4 --> B5

    D1 --> C1
    B1 --> C1
    C1 --> D2
    C1 --> D3
    C1 --> D4
    C1 --> D6

    B1 --> C2
    D6 --> C2
    C2 --> D7

    B1 --> B4
    B4 --> C6
    D7 --> C6
    C6 --> D8
    B4 --> C7
    D8 --> C7
    C7 --> D9

    B2 --> C3
    C3 --> D11
    B2 --> C4
    D11 --> C4
    D3 --> C4
    D4 --> C4
    C4 --> D5
    B2 --> C5
    D5 --> C5

    B3 --> C8
    C8 --> C10
    C8 -. verifier failure, max 2 retries .-> C9
    C9 --> C8
    C10 --> D10
    B3 --> C11
    B3 --> C12
    B3 --> C13

    B5 --> C14
    C14 --> D12
    D12 --> B5
    B5 --> C1
```

## Notes

- Reflects the current five-state-machine orchestration inventory: 15-minute, monthly baselines, training, prediction publication, and backfill/reprocessing.
- Includes replacement pipeline-native stages now represented in orchestration definitions: training verifier/remediation and model publish/attributes/deploy; monthly baselines now complete after canonical FG-B publication without a supplemental placeholder pipeline.
- Shows that prediction publication runs as a separate state machine started synchronously by the 15-minute workflow.
