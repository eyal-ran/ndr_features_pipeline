# NDR System Architecture (Complete View)

This diagram provides a current-state view across orchestration, processing, storage, configuration, and downstream consumption.

![System architecture chart](images/system_architecture.svg)

```mermaid
flowchart TB
    subgraph Ingestion[Ingestion and Triggering]
        I1[Upstream Producers]
        I2[EventBridge Rules\nschedules + batch completion events]
        I3[S3 Parsed Input Buckets]
    end

    subgraph Orchestration[Step Functions Orchestration]
        O1[15m Features + Inference]
        O2[Monthly FG-B Baselines]
        O3[Training Orchestrator]
        O4[Prediction Publication]
        O5[Backfill + Reprocessing]
        O6[(DynamoDB lock/config tables)]
    end

    subgraph Pipelines[SageMaker Pipelines]
        P1[15m Streaming\nDelta -> FG-A -> Pair-Counts -> FG-C]
        P2[Inference Predictions]
        P3[Machine Inventory Unload]
        P4[FG-B Baseline]
        P5[Supplemental Baseline]
        P6[Prediction Feature Join]
        P7[Prediction Publish]
        P8[Training Data Verifier]
        P9[Missing Feature Creation]
        P10[IF Training]
        P11[Model Publish]
        P12[Model Attributes]
        P13[Model Deploy]
        P14[Backfill Historical Extractor]
    end

    subgraph Storage[S3 Data Plane]
        D1[(Parsed Inputs)]
        D2[(Delta)]
        D3[(FG-A)]
        D4[(Pair-Counts)]
        D5[(FG-B Baselines)]
        D6[(FG-C)]
        D7[(Predictions)]
        D8[(Joined Prediction Outputs)]
        D9[(Published Outputs)]
        D10[(Training Artifacts)]
        D11[(Machine Inventory Snapshot)]
        D12[(Backfill Windows)]
    end

    subgraph Consumers[Downstream Consumers]
        C1[Security Analytics / SOC]
        C2[Model Monitoring + BI]
        C3[Ops / Reconciliation Event Consumers]
    end

    I1 --> I2
    I2 --> O1
    I2 --> O2
    I2 --> O3
    I2 --> O5
    I2 --> C3
    I3 --> D1

    O1 <--> O6
    O2 <--> O6
    O3 <--> O6
    O4 <--> O6

    O1 --> P1
    O1 --> P2
    O1 --> O4
    O2 --> P3
    O2 --> P4
    O2 --> P5
    O3 --> P8
    O3 --> P10
    O3 --> P11
    O3 --> P12
    O3 --> P13
    O4 --> P6
    O4 --> P7
    O5 --> P14
    O5 --> O1

    D1 --> P1
    P1 --> D2
    P1 --> D3
    P1 --> D4
    P1 --> D6

    P3 --> D11
    D11 --> P4
    D3 --> P4
    D4 --> P4
    P4 --> D5
    D5 --> P5

    D6 --> P2
    P2 --> D7
    D7 --> P6
    P6 --> D8
    D8 --> P7
    P7 --> D9

    P8 -. failure remediation path .-> P9
    P9 --> P8
    P10 --> D10

    P14 --> D12
    D12 --> O5

    D9 --> C1
    D10 --> C2
```

## Scope notes

- Focuses on implemented repository architecture: EventBridge + Step Functions + SageMaker pipelines + DynamoDB lock/config + S3 data products.
- Reflects pipeline-native replacements in orchestration (supplemental baseline, training verifier/remediation, and model lifecycle stages).
- Omits speculative optional services to keep this view aligned with active definitions.
