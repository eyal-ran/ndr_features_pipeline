# NDR System Architecture (Complete View)

This diagram provides a complete architecture view across orchestration, processing, storage, model lifecycle, and eventing integrations.

```mermaid
flowchart TB
    subgraph Ingestion[Ingestion and Triggers]
        I1[Upstream Producers]
        I2[EventBridge Rules\n(schedules + domain events)]
        I3[SNS Topics\n(optional fan-out)]
        I4[SQS Queues\n(optional buffering)]
    end

    subgraph Orchestration[Workflow Orchestration]
        O1[Step Functions\n15m Features + Inference]
        O2[Step Functions\nMonthly FG-B Baselines]
        O3[Step Functions\nTraining Orchestrator]
        O4[Step Functions\nPrediction Publication]
        O5[Lambda Callbacks / Utility Lambdas]
        O6[(DynamoDB Lock + Config Tables)]
    end

    subgraph Processing[SageMaker Pipelines and Jobs]
        P1[15m Streaming Pipeline\nDelta → FG-A → Pair-Counts → FG-C]
        P2[FG-B Baseline Pipeline]
        P3[Machine Inventory Unload Pipeline]
        P4[Inference Predictions Pipeline]
        P5[Prediction Feature Join Pipeline]
        P6[IF Training Pipeline]
        P7[SageMaker Processing\n(PySpark jobs)]
    end

    subgraph DataPlane[S3 and Feature Data]
        D1[(S3 Raw/Parsed Inputs)]
        D2[(S3 Delta)]
        D3[(S3 FG-A)]
        D4[(S3 Pair-Counts)]
        D5[(S3 FG-B Baselines)]
        D6[(S3 FG-C)]
        D7[(S3 Predictions)]
        D8[(S3 Joined/Published Outputs)]
        D9[(S3 Training Artifacts + Reports)]
        D10[(S3 Machine Inventory Snapshot)]
    end

    subgraph MLOps[SageMaker MLOps Integrations]
        M1[SageMaker Feature Store\n(optional mirror/publish)]
        M2[SageMaker Experiments\n(run tracking)]
        M3[SageMaker Model Registry\n(approval / versions)]
        M4[SageMaker Endpoint\n(optional online inference target)]
    end

    subgraph Consumers[Downstream Consumers]
        C1[Security Analytics / SOC]
        C2[Model Monitoring + BI]
        C3[External Integration Services]
    end

    I1 --> I2
    I2 --> I3
    I3 --> I4
    I2 --> O1
    I2 --> O2
    I2 --> O3
    I2 --> O4
    I4 --> O1

    O1 --> O5
    O2 --> O5
    O3 --> O5
    O4 --> O5
    O1 <--> O6
    O2 <--> O6
    O3 <--> O6

    O1 --> P1
    O1 --> P4
    O2 --> P3
    O2 --> P2
    O3 --> P6
    O4 --> P5

    P1 --> P7
    P2 --> P7
    P3 --> P7
    P4 --> P7
    P5 --> P7
    P6 --> P7

    D1 --> P1
    P1 --> D2
    P1 --> D3
    P1 --> D4
    P1 --> D6

    D10 --> P2
    D3 --> P2
    D4 --> P2
    P2 --> D5
    P3 --> D10

    D6 --> P4
    P4 --> D7
    D7 --> P5
    P5 --> D8

    D3 --> P6
    D6 --> P6
    P6 --> D9

    D3 -. optional publish .-> M1
    D6 -. optional publish .-> M1
    P6 --> M2
    P6 --> M3
    M3 -. approved deployment .-> M4

    D8 --> C1
    D9 --> C2
    I2 --> C3
```

## Scope notes

- **Implemented core path in repository:** Step Functions + SageMaker pipelines/jobs + S3 datasets + DynamoDB-backed config/locking + EventBridge signaling.
- **Optional/adjacent integrations shown for target-state completeness:** SNS/SQS fan-out buffering, SageMaker Feature Store publication, Model Registry + Endpoint deployment patterns.
