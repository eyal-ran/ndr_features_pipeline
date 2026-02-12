# NDR Pipeline Flow Diagram

```mermaid
flowchart LR
    subgraph Sources[Data Sources]
        A1[S3 Integration / Upstream Signals]
        A2[Monthly Machine Inventory Source]
    end

    subgraph Orchestration[Orchestration and Control]
        B1[Step Functions 15m Orchestrator]
        B2[Step Functions Monthly Baseline Orchestrator]
        B3[Step Functions Training Orchestrator]
        B4[Step Functions Prediction Publication]
    end

    subgraph FeaturePipelines[SageMaker Pipelines - Feature Layer]
        C1[15m Streaming Pipeline\nDelta → FG-A → Pair-Counts → FG-C]
        C2[FG-B Baseline Pipeline]
        C3[Machine Inventory Unload Pipeline]
    end

    subgraph ModelPipelines[SageMaker Pipelines - Model Layer]
        D1[Inference Predictions Pipeline]
        D2[IF Training Pipeline]
        D3[Prediction Feature Join Pipeline]
    end

    subgraph Storage[S3 Datasets and Artifacts]
        E1[(Delta Dataset)]
        E2[(FG-A Dataset)]
        E3[(Pair-Counts Dataset)]
        E4[(FG-B Baselines)]
        E5[(FG-C Dataset)]
        E6[(Inference Outputs)]
        E7[(Published / Joined Outputs)]
        E8[(Model Artifacts + Reports)]
        E9[(Machine Inventory Snapshot)]
    end

    A1 --> B1
    A2 --> B2

    B1 --> C1
    C1 --> E1
    C1 --> E2
    C1 --> E3
    C1 --> E5

    B2 --> C3
    C3 --> E9
    E9 --> C2

    B2 --> C2
    E2 --> C2
    E3 --> C2
    C2 --> E4

    B1 --> D1
    E5 --> D1
    D1 --> E6

    B4 --> D3
    E6 --> D3
    D3 --> E7

    B3 --> D2
    E2 --> D2
    E5 --> D2
    D2 --> E8

    E4 -. reference baselines .-> C1
```

## Notes

- Updated to align with current pipeline modules: the 15-minute streaming pipeline includes **FG-C** (`build_15m_streaming_pipeline`), while **FG-B baselines** remain in a separate monthly pipeline.
- Inference, training, and prediction-publication are represented as distinct SageMaker pipelines started by dedicated Step Functions state machines.
- The diagram emphasizes that FG-B outputs are used as reference data for FG-C computations in subsequent 15-minute runs.
