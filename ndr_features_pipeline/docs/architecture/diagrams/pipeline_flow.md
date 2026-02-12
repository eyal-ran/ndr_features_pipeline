# NDR Pipeline Flow Diagram

```mermaid
flowchart TD
    A[S3 Integration / Upstream Signals] --> B[Step Functions Orchestration]
    B --> C[SageMaker Pipeline Execution]

    C --> D[Delta Builder]
    D --> E[(S3 Delta Dataset)]

    E --> F[FG-A Builder]
    F --> G[(S3 FG-A Dataset)]

    F --> H[Pair-Counts Builder]
    H --> I[(S3 Pair-Counts Dataset)]

    G --> J[FG-B Baseline Builder]
    I --> J
    J --> K[(S3 FG-B Baselines)]

    G --> L[FG-C Correlation Builder]
    K --> L
    L --> M[(S3 FG-C Dataset)]

    B --> N[Inference Pipeline]
    M --> N
    N --> O[(S3 Prediction Outputs)]

    O --> P[Prediction Feature Join]
    P --> Q[(Published/Joined Outputs)]

    B --> R[IF Training Pipeline]
    G --> R
    M --> R
    R --> S[(Model Artifacts / Registration Flow)]

    B --> T[Monthly Machine Inventory Unload]
    T --> U[(S3 Machine Inventory Snapshot)]
    U --> J
```

## Notes

- This diagram is conceptual and reflects the repository's current processing components and orchestration roles.
- Runtime parameters are passed by Step Functions/SageMaker pipeline invocations; structural config is loaded by jobs from JobSpec/config sources.
