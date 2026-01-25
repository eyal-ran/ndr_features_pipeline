# NDR Feature Engineering Pipeline Flowchart

```mermaid
flowchart TD
    A[S3 Integration Bucket\nParsed Palo Alto .json.gz] -->|ObjectCreated| B[Generic Step Function]
    B --> C[Generic SageMaker Pipeline\nEntry Step]
    C -->|Read project config| D[(DynamoDB\nML Project Parameters)]

    C --> E[Delta Builder\nSpark ProcessingStep]
    E -->|Read| A
    E -->|Write Parquet| F[(S3 Features Bucket\nDelta Tables)]

    F --> G[FG-A Builder\nSpark ProcessingStep]
    G -->|Write Parquet| H[(S3 Features Bucket\nFG-A Dataset)]
    G -->|Optional| I[(SageMaker Feature Store\nFG-A Feature Group)]

    A --> J[Pair-Counts Builder\nSpark ProcessingStep]
    J -->|Write Parquet| K[(S3 Features Bucket\nPair-Counts Dataset)]

    H --> L[FG-B Baseline Builder\nSpark ProcessingStep]
    K --> L
    L -->|Write Parquet| M[(S3 Features Bucket\nFG-B Baselines)]

    H --> N[FG-C Correlation Builder\nSpark ProcessingStep]
    M --> N
    N -->|Write Parquet| O[(S3 Features Bucket\nFG-C Dataset)]
    N -->|Optional| P[(SageMaker Feature Store\nFG-C Feature Group)]

    B --> Q[Return Control to Step Function]
```

## Notes
- The **Step Function** is intended to be generic and reusable across ML projects.
- The **SageMaker Pipeline** resolves project/job configuration from DynamoDB (`ML_PROJECTS_PARAMETERS_TABLE_NAME`).
- Each processing step reads/writes through S3, allowing retries and batch reprocessing.
