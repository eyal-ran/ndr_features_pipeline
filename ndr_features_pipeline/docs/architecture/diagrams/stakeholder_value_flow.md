# Stakeholder Value Flow (Business-Friendly)

A simplified view of how data is transformed into operational outcomes.

```mermaid
flowchart LR
    A[Network + Asset Signals] --> B[Automated Feature Engineering]
    B --> C[Risk & Anomaly Scoring]
    C --> D[Published Security Insights]
    D --> E[Analyst Triage & Investigation]
    E --> F[Feedback for Model Retraining]
    F --> B

    B1[15m Refresh Cadence] -. supports .-> B
    B2[Monthly Baseline Refresh] -. stabilizes .-> B
    B3[Governed Model Lifecycle] -. improves .-> C
```

## Intended audience

- Security program managers
- Data/ML product owners
- Platform stakeholders who need outcomes without low-level service details
