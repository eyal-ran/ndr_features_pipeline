# Stakeholder Value Flow (Business-Friendly)

A simplified, current-state view of how operational signals become analyst-ready outcomes and model improvements.

![Stakeholder value flow chart](images/stakeholder_value_flow.svg)

```mermaid
flowchart LR
    A[Network + Asset Signals] --> B[Automated 15m Feature Engineering]
    B --> C[Inference Scoring]
    C --> D[Prediction Publication and Delivery]
    D --> E[SOC Triage and Investigation]
    E --> F[Training Feedback + Baseline Refresh]
    F --> B

    A1[Backfill + Replay Workflow] -. restores history .-> B
    A2[Monthly Inventory + FG-B Baselines] -. stabilizes context .-> B
    A3[Training Verifier + Controlled Remediation] -. improves model quality .-> F
    A4[Idempotency Locks + Native Polling] -. increases reliability .-> D
```

## Intended audience

- Security program managers
- Data/ML product owners
- Platform stakeholders who need outcomes without low-level service details
