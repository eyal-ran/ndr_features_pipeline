# Architecture Diagrams Index

- `pipeline_flow.md`: canonical processing/data flow across 15-minute, monthly baseline, training, publication, and backfill workflows.
- `system_architecture.md`: current-state platform architecture including EventBridge, Step Functions, SageMaker pipelines, DynamoDB control tables, and S3 data products.
- `orchestration_event_lifecycle.md`: sequence view of callback-free orchestration, polling loops, idempotency locks, training remediation, and backfill fan-out.
- `stakeholder_value_flow.md`: high-level business value flow for non-technical audiences.

Each diagram markdown now includes a rendered image chart under `docs/architecture/diagrams/images/` for environments that do not render Mermaid.

## Legend

- Solid arrows: implemented or canonical flow.
- Dotted arrows: conditional/exception flow (for example remediation or supporting controls).
