# NDR Feature Engineering Documentation

This `docs/` tree is organized to separate canonical architecture and operational docs from generated artifacts and historical records.

## Documentation map

- `architecture/`
  - `overview.md`: canonical summary of the end-to-end NDR feature engineering system.
  - `orchestration/step_functions.md`: Step Functions orchestration responsibilities, state machine inventory, and deployment notes.
  - `diagrams/pipeline_flow.md`: detailed runtime pipeline flow (15m, baselines, inference, training, publication).
  - `diagrams/system_architecture.md`: complete AWS architecture diagram (orchestration, storage, eventing, and MLOps integrations).
  - `diagrams/orchestration_event_lifecycle.md`: sequence diagram of callbacks, locks, and event publication.
  - `diagrams/stakeholder_value_flow.md`: business-friendly value stream from signals to security outcomes.
- `feature_builders/current_state.md`: production-grade current-state documentation for Delta, FG-A, Pair-Counts, FG-B, FG-C, inference prediction, IF training, machine inventory unload, and prediction feature join jobs.
- `palo_alto_raw_partitioning_strategy.md`: ML-orchestration guidance for consuming Palo Alto 15-minute ingestion batches and managing mini-batch runtime identity.
- `feature_catalog/` + `FEATURE_CATALOG.md`: generated feature catalog artifacts.
- `step_functions_jsonata/`: JSONata state machine definitions used for deployment.
- `legacy_specs/word/`: historical Word specifications kept for traceability.
- `archive/`: historical migration notes, debug records, and one-off validation logs.

## Repository map

- `src/ndr/`
  - `processing/`: Spark job implementations (delta/features/baselines/prediction/training).
  - `scripts/`: processing entrypoints used by SageMaker Processing steps.
  - `pipeline/`: SageMaker pipeline builders.
  - `config/`, `io/`, `catalog/`, `model/`, `logging/`: shared libraries and schema/config helpers.
- `tests/`: unit tests for processing, loaders/specs, and pipeline-adjacent behavior.
- `docs/`: architecture, reference docs, generated catalogs, and archives.

## Source-of-truth guidance

- For **current runtime behavior**, use code in `src/ndr/` and the canonical docs under `docs/architecture/` and `docs/feature_builders/current_state.md`.
- For **feature definitions**, use `docs/feature_catalog/` and `docs/FEATURE_CATALOG.md`.
- For historical context and migration history, use `docs/archive/`.
