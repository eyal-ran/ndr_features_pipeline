# AGENTS.md

## Project overview
- This repository implements the NDR feature engineering stack for AWS, centered on the Delta Builder Spark job and later FG-A/FG-B/FG-C feature group builders. The pipeline runs every 15 minutes for inference and on-demand for training/refitting, producing outputs compatible with SageMaker Feature Store while using intermediate S3 datasets for delta tables and feature group outputs.【F:docs/PROJECT_SPEC.md†L4-L4】
- The pipeline is invoked from Step Functions, runs a SageMaker ProcessingStep using a PySpark processor, and executes `python -m ndr.scripts.run_delta_builder ...` inside the container to read inputs, apply data quality rules, build 15-minute deltas, and write partitioned Parquet outputs to S3.【F:docs/README.md†L1-L41】

## Repository layout (high-level)
- `src/ndr`: Pipeline, model, and processing code (delta builder + FG-A/FG-B/FG-C builders).
- `tests/`: Unit tests for builder jobs and core utilities.
- `docs/`: Architecture and integration notes (pipeline flow, feature group specs).
  - Consult `docs/README.md` for the overall pipeline flow and data lifecycle.【F:docs/README.md†L1-L41】

## Local dev & environment notes
- Ensure the project root (`ndr_features_pipeline`) is the working directory and set `PYTHONPATH=src` so the `ndr` package resolves in tests. The test run log notes failures when `pyspark` or the `ndr` module are missing on `PYTHONPATH`.【F:docs/test_run_log.md†L1-L7】
- Install runtime dependencies used by tests (at minimum `pyspark` and `boto3`), since `tests/scripts/run_tests_and_report.py` imports `boto3` and the test suite references Spark utilities.【F:tests/scripts/run_tests_and_report.py†L1-L15】
- Use `tests/scripts/run_tests_and_report.py` only when S3 access is configured; it uploads the test report to an S3 bucket and exits non-zero on failures.【F:tests/scripts/run_tests_and_report.py†L12-L44】

## Testing & verification
- Preferred: run the full unit test suite with `pytest -q` (requires `pyspark` and `PYTHONPATH=src`).【F:docs/test_run_log.md†L1-L7】
- Targeted testing is acceptable when changing a specific builder. For example, run the Pair Counts builder tests with:
  - `python -m pytest tests/test_pair_counts_builder_job.py`【F:docs/README_PAIR_COUNTS_ADDITIONS.md†L86-L90】
- CI-style reporting: `tests/scripts/run_tests_and_report.py` discovers `tests/test_*.py` and uploads a report to S3; set `NDR_TEST_RESULTS_BUCKET` if you need a custom destination bucket.【F:tests/scripts/run_tests_and_report.py†L12-L44】

## Design quality checklist (use before finalizing changes)
- Validate pipeline expectations: inputs are JSON Lines GZIP in the integration S3 prefix, output is partitioned Parquet, and delta aggregation uses the existing 15-minute role-based logic described in the pipeline flow docs.【F:docs/README.md†L16-L41】
- Preserve FG-A/B/C contract expectations: the FG-A builder reads 15-minute deltas and emits windowed features, while FG-B/FG-C build baselines/correlations on top of those datasets; avoid schema or partition changes without updating downstream documentation/tests.【F:docs/README_FG_A.md†L1-L34】
- Keep inference cadence in mind: the pipeline is expected to run every 15 minutes for inference, so avoid designs that add heavyweight per-run overhead or long-lived state.【F:docs/PROJECT_SPEC.md†L4-L4】
- Update or extend unit tests in `tests/` when modifying builder logic or data schemas.

## Engineering best practices
- Follow the existing coding patterns and document any new behavior in `docs/` when it changes pipeline contracts or feature schemas.
- Prefer efficient, generic, reusable, and stateless code where possible; use OOP when it improves clarity or reuse.
- Keep inline comments focused on non-obvious logic, and ensure the codebase documentation stays current alongside implementation changes.
- Validate changes against the documented pipeline flow and data lifecycle to prevent breaking downstream consumers.【F:docs/README.md†L1-L41】

## Agent workflow reminders
- Check for nested `AGENTS.md` or `SKILL.md` files and follow their scope-specific instructions.
- Prefer `rg` for searching; avoid recursive `ls -R`/`grep -R` in large directories.
- For any front-end or visual change, capture a screenshot using the browser tooling; otherwise skip screenshots.
- Avoid wrapping imports in `try/except` blocks unless the existing codebase explicitly requires it.

## Mandatory execution protocol (all tasks)
- Before closing any coding or planning task, run a self-review of the proposed plan and/or generated code against production-quality criteria: best practices, scalable architecture, clean design, strong orchestration, observability/monitorability, reuse, OOP where applicable, and error-resistant behavior.
- If any part does not meet those standards, iterate and improve it before finalizing.
- In the final response, explicitly report the self-review outcome, including conclusions and any improvements made as a result of that review.
- As part of the same mandatory review, verify coherence and coordination of the current plan/code with other active plans, all relevant codebase components, and all documented project conventions; resolve and document any inconsistencies before finalizing.
- While executing any task, proactively identify missing context, ambiguous requirements, or instructions that could be made more specific. Ask all needed clarifying questions before final completion when additional information would improve the outcome, then incorporate the answers into the final solution.

## Additional excellence standards (all tasks)
- Start with a concise execution plan for non-trivial tasks, including assumptions, risks, dependencies, and explicit validation steps.
- Favor minimal, high-impact changes that preserve backward compatibility; when breaking changes are unavoidable, document migration and rollback considerations.
- Treat testing as part of delivery: run the most relevant automated checks for touched areas, report outcomes clearly, and explain any gaps or environment limitations.
- Include operational readiness in solutions: ensure logging, metrics, and failure signals are sufficient for troubleshooting and monitoring in production.
- Apply security and reliability hygiene by default (input validation, least-privilege assumptions, safe defaults, and clear error handling paths).
- Keep code and documentation synchronized: update tests/docs/changelogs (as applicable) whenever behavior, contracts, schemas, or interfaces change.
- Record trade-offs and rationale for major decisions so future contributors can understand why a solution was chosen.
