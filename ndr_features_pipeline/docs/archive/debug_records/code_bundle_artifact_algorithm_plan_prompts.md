# Code Bundle Artifact Algorithm Plan — Implementation Task Prompts

These are copy-paste prompts for implementing tasks in:
`docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md`.

Prompt format intentionally mirrors the style used in:
`docs/archive/debug_records/refactoring_fixes_plan_v3_prompts.md`.

Standard prompt sections (for every task):
1) Mission
2) Issues
3) In-scope
4) Prerequisites
5) Required outcomes
6) Producer/consumer alignment requirements
7) System-integration requirements
8) Validation/tests
9) Deliverables
10) Completion criteria
11) Ambiguity protocol

Global mandatory posture to include in every implementation:
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only field and no producer-only field unless explicitly validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude each task by updating all relevant project documentation files to reflect implemented behavior/contract/config changes.

---

## Task 0 — Prompt: Freeze artifact lifecycle contracts and acceptance criteria

```text
Implement Task 0 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Part A and Part B in docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.
- Read src/ndr/pipeline/io_contract.py, src/ndr/contracts.py.
- Read src/ndr/scripts/create_ml_projects_parameters_table.py and current deployment notebook artifacts section.

Mission
- Freeze machine-validated contracts for the artifact lifecycle before implementation changes.

Issues
- Build/validate/smoke outputs are currently unspecified as enforceable schemas, causing contract drift risk.

In-scope
- Define authoritative schemas for:
  - code_bundle_build_output.v1
  - code_artifact_validate_report.v1
  - code_smoke_validate_report.v1
- Define explicit error taxonomy (retriable vs non-retriable where relevant).
- Add validators and contract drift tests.

Prerequisites
- None.

Required outcomes
- Contract files/schemas committed and versioned.
- Validator utilities with deterministic failure messaging.
- CI tests that fail on missing/unknown/invalid fields.

Producer/consumer alignment requirements
- Producer mapping:
  - build script -> build output schema
  - validate script -> validate report schema
  - smoke script -> smoke report schema
- Consumer mapping:
  - downstream pipeline steps and deployment orchestration readers.
- No producer-only/consumer-only fields unless explicit metadata no-op.

System-integration requirements
- Contracts must support all families: streaming, dependent, FG-B baseline, unload, inference, join, training, backfill.

Validation/tests
- Positive/negative schema tests.
- Unknown-field and missing-field tests.
- Contract version compatibility tests.

Deliverables
- Schemas, validator code, tests, and error taxonomy docs.

Completion criteria
- Downstream tasks can implement against one frozen contract source with no ambiguity.

Ambiguity protocol
- Stop and escalate if any field ownership is unclear (producer, consumer, or lifecycle phase).

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 1 — Prompt: Implement deterministic code bundle build script

```text
Implement Task 1 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 1 and related Part A sections (core script fixes + additional file fixes).
- Read src/ndr/scripts/run_code_bundle_build.py.
- Read src/ndr/scripts/create_ml_projects_parameters_table.py and DDB contract structures.
- Read src/ndr/pipeline/io_contract.py and src/ndr/contracts.py.

Mission
- Replace the build stub with deterministic artifact construction, hashing, S3 publishing, and manifest emission.

Issues
- Current build script only logs and does not produce runnable artifacts or contract outputs.

In-scope
- Implement real build lifecycle in run_code_bundle_build.py.
- Use canonical contract discovery (no hardcoded partial pipeline list drift).
- Emit machine-readable build output manifest.
- Upload deterministic artifact and sidecars to per-step target URI.

Prerequisites
- Task 0 complete.

Required outcomes
- Deterministic archive (`source.tar.gz`) creation with reproducible hash.
- Per-step upload target path:
  - <code_prefix_s3>/artifacts/<artifact_build_id>/source.tar.gz
- Sidecar uploads:
  - manifest.json
  - source.tar.gz.sha256
- Build output manifest includes artifact_build_id, artifact_sha256, artifact_format, and step_artifacts entries.

Producer/consumer alignment requirements
- Producer: run_code_bundle_build.py.
- Consumers:
  - run_code_artifact_validate.py,
  - run_code_smoke_validate.py,
  - deployment orchestration readers,
  - step contract resolution and promotion logic.
- Ensure entry_script paths emitted are compatible with runtime execution context.

System-integration requirements
- Must support all active pipeline families, not only a subset.
- Must be idempotent for repeated same artifact_build_id runs.

Validation/tests
- Unit tests for deterministic archive creation and hash stability.
- Mocked S3 upload tests (path correctness and sidecar presence).
- Contract discovery tests across pipeline families.
- Negative tests for placeholder/invalid args.

Deliverables
- Functional build script implementation + tests + docs updates.

Completion criteria
- Running script with valid parameters produces correct S3 artifacts and manifest consumable by downstream tasks.

Ambiguity protocol
- Stop if step discovery from DDB is ambiguous; align with Task 0 contract definitions first.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 2 — Prompt: Implement artifact validation script with strict integrity checks

```text
Implement Task 2 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 2 and related Part A sections.
- Read src/ndr/scripts/run_code_artifact_validate.py.
- Read src/ndr/pipeline/io_contract.py and Task 0 schemas.

Mission
- Replace validation stub with deterministic integrity enforcement for built artifacts.

Issues
- Current validation script does not verify object existence, hash correctness, archive format, or entry script presence.

In-scope
- Parse and validate build manifest.
- Per-step S3 HEAD, download, hash compare, archive check, entry_script check.
- Emit validation report schema output.

Prerequisites
- Tasks 0–1 complete.

Required outcomes
- Validation fails fast on any mismatch/absence.
- Validation report is schema-compliant and machine-readable.
- Error codes align with taxonomy from Task 0.

Producer/consumer alignment requirements
- Producer: run_code_artifact_validate.py report.
- Consumers:
  - run_code_smoke_validate.py,
  - deployment pipeline/gating,
  - observability/release checks.

System-integration requirements
- Deterministic behavior across retries.
- Non-retriable contract failures clearly distinguished from transient I/O failures.

Validation/tests
- Positive tests (valid artifact).
- Negative tests:
  - missing object,
  - hash mismatch,
  - wrong format,
  - missing entry_script.
- Schema compliance tests for validation report.

Deliverables
- Functional validation script + tests + docs updates.

Completion criteria
- Validator reliably blocks invalid artifacts and produces deterministic report outputs.

Ambiguity protocol
- Stop if build output manifest fields do not match frozen schema.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 3 — Prompt: Implement smoke validation script for runnable entrypoints

```text
Implement Task 3 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 3 and related Part A sections.
- Read src/ndr/scripts/run_code_smoke_validate.py.
- Read src/ndr/pipeline/io_contract.py.

Mission
- Prove packaged artifacts are actually runnable in artifact-mode execution shape.

Issues
- Current smoke script is a no-op and cannot detect runtime execution failures.

In-scope
- Download and extract each artifact from build manifest.
- Execute python <entry_script> --help in artifact-like environment.
- Emit smoke report and fail on any step failure.

Prerequisites
- Tasks 0–2 complete.

Required outcomes
- Smoke report contains per-step pass/fail and deterministic failure reasons.
- Execution environment sets import path consistently (e.g., PYTHONPATH with extracted src).

Producer/consumer alignment requirements
- Producer: smoke report.
- Consumers:
  - deployment gating and release checks,
  - integration task (Task 7).

System-integration requirements
- Must cover all step artifacts emitted by build output, not an arbitrary subset.
- Must remain deterministic and reproducible.

Validation/tests
- Positive smoke tests for representative steps.
- Negative smoke tests with intentionally broken entry_script/imports.
- Report schema compliance tests.

Deliverables
- Functional smoke script + tests + docs updates.

Completion criteria
- Smoke stage can reliably prove artifact executability and fail safely when broken.

Ambiguity protocol
- Stop if runtime launch contract differs from io_contract assumptions; reconcile with Task 4 migration.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 4 — Prompt: Migrate unified_with_fgc pipeline definitions to contract-based artifact launch

```text
Implement Task 4 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 4 and Part A file-fix section for unified pipeline definitions.
- Read src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py.
- Read src/ndr/pipeline/io_contract.py (resolve_step_execution_contract and build_processing_step_launch_args).

Mission
- Remove mixed execution semantics and make unified pipelines artifact-mode aware via step contracts.

Issues
- Unified pipelines currently resolve code URI but still hardcode python -m launch args, reducing contract consistency.

In-scope
- Replace resolve_step_code_uri-only usage with resolve_step_execution_contract per step.
- Replace fixed launch args with build_processing_step_launch_args.
- Preserve existing step ordering/dependencies and business parameters.

Prerequisites
- Tasks 0–3 complete.

Required outcomes
- Each unified step supports dual-read/artifact-mode behavior through one consistent launch contract path.
- No regressions in existing runtime parameter wiring.

Producer/consumer alignment requirements
- Producer: pipeline definition step configuration.
- Consumers: SageMaker Processing runtime and all pipeline steps in unified file.

System-integration requirements
- Coherent behavior with inference/prediction-join/if-training/backfill definitions that already use contract-aware launch helpers.

Validation/tests
- Unit tests for each step’s resolved launch args under:
  - artifact_uri present,
  - artifact_uri absent (dual-read fallback).
- Contract readiness validation tests.

Deliverables
- Updated unified pipeline definitions + tests + docs updates.

Completion criteria
- Unified pipeline family executes via same contract-driven code path semantics as other families.

Ambiguity protocol
- Stop if any step’s module_name mapping is unclear; explicitly map and test each one.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 5 — Prompt: Fix deployment code pipeline definitions (bundle/validate/smoke wiring)

```text
Implement Task 5 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 5 and Part A file-fix section for deployment code pipeline definitions.
- Read:
  - src/ndr/pipeline/sagemaker_pipeline_definitions_code_bundle_build.py
  - src/ndr/pipeline/sagemaker_pipeline_definitions_code_artifact_validate.py
  - src/ndr/pipeline/sagemaker_pipeline_definitions_code_smoke_validate.py

Mission
- Make deployment code pipelines runnable end-to-end with explicit artifact handoffs.

Issues
- Placeholder code URIs and missing structured handoff artifacts make orchestration incomplete.

In-scope
- Replace placeholder script URI logic with deployable script resolution.
- Add ProcessingOutput/ProcessingInput wiring for build manifest and reports.
- Enforce dependency ordering build -> validate -> smoke.
- Fail fast when validate/smoke report status != PASS.

Prerequisites
- Tasks 0–4 complete.

Required outcomes
- Pipelines can be executed without manual placeholder patching.
- Manifest/report handoff artifacts are explicit, versioned, and test-verified.

Producer/consumer alignment requirements
- Producer/consumer chain:
  - build step produces build manifest,
  - validate consumes build manifest and produces validation report,
  - smoke consumes both and produces smoke report.

System-integration requirements
- Compatible with deployment lifecycle flow and promotion/readiness gating.

Validation/tests
- Pipeline-definition tests (static contract checks).
- Handoff artifact path and dependency tests.
- Failure-path tests for non-PASS reports.

Deliverables
- Updated deployment code pipeline definitions + tests + docs updates.

Completion criteria
- Deployment code pipelines are operationally coherent and deterministic.

Ambiguity protocol
- Stop if script packaging location is unresolved; define one canonical deployable path strategy.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 6 — Prompt: Align deployment notebook with mandatory explanatory markdown-before-code policy

```text
Implement Task 6 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 6 and notebook alignment requirements in Part A.
- Read both notebook assets:
  - src/ndr/deployment/canonical_end_to_end_deployment_plan.ipynb
  - src/ndr/deployment/canonical_end_to_end_deployment_plan.md

Mission
- Keep notebook code changes minimal while making operational behavior explicit and safe for implementors/operators.

Issues
- Current notebook lacks required explanatory markdown cell before each code cell.

In-scope
- Insert markdown cell immediately before every code cell.
- Each inserted markdown cell must include:
  - behavior,
  - purpose,
  - required input,
  - expected output,
  - expected result,
  - usage instructions,
  - runnable example.
- Keep logic changes minimal; only align text/flow where required for artifact lifecycle clarity.
- Maintain mirror parity between .ipynb and .md.

Prerequisites
- Tasks 0–5 complete.

Required outcomes
- Every code cell has a preceding explanatory markdown cell with required sections.
- Notebook remains runnable and coherent with planned artifact lifecycle.

Producer/consumer alignment requirements
- Producer: notebook instructions.
- Consumer: operators/deployers and reviewers who execute notebook flow.

System-integration requirements
- Notebook text must not conflict with actual contract/pipeline behavior.

Validation/tests
- Notebook structure test:
  - assert every code cell has preceding markdown.
  - assert required headings/fields exist in each added markdown block.
- Mirror parity check between .ipynb and .md.

Deliverables
- Updated ipynb + md mirror + structure tests + docs updates.

Completion criteria
- Notebook is self-explanatory, safe to execute, and aligned with artifact lifecycle plan.

Ambiguity protocol
- Stop if mirror generation process is unclear; define deterministic sync process before editing both files.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Task 7 — Prompt: End-to-end integration gate for full artifact lifecycle

```text
Implement Task 7 from docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md.

Mandatory orientation before coding (required)
- Read Task 7 and Part B matrix.
- Review outputs from Tasks 0–6 and all touched tests.

Mission
- Prove system-level correctness and readiness for release through deterministic integration gates.

Issues
- Isolated fixes are insufficient without end-to-end producer/consumer verification across all families.

In-scope
- Integration tests for build -> validate -> smoke -> promoted contract readiness -> runtime step consumption.
- Coverage across families:
  - 15m streaming,
  - 15m dependent,
  - FG-B baseline,
  - machine inventory unload,
  - inference,
  - prediction join,
  - IF training,
  - backfill.
- Release gate checks and rollback playbook verification.

Prerequisites
- Tasks 0–6 complete.

Required outcomes
- Green targeted pytest matrix.
- Contract drift checks pass.
- Pipeline definition checks pass.
- Notebook structure and parity checks pass.
- Clear rollback behavior documented and tested for failed promotions/smoke failures.

Producer/consumer alignment requirements
- Validate each flow output is correctly consumed by its next stage and no hidden assumptions remain.

System-integration requirements
- End-to-end deterministic behavior under retry/replay scenarios.

Validation/tests
- Full targeted integration suite.
- Negative-path tests for failed validation/smoke and rollback behavior.
- Final acceptance gate script execution.

Deliverables
- Integration tests, gate scripts, release checklist, rollback playbook docs.

Completion criteria
- System is operationally ready with deterministic, auditable artifact-mode behavior for every intended pipeline step family.

Ambiguity protocol
- Stop if any producer/consumer handoff lacks explicit contract or test coverage; backfill before release.

Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- No placeholder runtime field that does not drive behavior.
- No consumer-only or producer-only field unless explicit validated metadata no-op.
- Readiness/remediation decisions must be deterministic and auditable from source artifacts.
- All touched paths must remain idempotent, retry-safe, rollback-aware.
- Everything implemented as code must run only as SageMaker Pipeline Processing Steps (Python/PySpark). No Lambda-based or other execution mechanisms.
- Conclude by updating all relevant project documentation.
```

---

## Copy-paste usage note

Each task prompt is intentionally isolated in its own fenced `text` block so it can be copied directly into an implementation task pane without additional editing.
