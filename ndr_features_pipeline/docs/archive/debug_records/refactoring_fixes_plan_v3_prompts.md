# Refactoring Fixes Plan v3 — Implementation Task Prompts

These are copy-paste prompts for implementing tasks in:
`docs/archive/debug_records/refactoring_fixes_plan_v3.md`.

Prompt structure mirrors the v2 prompts format and is intentionally explicit so implementors can execute reliably without prior context.

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

## Task 0 — Prompt: Freeze v3 contracts and error taxonomy

```text
Implement Task 0 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Task 0 + SC1 + Tasks 8–9 in v3 plan.
- Read the full current Step Functions contracts in docs/step_functions_jsonata/.
- Read contract resolver and related code in src/ndr/pipeline/io_contract.py.
- Read DDB seeding logic in src/ndr/scripts/create_ml_projects_parameters_table.py.

Mission
- Freeze all v3 contracts and error taxonomy as authoritative machine-validated sources before feature changes.

Issues
- Repeated regressions came from contract drift, ambiguous ownership, and incomplete producer/consumer updates.

In-scope
- Contract schemas for readiness artifacts and backfill request evolution.
- Contract schema for step code artifact metadata.
- Error taxonomy (retriable vs non-retriable).
- Validator and CI drift checks.

Prerequisites
- None.

Required outcomes
- Authoritative schemas for:
  - monthly_fg_b_readiness.v3
  - rt_artifact_readiness.v3
  - backfill request with requested_families semantics
  - deployment/code artifact contract fields
- Deterministic error code catalog and classification.
- CI checks that fail undeclared field/version drift.

Producer/consumer alignment requirements
- Every schema field must have explicit producer and consumer owner.
- Validate that no field is producer-only or consumer-only unless explicit metadata no-op.

System-integration requirements
- Contracts must be reusable by monthly, RT, backfill, bootstrap, training, and deployment flows.

Validation/tests
- Positive/negative schema tests.
- Unknown-field and missing-field tests.
- Contract-version mismatch tests.
- CI drift test proving undeclared changes are blocked.

Deliverables
- Schemas, validators, error taxonomy, and tests.

Completion criteria
- Downstream tasks can implement deterministically against one frozen contract source.

Ambiguity protocol
- Stop and escalate if field ownership (DDB/runtime/derived artifact) is unclear.

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

## Task 1 — Prompt: Bootstrap JSONata semantics hardening (B1)

```text
Implement Task 1 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read SC1 and Task 1 details in v3 plan.
- Read docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json.
- Compare with other JSONata SFNs for consistency.

Mission
- Make bootstrap expression semantics explicit and deterministic.

Issues
- Bootstrap uses JSONata expressions but previously lacked explicit QueryLanguage declaration.

In-scope
- Bootstrap SFN definition.
- Contract tests enforcing JSONata declaration behavior.

Prerequisites
- Task 0 complete.

Required outcomes
- Bootstrap SFN explicitly declares QueryLanguage JSONata.
- No runtime payload contract changes.

Producer/consumer alignment requirements
- Producer: bootstrap SFN definition.
- Consumer: Step Functions runtime and nested flow callers.

System-integration requirements
- Bootstrap orchestration behavior remains backward-compatible.

Validation/tests
- SFN contract tests for QueryLanguage presence.
- Regression tests confirming unchanged input/output contract.

Deliverables
- Updated bootstrap SFN + tests + docs.

Completion criteria
- Bootstrap semantics are explicit, deterministic, and test-enforced.

Ambiguity protocol
- Stop if deployment rendering strips or mutates QueryLanguage.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 2 — Prompt: Monthly deterministic readiness artifact gating (M1, Option 1)

```text
Implement Task 2 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Finding M1 and Task 2 in v3 plan.
- Inspect monthly SFN and current readiness/remediation branches.

Mission
- Replace input-driven monthly readiness with deterministic computed artifact gating.

Issues
- Monthly readiness decision currently vulnerable to payload injection/staleness.

In-scope
- Monthly readiness checker (SageMaker Processing Step, Python/PySpark).
- Artifact schema monthly_fg_b_readiness.v3.
- Monthly SFN compute->evaluate->remediate->recompute->evaluate sequence.

Prerequisites
- Tasks 0–1 complete.

Required outcomes
- Implement Option 1 only (artifact produced and consumed deterministically).
- Remove authoritative use of states.input monthly readiness payload.
- Fail hard on unresolved dependencies after remediation cycle.

Producer/consumer alignment requirements
- Producer: readiness checker.
- Consumer: monthly SFN gate and remediation request builder.

System-integration requirements
- Deterministic idempotency keying by project/version/reference_month/cycle.

Validation/tests
- Ready path.
- Missing->remediate->ready path.
- Missing unresolved after recheck fail path.
- Invalid/missing artifact contract fail-fast path.

Deliverables
- Checker pipeline step, SFN updates, tests, docs.

Completion criteria
- Monthly gate decisions are source-derived, auditable, deterministic.

Ambiguity protocol
- Stop if required-family scope cannot be derived from frozen contracts.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 3 — Prompt: RT deterministic readiness artifact gating (R2)

```text
Implement Task 3 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Finding R2 and Task 3 in v3 plan.
- Inspect RT SFN readiness and backfill invocation branches.

Mission
- Make RT readiness deterministic and artifact-driven (not payload-driven).

Issues
- RT readiness decision currently trusts input manifest.

In-scope
- RT readiness checker (SageMaker Processing Step, Python/PySpark).
- Artifact schema rt_artifact_readiness.v3.
- RT SFN gate/remediate/recompute behavior.

Prerequisites
- Tasks 0–2 complete.

Required outcomes
- Computed readiness artifact drives gate outcome.
- Backfill invoked only for computed missing ranges.
- Fail on unresolved missing after remediation cycle.

Producer/consumer alignment requirements
- Producer: RT readiness checker.
- Consumer: RT SFN gate + remediation request builder.

System-integration requirements
- Same readiness semantics as monthly.
- Idempotency across retries/replays.

Validation/tests
- Ready path.
- Missing->remediate->ready.
- Missing unresolved fail.
- Stale payload override attempt rejected.

Deliverables
- Checker step + SFN updates + tests + docs.

Completion criteria
- RT gate behavior is deterministic and contract-safe.

Ambiguity protocol
- Stop if missing-range contract shape differs from frozen schema.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 4 — Prompt: RT raw ingestion fallback integration (R1)

```text
Implement Task 4 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Finding R1 and Task 4 in v3 plan.
- Inspect raw input resolver and redshift fallback modules.
- Inspect RT SFN pre-delta path and delta runtime config.

Mission
- Enforce deterministic ingestion/fallback input resolution before delta processing.

Issues
- RT raw-reader path currently lacks robust fallback contract execution.

In-scope
- Pre-delta raw resolver integration (SageMaker Processing Step, Python/PySpark).
- Batch Index provenance/status writes.
- Fail-fast error handling.

Prerequisites
- Tasks 0–3 complete.

Required outcomes
- Ingestion present -> use ingestion path.
- Ingestion missing + fallback enabled -> use fallback path.
- Disabled/malformed/empty fallback -> explicit hard-fail error codes.

Producer/consumer alignment requirements
- Producer: resolver output contract.
- Consumer: delta step input and Batch Index writers.

System-integration requirements
- DDB-owned policy/query configuration only.
- No hidden fallback behavior.

Validation/tests
- Ingestion available path.
- Fallback path.
- Disabled fallback failure.
- Missing query contract failure.
- Empty fallback result failure.

Deliverables
- Resolver integration + SFN/pipeline changes + tests + docs.

Completion criteria
- RT raw-read path is deterministic and contract-enforced.

Ambiguity protocol
- Stop if fallback query ownership/config is ambiguous.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 5 — Prompt: Backfill requested_families end-to-end honoring (BF1)

```text
Implement Task 5 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Finding BF1 and Task 5 in v3 plan.
- Inspect backfill contracts/extractor/dispatcher/verifier.

Mission
- Ensure backfill executes exactly requested families/ranges and reports completion truthfully.

Issues
- Requested family scope can be dropped and broadened during planning/execution.

In-scope
- Backfill request schema evolution.
- SFN wiring of requested_families.
- Extractor filtering.
- Dispatcher/verifier strict completion logic.

Prerequisites
- Tasks 0–4 complete.

Required outcomes
- requested_families preserved end-to-end.
- Unsupported family rejected.
- Completion blocked on unresolved requested family.

Producer/consumer alignment requirements
- Producers: RT/monthly/bootstrap/training request builders.
- Consumers: extractor/dispatcher/verifier.

System-integration requirements
- Dependency ordering retained.
- Idempotent replay behavior.

Validation/tests
- Requested subset honored.
- Unsupported family fail path.
- Mixed-family partial missing request path.
- Completion event gating tests.

Deliverables
- Schema + wiring + implementation + tests + docs.

Completion criteria
- Backfill is consumer-scoped and auditable.

Ambiguity protocol
- Stop if family dependency matrix is inconsistent.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 6 — Prompt: Cross-flow producer/consumer alignment hardening

```text
Implement Task 6 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read all updated contracts from Tasks 0–5.
- Inspect SFNs and runtime validators for touched interfaces.

Mission
- Remove residual producer/consumer drift and inert fields across flows.

Issues
- Historically partial fixes left producer-only/consumer-only fields.

In-scope
- Contract mapping tables.
- Validator updates.
- Payload cleanup and strict field controls.

Prerequisites
- Tasks 0–5 complete.

Required outcomes
- Every touched field mapped producer<->consumer.
- Unknown/invalid fields fail fast.
- Version mismatches fail fast.

Producer/consumer alignment requirements
- Explicit ownership and compatibility matrix in tests.

System-integration requirements
- Full-system compatibility across monthly/RT/backfill/bootstrap/training/deployment.

Validation/tests
- Cross-flow contract conformance tests.
- Replay/idempotency checks.
- Negative tests for unknown/missing fields.

Deliverables
- Validators + mapping tests + docs.

Completion criteria
- No contract drift remains in touched surfaces.

Ambiguity protocol
- Stop if any field has ambiguous ownership.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 7 — Prompt: Final release gate and acceptance enforcement

```text
Implement Task 7 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read full v3 plan and all prior task deliverables.
- Review release gate tests and startup validation scripts.

Mission
- Enforce strict final readiness gates that block any partial/unsafe rollout.

Issues
- Prior rounds allowed partial implementations to pass.

In-scope
- Release gate definitions.
- End-to-end acceptance checks.
- Final checklist and failure criteria.

Prerequisites
- Tasks 0–6, 8, 9 complete.

Required outcomes
- Gate fails on any correctness-critical defect.
- No warning-only pass for contract-critical checks.

Producer/consumer alignment requirements
- Acceptance suite verifies all producer-consumer touched contracts.

System-integration requirements
- Validate full flow set and deployment prerequisites.

Validation/tests
- Full targeted pytest matrix.
- Contract drift checks.
- Startup/deployment-precondition gate checks.

Deliverables
- Updated release gates + passing evidence + docs.

Completion criteria
- v3 changes are release-ready with deterministic behavior and verified rollback safety.

Ambiguity protocol
- Stop if any critical gate remains non-blocking.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 8 — Prompt: Implement per-step immutable code artifact contract + hybrid bootstrap seed (SC1 Part A)

```text
Implement Task 8 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Finding SC1 and Task 8 in v3 plan.
- Inspect code resolver, pipeline definitions, and DDB script spec seeding.
- Inspect deployment notebook flow in src/ndr/deployment/canonical_end_to_end_deployment_plan.*

Mission
- Implement immutable per-step code artifact contract and hybrid rollout Phase A/Phase B controls.

Issues
- Script-path-centric contract and runtime package assumptions are fragile.
- Need controlled bootstrap seed before full deployment SF authority.

In-scope
- DDB step artifact fields and validation.
- Resolver updates.
- Pipeline step consumption updates.
- Hybrid bootstrap seed specification and break-glass policy.

Prerequisites
- Tasks 0–6 complete.

Required outcomes
- Contract fields implemented and validated:
  - code_artifact_s3_uri, entry_script, artifact_build_id, artifact_sha256, artifact_format
- Hybrid model implemented:
  - Phase A manual seed with strict evidence
  - Phase B automated steady state via Task 9 flow
- Runtime preflight blocks non-READY deployment status.

Producer/consumer alignment requirements
- Producers: deployment build/publish logic and metadata writers.
- Consumers: resolver, pipeline builders, runtime preflight checks.

System-integration requirements
- Backward-compatible migration with temporary dual-read mode and clear cutover.

Validation/tests
- Contract schema tests.
- Resolver hash/contract failure tests.
- Pipeline consumption tests.
- Hybrid tests:
  - Phase A seed validity
  - Phase B supersedes seed without drift
  - break-glass marker + reconciliation.

Deliverables
- Contract schema/resolver/pipeline updates + tests + docs + migration notes.

Completion criteria
- Immutable artifact contract active; hybrid rollout controls implemented and verified.

Ambiguity protocol
- Stop if artifact metadata ownership between DDB and deployment flow is unclear.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Task 9 — Prompt: Implement dedicated deployment orchestrator flow (SC1 Part B)

```text
Implement Task 9 from docs/archive/debug_records/refactoring_fixes_plan_v3.md.

Mandatory orientation before coding (required)
- Read Finding SC1 and Task 9 in v3 plan.
- Review bootstrap SFN and runtime precondition behavior.
- Review all pipeline definition modules and deployment assets.

Mission
- Implement dedicated deployment flow for code artifact lifecycle with hybrid transition controls.

Issues
- No first-class deployment orchestrator for build/validate/promote/smoke/rollback.

In-scope
- New deployment SFN orchestrator.
- Three deployment SageMaker pipelines:
  - pipeline_code_bundle_build
  - pipeline_code_artifact_validate
  - pipeline_code_smoke_validate
- DDB promotion/rollback and status/checkpoint contracts.
- Shadow->authoritative transition controls.

Prerequisites
- Tasks 0–8 complete (including Phase A seed).

Required outcomes
- Deployment SFN states implemented exactly per plan.
- Shadow mode executes without mutation.
- Authoritative mode controls promotion/rollback.
- Manual break-glass marker enforces reconciliation before next promotion.

Producer/consumer alignment requirements
- Producer: deployment SF writes deployment pointers/status.
- Consumers: bootstrap and runtime flows read deployment status/pointers.

System-integration requirements
- Deployment flow isolated from runtime business flow execution.
- Runtime flows only consume READY promoted pointers.

Validation/tests
- SFN transition tests (including rollback).
- Build/validate/smoke pipeline tests.
- Atomic promotion contention tests.
- Hybrid transition tests (shadow/authoritative/break-glass reconciliation).
- End-to-end request->READY integration test.

Deliverables
- Deployment SFN + deployment pipelines + DDB contract changes + tests + docs.

Completion criteria
- Automated steady-state deployment authority established with verified rollback and auditability.

Ambiguity protocol
- Stop if promotion authority boundaries between deployment SF and runtime flows are unclear.

Mandatory general instructions (required in this task)
- [apply full mandatory list from Task 0 verbatim]
```

---

## Notes for operators using these prompts
- Apply prompts in execution order defined in v3 plan.
- Do not skip tests; each task must pass both local scope and system-integration checks.
- If a task fails acceptance criteria, fix implementation and rerun tests before moving to next task.
