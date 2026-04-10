# Refactoring Fixes Plan v2 — Implementation Task Prompts

These are copy-paste prompts for implementing the tasks in:
`docs/archive/debug_records/refactoring_fixes_plan_v2.md`.

Prompt format is standardized across all tasks:
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

Additional mandatory execution posture for all prompts:
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

---

## Task 0 — Contract matrix freeze and implementation bootstrap prompt

```text
Implement Task 0 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read: docs/archive/debug_records/refactoring_fixes_plan_v2.md (Part A and Task 0 in Part B).
- Review all producer->consumer interfaces in monthly, RT, backfill, training, and control-plane paths.
- Build a current-state matrix from SFN payload -> pipeline params -> CLI args -> runtime fields -> DDB keys.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Freeze canonical v2 contracts and error taxonomy so all downstream fixes implement against one authoritative interface.

Issues
- Cross-flow contract drift, inconsistent error semantics, and ownership ambiguity.

In-scope
- flow_contract_matrix_v2 artifact.
- error_taxonomy_v2 artifact.
- Contract validators/tests and pre-merge drift checks.

Prerequisites
- None.

Required outcomes
- One machine-readable contract matrix with owner/source/requiredness/validator ownership per field.
- One deterministic error taxonomy with retriable vs non-retriable classification.
- CI checks that fail on undeclared interface drift.

Producer/consumer alignment requirements
- Every field must have explicit producer and consumer ownership.
- No producer-only fields without mapped consumer behavior (or explicit validated metadata no-op).

System-integration requirements
- Matrix and taxonomy must be reusable by monthly, RT, backfill, training, and deployment validation.

Validation/tests
- Positive/negative contract-schema tests.
- Unknown field / missing required field tests.
- Drift-detection test proving CI blocks undeclared change.

Deliverables
- Contract matrix + error taxonomy + validators + tests + execution notes.

Completion criteria
- Downstream tasks can rely on one frozen contract source with deterministic failure semantics.

Ambiguity protocol
- Stop and request direction if any field ownership (DDB vs runtime vs derived) is unclear.
```

---

## Task 1 — Deterministic readiness gates for monthly + RT prompt

```text
Implement Task 1 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding 1.1 and Finding 2.1 plus Task 1 details.
- Inspect monthly and RT gate states and current manifest/default-ready behavior.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Replace placeholder/default-ready gate behavior with deterministic computed readiness.

Issues
- False-ready progression from payload defaults; stale manifest risk.

In-scope
- monthly_fg_b_readiness.v2 checker (SageMaker Processing Step, Python/PySpark).
- rt_artifact_readiness.v2 checker (SageMaker Processing Step, Python/PySpark).
- Monthly/RT SFN gate+remediate+recheck orchestration states.

Prerequisites
- Task 0 complete.

Required outcomes
- Canonical readiness output contract: ready flag, missing ranges, decision code, as-of timestamp.
- Strict orchestration sequence: compute -> remediate (if needed) -> recompute -> continue/fail.
- No fallback/default-ready branch when manifest is absent.

Producer/consumer alignment requirements
- Checker output schema must match downstream remediation consumer inputs exactly.
- Remediation invoker must consume missing ranges without ad-hoc transformation.

System-integration requirements
- Identical readiness semantics between monthly and RT.
- Deterministic behavior under retries/replays.

Validation/tests
- Ready path.
- Missing path with successful remediation.
- Missing path unresolved after remediation (hard fail).
- Stale manifest supplied externally should not override recomputed readiness.

Deliverables
- Updated checkers + SFN definitions + tests + run evidence.

Completion criteria
- Gate decisions are deterministic from source-of-truth data and no placeholder gates remain.

Ambiguity protocol
- Stop if required-family scope for readiness cannot be derived from frozen contracts.
```

---

## Task 2 — Backfill provider hardening (full families + strict completion) prompt

```text
Implement Task 2 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding 4.1 and 4.2 plus Task 2 details.
- Inspect current backfill dispatcher, family handling, and completion event logic.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Make backfill a real multi-family provider with strict completion truthfulness.

Issues
- No-op handling for non-FG-B families and false completion signaling.

In-scope
- BackfillFamilyDispatcher and BackfillCompletionVerifier.
- Family handler payload/response contracts.
- Completion event gating semantics.

Prerequisites
- Tasks 0–1 complete.

Required outcomes
- Concrete dispatch for delta, fg_a, pair_counts, fg_c, fg_b_baseline.
- Normalized missing-entry input schema and deterministic per-family payload builder.
- Completion event only when all requested families are terminal success.

Producer/consumer alignment requirements
- Planner/extractor missing-entry schema must map 1:1 to family handler inputs.
- Family handler outputs must map 1:1 to completion verifier inputs.

System-integration requirements
- Dependency-safe execution ordering and deterministic aggregation.
- Idempotent replay by correlation/request id.

Validation/tests
- Unsupported family rejection.
- Mixed-family partial-missing request.
- Dependency-order correctness.
- Retry success and duplicate replay idempotency.
- Completion event blocked on any family failure.

Deliverables
- Dispatcher/verifier implementation + contract docs + tests + trace samples.

Completion criteria
- Backfill completion semantics are strict, auditable, and consumer-trustworthy.

Ambiguity protocol
- Stop if any family dependency contract is missing or contradictory.
```

---

## Task 3 — Pair-counts Redshift fallback parity prompt

```text
Implement Task 3 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding 2.2 and Task 3 details.
- Inspect pair-counts runtime input resolution and existing RawInputResolver behavior.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Add deterministic ingestion/fallback parity for pair-counts with provenance.

Issues
- Pair-counts lacks shared fallback behavior and auditable source provenance.

In-scope
- Pair-counts integration with RawInputResolver.
- DDB-owned fallback policy/query validation.
- Provenance contract persistence.

Prerequisites
- Tasks 0–2 complete.

Required outcomes
- Deterministic source resolution (ingestion vs redshift_unload_fallback).
- Explicit fail-fast codes for disabled fallback, missing query contract, empty result.
- Output metadata includes source_mode and resolver decision rationale.

Producer/consumer alignment requirements
- Resolver output schema must be directly consumable by pair-counts reader/writer path.
- Provenance fields must be preserved for downstream audit consumers.

System-integration requirements
- Same fallback semantics as other flows that use RawInputResolver.

Validation/tests
- Ingestion-available path.
- Ingestion-missing fallback-enabled path.
- Fallback-disabled failure path.
- Empty fallback result failure path.

Deliverables
- Resolver integration + runtime contract updates + tests + sample metadata outputs.

Completion criteria
- Pair-counts no longer has asymmetric fallback behavior.

Ambiguity protocol
- Stop if DDB fallback policy ownership/shape is unclear.
```

---

## Task 4 — Monthly inventory + FG-B contract realignment and month partitioning prompt

```text
Implement Task 4 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Findings 1.2 and 1.3 plus Task 4 details.
- Confirm current FG-B partitioning and mapping pointer behavior.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Add monthly partitioning without breaking baseline_horizon consumers, and enforce canonical inventory pointer strategy (Option 1).

Issues
- Missing month partition for FG-B and inventory->FG-B contract misalignment.

In-scope
- Inventory monthly snapshot producer.
- FG-B writer partition contract.
- Pointer contract: project_parameters.ip_machine_mapping_s3_prefix.

Prerequisites
- Tasks 0–3 complete.

Required outcomes
- FG-B partitioning includes feature_spec_version + baseline_horizon + baseline_month.
- baseline_horizon semantics remain intact for FG-C and other consumers.
- ip_machine_mapping_s3_prefix points atomically to latest validated monthly snapshot.

Producer/consumer alignment requirements
- Inventory producer output schema and FG-B mapping loader schema must be canonical and versioned.
- FG-B must read only canonical pointer (no fallback to inferred/legacy paths).

System-integration requirements
- Monthly orchestration order must be: inventory success -> pointer update -> FG-B execution.
- Deterministic replay + pointer rollback behavior.

Validation/tests
- Partition path correctness including baseline_month.
- baseline_horizon compatibility regression tests for consumers.
- Pointer update and pointer-read conformance tests.
- Missing/invalid pointer fail-fast tests.

Deliverables
- Updated producer/consumer contracts + writer logic + tests + migration/backfill utility notes.

Completion criteria
- Month-level replayability and consumer compatibility are both guaranteed.

Ambiguity protocol
- Stop if consumer expectations for baseline_horizon filtering are not fully documented.
```

---

## Task 5 — Bootstrap-to-monthly orchestration correction prompt

```text
Implement Task 5 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding 2.3 and Task 5 details.
- Inspect bootstrap SFN responsibility boundaries and monthly SFN contract.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Keep bootstrap as readiness orchestrator and route monthly business execution through monthly SFN.

Issues
- Bootstrap bypasses monthly SFN semantics via direct monthly pipeline invocation.

In-scope
- Bootstrap monthly invocation state.
- Monthly entry payload schema.
- Deployment config/permission updates.

Prerequisites
- Tasks 0–4 complete.

Required outcomes
- Bootstrap invokes monthly SFN using startExecution.sync:2.
- Payload minimized to monthly-required fields only.
- Deterministic execution naming for idempotent duplicate suppression.
- Deployment wiring includes MonthlyStateMachineArn and IAM permissions.

Producer/consumer alignment requirements
- Bootstrap invocation payload must match monthly SFN input contract exactly.
- Remove unsupported payload fields from producer side.

System-integration requirements
- Bootstrap retains readiness/checkpointing responsibilities only.
- Monthly orchestration gates remain centralized in monthly SFN.

Validation/tests
- Happy path bootstrap->monthly SFN integration.
- Duplicate execution idempotency test.
- Contract-violation payload rejection test.
- Deployment permission smoke test for states:StartExecution.

Deliverables
- Updated bootstrap orchestration + config/IAM updates + tests + rollback guard notes.

Completion criteria
- No direct monthly pipeline invocation remains in bootstrap path.

Ambiguity protocol
- Stop if bootstrap and monthly responsibility boundaries are not explicit.
```

---

## Task 6 — Production artifact promotion contract in training prompt

```text
Implement Task 6 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding 3.1 and Task 6 details.
- Inspect training publish/deploy artifact path handling.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Enforce deterministic production artifact promotion contract for publish/deploy.

Issues
- Ambiguous production artifact URI usage and weak rollback safety.

In-scope
- Promotion step contract.
- Publish output contract.
- Deploy input validation and rollback pointer handling.

Prerequisites
- Tasks 0–5 complete.

Required outcomes
- Promotion emits immutable production_model_uri/hash/version.
- Publish emits only production contract fields.
- Deploy rejects non-production artifact references.
- Rollback pointer to last-known-good version is supported.

Producer/consumer alignment requirements
- Publish output fields must be exactly consumed by deploy without remapping.

System-integration requirements
- Promotion behavior deterministic under retries and reruns.

Validation/tests
- Promotion success.
- Hash mismatch fail-fast.
- Deploy refuses non-production URI.
- Rollback pointer restore test.

Deliverables
- Promotion implementation + publish/deploy contract updates + tests + operational notes.

Completion criteria
- Production deployment always consumes validated production artifact contract.

Ambiguity protocol
- Stop if artifact ownership/source-of-truth is unclear.
```

---

## Task 7 — Training readiness recomputation at gate points prompt

```text
Implement Task 7 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding 3.2 and Task 7 details.
- Inspect plan/remediate/train gate logic and current manifest usage.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Eliminate stale-manifest gating by recomputing readiness at critical gate points.

Issues
- Train decisions can be based on stale readiness manifests.

In-scope
- Recompute utility.
- Gate integration points.
- Manifest versioning/delta metrics.

Prerequisites
- Tasks 0–6 complete.

Required outcomes
- Recomputed readiness used at plan, remediate, pre-train gates.
- Versioned manifests with as-of timestamps and drift visibility.
- Hard fail when required windows remain unresolved pre-train.

Producer/consumer alignment requirements
- Recomputed manifest schema must match gate consumer expectations with no ad-hoc parsing.

System-integration requirements
- Deterministic gate behavior across retries/replays.

Validation/tests
- Stale manifest override by recomputation.
- Unresolved-window hard-fail.
- Resolved retry success.
- Drift metric/log output checks.

Deliverables
- Recompute utility + gate wiring + tests + run evidence.

Completion criteria
- Pre-train gate decisions are always based on fresh authoritative state.

Ambiguity protocol
- Stop if required-window policy is underspecified.
```

---

## Task 8 — Formalize DDB exception-table contracts prompt

```text
Implement Task 8 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read Finding X.1 and Task 8 details.
- Inspect current usage of routing, processing lock, and publication lock tables.
- Inspect deployment/provisioning and runbook docs where table inventory is defined.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Promote three additional DDB tables to explicit, validated, observable control-plane contracts.

Issues
- Implicit table dependencies with unclear schema/ownership/validation.

In-scope
- Exception-table contract registry.
- Startup preflight validators (SageMaker Processing preflight step in dependent flows).
- Table observability standards.
- Deployment integration (provisioning, seeding, smoke checks, docs).

Prerequisites
- Tasks 0–7 complete.

Required outcomes
- Explicit contracts for routing, processing lock, publication lock tables.
- Startup schema/access validation with explicit errors.
- Structured logs/metrics for table operations and lock contention.
- Deployment inventory/templates/scripts include all three tables.

Producer/consumer alignment requirements
- Every table operation must map to an allowed operation in contract registry.
- Consumer modules must use contract helpers only (no ad-hoc direct access).

System-integration requirements
- Validation, logging, and error semantics consistent across monthly/RT/backfill/training flows.

Validation/tests
- Schema-valid and schema-drift tests.
- Missing table and permission denial tests.
- Lock acquire/release/retry behavior tests.
- Routing lookup correctness tests.
- Deploy-time smoke checks for all 3 tables.

Deliverables
- Contract registry + validators + deployment updates + tests + runbook updates.

Completion criteria
- Additional tables are first-class, testable control-plane infrastructure and no longer implicit dependencies.

Ambiguity protocol
- Stop if table ownership or lock semantics are undefined.
```

---

## Task 9 — End-to-end hardening and rollout gate prompt

```text
Implement Task 9 from refactoring_fixes_plan_v2.md.

Mandatory orientation before coding (required)
- Read all Part A findings and outputs of Tasks 0–8.
- Inventory all touched contracts and integration points.

Mandatory execution posture (must be followed in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and minimizes moving parts/parameters.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback handling.
- Ensure every task result works standalone and in full-system integration.
- Run all required tests and fix implementation gaps before concluding the task.
- Conclude each task by updating all relevant project documentation files to reflect behavior/contract/config changes.
- When a new execution component is needed, use SageMaker Pipeline Processing Steps (Python/PySpark); avoid introducing Lambda-based alternatives.

Mission
- Ensure full-system correctness through release-gating scenario matrix and rollback readiness.

Issues
- Cross-task regressions may survive isolated unit-level fixes.

In-scope
- End-to-end regression matrix.
- Release gate thresholds.
- Rollout checklist and rollback playbook.

Prerequisites
- Tasks 0–8 complete.

Required outcomes
- Scenario matrix covering normal, missing dependency, fallback, duplicate replay, partial failure+retry.
- Release blocked unless critical scenario threshold passes.
- Rollback procedure validated and executable.

Producer/consumer alignment requirements
- Verify every producer output contract is accepted by designated consumer in end-to-end paths.

System-integration requirements
- Demonstrate deterministic behavior and coherent failure handling across monthly/RT/backfill/training/control-plane.

Validation/tests
- Full matrix execution with evidence artifacts.
- Critical-path integration tests across all contract edges.
- Rollback drill with successful recovery and post-rollback validation.

Deliverables
- Regression suite + release gate config + rollout checklist + rollback playbook + evidence report.

Completion criteria
- Candidate release is accepted only when matrix and rollback readiness pass.

Ambiguity protocol
- Stop if any cross-flow contract expectation is undocumented or contradictory.
```

