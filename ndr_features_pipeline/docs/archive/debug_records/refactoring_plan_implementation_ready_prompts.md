# Refactor Plan — Unified Atomic Task Prompts

These are copy-paste prompts for **atomic execution units**. Large epics (Task 5, 7, 8) are intentionally split.

Prompt format is standardized across all units:
1) Mission
2) Issues
3) In-scope
4) Prerequisites
5) Required outcomes
6) Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
7) System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
8) Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
9) Deliverables
10) Completion criteria
11) Ambiguity protocol

---

## Task 0 — Contract compatibility matrix and normalization gates

```text
Implement Task 0 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Lock interface compatibility and normalization ordering before flow refactors.

Issues
- 1A, 1B, 1D, 1K, 1L.

In-scope
- SF->pipeline->CLI->runtime->DDB compatibility matrix.
- Array-first ml_project_names normalization gates.

Prerequisites
- None.

Required outcomes
- One auditable compatibility matrix.
- Validation-before-read ordering enforcement.
- Business parameter source policy (DDB/payload only).

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Every producer field has explicit consumer mapping.
- No consumer depends on undeclared fields.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Fail fast on interface drift.
- Reusable validators for all downstream tasks.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Alignment tests for SF/pipeline/CLI/runtime/DDB mappings.
- Negative tests for undeclared/missing parameters.
- Ordering tests for array normalization.
- Fix until all required tests pass.

Deliverables
- Matrix artifact + tests + pass evidence.

Completion criteria
- Interface and ordering model is locked for all downstream tasks.

Ambiguity protocol
- Stop and request decision if field ownership (DDB vs payload) is unclear.
```

---

## Task 1 — Contract freeze (Batch Index + DPP + MLP + S3 schemas)

```text
Implement Task 1 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Freeze authoritative contracts for Batch Index, DPP, MLP, and canonical s3_prefixes/code_metadata.

Issues
- Foundational support for 1A–1L.

In-scope
- Contract/schema modules and shared loaders/builders.

Prerequisites
- Task 0 complete.

Required outcomes
- Batch Index dual-item schema frozen.
- DPP/MLP ownership boundaries frozen.
- Canonical s3_prefixes and code_metadata fields frozen.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Contracts must satisfy RT, monthly FG-B, backfill, training consumers without out-of-contract assumptions.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Greenfield contract mode (no migration ambiguity in this phase).
- Minimal API surface.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Direct/reverse/branch lookup tests.
- Contract shape/presence tests.
- Fix until all required tests pass.

Deliverables
- Frozen contracts + updated shared loaders/builders + test evidence.

Completion criteria
- Stable machine-readable contracts consumed by downstream tasks.

Ambiguity protocol
- Stop and request ownership decision for overlapping DPP/MLP keys.
```

---

## Task 2 — Shared runtime-model refactor + pipeline-builder cleanup

```text
Implement Task 2 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Remove placeholder behavior and payload bloat; align runtime/builders with DDB-first contracts.

Issues
- 1B, 1C, 1D, 1J, 1L.

In-scope
- Pipeline io contract resolver, pipeline definition files, affected wrappers/runtime config models.

Prerequisites
- Tasks 0–1 complete.

Required outcomes
- No placeholder/default-value code URI resolution.
- No declared-but-unused or consumed-but-undeclared runtime fields.
- Runtime payload reduced to run-varying fields.
- Branch-context propagation where required.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Declared parameters and consumed inputs are perfectly aligned across producer/consumer boundaries.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No env fallback for business behavior.
- No interface drift with SF contracts.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Contract drift tests.
- Placeholder-resolution guard tests.
- Required-field wire-or-remove tests.
- Pipeline smoke builds.
- Fix until all required tests pass.

Deliverables
- Updated builders/wrappers/models + test evidence.

Completion criteria
- Shared runtime layer deterministic and contract-clean.

Ambiguity protocol
- Stop and request keep/remove decision for any uncertain runtime field.
```

---

## Task 3 — Batch Index writer + RT prefix precomputation

```text
Implement Task 3 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Implement canonical writer for RT precomputed Batch Index records.

Issues
- 1A, 1J.

In-scope
- Batch Index writer module + shared prefix precompute modules.

Prerequisites
- Tasks 0–2 complete.

Required outcomes
- Idempotent dual-item writer API.
- Full s3_prefixes + code_metadata prewritten before child execution.
- Post-write updates limited to status/timestamps.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Records include all fields needed by batch-id and reverse-date consumers.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Writer reusable by backfill.
- No alternate ad-hoc record shapes.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Completeness, reverse lookup, branch mapping, idempotency tests.
- Fix until all required tests pass.

Deliverables
- Writer API + tests + sample validated records.

Completion criteria
- Canonical writer proven consumer-compatible.

Ambiguity protocol
- Stop and request unified status contract if RT/backfill status semantics conflict.
```

---

## Task 4 — RT father flow refactor

```text
Implement Task 4 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Refactor RT father orchestration for strict array-first fan-out and contract-clean pipeline starts.

Issues
- 1A, 1B, 1D.

In-scope
- RT father Step Function definition + contract tests.

Prerequisites
- Tasks 0–3 complete.

Required outcomes
- Payload validation + array normalization-before-read.
- Per-ml_project_name Map fan-out with explicit branch context.
- Only declared/required pipeline params.
- Batch Index prewrite integration.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Branch consumers get exact required context; no hidden global context coupling.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Deterministic retry/failure behavior.
- Explicit contract validation errors.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- SF schema validation.
- Parameter alignment tests.
- Ordering tests.
- Branch execution behavior tests.
- Fix until all required tests pass.

Deliverables
- Updated RT SF + test evidence.

Completion criteria
- RT orchestration deterministic and contract-safe.

Ambiguity protocol
- Stop and request explicit contract change approval if extra runtime context is needed.
```

---

## Task 5.1 — Delta + FG-A contract hardening

```text
Implement Task 5.1 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Harden Delta/FG-A contract surface (including deterministic mini_batch_id behavior).

Issues
- 1E (and supporting 1D path-contract correctness).

In-scope
- Delta + FG-A run/job code and immediate contract tests.

Prerequisites
- Tasks 0–4 complete.

Required outcomes
- Deterministic Delta->FG-A mini_batch_id contract.
- Batch-Index-resolved paths for Delta/FG-A IO.
- Partition contract correctness.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- FG-A inputs and downstream consumer assumptions remain explicit and validated.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No placeholder path resolution.
- No hidden fallback contract logic.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Delta partition tests.
- Delta->FG-A mini_batch tests.
- Path-resolution tests using Batch Index fixtures.
- Fix until all required tests pass.

Deliverables
- Updated Delta/FG-A modules + tests + evidence.

Completion criteria
- Delta and FG-A contract-safe and consumer-aligned.

Ambiguity protocol
- Stop and request decision if strict vs compatibility-mode mini_batch behavior is unclear.
```

---

## Task 5.2 — PairCounts + FG-C contract hardening

```text
Implement Task 5.2 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Harden PairCounts/FG-C sufficiency and join contracts.

Issues
- 1D support and Task 5 contract closure.

In-scope
- PairCounts + FG-C run/job code and contract tests.

Prerequisites
- Task 5.1 complete.

Required outcomes
- PairCounts outputs sufficient for FG-C and downstream consumers.
- FG-C join-key granularity contract enforced.
- Under-specified fallback joins rejected.
- Missing required baseline dependencies fail fast.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Produced fields/horizons align with all designated FG-C consumers.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Deterministic failure signaling for unmet prerequisites.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Sufficiency tests.
- FG-C join-key/fallback tests.
- Baseline-missing fail-fast tests.
- Fix until all required tests pass.

Deliverables
- Updated PairCounts/FG-C modules + test evidence.

Completion criteria
- PairCounts/FG-C outputs and constraints are consumer-safe.

Ambiguity protocol
- Stop and request canonical join contract decision if competing join-key definitions exist.
```

---

## Task 5.3 — Inference/join/publication contract alignment

```text
Implement Task 5.3 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Align inference, prediction join, and publication contracts with branch context and consumer expectations.

Issues
- 1D and Task 5 closure.

In-scope
- Inference/join/publication wrappers/jobs/specs and related contract tests.

Prerequisites
- Tasks 5.1 and 5.2 complete.

Required outcomes
- ml_project_name propagated end-to-end where required.
- Inference metadata contract deterministic.
- Join/publication consume stable upstream schema/contracts.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- All publication-facing outputs match designated consumer schemas and metadata requirements.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No duplicate branch identifiers.
- No drift-prone re-resolution behavior.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Inference->join schema tests.
- Branch propagation tests.
- Output path determinism tests.
- Fix until all required tests pass.

Deliverables
- Updated inference/join/publication components + test evidence.

Completion criteria
- Flow-1 consumer-path fully aligned and deterministic.

Ambiguity protocol
- Stop and request canonical metadata contract if competing definitions exist.
```

---

## Task 6 — Monthly machine inventory + FG-B refactor

```text
Implement Task 6 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Deliver deterministic UNLOAD-only monthly machine-inventory + FG-B flow.

Issues
- 1G.

In-scope
- Monthly SF and machine inventory/FG-B components.

Prerequisites
- Tasks 0–5.3 complete.

Required outcomes
- Deterministic machine-update -> FG-B dependency chain.
- UNLOAD-only machine inventory path.
- Canonical monthly inventory merge semantics.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- FG-B outputs keep schema/keys required by FG-C/training/inference consumers.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- DPP flow-specific query descriptors.
- Local staged processing for unload output.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Reference-month tests.
- UNLOAD integration tests.
- Inventory transition tests.
- FG-B contract tests.
- Fix until all required tests pass.

Deliverables
- Updated monthly components + test evidence.

Completion criteria
- Monthly flow deterministic and consumer-compatible.

Ambiguity protocol
- Stop and request explicit deprecation decision if fallback behavior is requested.
```

---

## Task 7.1 — Backfill planner + manifest contract freeze

```text
Implement Task 7.1 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Freeze planner/manifest contracts for selective backfill.

Issues
- 1F, 1I foundation.

In-scope
- Planner schema/contracts and manifest definitions consumed by map execution.

Prerequisites
- Tasks 0–6 complete.

Required outcomes
- Canonical family/range planner contract.
- Canonical manifest schema for extractor->map consumption.
- Selective execution policy contract.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Manifest schema covers all consumers (backfill map + training-triggered remediation caller).

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Minimal manifest-driven control surface; no extra toggles.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Planner schema tests.
- Selective-decision tests.
- Caller compatibility tests.
- Fix until all required tests pass.

Deliverables
- Frozen planner/manifest contracts + tests.

Completion criteria
- Planner/manifest contracts are stable and reusable.

Ambiguity protocol
- Stop and request contract decision if any required family/range semantics are unclear.
```

---

## Task 7.2 — Extractor-manifest to map execution wiring

```text
Implement Task 7.2 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Wire backfill map execution to extractor manifest outputs deterministically.

Issues
- 1F.

In-scope
- Backfill SF/map orchestration and extractor output wiring.

Prerequisites
- Task 7.1 complete.

Required outcomes
- Map reads extractor manifest outputs directly.
- Deterministic map input shaping and selective execution behavior.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Executed ranges/families exactly reflect planner manifest consumed by downstream writers.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Deterministic retries and branch failure handling.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Extractor->map E2E tests.
- Selective execution tests.
- Retry/failure determinism tests.
- Fix until all required tests pass.

Deliverables
- Updated map wiring + test evidence.

Completion criteria
- Manifest-driven map execution fully operational.

Ambiguity protocol
- Stop and request decision if map input contract differs from manifest schema.
```

---

## Task 7.3 — Redshift fallback unload path and range execution

```text
Implement Task 7.3 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Implement deterministic Redshift fallback path for missing raw data in backfill.

Issues
- 1I support and Task 7 fallback closure.

In-scope
- Flow-specific DPP unload query loading/execution, multi-range handling, local staging.

Prerequisites
- Task 7.1 complete.

Required outcomes
- DPP-owned flow-specific unload query execution.
- Multi-range deterministic execution and retry behavior.
- Local RAM/FS staging before downstream reconstruction.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Reconstructed data must match same downstream consumer contracts as ingestion path.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Fallback path contract-compatible with planner/map and writer stages.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Ingestion-miss fallback tests.
- Multi-range execution tests.
- Staging integrity tests.
- Fix until all required tests pass.

Deliverables
- Fallback execution modules + test evidence.

Completion criteria
- Backfill fallback path deterministic and consumer-compatible.

Ambiguity protocol
- Stop and request decision if generic query fallback is proposed (not allowed by contract).
```

---

## Task 7.4 — Backfill Batch Index writes + idempotency/status reconciliation

```text
Implement Task 7.4 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Finalize backfill write path with dual-item Batch Index writes, idempotency, and status/reconciliation contract.

Issues
- 1F, 1I closure.

In-scope
- Backfill writer integration, status progression logic, reconciliation handling.

Prerequisites
- Tasks 7.1, 7.2, 7.3 complete.

Required outcomes
- Dual-item writes for reconstructed batches.
- Branch-level s3_prefixes coverage.
- Deterministic status progression and reconciliation on partial writes.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Batch Index outputs match RT output contracts for all downstream consumers.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Idempotent reruns and conditional-write safety.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Dual-item write tests.
- Idempotency tests.
- Partial-write reconciliation tests.
- Status transition tests.
- Fix until all required tests pass.

Deliverables
- Backfill writer closure + evidence.

Completion criteria
- Backfill output contract aligned with whole system.

Ambiguity protocol
- Stop and request explicit rollback semantics if partial success handling is unclear.
```

---

## Task 8.1 — Training runtime contract minimization (trigger vs DDB ownership)

```text
Implement Task 8.1 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Minimize runtime payload and move business policy ownership to DDB contracts.

Issues
- 1L, 1J foundation.

In-scope
- Training SF/pipeline/runtime contract surfaces.

Prerequisites
- Tasks 0–7.4 complete.

Required outcomes
- Trigger payload limited to run-varying fields.
- Business toggles/policy owned by DDB.
- Removed payload bloat fields from runtime interfaces.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Downstream training components consume one canonical runtime contract.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No env/placeholder business fallback paths.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Contract tests for minimal payload.
- Runtime configuration ownership tests.
- Fix until all required tests pass.

Deliverables
- Updated runtime contract + evidence.

Completion criteria
- Trigger/DDB ownership boundaries fully explicit and enforced.

Ambiguity protocol
- Stop and request source-of-truth decision for any contested field.
```

---

## Task 8.2 — Training sequencing correction

```text
Implement Task 8.2 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Enforce verify->plan->remediate->reverify->train lifecycle.

Issues
- 1H.

In-scope
- Training orchestration stage sequencing and gates.

Prerequisites
- Task 8.1 complete.

Required outcomes
- Remediation planning occurs before train.
- Hard train gate on unresolved windows.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Stage outputs are consumable by next stages without ad-hoc translation.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Deterministic stage transitions and failure behavior.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Stage sequence tests.
- Gate-block tests for unresolved windows.
- Fix until all required tests pass.

Deliverables
- Updated sequencing logic + evidence.

Completion criteria
- Pre-train remediation flow guaranteed by orchestration contract.

Ambiguity protocol
- Stop and request decision if stage ownership overlaps with existing SF responsibilities.
```

---

## Task 8.3 — Unified missing-window manifest contract

```text
Implement Task 8.3 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Replace mixed missing-window semantics with one canonical manifest schema.

Issues
- 1I.

In-scope
- Verify/planner/remediation manifest structures.

Prerequisites
- Tasks 8.1 and 8.2 complete.

Required outcomes
- One schema used by verify, plan, remediate, reverify.
- No partition-day vs window-range semantic split.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- All remediation consumers can execute directly from manifest entries.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Schema versioning and backward-incompatible change guard.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Manifest schema tests.
- Verify->remediate compatibility tests.
- Fix until all required tests pass.

Deliverables
- Canonical manifest schema + validation evidence.

Completion criteria
- Missing-window contract is singular and deterministic.

Ambiguity protocol
- Stop and request schema decision if any family/range field is disputed.
```

---

## Task 8.4 — Selective remediation execution mechanics

```text
Implement Task 8.4 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Ensure remediation executes exact required ranges/families only.

Issues
- 1I.

In-scope
- Remediation execution logic for backfill/FG-B invocation shaping.

Prerequisites
- Task 8.3 complete.

Required outcomes
- No coarse global range collapse.
- No retry-slice truncation of unresolved gaps.
- Deterministic chunking and idempotent retry semantics.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Remediation outputs map 1:1 to planner manifest units consumed by reverify.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Deterministic invocation metadata for observability and replay.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Sparse-range selective execution tests.
- Chunking/idempotency tests.
- Fix until all required tests pass.

Deliverables
- Updated remediation mechanics + evidence.

Completion criteria
- Remediation is precise and non-overprocessing.

Ambiguity protocol
- Stop and request policy decision if selective granularity rules conflict.
```

---

## Task 8.5 — Batch Index authoritative training-readiness planner

```text
Implement Task 8.5 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Make Batch Index authoritative for training readiness and missing-range derivation.

Issues
- 1J support, 1I support.

In-scope
- Training readiness planner/verifier inputs/outputs.

Prerequisites
- Tasks 8.2 and 8.3 complete.

Required outcomes
- Planner derives expected/observed/unresolved windows from Batch Index.
- Readiness artifacts produced with traceable evidence.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Readiness outputs feed remediation and training gates without transformation drift.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Deterministic range derivation logic and artifact naming.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Batch Index driven readiness tests.
- Artifact completeness tests.
- Fix until all required tests pass.

Deliverables
- Planner updates + readiness artifact evidence.

Completion criteria
- batch_index_table_name is operationally authoritative, not merely validated.

Ambiguity protocol
- Stop and request contract decision if Batch Index signals conflict with feature-store observations.
```

---

## Task 8.6 — `ml_project_name` propagation and branch isolation

```text
Implement Task 8.6 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Enforce branch context propagation and branch-isolated outputs across training and evaluation paths.

Issues
- 1D.

In-scope
- Training SF/pipeline/wrapper/runtime propagation chain and downstream evaluation invocations.

Prerequisites
- Task 8.1 complete.

Required outcomes
- ml_project_name propagates end-to-end where required.
- Branch-scoped outputs and metadata remain isolated.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Inference/join evaluation consumers receive required branch context.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No hidden global-default branch behavior.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Propagation tests across stages.
- Branch-isolation output tests.
- Fix until all required tests pass.

Deliverables
- Propagation closure + evidence.

Completion criteria
- No branch context drops in training/evaluation lifecycle.

Ambiguity protocol
- Stop and request canonical branch-key decision if multiple branch identifiers exist.
```

---

## Task 8.7 — Strict DDB-only orchestration target resolution

```text
Implement Task 8.7 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Enforce DDB-only target resolution for enabled training branches.

Issues
- 1K.

In-scope
- Training target resolution code and readiness checks.

Prerequisites
- Task 8.1 complete.

Required outcomes
- Required branch targets come from DDB contract only.
- Invalid/missing targets fail fast with explicit contract errors.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Downstream orchestrator consumers receive validated target descriptors only.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Remove business fallback behavior (code-default/env).

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- DDB resolution tests.
- Missing/invalid target fail-fast tests.
- Fix until all required tests pass.

Deliverables
- Resolution logic update + evidence.

Completion criteria
- Target resolution deterministic and contract-owned by DDB.

Ambiguity protocol
- Stop and request target contract decision if mixed ARN/name requirements appear.
```

---

## Task 8.8 — Placeholder-free training code-URI resolution

```text
Implement Task 8.8 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Remove placeholder/default-value-based code URI resolution from training.

Issues
- 1C.

In-scope
- Training pipeline step code URI resolution paths.

Prerequisites
- Task 8.1 complete.

Required outcomes
- Code URI resolution uses concrete DDB identities only.
- code_metadata checks enforced at resolution/use time.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Resolved code artifacts must match step consumer expectations and metadata contracts.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No hidden fallback resolution behavior.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Guard tests failing on placeholder-based resolution.
- Positive tests for concrete DDB-resolved URIs.
- Fix until all required tests pass.

Deliverables
- Updated resolution logic + evidence.

Completion criteria
- Training step code resolution deterministic and contract-safe.

Ambiguity protocol
- Stop and request packaging metadata decision if required code_metadata fields are missing.
```

---

## Task 8.9 — Required runtime-field hygiene (wire-or-remove)

```text
Implement Task 8.9 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Ensure every required runtime field is functionally used or removed.

Issues
- 1J.

In-scope
- Training runtime contracts and usage sites.

Prerequisites
- Task 8.1 complete.

Required outcomes
- Required field inventory with usage mapping.
- Remove/rewire required-but-unused fields.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Required runtime fields match actual consumer logic and no-op fields are eliminated.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Keep runtime contracts minimal and explicit.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Static/contract tests for required field usage.
- Seeded required_runtime_params alignment tests.
- Fix until all required tests pass.

Deliverables
- Runtime field mapping report + code updates + evidence.

Completion criteria
- No required runtime field is dead/unconsumed.

Ambiguity protocol
- Stop and request decision where field removal may impact external callers.
```

---

## Task 8.10 — Task 8 integration order lock

```text
Implement Task 8.10 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Lock and validate integration order across Task 8.1–8.9 outputs.

Issues
- Task 8 integration-risk closure.

In-scope
- Orchestration/integration wiring and dependency gate checks.

Prerequisites
- Tasks 8.1–8.9 complete.

Required outcomes
- Integration order enforced by explicit gates.
- Cross-unit compatibility checks pass.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Integrated outputs remain consumer-compatible after composition.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- No regression of prior Task 8 unit guarantees.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Cross-unit integration tests.
- Dependency-gate tests.
- Fix until all required tests pass.

Deliverables
- Integration gate definitions + evidence.

Completion criteria
- Task 8 units compose deterministically.

Ambiguity protocol
- Stop and request sequencing decision if gate ownership is unclear.
```

---

## Task 8.11 — Training verification bundle and closure gates

```text
Implement Task 8.11 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Execute final training verification bundle and close Task 8 with explicit evidence.

Issues
- 1D, 1F, 1H, 1I, 1J, 1K, 1L closure for training domain.

In-scope
- Task 8 verification suite and closure reporting.

Prerequisites
- Tasks 8.1–8.10 complete.

Required outcomes
- Full Task 8 verification bundle executed and passing.
- Known risks and residuals explicitly documented.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Verify training outputs and evaluation outputs satisfy all designated downstream consumers.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Validate behavior in integrated system context, not only isolated unit context.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Contract, orchestration, branch-propagation, and code-resolution guard tests.
- Additional integration tests as needed for closure confidence.
- Fix until all required tests pass.

Deliverables
- Consolidated verification report + pass evidence.

Completion criteria
- Task 8 officially closed with auditable verification artifacts.

Ambiguity protocol
- Stop and request decision if any closure gate remains inconclusive.
```

---

## Task 9 — Hardening + final rollout validation

```text
Implement Task 9 from the refactor plan.

Mandatory orientation before coding (required for this task)
- Locate and study the refactoring plan in: `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- Familiarize yourself with all project code and configuration files, then follow the task-specific reading instructions in the refactoring plan before changing code.
- Familiarize yourself with all project documentation files; for any code/config change you implement, update every relevant documentation file so docs stay aligned with implemented behavior.
- Proactively gather any additional project context (tests, scripts, operational notes, architecture docs, contracts, examples) needed to maximize correctness of this task in isolation and in full-system integration, while preserving the plan intent: simplest viable design, minimal moving parts, DDB-driven contracts, and deterministic behavior.

Mission
- Close all regressions and validate production readiness across the full system.

Issues
- 1A–1L.

In-scope
- Cross-cutting validators/tests/hardening updates.

Prerequisites
- Tasks 0–8.11 complete.

Required outcomes
- No interface drift.
- Full contract/integration verification for all flows.
- Explicit evidence no placeholder/env business fallback remains.

Consumer-alignment requirements
- Ensure every output produced by this task is explicitly aligned with the expectations and contracts of all designated consumers.
- Validate producer->consumer compatibility for RT, monthly FG-B, backfill, training flows.

System-integration requirements
- Verify this task's implementation is properly working in isolation and in full-system context, and remains aligned with the refactoring plan's intentions and design.
- Validate normal, cold-start bootstrap, and targeted-recovery operations.

Validation/tests
- Conduct all tests needed to verify successful implementation for this task in isolation and in end-to-end system context; if any check fails, fix implementation and re-run until successful.
- Unit, targeted job, contract, pipeline smoke-build, SF validation, fixture integration, cold-start E2E, targeted-recovery E2E.
- Fix until all required tests pass.

Deliverables
- Final hardening patch set + consolidated pass/fail report.

Completion criteria
- Rollout-ready system with verified contract integrity and regression safety.

Ambiguity protocol
- Stop and request explicit resolution if any gate failure indicates upstream contract conflict.
```
