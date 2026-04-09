# Fixes Plan — Unified Atomic Task Prompts

These are copy-paste prompts for implementing the fixes plan tasks in
`docs/archive/debug_records/flow_audit_issues_fixes_and_execution_plan.md`.

Prompt format is standardized across all units:
1) Mission
2) Issues
3) In-scope
4) Prerequisites
5) Required outcomes
6) Consumer-alignment requirements
7) System-integration requirements
8) Validation/tests
9) Deliverables
10) Completion criteria
11) Ambiguity protocol

Additional mandatory execution posture for all prompts:
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior.
- Enforce DDB-first business configuration, minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.

---

## Task 0 — Contract freeze for remediation contracts

```text
Implement Task 0 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Read: docs/archive/debug_records/flow_audit_issues_fixes_and_execution_plan.md
- Study all code/config touched by remediation contracts (RT, monthly, training, backfill SFNs and contract modules).
- Build a contract matrix from SF input -> contract payload -> consumer execution fields.

Mission
- Freeze canonical remediation payload and response contracts.

Issues
- F2.1, F3.1, F3.2, F4.1, IDP-2, IDP-4, IDP-6.

In-scope
- NdrBackfillRequest.v1, NdrBaselineRemediationRequest.v1, NdrTrainingRemediationRequest.v1.
- Contract validators, idempotency key strategy, provenance fields.

Prerequisites
- None.

Required outcomes
- Machine-readable schemas for all three contracts.
- Validator library used by all producers/consumers.
- Deterministic idempotency key recipe documented and tested.

Consumer-alignment requirements
- Every producer field must map to a consumer runtime field or a validated no-op metadata field.

System-integration requirements
- Contracts must be reusable across RT/monthly/training with no ad-hoc per-flow payload variants.

Validation/tests
- Positive/negative schema tests.
- Version-gate tests.
- Missing-field/unknown-field behavior tests.

Deliverables
- Contract schema module + validator integration + test evidence.

Completion criteria
- All remediation contracts accepted by validators and consumed uniformly.

Ambiguity protocol
- Stop and request decision if ownership of a field (DPP vs runtime) is unclear.
```

---

## Task 1 — Machine inventory full monthly snapshot semantics

```text
Implement Task 1 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Read machine inventory unload job and monthly SFN orchestration.
- Validate current query + write behavior against month snapshot requirements.

Mission
- Ensure monthly machine inventory is complete and replay-safe.

Issues
- F1.1.

In-scope
- machine_inventory_unload_job and related runtime wrappers.

Prerequisites
- Task 0 complete.

Required outcomes
- Full monthly snapshot overwrite (Option A).
- Historical monthly partitions preserved.
- No incremental new-only partition writes.

Consumer-alignment requirements
- Output must preserve fields required for persistent/non-persistent and history sufficiency derivations.

System-integration requirements
- Monthly run must remain idempotent and deterministic for replays.

Validation/tests
- Row-count parity vs source for month snapshot.
- Re-run idempotency test.
- Month-over-month historical retention test.

Deliverables
- Updated unload logic + tests + sample run evidence.

Completion criteria
- Monthly partition always represents complete month snapshot.

Ambiguity protocol
- Stop and request policy decision if source query semantics conflict with snapshot completeness.
```

---

## Task 2 — Monthly FG-A readiness/remediation gate before FG-B

```text
Implement Task 2 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Read monthly SFN and FG-B builder dependencies.
- Confirm minimum upstream artifacts needed for FG-B execution.

Mission
- Prevent FG-B execution when required FG-A history is absent.

Issues
- F1.2, IDP-3.

In-scope
- Monthly SFN states for readiness detection, remediation invocation, and revalidation.

Prerequisites
- Tasks 0 and 1 complete.

Required outcomes
- Deterministic dependency check state.
- Conditional remediation call.
- Recheck gate before StartFGBBaselinePipeline.

Consumer-alignment requirements
- Readiness manifest fields must align with backfill consumer contract.

System-integration requirements
- No false positives that block valid monthly runs.

Validation/tests
- No-missing path skips remediation.
- Missing path remediates then proceeds.
- Unresolved path fails with explicit reason.

Deliverables
- Updated monthly SFN + contract tests + execution traces.

Completion criteria
- Monthly baseline is startup-safe and contract-clean.

Ambiguity protocol
- Stop if FG-B minimum artifact scope is unclear.
```

---

## Task 3 — Shared RawInputResolver and DPP-driven Redshift fallback integration

```text
Implement Task 3 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect base_runner read path and existing backfill_redshift_fallback utilities.
- Confirm where ingestion completeness checks are needed.

Mission
- Add one reusable resolver for ingestion-vs-Redshift source selection.

Issues
- F2.2, F2.3, IDP-7.

In-scope
- RawInputResolver module, integration into delta/backfill extraction paths, provenance writes.

Prerequisites
- Tasks 0–2 complete.

Required outcomes
- Deterministic source_mode resolution.
- DPP-owned query + connection parameters only.
- Provenance persisted for observability.

Consumer-alignment requirements
- Resolver outputs must be directly consumable by job readers and orchestration status writers.

System-integration requirements
- Resolver must behave identically across RT and backfill contexts.

Validation/tests
- Complete ingestion -> ingestion mode.
- Missing ingestion + fallback enabled -> redshift_unload_fallback mode.
- Fallback disabled -> explicit failure.

Deliverables
- Resolver implementation + integrations + tests.

Completion criteria
- No duplicated fallback logic paths and no hidden env business fallbacks.

Ambiguity protocol
- Stop if DPP fallback query ownership is undefined.
```

---

## Task 4 — RT missing-range detection and remediation orchestration

```text
Implement Task 4 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Read RT SFN flow and existing per-branch execution states.
- Identify insertion point between features and inference.

Mission
- Add RT-side missing artifact detection and remediation invocation.

Issues
- F2.1, IDP-2.

In-scope
- RT SFN states for compute-missing, branch choice, remediation call, and post-remediation revalidation.

Prerequisites
- Tasks 0–3 complete.

Required outcomes
- Backfill contract invocation from RT when missing ranges exist.
- Revalidation gate before inference/publication.
- Batch Index status updates for remediation request/result.

Consumer-alignment requirements
- RT request payload must strictly conform to NdrBackfillRequest.v1.

System-integration requirements
- Ensure branch fanout behavior remains deterministic under retries.

Validation/tests
- Missing/no-missing branch tests.
- Retry/idempotency tests using same idempotency key.
- Failure handling tests.

Deliverables
- Updated RT SFN + tests + run evidence.

Completion criteria
- RT no longer depends on implicit availability of all artifacts.

Ambiguity protocol
- Stop if authoritative missing-range derivation source is unclear.
```

---

## Task 5 — Backfill SFN ↔ executable pipeline contract reconciliation

```text
Implement Task 5 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Compare backfill SFN pipeline parameters with actual pipeline definitions and CLI contracts.

Mission
- Remove contract mismatch between backfill orchestration and execution target.

Issues
- F4.1, IDP-4.

In-scope
- Backfill execution pipeline contract and/or SFN invocation contract (choose one canonical interface and apply both sides).

Prerequisites
- Tasks 0–4 complete.

Required outcomes
- Family/range parameters accepted and consumed by execution target.
- Deterministic family dependency ordering.

Consumer-alignment requirements
- Backfill completion outputs usable by RT/monthly/training consumers.

System-integration requirements
- Maintain backward-safe rollout (versioned contract or phased cutover).

Validation/tests
- Family-specific and mixed-family execution tests.
- Parameter compatibility tests.
- Polling and completion status tests.

Deliverables
- Reconciled orchestration + execution contract + tests.

Completion criteria
- No SFN-to-pipeline parameter drift.

Ambiguity protocol
- Stop if two competing backfill execution interfaces remain unresolved.
```

---

## Task 6 — Historical extractor reliability hardening

```text
Implement Task 6 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect historical extractor control flow for batch-index and S3 fallback paths.

Mission
- Make extractor robust for cold-start and partial-index conditions.

Issues
- F4.2, F4.3, IDP-5.

In-scope
- Extractor fallback gating logic, project resolution strategy, manifest provenance.

Prerequisites
- Tasks 0–5 complete.

Required outcomes
- S3 fallback attempted whenever batch-index rows unavailable.
- Explicit project_name runtime support.
- No hardcoded project token dependency.

Consumer-alignment requirements
- Manifest schema must remain consumable by backfill SFN map.

System-integration requirements
- Preserve deterministic row ordering and manifest stability.

Validation/tests
- Index-only, S3-only, and both-empty scenarios.
- Project resolution tests.
- Manifest validity tests.

Deliverables
- Updated extractor + tests.

Completion criteria
- Extractor supports startup reconstruction reliably.

Ambiguity protocol
- Stop if canonical project resolution precedence is unclear.
```

---

## Task 7 — Extend backfill family planner/executor to include FG-B baseline support

```text
Implement Task 7 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect backfill family planner and historical extractor generated family ranges.

Mission
- Support FG-B baseline recovery in unified backfill provider path.

Issues
- F4.4, IDP-5.

In-scope
- Family plan generation, map items, executor dispatch, completion reporting.

Prerequisites
- Tasks 0–6 complete.

Required outcomes
- FG-B family ranges can be planned and executed when needed.
- Completion payload includes baseline generation results.

Consumer-alignment requirements
- Monthly and training consumers must be able to consume FG-B remediation outputs without custom adapters.

System-integration requirements
- Preserve family dependency correctness and minimal duplicated execution.

Validation/tests
- FG-B only, FG-A+FG-B mixed, and no-FG-B scenarios.

Deliverables
- Updated planner/executor + tests + compatibility evidence.

Completion criteria
- Unified backfill provider can remediate baseline gaps.

Ambiguity protocol
- Stop if FG-B reference window derivation policy is not specified.
```

---

## Task 8 — Training mode propagation and mode-aware runtime behavior

```text
Implement Task 8 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect training SFN, IF training pipeline builder, training CLI/runtime models.

Mission
- Add explicit training/evaluation/production mode semantics end-to-end.

Issues
- F3.1, IDP-6.

In-scope
- Training SFN runtime contract, pipeline parameters, CLI args, runtime dispatch behavior.

Prerequisites
- Tasks 0–7 complete.

Required outcomes
- Mode param validated and propagated.
- Stage execution behavior conditioned by mode.
- Mode-appropriate timestamp requirements.

Consumer-alignment requirements
- Downstream publish/deploy and evaluation consumers receive only mode-allowed outputs.

System-integration requirements
- Backward compatibility strategy for existing triggers without mode.

Validation/tests
- training mode, evaluation mode, production mode branch tests.
- Invalid mode negative tests.

Deliverables
- Updated orchestrator/runtime + tests.

Completion criteria
- Mode behavior deterministic and contract-validated.

Ambiguity protocol
- Stop if production-mode deployment policy conflicts with current governance rules.
```

---

## Task 9 — Training per-branch MLP config lookup hardening

```text
Implement Task 9 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect training SFN ordering of ml_project_names normalization vs MLP reads.

Mission
- Ensure MLP config lookup is branch-correct and not pre-branch ambiguous.

Issues
- F3.2.

In-scope
- Training SFN branch normalization and per-branch config lookup states.

Prerequisites
- Tasks 0–8 complete.

Required outcomes
- MLP lookup executed per normalized branch.
- Branch-level reciprocal mapping validation.

Consumer-alignment requirements
- Branch context propagated unchanged to pipeline calls.

System-integration requirements
- Invalid one-branch behavior must not silently corrupt other branches.

Validation/tests
- list-only input path.
- one invalid branch path.
- branch context integrity tests.

Deliverables
- Updated SFN + tests.

Completion criteria
- Branch-safe MLP configuration consumption.

Ambiguity protocol
- Stop if failure policy (fail-all vs fail-branch) is unclear.
```

---

## Task 10 — Backfill extractor pipeline code URI resolution hardening

```text
Implement Task 10 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect historical extractor pipeline builder and code URI resolver usage.

Mission
- Eliminate placeholder-based code URI resolution.

Issues
- F4.5, IDP-8.

In-scope
- Pipeline builder signatures/call sites and guard checks.

Prerequisites
- Tasks 0–9 complete.

Required outcomes
- Concrete contract identities required for code URI resolution.
- Guard rejects placeholder tokens.

Consumer-alignment requirements
- Deployment/upsert callers must provide required identities explicitly.

System-integration requirements
- No regression in existing pipeline upsert flows.

Validation/tests
- positive concrete identity resolution tests.
- placeholder negative tests.

Deliverables
- Updated builder + tests.

Completion criteria
- No runtime/build-time placeholder resolution behavior remains.

Ambiguity protocol
- Stop if identity source (DPP vs static deploy config) is undefined.
```

---

## Task 11 — End-to-end integration gates and rollout hardening

```text
Implement Task 11 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Review all completed tasks and integrated finding matrix.

Mission
- Validate and harden full-system behavior before rollout.

Issues
- Integrated closure for F1.1–F5.1.

In-scope
- Cross-flow integration tests, observability checks, rollout/rollback rehearsal.

Prerequisites
- Tasks 0–10 complete.

Required outcomes
- Green integration gates for RT, monthly, training, backfill interactions.
- Replay/idempotency verified.
- Rollback dry run validated.

Consumer-alignment requirements
- All producer/consumer interfaces verified under real execution order.

System-integration requirements
- No unresolved contract drift remains.

Validation/tests
- End-to-end orchestration tests.
- Synthetic failure path tests.
- Monitoring signal correctness tests.

Deliverables
- Integration validation bundle + go/no-go record.

Completion criteria
- System readiness gate passed for production deployment.

Ambiguity protocol
- Stop rollout if any required gate is red.
```

---

## Task 12 — Initial deployment bootstrap orchestration

```text
Implement Task 12 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Review startup path dependencies across monthly, backfill, RT.

Mission
- Create deterministic day-0 bootstrap orchestration.

Issues
- IDP-1, IDP-2, IDP-3.

In-scope
- Bootstrap state machine/runbook flow, readiness checkpoints, status persistence.

Prerequisites
- Tasks 0–11 complete.

Required outcomes
- Empty environment reaches operational-ready state deterministically.
- Bootstrap re-run is idempotent.

Consumer-alignment requirements
- Bootstrap outputs become authoritative inputs for RT steady-state activation.

System-integration requirements
- Bootstrap must not interfere with already-ready environments.

Validation/tests
- Empty-system bootstrap success.
- Re-run no-op/idempotency.
- Partial-bootstrap recovery tests.

Deliverables
- Bootstrap orchestration + tests + ops handoff docs.

Completion criteria
- Startup path no longer manual or brittle.

Ambiguity protocol
- Stop if readiness definition lacks measurable criteria.
```

---

## Task 13 — RT two-phase cold-start-safe execution split

```text
Implement Task 13 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Review 15m pipeline composition and FG-C baseline dependencies.

Mission
- Split RT execution into core and dependent phases to avoid cold-start deadlock.

Issues
- IDP-1, IDP-2.

In-scope
- Pipeline decomposition and SFN orchestration updates.

Prerequisites
- Tasks 0–12 complete.

Required outcomes
- Core artifacts (delta/fg_a/pair_counts) can progress before FG-C readiness.
- Dependent phase starts only after remediation/readiness success.

Consumer-alignment requirements
- Preserve downstream inference/publication contracts.

System-integration requirements
- Keep steady-state latency efficient (bypass remediation when not needed).

Validation/tests
- Cold-start path tests.
- Steady-state bypass tests.
- Failure/rollback tests.

Deliverables
- Split pipelines + SFN updates + tests.

Completion criteria
- RT is cold-start safe without sacrificing steady-state efficiency.

Ambiguity protocol
- Stop if phase boundary ownership is unclear.
```

---

## Task 14 — Initial-deployment contract conformance hardening

```text
Implement Task 14 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Reconcile startup contracts across backfill, extractor, training remediation, and pipeline definitions.

Mission
- Add startup-focused contract conformance gates.

Issues
- IDP-4, IDP-6, IDP-8.

In-scope
- CI/startup contract validator suite and explicit mismatch diagnostics.

Prerequisites
- Tasks 0–13 complete.

Required outcomes
- Startup contract matrix validated pre-deploy.
- Deterministic diagnostics for every mismatch class.

Consumer-alignment requirements
- Validate both directions of each startup-critical interface.

System-integration requirements
- Blocks release on startup contract red status.

Validation/tests
- Intentional mismatch fixtures.
- Valid startup matrix green tests.

Deliverables
- Validator suite + CI integration + evidence.

Completion criteria
- No startup contract drift can ship undetected.

Ambiguity protocol
- Stop if contract source-of-truth is disputed.
```

---

## Task 15 — Initial-deployment observability and rollback package

```text
Implement Task 15 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Review startup failure classes and remediation paths.

Mission
- Make cold-start operation observable and rollback-safe.

Issues
- IDP-2, IDP-5, IDP-7.

In-scope
- Metrics, alerts, dashboards, startup runbooks, rollback switch.

Prerequisites
- Tasks 0–14 complete.

Required outcomes
- Startup health and remediation metrics emitted.
- Actionable alarms and operator runbooks.
- Validated rollback process.

Consumer-alignment requirements
- Signals must reflect real consumer-facing readiness and failure causes.

System-integration requirements
- Alerting noise budget and severity mapping tuned for production.

Validation/tests
- Synthetic startup failures trigger correct alarms.
- Rollback drill restores stable state.

Deliverables
- Observability artifacts + runbooks + test evidence.

Completion criteria
- Startup incidents are detectable, diagnosable, and recoverable.

Ambiguity protocol
- Stop if incident ownership/escalation model is undefined.
```

---

## Task 16 — Remove unnecessary MLP coupling from monthly/backfill orchestration

```text
Implement Task 16 from the fixes plan.

Mandatory orientation before coding (required for this task)
- Inspect monthly/backfill SFN definitions and identify unconditional MLP lookups.

Mission
- Eliminate non-essential MLP dependencies from DPP-owned flows.

Issues
- F5.1, IDP-9.

In-scope
- Monthly/backfill SFN MLP lookup states and validation contracts.

Prerequisites
- Tasks 0–15 complete.

Required outcomes
- Monthly and backfill can run with DPP-only config when MLP involvement is not required.
- RT/training retain strict MLP requirements where needed.

Consumer-alignment requirements
- Removal of MLP coupling must not alter any downstream payload/schema consumed by RT/training.

System-integration requirements
- Prevent accidental regression that weakens required MLP checks in ML-owned paths.

Validation/tests
- monthly and backfill positive tests without ml_project_name.
- RT/training negative tests when MLP contracts absent.

Deliverables
- Updated orchestration definitions + tests.

Completion criteria
- Non-ML flows are decoupled from non-essential MLP dependencies.

Ambiguity protocol
- Stop if any monthly/backfill sub-step is reclassified as MLP-owned during implementation.
```
