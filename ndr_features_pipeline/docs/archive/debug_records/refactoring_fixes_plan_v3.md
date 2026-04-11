# Refactoring Fixes Plan v3

## Scope
This document contains:
1. A cleaned and implementation-ready findings/fixes report, based on the latest code/config audit.
2. A complete, ordered implementation task plan with detailed, build-level instructions.

This v3 plan intentionally removes non-essential narrative and focuses on deterministic, executable engineering guidance.

---

## Part A — Findings and fixes report (implementation-grade)

## Flow 1 — Bootstrap orchestration

### Finding B1 — Bootstrap state machine uses JSONata expressions without explicit QueryLanguage

#### Problem found
`docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json` uses JSONata syntax (`{% ... %}` and `Assign`) but does not declare top-level `"QueryLanguage": "JSONata"`, unlike the other orchestrators.

#### Suggested fix
Add top-level `"QueryLanguage": "JSONata"` to bootstrap SFN and lock this requirement with tests.

#### Why this is the correct fix
- It aligns bootstrap with the rest of the state machines and with the existing expression style.
- It removes interpreter ambiguity and deployment/runtime fragility.
- It is the smallest coherent fix: no interface or business-logic changes, only explicit semantics.

#### Detailed build instructions
1. Update bootstrap JSON definition root:
   - Add `"QueryLanguage": "JSONata"` immediately under `"Comment"`.
2. Add/update tests to enforce this contract:
   - state-machine contract test that fails if bootstrap has JSONata expressions but no JSONata QueryLanguage.
3. Keep all state names, transitions, input/output payload contracts unchanged.
4. Ensure deployment rendering/substitution tooling preserves this field.

#### Connected components
- Producer: bootstrap SFN JSON definition.
- Consumer: AWS Step Functions evaluator + any deployment tooling.

#### Inputs/Outputs impact
- No input/output schema change.
- Only execution semantics declaration becomes explicit.

---

## Flow 2 — Monthly baselines orchestration

### Finding M1 — Monthly readiness pipeline is invoked, but decision is read from input payload instead of computed artifact

#### Problem found
Monthly flow starts readiness pipeline but then evaluates `$states.input.monthly_fg_b_readiness` instead of pipeline-produced output artifact.

#### Suggested fix (required option)
**Use Option 1**: readiness pipeline writes deterministic readiness artifact (S3 manifest and/or DDB item), and the SFN polls/reads it and evaluates readiness from that artifact.

#### Why this is the correct fix
- Enforces deterministic and reproducible gating decisions from source-of-truth compute output.
- Prevents false-ready progression caused by trigger payload contamination/staleness.
- Preserves the system’s design principle: runtime payloads are minimal and business decisions come from computed contracts/DDB-owned config.

#### Detailed build instructions
1. Define canonical readiness artifact contract `monthly_fg_b_readiness.v3`:
   - Required fields: `contract_version`, `project_name`, `feature_spec_version`, `reference_month`, `ready`, `required_families`, `missing_ranges`, `decision_code`, `as_of_ts`, `idempotency_key`.
2. Implement readiness checker as SageMaker Pipeline Processing Step (Python/PySpark):
   - Inputs: `ProjectName`, `FeatureSpecVersion`, `ReferenceMonth`, `ReadinessCycle`.
   - Reads DDB + Batch Index + required artifact locations.
   - Writes deterministic artifact at stable key (or DDB key) parameterized by cycle.
3. Monthly SFN changes:
   - Compute readiness step -> poll/read artifact -> evaluate.
   - If not ready and `cycle=0`: build remediation request -> invoke Backfill -> increment cycle -> recompute.
   - If still not ready after recompute: hard fail with explicit error.
4. Remove any branch that trusts `states.input.monthly_fg_b_readiness` as authoritative signal.
5. Add fail-fast contract checks:
   - missing artifact,
   - stale/incorrect contract version,
   - missing required fields.

#### Connected components
- Producer: monthly readiness processing step.
- Consumer: monthly SFN dependency gate/remediation path.

#### Inputs/Outputs impact
- Runtime payload shrinks in authority (no external readiness injection).
- New deterministic readiness artifact becomes gate input.

---

## Flow 3 — Real-time orchestration

### Finding R1 — No raw ingestion fallback path before delta stage

#### Problem found
RT orchestration requires `raw_parsed_logs_s3_prefix` and proceeds directly; there is no integrated resolution path to Redshift fallback when ingestion data is missing/incomplete.

#### Suggested fix
Add deterministic pre-delta raw-input resolution using `RawInputResolver` and fallback contracts in DPP.

#### Why this is the correct fix
- Satisfies the global requirement for raw-reading flows: fallback to Redshift when ingestion is unavailable.
- Reuses existing resolver/fallback primitives already present in code (simpler and less risky than new custom logic).
- Keeps design coherent: DDB-owned fallback policy/query config, minimal runtime payload, explicit provenance.

#### Detailed build instructions
1. Introduce RT raw-input resolution stage (SageMaker Pipeline Processing Step, Python/PySpark) before delta processing:
   - Inputs: `ProjectName`, `FeatureSpecVersion`, `MiniBatchId`, `BatchStartTsIso`, `BatchEndTsIso`, candidate raw prefix.
   - Uses `RawInputResolver`.
2. Resolver behavior:
   - If ingestion present -> `source_mode=ingestion`.
   - Else if fallback enabled + valid query contract -> execute fallback and return fallback prefix.
   - Else fail fast with explicit error codes:
     - `RAW_INPUT_FALLBACK_DISABLED`
     - `RAW_INPUT_FALLBACK_QUERY_CONTRACT_MISSING`
     - `RAW_INPUT_FALLBACK_EMPTY_RESULT`
3. Feed resolved prefix to delta stage.
4. Persist provenance to Batch Index for auditability:
   - `source_mode`, `resolution_reason`, request id, resolved timestamp.
5. Ensure retries are bounded and idempotency key is deterministic per batch window.

#### Connected components
- Producer: RT raw-input resolver step.
- Consumer: delta stage + Batch Index status/provenance writers.

#### Inputs/Outputs impact
- Adds resolved raw-input contract object.
- Existing downstream field (`raw_parsed_logs_s3_prefix`) preserved, now canonicalized.

---

### Finding R2 — RT readiness pipeline invoked, but decision is read from input payload

#### Problem found
RT readiness follows the same anti-pattern as monthly: pipeline call exists, but decision uses `$states.input.rt_artifact_readiness`.

#### Suggested fix
Adopt deterministic readiness artifact model (parallel to M1): pipeline writes artifact; SFN polls/reads/evaluates it.

#### Why this is the correct fix
- Unifies readiness semantics between monthly and RT, reducing drift.
- Prevents stale/forged payload readiness.
- Maintains deterministic remediation behavior under replay/retry.

#### Detailed build instructions
1. Define canonical contract `rt_artifact_readiness.v3`:
   - Fields: `contract_version`, `project_name`, `feature_spec_version`, `ml_project_name`, `mini_batch_id`, `ready`, `required_families`, `missing_ranges`, `decision_code`, `as_of_ts`, `idempotency_key`.
2. Implement readiness checker as SageMaker Processing Step (Python/PySpark).
3. RT SFN sequence:
   - compute -> read artifact -> evaluate.
   - if missing and `cycle=0`: call backfill -> recompute -> evaluate.
   - unresolved after recompute -> fail with explicit gate error.
4. Remove authoritative dependency on `states.input.rt_artifact_readiness`.

#### Connected components
- Producer: RT readiness checker.
- Consumer: RT SFN gate and backfill invocation branch.

#### Inputs/Outputs impact
- Same structural pattern as monthly; consistent operator mental model.

---

## Flow 4 — Backfill provider

### Finding BF1 — `requested_families` from consumers is not honored end-to-end

#### Problem found
Backfill consumers provide family intent, but extractor/manifest generation still hardcodes broad family sets, causing over-execution.

#### Suggested fix
Make `requested_families` a strict first-class contract from request -> extractor -> manifest -> dispatcher -> completion verifier.

#### Why this is the correct fix
- Aligns backfill with service-provider intent: execute exactly what consumer requests.
- Improves efficiency and lowers blast radius.
- Eliminates false “success” from unrelated family executions.

#### Detailed build instructions
1. Evolve backfill request contract to include validated `requested_families` list (required for consumer-guided mode).
2. Pass `requested_families` through backfill SFN into extractor step input.
3. Update historical extractor:
   - build family ranges only for requested families.
   - emit manifest containing only requested families.
4. Update dispatcher/verifier:
   - reject unsupported families immediately.
   - verify completion only for requested families.
   - block completion event on unresolved requested family.
5. Keep dependency ordering logic explicit and deterministic.

#### Connected components
- Producers: RT/monthly/bootstrap/training backfill request builders.
- Consumer/provider: backfill SFN + extractor + dispatcher + completion verifier.

#### Inputs/Outputs impact
- Adds strict family scope semantics to existing backfill contract.

---

## Flow 5 — Training orchestrator

### Current status
No critical new structural malfunction identified in this audit pass; training mode support and MLP reciprocal checks are present.

### Required alignment work
Training remains in scope for contract alignment when backfill/readiness contracts are updated, to prevent partial drift in producer/consumer interfaces.

---

## Flow 6 — Per-step SageMaker code consumption and deployment packaging

### Finding SC1 — Current per-step code contract is script-path-centric and not aligned with AWS-recommended bundle-based consumption

#### Problem found
Current pipeline code resolution is `code_prefix_s3 + entry_script` and pipeline steps execute module-based commands (`python -m ndr.scripts...`). This design assumes the full `ndr` package is already present at runtime and does not make step artifacts self-sufficient from the declared code contract alone.

Concretely:
- DDB pipeline specs define `scripts.steps.<Step>.code_prefix_s3` + `entry_script`.
- Resolver returns a single script URI.
- Processing steps consume that URI while still relying on internal package imports.

This causes a contract gap: designated step code is not truly consumable as an isolated deploy artifact unless runtime image/environment already includes matching project package content.

In addition, there is no dedicated deployment flow/state machine in the active flow inventory to automate code-artifact build/publish/promote/verify lifecycle. That leaves artifact release behavior partially implicit and operationally fragile.

#### Suggested fix
Adopt AWS-recommended code consumption model:
- Per step, provide a versioned source bundle artifact in S3 (zip/tar.gz) that includes the step entry script and all required project modules for that step.
- Keep an explicit entry-point field and artifact metadata (`artifact_build_id`, `artifact_sha256`) in DDB.
- Have pipeline steps consume the bundle artifact deterministically, not bare script-path assumptions.
- Add a dedicated deployment flow orchestrated by Step Functions, where each code-executing stage is implemented as a SageMaker Pipeline Step (Python/PySpark), to automate build/publish/promote/verify/rollback.
- Use a hybrid rollout strategy:
  - Phase A (bootstrap seed): one-time/manual artifact seeding from SageMaker JupyterLab notebook to break first-deploy recursion.
  - Phase B (steady state): dedicated deployment Step Function performs all subsequent automated builds/promotions/rollbacks.

#### Why this is the correct fix
- Aligns code consumption to explicit deploy artifacts rather than implicit image/package coupling.
- Reduces runtime drift risk between repo source and deployed code.
- Preserves existing per-step ownership model already present in DDB, while making artifact semantics explicit.
- Avoids fragile monolithic hand-maintained single-file scripts; build artifacts remain generated from canonical modular source.
- Separates release-control responsibilities from runtime bootstrap/business flows, improving operability and failure isolation.
- Enables deterministic promotion and rollback with auditable metadata.
- Hybrid rollout minimizes initial cutover risk while converging to fully automated, repeatable operations.

#### Detailed build instructions
1. Extend step code contract schema in DDB:
   - Keep `code_prefix_s3` for path ownership.
   - Add required fields:
     - `code_artifact_s3_uri` (immutable bundle path),
     - `entry_script` (path within artifact),
     - `artifact_build_id`,
     - `artifact_sha256`,
     - optional `artifact_format` (`tar.gz|zip`).
2. Build pipeline for code artifacts:
   - From canonical source (`src/ndr/...`), generate per-step bundle artifacts.
   - Include step entry script + required internal modules.
   - Write artifact manifest with hash/build metadata.
3. Pipeline definitions/code resolver:
   - Update resolver to return artifact URI + entry script contract, not just script URI concatenation.
   - Update step execution wiring so each step runs from the artifact contract deterministically.
4. Validation and fail-fast behavior:
   - Reject missing/placeholder artifact metadata.
   - Reject mismatched artifact hash.
   - Reject contract rows missing entry script.
5. Operability protections:
   - Artifacts are generated-only (no manual edits).
   - One-way flow: source -> build -> artifact -> deploy.
   - CI enforces source/artifact hash coherence.
6. Add dedicated deployment flow (`sfn_ndr_code_deployment_orchestrator`) with ordered states:
   - `ResolveDeploymentRequest` (normalize project/version/revision/build id),
   - `LoadDeploymentConfigFromDdb`,
   - `StartCodeBundleBuildPipeline` (SageMaker pipeline; Python/PySpark packaging step),
   - `Wait/DescribeBuildPipeline`,
   - `StartArtifactValidationPipeline` (hash/manifest/entry-script verification),
   - `Wait/DescribeValidationPipeline`,
   - `PromoteArtifactPointers` (DDB atomic update to new artifact metadata),
   - `StartDeploymentSmokeValidationPipeline` (import/runtime smoke for all step contracts),
   - `Wait/DescribeSmokeValidationPipeline`,
   - `CommitDeploymentCheckpoint` or `RollbackArtifactPointers` on failure.
7. Deployment rollback contract:
   - Persist `previous_artifact_*` metadata before pointer promotion.
   - On post-promotion failure, execute atomic rollback to prior metadata.
8. Runtime bootstrap interaction:
   - Bootstrap checks deployment checkpoint/status only; it does not build artifacts.
   - If deployment status not READY, fail fast with explicit deployment-prerequisite error.
9. Hybrid rollout details (required):
   - Phase A (manual bootstrap seed in deployment notebook):
     - Build per-step bundles from canonical repository source.
     - Compute and persist `artifact_build_id` + `artifact_sha256`.
     - Upload to versioned S3 artifact paths.
     - Write DDB artifact pointers and deployment status/checkpoint as `READY`.
     - Run smoke checks for each step entry script import/launch.
   - Phase B (automated steady state):
     - All future builds/promotions executed only through deployment Step Function.
     - Manual notebook build path is retained for emergency break-glass only and must emit audit marker when used.
10. Cutover criteria from Phase A -> Phase B:
   - At least one successful end-to-end automated deployment execution.
   - Successful rollback drill restoring prior pointers.
   - Runtime precondition checks validated in bootstrap/operational flows.
   - CI checks enforce artifact metadata completeness and hash verification.

#### Connected components
- Producers:
  - DDB spec seeding/updating logic for `scripts.steps`.
  - Artifact build job that publishes per-step bundles.
  - Dedicated deployment Step Function + deployment SageMaker pipelines.
- Consumers:
  - `resolve_step_code_uri`/contract resolver layer.
  - Pipeline definition builders for all pipelines.
  - Runtime step launch behavior in ProcessingStep definitions.
  - Bootstrap/operational flows that consume deployed artifact pointers.

#### Inputs/Outputs impact
- Inputs (control plane): explicit bundle URI/hash metadata added.
- Runtime behavior: designated step code is consumed from immutable artifact contract.
- S3 schema impact: minimal extension only (versioned artifact subtree under existing per-step code prefixes).
- New flow artifact: deployment status/checkpoint records in DDB for promote/rollback state tracking.

---

## Highest-priority implementation order
1. B1 (bootstrap JSONata explicit declaration)
2. M1 + R2 (deterministic readiness artifact model)
3. R1 (RT raw fallback integration)
4. BF1 (`requested_families` end-to-end honoring)
5. SC1 (per-step code consumption alignment with AWS-recommended approach)
6. Cross-flow producer/consumer alignment and hardening

---

## Part B — Ordered implementation tasks (detailed prompts)

> **Mandatory instruction block (must be included and enforced in EACH task):**
> - Implement producer and consumer changes together for each contract touched.
> - Prefer the simplest design that preserves deterministic behavior, and that is coherent with the holistic system’s design.
> - Enforce DDB-first business configuration, minimal runtime payloads.
> - Fail fast with explicit error codes/messages for contract violations.
> - Include idempotency, retry, and rollback considerations in implementation, implementing in such a way to promote a complete working system that includes aligning contracts between provider and consumer.
> - Conclude the task by updating all of the documentation files of the project according to the implemented changes.
> - If a new execution component is needed, implement it only as a SageMaker Pipeline Processing Step running Python/PySpark (no Lambda alternatives).

---

## Task 0 — Freeze v3 contracts and error taxonomy

### Mandatory reading
- `docs/archive/debug_records/refactoring_fixes_plan_v3.md` (Part A and Part B).
- `docs/archive/debug_records/refactoring_fixes_plan_v2.md`.
- `docs/archive/debug_records/refactoring_fixes_plan_v2_prompts.md`.
- `docs/archive/debug_records/refactoring_plan_implementation_ready.md`.
- `docs/archive/debug_records/flow_audit_issues_fixes_and_execution_plan.md`.
- `docs/step_functions_jsonata/sfn_ndr_*.json`.
- `src/ndr/processing/raw_input_resolver.py`.
- `src/ndr/processing/backfill_redshift_fallback.py`.
- `src/ndr/processing/historical_windows_extractor_job.py`.

### Finding/issue (detailed)
Repeated rework indicates unresolved interface drift across readiness, fallback, and backfill-family contracts.

### Fix objective
Create one authoritative, machine-validated v3 contract matrix and error taxonomy before implementation tasks proceed.

### Detailed build instructions
1. Add/update machine-readable schemas:
   - `monthly_fg_b_readiness.v3`
   - `rt_artifact_readiness.v3`
   - backfill request v2 with `requested_families` semantics.
2. Add explicit error codes and retriable flags for all new failure paths.
3. Add validators and CI tests to fail on undeclared field drift.
4. Map each field to producer + consumer ownership.

### Validation/tests
- Schema pass/fail tests.
- Unknown-field and missing-field tests.
- Contract drift CI test.

### Deliverables
- Schemas, validators, taxonomy, and tests.

### Completion criteria
No downstream task can implement against ambiguous or unstated interfaces.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 1 — Bootstrap JSONata semantics hardening (B1)

### Mandatory reading
- `docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json`.
- `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`.
- `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`.
- Related contract tests in `tests/` for Step Functions definitions.

### Finding/issue (detailed)
Bootstrap expression semantics are under-specified due missing QueryLanguage declaration while JSONata syntax is used.

### Fix objective
Make bootstrap execution semantics explicit and enforceable.

### Detailed build instructions
1. Add `"QueryLanguage":"JSONata"` to bootstrap root definition.
2. Add test that fails if any JSONata expression exists without declared QueryLanguage.
3. Ensure deployment tooling preserves the field.
4. Keep runtime contracts untouched.

### Validation/tests
- Step Functions contract tests including bootstrap.

### Deliverables
- Bootstrap JSON update + tests.

### Completion criteria
Bootstrap JSONata semantics are explicit and stable across deployment/runtime.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 2 — Deterministic monthly readiness artifact flow (M1, Option 1)

### Mandatory reading
- `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`.
- Monthly readiness-related pipeline definitions and processing scripts.
- Backfill request/remediation contracts.

### Finding/issue (detailed)
Monthly flow computes nothing authoritative for readiness at gate-time; it reads payload-injected manifest.

### Fix objective
Implement strict compute->artifact->evaluate gating with one remediation cycle and deterministic recheck.

### Detailed build instructions
1. Implement monthly readiness checker as SageMaker Processing Step (Python/PySpark):
   - input params: project/version/reference_month/readiness_cycle.
   - compute required-family completeness from Batch Index + artifact existence checks.
2. Write canonical readiness artifact (`monthly_fg_b_readiness.v3`) to deterministic key.
3. Update monthly SFN:
   - start checker pipeline,
   - poll/read artifact,
   - evaluate,
   - remediate via backfill if needed,
   - recompute once,
   - fail if unresolved.
4. Remove authoritative use of input readiness payload.
5. Add explicit error codes for missing/invalid/stale readiness artifact.

### Validation/tests
- ready path.
- remediation success path.
- unresolved after recheck fail path.
- stale/external payload injection ignored.

### Deliverables
- Updated monthly checker, SFN, contracts, tests.

### Completion criteria
Monthly dependency gate decisions are deterministic and source-derived.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 3 — Deterministic RT readiness artifact flow (R2)

### Mandatory reading
- `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`.
- RT readiness-related pipeline and processing definitions.
- Backfill remediation contract builders.

### Finding/issue (detailed)
RT readiness gate currently trusts input payload instead of computed readiness output.

### Fix objective
Use same deterministic artifact gating model as monthly.

### Detailed build instructions
1. Implement RT readiness checker as SageMaker Processing Step (Python/PySpark):
   - inputs: project/version/ml_project/batch bounds/mini_batch_id/readiness_cycle.
2. Write canonical artifact `rt_artifact_readiness.v3` with required fields.
3. Update RT SFN flow to:
   - compute readiness,
   - read/evaluate artifact,
   - backfill if missing,
   - recompute,
   - fail unresolved.
4. Remove authoritative dependence on `states.input.rt_artifact_readiness`.
5. Add explicit errors for contract/version/field validation failures.

### Validation/tests
- ready path.
- missing->remediate->ready path.
- unresolved path fails.
- stale payload does not override computed artifact.

### Deliverables
- Updated RT readiness checker, SFN logic, tests.

### Completion criteria
RT gate decisions become deterministic and consistent with monthly semantics.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 4 — RT raw ingestion fallback integration (R1)

### Mandatory reading
- `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`.
- `src/ndr/scripts/run_delta_builder.py`.
- `src/ndr/processing/delta_builder_job.py`.
- `src/ndr/processing/raw_input_resolver.py`.
- `src/ndr/processing/backfill_redshift_fallback.py`.
- Batch Index write/read contracts.

### Finding/issue (detailed)
RT raw-reader path has no integrated fallback when ingestion artifacts are missing.

### Fix objective
Add canonical pre-delta input resolution with ingestion/fallback parity and provenance.

### Detailed build instructions
1. Add RT pre-delta resolver step as SageMaker Processing Step (Python/PySpark).
2. Resolve source using DPP fallback policy + queries via `RawInputResolver`.
3. Return canonical resolved prefix and source metadata.
4. Feed resolved prefix into delta stage.
5. Persist source metadata in Batch Index status/provenance.
6. Enforce explicit fail-fast errors for disabled/invalid/empty fallback.
7. Ensure idempotent fallback execution keying by batch identity.

### Validation/tests
- ingestion present.
- ingestion missing + fallback enabled.
- fallback disabled error.
- missing query contract error.
- empty fallback result error.

### Deliverables
- Resolver integration, SFN/pipeline wiring updates, tests.

### Completion criteria
RT raw-reader contract fully satisfies ingestion/fallback requirement.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 5 — Backfill `requested_families` end-to-end honoring (BF1)

### Mandatory reading
- `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`.
- `src/ndr/processing/historical_windows_extractor_job.py`.
- `src/ndr/orchestration/backfill_contracts.py`.
- `src/ndr/orchestration/backfill_execution_contract.py`.
- `src/ndr/orchestration/backfill_family_dispatcher.py`.

### Finding/issue (detailed)
Requested family scope is dropped during backfill planning/execution, causing overbroad execution.

### Fix objective
Make family scope contract-preserving from consumer request to completion verdict.

### Detailed build instructions
1. Backfill request schema:
   - validate `requested_families` required (for consumer-guided mode).
2. SFN wiring:
   - pass requested families into extractor pipeline params.
3. Extractor update:
   - generate family_ranges only for requested families.
   - emit manifest map_items for requested families only.
4. Dispatcher/verifier update:
   - reject unsupported family.
   - execute only requested families.
   - completion verifier gates success only on requested families.
5. Event emission:
   - include executed families and unresolved list in detail.

### Validation/tests
- requested subset execution.
- unsupported family rejection.
- completion blocked on unresolved requested family.

### Deliverables
- Updated schema/wiring/extractor/dispatcher/verifier/tests.

### Completion criteria
Backfill behaves as a true consumer-scoped service provider.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 6 — Cross-flow producer/consumer alignment and hardening

### Mandatory reading
- Updated contracts/schemas from Tasks 0–5.
- Monthly, RT, bootstrap, training, and backfill SFN definitions.
- Runtime validators and status writers.

### Finding/issue (detailed)
Historically, fixes were partial: producer/consumer drift and inert fields remained.

### Fix objective
Guarantee no touched interface has producer-only or consumer-only fields.

### Detailed build instructions
1. For each touched contract, create producer->consumer mapping table in code/tests.
2. Remove deprecated/unused fields or make them explicit metadata no-ops with validation.
3. Add strict validators in all entrypoints:
   - reject unknown fields,
   - reject missing required fields,
   - reject incompatible contract versions.
4. Add idempotency/retry/rollback assertions for new paths.

### Validation/tests
- cross-flow integration tests for touched interfaces.
- matrix conformance tests.
- replay/idempotency tests.

### Deliverables
- aligned payload builders, validators, tests, and migration notes.

### Completion criteria
No contract drift remains on touched paths.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 7 — Final release gate for v3 fixes

### Mandatory reading
- Entire `docs/archive/debug_records/refactoring_fixes_plan_v3.md`.
- Existing release/integration gates in `tests/` and startup scripts.

### Finding/issue (detailed)
Repeated rework indicates final acceptance gates were not strict enough to block partial implementation.

### Fix objective
Enforce final gate criteria that prevent recurrence of these exact failure classes.

### Detailed build instructions
1. Add release-gate checks that fail on:
   - missing QueryLanguage in JSONata SFNs,
   - readiness-from-input anti-pattern,
   - requested_families drop/ignore,
   - RT fallback contract violations.
2. Require passing targeted integration suite for monthly/RT/backfill/bootstrap/training.
3. Add explicit rollback readiness checklist for touched flows.
4. Block release approval if any correctness-critical check is warning-only.

### Validation/tests
- full targeted pytest matrix.
- contract drift checks.
- startup/readiness gate checks.

### Deliverables
- release gate updates + passing evidence + release recommendation.

### Completion criteria
v3 changes are releasable with deterministic, contract-safe behavior.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 8 — Implement AWS-recommended per-step code consumption model (SC1)

### Mandatory reading
- `src/ndr/pipeline/io_contract.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_backfill_historical_extractor.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_backfill_15m_reprocessing.py`.
- `src/ndr/scripts/create_ml_projects_parameters_table.py`.
- All `src/ndr/scripts/run_*.py` step entry scripts.

### Finding/issue (detailed)
Current designated step code consumption is script-path-centric (`code_prefix_s3 + entry_script`) while runtime execution depends on internal package imports (`ndr.*`). This is not a robust deploy artifact contract and is vulnerable to package/image drift.

### Fix objective
Make designated code consumption explicit, immutable, and artifact-driven per step, using AWS-recommended bundle-based script consumption.

### Detailed build instructions
0. Hybrid execution model (must implement explicitly):
   - Phase A bootstrap seed (manual notebook-assisted, one-time):
     - build and publish initial per-step bundles from canonical repo source,
     - write artifact metadata and pointers,
     - set deployment status/checkpoint to READY,
     - run smoke checks and capture evidence.
   - Phase B steady-state automation:
     - all subsequent bundle builds/promotions occur through deployment SF (Task 9),
     - manual path is break-glass only with explicit audit marker.
1. Contract/schema changes (DDB `scripts.steps.<step>`):
   - Keep existing `code_prefix_s3`.
   - Add required fields:
     - `code_artifact_s3_uri`,
     - `entry_script`,
     - `artifact_build_id`,
     - `artifact_sha256`,
     - `artifact_format`.
   - Validate that placeholder markers are rejected.
2. Build/deploy artifact generator:
   - Add deployment build logic to produce per-step source bundles (zip/tar.gz) from canonical repository sources.
   - Include entry script and required internal modules used by that step.
   - Publish bundles under versioned subpaths beneath each step’s existing code prefix:
     - `<code_prefix_s3>/artifacts/<artifact_build_id>/source.<ext>`.
   - Publish sidecar manifest:
     - build id, sha256, source revision, generation timestamp.
3. Resolver and pipeline builder changes:
   - Replace script-URI-only resolution with artifact contract resolution.
   - Update pipeline step wiring to consume artifact + entry script deterministically.
   - Ensure all pipelines (15m, dependent, fg_b, machine inventory, inference, join, training, backfill extractor, backfill executor) use the same contract.
4. Runtime launch behavior:
   - Execute designated entry script from extracted artifact context.
   - Do not rely on hidden package availability outside artifact/image contract.
5. Validation and fail-fast rules:
   - Fail on missing artifact contract fields.
   - Fail on hash mismatch.
   - Fail on missing entry script inside artifact.
   - Fail on unresolved module imports caused by incomplete artifact packaging.
6. Backward compatibility and rollout:
   - Add a temporary dual-read mode (old fields + new fields) behind a strict migration flag.
   - After migration verification, remove legacy script-only resolution path.
7. Observability and rollback:
   - Emit artifact build id/hash in job metadata and status records.
   - Rollback by atomically repointing to previous known-good artifact metadata.
8. Deployment flow contract hook:
   - Add deployment status contract fields consumed by dedicated deployment flow:
     - `deployment_status`,
     - `deployment_checkpoint`,
     - `deployment_last_build_id`,
     - `deployment_last_error`,
     - `deployment_updated_at`.
   - Ensure runtime flows read only promoted READY artifact pointers.
9. Notebook bootstrap seed specification (Phase A) must include:
   - exact notebook cell sequence,
   - deterministic naming format for `artifact_build_id`,
   - hash generation algorithm and storage field mapping,
   - required validation outputs/logs before setting READY.
10. Break-glass policy:
   - document explicit conditions when manual notebook artifact publish is allowed,
   - require post-incident reconciliation by rerunning automated deployment flow.

### Validation/tests
- Contract schema tests for new step artifact fields.
- Resolver tests for success/failure/hash mismatch paths.
- Pipeline definition tests asserting artifact contract consumption across all pipelines.
- Integration smoke tests that each step runs from artifact bundle.
- Migration tests for dual-read mode and cutover completion.
- Runtime preflight tests proving non-READY deployment status blocks operational runs with explicit error code.
- Hybrid-mode tests:
  - Phase A seed path sets valid READY pointers and artifacts.
  - Phase B automated path supersedes manual seed without drift.
  - Break-glass manual run emits required audit marker and is reconciled.

### Deliverables
- Updated step code contract schema + resolver + pipeline builders + artifact build/deploy flow + tests + rollout docs.

### Completion criteria
All SageMaker Pipeline steps consume designated code via explicit immutable artifacts with deterministic metadata validation; hybrid rollout is completed with automated steady state active and manual path restricted to audited break-glass usage.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Task 9 — Add dedicated deployment flow orchestrated by Step Functions for code-artifact lifecycle

### Mandatory reading
- `docs/archive/debug_records/refactoring_fixes_plan_v3.md` (SC1 + Task 8 + execution order).
- `docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_*.py`.
- `src/ndr/pipeline/io_contract.py`.
- `src/ndr/scripts/create_ml_projects_parameters_table.py`.
- Deployment assets under `src/ndr/deployment/`.

### Finding/issue (detailed)
Code-artifact lifecycle is not orchestrated by a dedicated deployment flow, so build/publish/promote/verify/rollback behavior is not first-class, deterministic, and isolated from runtime business flows.

### Fix objective
Implement a dedicated deployment flow (`sfn_ndr_code_deployment_orchestrator`) that automates per-step artifact lifecycle using only SageMaker Pipeline Steps (Python/PySpark) for code-executing stages.

### Detailed build instructions
0. Hybrid rollout prerequisite and transition:
   - Assume Phase A bootstrap seed (Task 8) is complete and pointers are READY.
   - Deployment SF initial runs must operate in shadow/verify mode before taking promotion authority.
   - Promote to authoritative mode only after shadow success criteria are met.
1. Define deployment flow state machine (`sfn_ndr_code_deployment_orchestrator`) with states:
   - `NormalizeDeploymentRequest`,
   - `LoadDeploymentConfigFromDdb`,
   - `StartCodeBundleBuildPipeline`,
   - `DescribeCodeBundleBuildPipeline` (+ wait/retry loop),
   - `StartArtifactValidationPipeline`,
   - `DescribeArtifactValidationPipeline` (+ wait/retry loop),
   - `PromoteArtifactPointers`,
   - `StartDeploymentSmokeValidationPipeline`,
   - `DescribeDeploymentSmokeValidationPipeline` (+ wait/retry loop),
   - `CommitDeploymentReady`,
   - `RollbackArtifactPointers` (on any post-promotion failure),
   - terminal success/fail states with explicit error codes.
2. Implement required SageMaker pipelines (all Python/PySpark steps):
   - `pipeline_code_bundle_build`:
     - packages per-step source bundles from canonical repo sources,
     - uploads to versioned S3 artifact paths,
     - writes artifact manifests/hashes.
   - `pipeline_code_artifact_validate`:
     - validates hash, manifest schema, entry-script presence, and metadata completeness.
   - `pipeline_code_smoke_validate`:
     - executes import/entrypoint/runtime smoke checks against promoted metadata.
3. DDB schema and promotion logic:
   - Add deployment-status/checkpoint fields and previous-pointer fields.
   - Promotion must be atomic and conditional (optimistic concurrency).
   - Persist previous metadata before pointer switch to support rollback.
4. Runtime flow integration:
   - Bootstrap and operational SFs perform deployment-precondition check:
     - require `deployment_status=READY`,
     - fail fast with explicit deployment prerequisite error otherwise.
   - Operational flows do not build artifacts.
5. Idempotency/retry:
   - deterministic deployment request id/build id keys,
   - safe retries on throttling/transients,
   - non-retriable fail-fast on contract/hash violations.
6. Observability:
   - Emit deployment lifecycle events (build started/validated/promoted/rolled-back),
   - include build id, artifact hashes, and failure reasons.
7. Hybrid transition controls:
   - Add deployment mode flag: `shadow|authoritative`.
   - In `shadow` mode, deployment SF executes full build/validate/smoke but does not mutate pointers.
   - In `authoritative` mode, promotion/rollback states are enabled.
8. Manual break-glass integration:
   - SF reads last-manual-deployment marker from DDB.
   - If marker detected, enforce reconciliation run before allowing next normal promotion.
9. Ownership boundaries:
   - Bootstrap and runtime SFs are consumers of deployment status only.
   - Deployment SF is sole automated producer of pointer promotions in steady state.

### Validation/tests
- State machine contract tests for all transitions and rollback paths.
- Build/validate/smoke pipeline contract tests.
- Promotion atomicity tests (concurrent update contention).
- Rollback correctness tests (pointer restoration on failure).
- Bootstrap/runtime precondition tests (blocked when deployment not READY).
- End-to-end deployment flow integration test from request -> READY.
- Hybrid transition tests:
  - shadow-mode run succeeds without pointer mutation,
  - authoritative-mode run promotes pointers correctly,
  - manual break-glass marker forces reconciliation gate.

### Deliverables
- New deployment SFN definition, deployment SageMaker pipeline definitions, DDB contract updates, resolver/runtime precondition checks, and complete tests.

### Completion criteria
Code-artifact deployment lifecycle is fully automated, deterministic, auditable, and isolated from runtime business orchestration; hybrid rollout transition is completed, rollback is verified, and steady-state promotion authority resides in deployment SF.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.
- No placeholder runtime field that does not drive behavior is allowed in task outputs.
- No consumer field without a mapped producer is allowed.
- No producer field without a mapped consumer is allowed, unless explicitly validated as metadata no-op.
- All readiness/remediation decisions implemented by the task must be deterministic and auditable from source artifacts.
- All touched paths in task scope must remain idempotent, retry-safe, and rollback-aware.
- Everything that needs to be implemented as code must be implemented only as a SageMaker Pipeline Step running Python/PySpark code; no Lambda-based or other code-running solutions are allowed.

---

## Execution order
1. Task 0
2. Task 1
3. Task 2
4. Task 3
5. Task 4
6. Task 5
7. Task 6
8. Task 8
9. Task 9
10. Task 7

Rationale: freeze contracts first, then semantic correctness of bootstrap/gates, then remediation/fallback and service-provider scope correctness, then code-consumption artifact hardening, then dedicated deployment-flow automation, then final release gating.

---

## Non-negotiable acceptance bar
- No placeholder runtime field that does not drive behavior.
- No consumer field without producer.
- No producer field without consumer (unless explicitly validated metadata no-op).
- Contract violations fail fast with explicit error code and message.
- Readiness/remediation decisions are deterministic and auditable from source artifacts.
- All touched paths are idempotent, retry-safe, rollback-aware.
