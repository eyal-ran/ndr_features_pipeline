# Refactoring Fixes Plan v2

## Scope
This document provides:
1. The flow-audit findings and fixes report (cleaned and reformatted for implementation use).
2. A complete, ordered implementation plan with detailed build instructions per fix.

It intentionally excludes process-noise sections and focuses on actionable engineering content.

---

## Part A — Findings and fixes report (implementation-focused)

## Flow 1 — Monthly Baselines (machine inventory + FG-B)

### Finding 1.1 — Monthly dependency gate is placeholder-driven, not computed
**Problem**
The monthly SFN uses `states.input.baseline_dependency_manifest.missing_ranges` and defaults to empty if not present. This can incorrectly mark readiness and skip remediation.

**Suggested fix**
Replace input-driven readiness with an explicit, deterministic dependency-check step that computes missing ranges from system-of-record data (Batch Index + S3 existence checks).

**Why this is the correct fix**
- Removes hidden dependency on external payload shims.
- Makes readiness deterministic and reproducible for reruns.
- Aligns with DDB-first/source-of-truth principles and prevents false-positive “ready” states.

**How to build it (detailed)**
1. Add a dedicated readiness checker as a SageMaker Processing Pipeline Step (Python/PySpark) that accepts:
   - `project_name`, `feature_spec_version`, `reference_month`.
2. Checker behavior:
   - Resolve required families for monthly baseline (`fg_a`, optionally `delta` when policy requires).
   - Query Batch Index for month/day windows relevant to `reference_month`.
   - Verify required family prefixes/objects exist for each required window.
   - Output canonical manifest:
     ```json
     {
       "contract_version": "monthly_fg_b_readiness.v2",
       "required_families": ["fg_a"],
       "missing_ranges": [{"family":"fg_a","start_ts_iso":"...","end_ts_iso":"...","reason_code":"..."}],
       "check_status": "ready|missing",
       "check_reason": "ready|dependency_missing:*"
     }
     ```
3. Monthly SFN changes:
   - Replace `BuildBaselineDependencyCheckManifest` + fallback logic with checker task output.
   - Keep one remediation cycle + one recheck cycle.
   - Fail hard if still missing after remediation.
4. Tests:
   - complete-data path -> no remediation;
   - missing-data path -> remediation invoked;
   - unresolved-after-remediation -> deterministic fail state.

---

### Finding 1.2 — FG-B outputs are not month-partitioned
**Problem**
FG-B outputs are partitioned by `feature_spec_version` and `baseline_horizon`, but not by month. This conflicts with month-scoped baseline lifecycle and efficient month-level backfills/replays.

**Suggested fix**
Add explicit monthly partitioning to FG-B outputs (`reference_month` or derived `baseline_month`) **without replacing `baseline_horizon`**; enforce contract checks before write so FG-C baseline-horizon expectations stay intact.

**Why this is the correct fix**
- Matches monthly orchestration semantics.
- Enables deterministic month-level replay and rollback.
- Improves query/read performance for month-oriented workflows.
- Preserves existing consumer expectations (including FG-C) that depend on `baseline_horizon` partition semantics.

**How to build it (detailed)**
1. Add a canonical partition field in FG-B output rows:
   - `baseline_month = YYYY-MM` derived from `reference_time_iso` used by FG-B run.
2. Update FG-B writer partition columns to include month:
   - `partition_cols=["feature_spec_version","baseline_horizon","baseline_month"]`.
3. Add validation in FG-B writer:
   - fail if `baseline_month` missing/invalid.
4. Update consumers that read FG-B baselines:
   - include month filter logic where applicable, while preserving `baseline_horizon`-based filtering semantics.
5. Update seed/bootstrap contracts to include expected monthly partitioning key.
6. Add tests for partition path correctness and deterministic reruns.

---

### Finding 1.3 — Machine-inventory producer path is not contract-aligned with FG-B consumer path
**Problem**
Machine inventory writes to its own output prefix, while FG-B mapping loader reads `project_parameters.ip_machine_mapping_s3_prefix`. Without explicit linkage, monthly inventory refresh may not feed FG-B.

**Suggested fix**
Implement **Option 1 (canonical and recommended)**: `project_parameters.ip_machine_mapping_s3_prefix` must always point to the **latest monthly snapshot root produced by the machine inventory job**, and FG-B must resolve mapping input only from this key.

**Why this is the correct fix**
- Eliminates implicit/manual synchronization.
- Prevents stale mapping usage by FG-B.
- Improves consistency and reduces operational errors.

**How to build it (detailed)**
1. Define canonical ownership:
   - Producer owner: machine inventory job writes monthly snapshot datasets.
   - Contract owner: DPP `project_parameters.ip_machine_mapping_s3_prefix`.
   - Consumer owner: FG-B mapping loader reads only that key.
2. Producer write contract:
   - Inventory job writes immutable month snapshots under:
     - `<inventory_root>/snapshot_month=YYYY-MM/`
   - Snapshot must include success marker + manifest with schema hash and row count.
3. Pointer update contract (Option 1 core behavior):
   - After successful snapshot write/validation, set
     - `project_parameters.ip_machine_mapping_s3_prefix = <inventory_root>/snapshot_month=YYYY-MM/`
   - Update must be atomic from consumer perspective (single committed pointer value per month).
4. Consumer read contract:
   - FG-B mapping loader resolves only `ip_machine_mapping_s3_prefix`.
   - Remove/disable any fallback path to legacy or inferred locations.
   - If pointer missing/invalid, fail fast with explicit contract error.
5. Operational behavior:
   - Monthly orchestration order: inventory snapshot success -> pointer update -> FG-B execution.
   - Rollback: pointer can be moved back to prior month snapshot if validation fails post-cutover.
6. Tests:
   - Inventory success updates pointer to new month.
   - FG-B reads exact pointed month path.
   - Invalid/missing pointer fails fast.
   - Rollback pointer is honored deterministically by FG-B on rerun.

---

## Flow 2 — Real-Time (15m + backfill gate + inference + publication)

### Finding 2.1 — RT missing-artifact gate is placeholder-driven
**Problem**
RT readiness manifest (`rt_artifact_readiness_manifest`) is taken from input and defaults to empty, enabling false-ready progression.

**Suggested fix**
Replace with computed readiness task that checks required artifacts (`fg_c` dependencies and any configured families) from Batch Index + S3 evidence.

**Why this is the correct fix**
- Ensures backfill is triggered only when truly needed.
- Prevents silent dependency drift from payload artifacts.
- Aligns with deterministic orchestration and replay semantics.

**How to build it (detailed)**
1. Add RT readiness checker component with inputs:
   - `project_name`, `feature_spec_version`, `mini_batch_id`, `ml_project_name`, `batch_start_ts_iso`, `batch_end_ts_iso`.
2. Resolve required families for RT dependent stage from DPP contract.
3. Verify exact per-batch paths from Batch Index `s3_prefixes`.
4. Produce canonical manifest:
   ```json
   {
     "contract_version":"rt_artifact_readiness.v2",
     "required_families":["fg_c"],
     "missing_ranges":[...],
     "check_status":"ready|missing",
     "idempotency_key":"..."
   }
   ```
5. In SFN:
   - call checker;
   - if missing -> call Backfill;
   - recheck using same checker;
   - fail if unresolved.
6. Add test matrix for ready/missing/recheck-fail paths.

---

### Finding 2.2 — Pair-counts lacks Redshift fallback parity for missing raw logs
**Problem**
Pair-counts requires raw S3 prefix and does not use `RawInputResolver`; if ingestion objects are missing, it fails instead of falling back to Redshift UNLOAD.

**Suggested fix**
Integrate `RawInputResolver` into pair-counts runtime path, with `artifact_family="pair_counts"` and full provenance/status writing.

**Why this is the correct fix**
- Meets system requirement: all raw-log readers must fallback when ingestion is missing.
- Makes pair-counts behavior consistent with delta-builder.
- Reduces RT fragility for partial ingestion gaps.

**How to build it (detailed)**
1. In pair-counts runtime builder:
   - gather ingestion candidate prefix from runtime/batch-index.
   - load DPP `backfill_redshift_fallback` contract.
2. Resolve source with `RawInputResolver.resolve(...)`.
3. Use resolved prefix for input read.
4. Persist source provenance (`ingestion|redshift_unload_fallback`) in status artifact / Batch Index status update.
5. Add explicit error codes:
   - fallback disabled,
   - fallback query missing,
   - fallback empty result.
6. Add tests:
   - ingestion present;
   - ingestion missing + fallback enabled;
   - ingestion missing + fallback disabled.

---

### Finding 2.3 — Bootstrap calls monthly baseline as SageMaker pipeline instead of monthly SFN
**Problem**
Bootstrap state machine (the control-plane state machine that establishes initial deployment readiness and is invoked by RT when readiness is not yet READY) starts `${PipelineNameMonthlyFgBBaselines}` via `startPipelineExecution`, bypassing monthly SFN orchestration semantics.

**Suggested fix**
Switch bootstrap-to-monthly invocation to Step Functions `startExecution` using the monthly SFN ARN.

**Why this is the correct fix**
- Keeps bootstrap focused on readiness orchestration responsibilities (checkpointing, readiness gating, and prerequisite sequencing) while monthly business flow remains in monthly SFN.
- Preserves monthly dependency/remediation logic in one orchestrator.
- Avoids bypassing flow gates.
- Keeps orchestration layering coherent.

**How to build it (detailed)**
1. Replace bootstrap state `StartMonthlyBaselinePipeline` resource:
   - from `arn:aws:states:::aws-sdk:sagemaker:startPipelineExecution`
   - to `arn:aws:states:::states:startExecution.sync:2`.
2. Pass minimal monthly SFN input:
   - `project_name`, `feature_spec_version`, `reference_month`.
3. Remove irrelevant params (`MlProjectName`) from this handoff.
4. Add idempotency execution naming strategy in bootstrap for duplicate-suppression.
5. Add contract test validating bootstrap -> monthly SFN integration.
6. Deployment/config updates:
   - Ensure bootstrap SFN receives `MonthlyStateMachineArn` configuration.
   - Ensure IAM policy for bootstrap execution role includes `states:StartExecution` permission on monthly SFN ARN.
   - Ensure legacy monthly pipeline direct invocation permissions are removed from bootstrap role to prevent regression.

---

## Flow 3 — Training (training/evaluation/production)

### Finding 3.1 — Production mode does not publish a distinct production artifact URI
**Problem**
Publish stage references training artifact path; no explicit production artifact promotion path is guaranteed.

**Suggested fix**
Add explicit production artifact promotion/copy and publish canonical production URI + hash.

**Why this is the correct fix**
- Matches production model lifecycle requirements.
- Enables clean rollback/version pinning.
- Prevents ambiguity between experimental and production artifact locations.

**How to build it (detailed)**
1. Extend IF training spec with production artifact destination root.
2. In production mode:
   - copy model tarball from run artifact path to deterministic production key,
   - include checksum/etag in published metadata,
   - record model version pointer.
3. Publish stage writes:
   - `production_model_uri`, `production_model_hash`, `published_at`, `source_run_id`.
4. Deploy stage consumes `production_model_uri` only.
5. Add tests for:
   - production publish creates expected path,
   - deploy reads production path,
   - rollback pointer to previous model version.

---

### Finding 3.2 — Readiness/reverification can rely on stale manifests
**Problem**
Training gating depends on precomputed manifests/toggles and may skip needed remediation if manifests are stale/incomplete.

**Suggested fix**
Recompute readiness/missing windows from Batch Index at each critical gate (`plan`, `remediate`, `train`).

**Why this is the correct fix**
- Eliminates stale-state gate bypass.
- Ensures each stage decisions reflect current data reality.
- Improves reliability for long-running or retried training runs.

**How to build it (detailed)**
1. Add `recompute_missing_windows()` utility that reads Batch Index + required-family contracts.
2. Invoke at start of `plan`, `remediate`, and pre-`train` gate.
3. Persist recomputed manifest with deterministic version key (`missing_windows_manifest.v2`).
4. Train gate must fail if unresolved required windows remain.
5. Add tests with intentionally stale manifest inputs to verify recomputation wins.

---

## Flow 4 — Backfill (service provider)

### Finding 4.1 — Backfill executor is no-op for non-FG-B families
**Problem**
Backfill executor currently executes only `fg_b_baseline`; other families return placeholder “HandledByBackfill15mPipeline”.

**Suggested fix**
Implement concrete execution dispatch for all supported families (`delta`, `fg_a`, `pair_counts`, `fg_c`, `fg_b_baseline`) with strict failure on unsupported requests.

**Why this is the correct fix**
- Backfill must function as a true dependency provider.
- Prevents false remediation success.
- Enables monthly/RT/training flows to trust remediation outcomes.

**How to build it (detailed)**
1. Extend executor dispatch table into concrete handlers (not direct builder invocation inside dispatcher):
   - `delta` -> dispatches Delta reconstruction execution via existing orchestration entrypoint.
   - `fg_a` -> dispatches FG-A execution, depends on `delta` artifacts for same window.
   - `pair_counts` -> dispatches PairCounts execution with resolved ingestion/fallback input.
   - `fg_c` -> dispatches FG-C execution, depends on `fg_a` + `pair_counts` + baseline availability.
   - `fg_b_baseline` -> dispatches baseline rebuild path (existing behavior retained).
2. Dispatcher responsibilities:
   - Validate requested families are supported.
   - Build per-family execution request payloads from missing-entry manifest.
   - Submit work to orchestration layer (SFN/pipeline entrypoints), not local ad-hoc builder calls.
3. Missing-entry propagation contract:
   - Input from planner/extractor must include normalized missing entries:
     - `family`, `window_start_ts`, `window_end_ts`, `batch_ids[]`, `required_inputs`, `reason_code`.
   - Dispatcher groups missing entries per family and passes exact grouped windows/batches to each family handler.
4. Family handler payload contract (minimum):
   - `project_name`, `feature_spec_version`, `family`, `window_ranges`, `batch_ids`, `correlation_id`, `retry_attempt`.
5. Response aggregation contract:
   - Each handler returns `status`, `execution_arn`, `produced_artifacts`, `failure_reason`, `retryable`.
   - Aggregator composes deterministic overall verdict; any failed requested family => backfill failure.
6. Dependency-safe execution model:
   - Stage 1: `delta`
   - Stage 2 (parallel where allowed): `fg_a`, `pair_counts`
   - Stage 3: `fg_c`
   - Stage 4: `fg_b_baseline` (independent or ordered per policy)
7. Add tests:
   - unsupported family reject;
   - mixed-family request with partial missing entries;
   - dependency ordering correctness;
   - exact missing windows propagated to family payloads;
   - retry/idempotent replay with same `correlation_id`.

---

### Finding 4.2 — Completion event can be emitted despite no real work for requested families
**Problem**
With current placeholder behavior, SFN can emit success event despite non-materialized artifacts.

**Suggested fix**
Gate success event on strict completion contract: all requested families executed and terminal status `Succeeded`.

**Why this is the correct fix**
- Prevents downstream trust in false completion.
- Aligns event semantics with real materialization.

**How to build it (detailed)**
1. In SFN map aggregation, require per-item output contract with explicit family status.
2. Add post-map verifier state:
   - check all requested families present and succeeded.
3. Emit completion event only after verifier pass.
4. Otherwise fail with explicit `BackfillCompletionContractError`.

---

## Cross-flow finding — Control-plane DDB scope ambiguity

### Finding X.1 — Additional operational tables are used but not explicitly formalized
**Problem**
System uses routing/lock/publication-lock tables beyond `dpp_config`, `mlp_config`, `batch_index` without explicit “allowed exceptions” contract.

**Suggested fix**
Formalize the three additional DDB tables as explicit control-plane contracts (routing, processing lock, publication lock) with schema/ownership/validation/observability and deployment-managed lifecycle, so they are first-class, testable infrastructure rather than implicit dependencies.

**Why this is the correct fix**
- Stabilizes current runtime behavior quickly without risky cross-table migration.
- Removes architectural ambiguity by declaring authoritative schemas and owners.
- Makes health/failure states observable and testable.
- Keeps minimal-change path: formalize first, consolidate later only if justified by dedicated migration.

**How to build it (detailed)**
1. Define explicit contract objects for each table:
   - `ml_projects_routing` (or equivalent routing table),
   - `processing_lock`,
   - `publication_lock`.
2. For each table, define and document:
   - table purpose and allowed operations,
   - key schema and required GSIs,
   - TTL/lease/idempotency semantics,
   - producer and consumer ownership boundaries,
   - expected capacity/throughput mode.
3. Implement startup validators (as SageMaker Processing Pipeline Step preflight checks in orchestration flows that depend on these tables):
   - verify table existence and schema shape,
   - verify IAM access permissions required by runtime role,
   - emit explicit non-retriable contract errors for schema/access mismatch.
4. Implement observability contract:
   - structured logs per table operation (`table`, `operation`, `result`, `error_code`),
   - metrics for lock contention, stale lock cleanup, routing misses.
5. Deployment integration changes:
   - add these 3 tables to canonical deployment plan/inventory docs and infrastructure provisioning scripts,
   - add seeding/bootstrap requirements where applicable (e.g., routing records),
   - add pre-deploy validation and post-deploy smoke checks.
6. CI/test integration:
   - contract tests for schema drift and permission failures,
   - integration tests for lock acquire/release/retry and routing lookup behavior.

---

## Part B — Complete implementation plan (ordered, detailed, and task-owned)

> **Task format note:** Every task below restores the implementation-ready task format sections (`Read context`, `Component-behaviour references`, `Role`, `Issues`, `Desired end state`, `Scope/files`, `Checks`, `Completion criteria`) while preserving the newer detailed issue/fix/build content.

> **Mandatory task execution packet format (must be present for every task):**
> 1. Issue coverage and exact fix bullets.
> 2. Context references used during implementation.
> 3. Implementation instructions by file/component and behavior.
> 4. Contract invariants (must remain true).
> 5. Acceptance tests with expected outcomes.
> 6. Out-of-scope list.
> 7. Escalation list (questions requiring direction before coding).

---

### Task 0 — Contract matrix freeze and implementation bootstrap
- **Read context before implementation (mandatory):** Part A Findings 1.1, 1.2, 2.1, 4.1, X.1.
- **Component-behaviour references (mandatory):** SFN input contracts, pipeline parameter mapping, runtime validator layer, DDB contract loaders.
- **Role:** establish non-negotiable contract and error semantics foundation.
- **Issues covered:** cross-flow contract drift; inconsistent error semantics; implicit ownership ambiguity.
- **Desired end state:** one auditable contract matrix and one error taxonomy consumed by all producer and consumer implementations.
- **Scope/files to create or update:** `docs/contracts/flow_contract_matrix_v2.md`, `docs/contracts/error_taxonomy_v2.md`, contract test fixtures under `tests/`.

**Detailed relevant finding/issue**
- Interfaces currently drift across SF → pipeline → CLI → runtime and DDB boundaries, causing late and inconsistent failures.

**Detailed fix**
- Freeze canonical field ownership/source-of-truth and enforce test-time validation before downstream feature work.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** `flow_contract_matrix_v2` (docs + machine-readable fixture).
   - **Belongs to:** shared contract layer (control-plane).
   - **Inputs:** current payload fields, pipeline parameter lists, runtime model fields, DDB keys.
   - **Outputs:** canonical field table with owner, source (`ddb|runtime|derived`), requiredness, validator owner, and edge.
   - **Responsibility:** prevent producer/consumer schema drift.
2. **Component:** `error_taxonomy_v2`.
   - **Inputs:** failure classes from orchestration/runtime/data-readiness/publication/backfill.
   - **Outputs:** deterministic error codes/messages and retriable/non-retriable classification.
   - **Responsibility:** fail-fast, diagnosable failures.
3. Add CI contract tests that diff implemented interfaces against matrix entries and fail on undeclared fields.
4. Add pre-merge rule requiring producer+consumer approvers when a matrix field changes.
5. Publish task handoff packet with invariants and out-of-scope constraints.

**Checks**
- Contract tests fail on undeclared field additions/removals.
- Error-code mapping exists for all mandatory gate failures.

**Completion criteria**
- Downstream tasks can import one frozen contract source and one error taxonomy without local reinterpretation.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 1 — Deterministic readiness gates for monthly + RT
- **Read context before implementation (mandatory):** Part A Findings 1.1 and 2.1.
- **Component-behaviour references (mandatory):** monthly gate state machine, RT gate state machine, readiness checker contracts.
- **Role:** replace false-ready behavior with computed readiness.
- **Issues covered:** placeholder/default-ready manifests; non-deterministic gate transitions.
- **Desired end state:** monthly and RT gates operate on computed readiness only.
- **Scope/files:** monthly and RT orchestration definitions; readiness checker modules.

**Detailed relevant finding/issue**
- Current gates can proceed on placeholder or stale manifests, producing false-ready execution.

**Detailed fix**
- Use deterministic checkers backed by Batch Index + object existence and enforce recheck/fail semantics.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** `monthly_fg_b_readiness.v2` checker.
   - **Belongs to:** monthly SageMaker Pipeline preflight Processing Step (Python/PySpark).
   - **Inputs:** target period, project_name, expected windows from policy, Batch Index window map, S3 FG-B partition existence.
   - **Outputs:** `{ready: bool, missing_ranges: [...], unresolved_count, as_of_ts, decision_code}`.
   - **Responsibility:** authoritative monthly readiness decision.
2. **Component:** `rt_artifact_readiness.v2` checker.
   - **Belongs to:** RT SageMaker Pipeline preflight Processing Step (Python/PySpark).
   - **Inputs:** batch_id/date partition context, required artifact set, Batch Index pointers, S3 artifact existence.
   - **Outputs:** same readiness schema as monthly checker.
   - **Responsibility:** authoritative RT readiness decision.
3. Modify SFN states to strict flow: `compute -> branch_on_ready -> remediate -> recompute -> terminal_success_or_fail`.
4. Remove all default-ready/fallback-ready branches when manifests are missing.
5. Emit structured logs/metrics (`decision_code`, `missing_count`, `recheck_attempt`).
6. Add acceptance tests for ready, missing, unresolved-after-remediation, stale-manifest inputs.

**Checks**
- Missing manifest always produces explicit fail/remediate path (never silent ready).

**Completion criteria**
- Gate outcomes are deterministic and reproducible from source-of-truth data.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 2 — Backfill provider hardening (full families + strict completion)
- **Read context before implementation (mandatory):** Part A Findings 4.1 and 4.2.
- **Component-behaviour references (mandatory):** backfill map dispatcher, family execution contract, completion event publisher.
- **Role:** ensure requested families execute and completion means fully complete.
- **Issues covered:** no-op family behavior; false completion publication.
- **Desired end state:** every requested family has concrete dispatch and verified completion.
- **Scope/files:** backfill provider runtime module, SFN map/verifier states, completion publisher.

**Detailed relevant finding/issue**
- Backfill accepts requests it cannot execute fully and can emit success before all families finish.

**Detailed fix**
- Add complete family dispatch + terminal verifier; only publish completion after verifier success.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** `BackfillFamilyDispatcher`.
   - **Belongs to:** backfill runtime orchestration adapter.
   - **Inputs:** request id, project_name, families list, window/range spec, retry policy.
   - **Outputs:** per-family execution tickets with execution_id and status.
   - **Responsibility:** dispatch concrete jobs for `delta`, `fg_a`, `pair_counts`, `fg_c`, `fg_b_baseline`.
2. **Component:** `BackfillCompletionVerifier`.
   - **Inputs:** requested families and execution tickets.
   - **Outputs:** `{all_succeeded, failed_families, unresolved_families, verifier_code}`.
   - **Responsibility:** block completion publication unless all requested families are terminal success.
3. Add deterministic status record schema and persist in DDB/S3 audit log.
4. Add idempotent replay behavior keyed by `{project_name, request_id}`.
5. Wire completion event publisher to verifier output only.
6. Add tests: unsupported family rejection, partial failure, retry success, duplicate request replay.

**Checks**
- Completion event is impossible unless verifier `all_succeeded=true`.

**Completion criteria**
- Backfill completion semantics are strict, auditable, and deterministic.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 3 — Pair-counts Redshift fallback parity
- **Read context before implementation (mandatory):** Part A Finding 2.2.
- **Component-behaviour references (mandatory):** `RawInputResolver`, fallback config loader, pair-counts input contract.
- **Role:** provide deterministic fallback parity with provenance.
- **Issues covered:** pair-counts fallback gap; missing provenance.
- **Desired end state:** pair-counts resolves ingestion/fallback via shared resolver with explicit decision output.
- **Scope/files:** pair-counts runtime + resolver wiring + fallback contract validators.

**Detailed relevant finding/issue**
- Pair-counts fails asymmetrically when ingestion is unavailable and fallback isn’t integrated under the shared contract.

**Detailed fix**
- Route pair-counts input through `RawInputResolver` and persist decision provenance.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** pair-counts input resolution adapter.
   - **Belongs to:** pair-counts runtime preflight stage.
   - **Inputs:** batch/window context, ingestion presence check, DDB fallback policy/query metadata.
   - **Outputs:** resolved input descriptor `{source_mode, source_uri/query_id, schema_version, provenance}`.
   - **Responsibility:** choose deterministic input source and expose rationale.
2. Validate fallback policy at startup (enabled flag, query reference, schema compatibility, allowed window scope).
3. Execute Redshift fallback only when ingestion missing AND policy allows.
4. Fail fast with explicit errors for disabled fallback, missing query contract, empty result set.
5. Persist provenance fields with output artifact metadata.
6. Add tests for ingestion path, fallback path, fallback-disabled fail, and empty-fallback fail.

**Checks**
- Every pair-counts run writes `source_mode` provenance.

**Completion criteria**
- Pair-counts input behavior is deterministic and auditable across ingestion/fallback modes.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 4 — Monthly inventory + FG-B contract realignment and month partitioning
- **Read context before implementation (mandatory):** Part A Findings 1.2 and 1.3.
- **Component-behaviour references (mandatory):** inventory producer schema, FG-B mapper/reader contract, FG-B writer partition contract.
- **Role:** fix monthly contract alignment and partition determinism.
- **Issues covered:** missing month partition; producer/consumer mapping mismatch.
- **Desired end state:** canonical inventory->FG-B mapping plus enforced `baseline_month` partition.
- **Scope/files:** inventory job outputs, FG-B mapper/reader, FG-B writer and schema docs.

**Detailed relevant finding/issue**
- FG-B retrieval and reproducibility are unstable without month partitioning and canonical mapping keys.
- Existing consumers (including FG-C paths) rely on `baseline_horizon`; replacing it would break expected filtering semantics.

**Detailed fix**
- Align producer/consumer schema and enforce month-partition contract at write/read time, using **Option 1 canonical pointer strategy**: `project_parameters.ip_machine_mapping_s3_prefix` always points to the latest validated monthly inventory snapshot root.
- Preserve `baseline_horizon` as an existing partition/filter dimension and add `baseline_month` as an additional dimension.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** inventory monthly snapshot emitter.
   - **Belongs to:** monthly inventory producer.
   - **Inputs:** month scope, source inventory rows, DDB contract mapping config.
   - **Outputs:** canonical mapping records with stable key fields consumed by FG-B.
   - **Responsibility:** produce canonical machine mapping snapshot.
2. **Component:** FG-B writer partition module.
   - **Inputs:** enriched FG-B baseline rows + canonical mapping + target month.
   - **Outputs:** partitioned dataset with `feature_spec_version`, `baseline_horizon`, and required `baseline_month=YYYY-MM`.
   - **Responsibility:** deterministic monthly persistence for FG-B baselines while preserving FG-C/baseline-horizon compatibility.
3. Add schema validators on producer and consumer edges for canonical keys.
4. After successful inventory snapshot validation, atomically set `project_parameters.ip_machine_mapping_s3_prefix` to the new month snapshot root; FG-B must read only this pointer.
5. Add migration/backfill utility for historical non-partitioned records.
6. Add replay tests proving same inputs produce same partition paths/row counts and that FG-B reads exactly the pointed snapshot.
7. Add explicit rollback plan: move pointer back to prior valid month snapshot if post-cutover validation fails.

**Checks**
- FG-B writer rejects missing/invalid `baseline_month`.

**Completion criteria**
- Monthly inventory and FG-B are contract-aligned with deterministic month partitioning.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 5 — Bootstrap-to-monthly orchestration correction
- **Read context before implementation (mandatory):** Part A Finding 2.3.
- **Component-behaviour references (mandatory):** bootstrap orchestration state, monthly SFN entry contract, payload minimization policy.
- **Role:** enforce monthly path through monthly SFN contract.
- **Issues covered:** bootstrap bypasses monthly orchestration semantics.
- **Desired end state:** bootstrap calls monthly SFN synchronously with monthly-minimal payload.
- **Scope/files:** bootstrap SFN definition/caller module, monthly entry validator.

**Detailed relevant finding/issue**
- Bootstrap is a control-plane readiness orchestrator (checkpointing and readiness transitions); direct monthly pipeline invocation makes it bypass monthly flow semantics and carry irrelevant payload fields.

**Detailed fix**
- Replace bootstrap monthly call with `states:startExecution.sync:2` to monthly SFN + strict payload contract.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** bootstrap monthly invocation adapter.
   - **Belongs to:** bootstrap orchestration layer.
   - **Inputs:** run request context (project, month window, correlation id).
   - **Outputs:** monthly SFN execution response with status and trace ids.
   - **Responsibility:** invoke monthly flow through canonical monthly orchestrator only; bootstrap remains responsible for readiness control-plane state, not monthly business-flow execution logic.
2. Define monthly payload schema with required fields only; reject ML-only/training-only fields.
3. Implement deterministic execution name strategy for idempotency and duplicate suppression.
4. Add retriable policy for transient API failures and explicit non-retriable policy for contract violations.
5. Add integration tests for happy path, duplicate execution, contract violation.
6. Add rollback guard flag to restore legacy path only during controlled rollback window.
7. Deployment/config updates:
   - add/validate `MonthlyStateMachineArn` wiring in bootstrap deployment template/config;
   - grant `states:StartExecution` on monthly SFN ARN to bootstrap role;
   - remove direct monthly SageMaker pipeline invoke permission from bootstrap role to prevent regression.

**Checks**
- Bootstrap monthly calls always target monthly SFN; direct pipeline path is removed/disabled.

**Completion criteria**
- Bootstrap-to-monthly flow honors monthly orchestration contracts end-to-end.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 6 — Production artifact promotion contract in training
- **Read context before implementation (mandatory):** Part A Finding 3.1.
- **Component-behaviour references (mandatory):** training publish stage, deployment stage, artifact registry/pointer contract.
- **Role:** make production artifact promotion deterministic and contract-driven.
- **Issues covered:** ambiguous production artifact URI and weak rollback metadata.
- **Desired end state:** publish and deploy consume immutable production artifact contract fields only.
- **Scope/files:** training pipeline config, promotion job module, publish/deploy consumers.

**Detailed relevant finding/issue**
- Production deploy path can reference non-canonical or transient artifact locations.

**Detailed fix**
- Add explicit promotion step producing immutable URI/hash/version contract.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** production promotion job.
   - **Belongs to:** training post-train stage.
   - **Inputs:** candidate model artifact URI, target production root, model metadata.
   - **Outputs:** `{production_model_uri, production_model_hash, production_model_version, promoted_at}`.
   - **Responsibility:** atomically copy/promote and verify production artifact.
2. Write promotion metadata record to durable control-plane store.
3. Make publish stage emit only production contract fields.
4. Make deploy stage reject non-production URIs.
5. Add rollback pointer management to last-known-good version.
6. Add tests for success, hash mismatch fail, pointer rollback.

**Checks**
- Deploy cannot proceed without valid production URI+hash+version tuple.

**Completion criteria**
- Training production promotion is deterministic, immutable, and rollback-safe.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 7 — Training readiness recomputation at gate points
- **Read context before implementation (mandatory):** Part A Finding 3.2.
- **Component-behaviour references (mandatory):** training plan/remediate/train gates, manifest recomputation utility.
- **Role:** prevent stale-manifest decisions.
- **Issues covered:** stale readiness artifacts causing incorrect train gating.
- **Desired end state:** readiness recomputed at gate points from authoritative state.
- **Scope/files:** training planner/remediator gate logic, manifest utility, metrics.

**Detailed relevant finding/issue**
- Train gate may rely on outdated manifest snapshots.

**Detailed fix**
- Recompute readiness at each critical gate and enforce unresolved-window hard block.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** readiness recomputation utility.
   - **Belongs to:** shared training control-plane helper.
   - **Inputs:** project/train window, Batch Index snapshot, artifact presence checks.
   - **Outputs:** versioned manifest `{required_windows, ready_windows, missing_windows, as_of}`.
   - **Responsibility:** authoritative training readiness snapshot.
2. Invoke at `plan`, `remediate`, and pre-`train` gates.
3. Persist versioned manifests and comparison deltas.
4. Gate policy: if `missing_windows` non-empty at pre-train gate -> fail with explicit code.
5. Add metrics/logs for manifest drift between gates.
6. Add tests for stale-manifest overwrite, unresolved window fail, resolved retry success.

**Checks**
- Pre-train gate always evaluates fresh recomputed readiness snapshot.

**Completion criteria**
- Training gate decisions are deterministic and stale-manifest-safe.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 8 — Formalize DDB exception-table contracts
- **Read context before implementation (mandatory):** Part A Finding X.1.
- **Component-behaviour references (mandatory):** DDB routing-table consumer, lock-table manager, publication-lock manager.
- **Role:** make implicit exception tables explicit and validated.
- **Issues covered:** schema/ownership ambiguity and runtime surprises.
- **Desired end state:** explicit exception-table schema contract with startup validation and deployment-managed lifecycle for all 3 additional control-plane tables (routing, processing lock, publication lock).
- **Scope/files:** control-plane DDB contract module + startup checks + readers/writers.

**Detailed relevant finding/issue**
- Required DDB tables are used implicitly without formal schema contracts.

**Detailed fix**
- Publish explicit contracts for routing/processing-lock/publication-lock tables, enforce startup validation with clear failure codes, and integrate these tables into canonical deployment inventory/provisioning.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** exception-table contract registry.
   - **Belongs to:** shared DDB contract layer.
   - **Inputs:** table names, expected key schema, expected GSIs, ownership tags, allowed operations.
   - **Outputs:** validated table descriptor objects for consumers.
   - **Responsibility:** single source of truth for exception-table schema expectations.
2. Add startup validator (executed from SageMaker Processing preflight step where flow depends on these tables) that verifies existence, key schema, GSIs, permissions.
3. Route all table access through contract helpers; remove ad-hoc clients.
4. Emit explicit non-retriable errors for schema mismatch; retriable for transient DDB outages.
5. Add tests for valid schema, schema drift, missing table, permission denied.
6. Add deployment changes:
   - include all 3 additional tables in deployment templates/scripts and inventory docs;
   - include routing seed/bootstrap records as deployment prerequisites;
   - add deploy-time smoke checks for lock acquire/release and routing lookups.
7. Add operations runbook for schema evolution, lock cleanup, and rollback safety.

**Checks**
- Services fail fast at startup on invalid exception-table schemas.

**Completion criteria**
- Exception-table usage is explicit, validated, and operationally diagnosable.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

### Task 9 — End-to-end hardening and rollout gate
- **Read context before implementation (mandatory):** all Part A findings and Tasks 0–8 outputs.
- **Component-behaviour references (mandatory):** integration scenario matrix, release gate policy, rollback playbook.
- **Role:** validate cross-task coherence before production release.
- **Issues covered:** integration regressions and release risk after multi-component changes.
- **Desired end state:** release proceeds only after deterministic scenario matrix and rollback readiness pass.
- **Scope/files:** regression suites, release checklist docs, rollback runbook, deployment gate config.

**Detailed relevant finding/issue**
- Individually-correct fixes can still fail in cross-flow interaction without explicit full-system gating.

**Detailed fix**
- Create strict release gate based on end-to-end scenario matrix and rollback readiness criteria.

**Detailed building instructions (exact build/operation contract)**
1. **Component:** cross-flow regression matrix runner.
   - **Belongs to:** CI/CD release validation layer.
   - **Inputs:** deploy candidate version, fixture datasets, contract fixtures, environment config.
   - **Outputs:** per-scenario pass/fail report + aggregated release verdict.
   - **Responsibility:** validate monthly/RT/training/backfill/control-plane interactions.
2. Required scenarios: normal path, missing dependency, fallback mode, duplicate trigger replay, partial failure + retry recovery.
3. Define release gate thresholds (all critical scenarios pass; zero unresolved contract violations).
4. Add staged rollout checklist with observability checkpoints and abort conditions.
5. Add rollback playbook restoring prior contracts/config/artifact pointers.
6. Block deployment approval unless matrix + checklist + rollback readiness all pass.

**Checks**
- Release gate blocks when any critical scenario fails.

**Completion criteria**
- Full-system behavior is validated under expected and failure conditions before rollout.

**Mandatory general instructions (required in this task)**
- Implement producer and consumer changes together for each contract touched.
- Prefer the simplest design that preserves deterministic behavior and remains coherent with the holistic system design.
- Enforce DDB-first business configuration and minimal runtime payloads.
- Fail fast with explicit error codes/messages for contract violations.
- Include idempotency, retry, and rollback considerations in implementation.
- Build toward a complete working system with explicit provider/consumer contract alignment and no hidden fallback behavior.
- Conclude the task by updating all relevant project documentation files to reflect the implemented behavior/contract/config changes.

---

## Execution order rationale
1. Freeze contracts and validation semantics first (Task 0).
2. Remove false readiness and backfill ambiguity next (Tasks 1–3).
3. Correct monthly structural contracts and orchestration path (Tasks 4–5).
4. Complete training promotion/readiness correctness (Tasks 6–7).
5. Formalize DDB exception contracts and enforce full rollout gate (Tasks 8–9).
