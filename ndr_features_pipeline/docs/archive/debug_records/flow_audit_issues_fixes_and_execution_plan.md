# NDR Flows Audit Report + Agreed Fixes + Implementation Execution Plan

## 0) Validation context and how findings were produced

This plan is based on a full post-refactor pass focused on production behavior, not intent.

### 0.1 Checks and analyses conducted
- End-to-end orchestration tracing for all four flows:
  - monthly baselines,
  - real-time processing + inference + publication,
  - training + remediation,
  - backfill extraction + map execution.
- Contract-chain validation for each critical handoff:
  - Step Functions input/output fields,
  - SageMaker Pipeline parameters,
  - CLI runtime arguments,
  - processing job runtime config and expected artifact dependencies.
- Dependency-readiness analysis for cold-start/initial deployment:
  - behavior when no baselines exist,
  - behavior when no final features exist,
  - behavior when Batch Index and historical artifacts are incomplete.
- Fallback-path analysis:
  - ingestion S3 read behavior,
  - Redshift fallback design availability vs actual integration,
  - project resolution and manifest-generation robustness.

### 0.2 Why another fixes round is required
The refactoring implementation introduced/left behind contract drift and startup fragility in multiple cross-flow boundaries. This plan therefore emphasizes:
- contract coherence before rollout,
- startup (day-0) operability,
- deterministic remediation semantics,
- explicit acceptance gates to prevent recurrence of partial/incorrect implementation.

---

## Part A — Discovered and analyzed issues/fixes report

The following sections capture the complete findings and recommended fixes by flow.

---

## Flow 1 — Monthly Baselines

### Observed flow path
Monthly SFN executes:
1) Machine inventory unload pipeline
2) FG-B baseline pipeline
3) completion event.

---

### 1) Machine inventory monthly completeness risk

**Why this is a flaw (reconfirmed):**
`machine_inventory_unload_job` currently:
- Loads all existing IPs from historical inventory,
- Excludes those IPs from Redshift unload query,
- Then overwrites the current month partition with unload result only.

That can produce an incomplete monthly inventory snapshot (new-only behavior).

#### Recommendation (given constraints + ease of implementation)
Recommended **Option A: full monthly snapshot overwrite** (not incremental exclude-then-overwrite).

- **Why:** simplest and safest for required behavior (status changes, sufficient-history transitions, persistent/non-persistent relabeling by name prefix, retro analytics, retro training).
- **Implementation:**
  1. Remove existing-IP exclusion path from unload query (`tmp_existing_ips` branch).
  2. Always unload full deduplicated snapshot for target month/reference.
  3. Overwrite month partition with full snapshot.
  4. Keep all month partitions as history.
- **Result:** each month is self-contained and replay-safe.

If compute/cost becomes an issue later, optimize with incremental merge, but full snapshot is the lowest-risk functional fix.

---

### 2) Is Redshift raw-log fallback needed in baseline flow?

#### Code answer
FG-B baseline builder reads FG-A parquet from configured `fg_a_input.s3_prefix`; it does **not** read ingestion raw logs directly in its main path.

So for **FG-B baseline stage itself**, raw-log fallback is **not required**.

#### Where fallback *is* relevant in monthly flow
If initial deployment or gaps mean **FG-A is missing**, baseline cannot build. In that case fallback/remediation belongs upstream:
- create missing delta/FG-A via backfill (which may need ingestion S3→Redshift fallback),
- then rerun FG-B.

So for monthly flow:
- Add FG-A readiness check before FG-B execution.
- If missing windows detected, invoke backfill remediation contract.
- Keep FG-B artifact-only (no raw fallback inside FG-B builder).

---

### 3) New finding from project-wide recheck: unnecessary MLP-config dependency in monthly flow

#### Code answer
Monthly baseline SFN performs an unconditional MLP config lookup even though the flow executes machine inventory + FG-B baseline only (DPP-owned operations).

#### Risk
- If `ml_project_name` resolves to empty/missing, this lookup can fail before monthly baseline execution, creating a non-essential blocker.
- The failure path is unrelated to the monthly flow’s business purpose.

#### Required fix
- Remove unconditional `LoadMlpConfigFromDynamo` from monthly baseline SFN, or convert it into an optional non-blocking branch.
- Keep monthly baseline orchestration DPP-scoped unless an explicit MLP-owned action is added.

#### Why this is the correct fix
- Reduces avoidable coupling/failure surface.
- Improves cold-start reliability and operational simplicity.
- Preserves clear ownership boundaries between DPP and MLP flows.

---

## Flow 2 — Real-Time Flow

### Observed flow path
Per MLP branch:
15m pipeline → inference pipeline → publication workflow; no explicit backfill orchestration step in-between.

---

### 1) Missing RT↔Backfill contract (detailed design)

#### Proposed RT→Backfill contract (v1)

### Payload schema (`NdrBackfillRequest.v1`)
```json
{
  "contract_version": "NdrBackfillRequest.v1",
  "consumer": "realtime",
  "project_name": "...",
  "feature_spec_version": "...",
  "ml_project_name": "...",
  "batch_id": "...",
  "requested_families": ["delta","fg_a","pair_counts","fg_c","fg_b_baseline"],
  "missing_ranges": [
    {
      "family": "fg_b_baseline",
      "start_ts_iso": "2026-04-01T00:00:00Z",
      "end_ts_iso": "2026-04-01T23:59:59Z",
      "reason_code": "baseline_missing_for_reference"
    }
  ],
  "source_hint": {
    "raw_parsed_logs_s3_prefix": "s3://.../<batch>/"
  },
  "idempotency_key": "sha256(...)"
}
```

### RT side implementation
- Add state after `Start15mFeaturesPipeline` success and before inference:
  - compute/read readiness manifest (Batch Index + existence checks),
  - if missing ranges exist, call Backfill SFN synchronously with payload above,
  - re-check readiness, then continue to inference.
- Persist request + result summary in Batch Index (`backfill_status`, `backfill_request_id`).

### Backfill side implementation
- Accept `missing_ranges` directly (consumer-guided mode).
- Validate contract + normalize ranges.
- Execute only requested families/ranges.
- Emit completion payload including produced ranges and S3 outputs.

---

### 2) Exact implementation for Redshift fallback in RT (custom DPP query + params)

#### DPP contract extension
Under DPP spec:
```json
"raw_log_fallback": {
  "enabled": true,
  "redshift": {
    "cluster_identifier": "...",
    "database": "...",
    "secret_arn": "...",
    "region": "...",
    "iam_role": "...",
    "db_user": "...",
    "unload_s3_prefix": "s3://.../fallback/raw/"
  },
  "queries": {
    "delta": {
      "descriptor_id": "delta_raw_logs_v1",
      "sql_template": "SELECT ... WHERE event_ts >= '{{start_ts}}' AND event_ts < '{{end_ts}}' ..."
    }
  }
}
```

#### Runtime behavior
1. Delta step checks ingestion window completeness (object presence + minimum row heuristics).
2. If incomplete and fallback enabled:
   - render `sql_template` with window bounds,
   - execute Redshift UNLOAD to fallback prefix,
   - read fallback files as input (same normalization schema),
   - tag `source_mode=redshift_unload_fallback` in Batch Index.
3. If fallback disabled, fail with explicit contract error.

#### Wiring
A fallback implementation module exists (`backfill_redshift_fallback.py`) with query rendering + UNLOAD + staging primitives; main fix is integrating it into RT/backfill execution paths.

---

### 3) Exact implementation for making points 2/3 production-ready
- Add a reusable `RawInputResolver` component used by delta and backfill extractor.
- Resolver inputs: project, feature_spec_version, family, start/end, preferred S3 prefix.
- Resolver outputs:
  - `resolved_input_prefix`,
  - `source_mode` (`ingestion` / `redshift_unload_fallback`),
  - provenance metadata.
- Update Batch Index writer to persist provenance and fallback statement IDs.
- Add alarms:
  - fallback frequency threshold,
  - fallback failure rate,
  - missing-range recurrence.

---

## Flow 3 — Training Flow

---

### 1) Mode parameter: how to set, pass, consume

Current system is stage-oriented, not business-mode-oriented.

#### Recommended `mode` integration

### Mode values
- `training`
- `evaluation`
- `production`

### Trigger-time set
Caller (scheduler/manual/API) sets `mode` in SFN input.

### SFN pass-through
Training SFN:
- Validate `mode in {training,evaluation,production}`
- Pass `Mode` as pipeline param to IF training pipeline step parameters.

### Pipeline/runtime pass-through
- Add CLI arg `--mode` in `run_if_training.py`.
- Add `mode` field in `IFTrainingRuntimeConfig`.
- In `IFTrainingJob.run()`:
  - `training`: run verify→plan→remediate→reverify→train only.
  - `evaluation`: run training + evaluation branch; no production publish/deploy.
  - `production`: run training, then production publish/attributes/deploy flow.

### Timestamp validation by mode
- `training`: eval windows optional.
- `evaluation`: eval windows required.
- `production`: eval optional/required per policy, but deployment gates mandatory.

---

### 2) `ml_project_names` pre-load bug fix (exact)

Current training SFN loads MLP config before branch normalization and uses only singular `ml_project_name` key.

#### Fix
- Move `LoadMlpConfigFromDynamo` inside per-branch Map (after `NormalizeMlProjectBranches`).
- Key each lookup by `Map.Item.Value`.
- Validate reciprocal linkage (`project_name`) per branch.
- Fail only failing branch (or whole run by policy).

This removes empty-key risk and aligns validation with actual execution branches.

---

## Flow 4 — Backfill Flow (Service Provider)

---

### 1) Detailed cross-flow contracts (RT, monthly baseline, training)

#### A) RT ↔ Backfill
- **Request:** `NdrBackfillRequest.v1`.
- **Backfill action:** produce missing families/ranges and return produced artifacts + status.
- **RT action after return:** re-validate readiness then continue inference/publication.

#### B) Monthly baseline ↔ Backfill
Use contract `NdrBaselineRemediationRequest.v1`:
```json
{
  "contract_version":"NdrBaselineRemediationRequest.v1",
  "consumer":"monthly_baseline",
  "project_name":"...",
  "feature_spec_version":"...",
  "reference_month":"YYYY/MM",
  "required_families":["delta","fg_a","fg_b_baseline"],
  "missing_ranges":[...],
  "idempotency_key":"..."
}
```
- **Monthly side:** if FG-A missing for baseline windows, call backfill before running FG-B.
- **Backfill side:** prioritize `delta/fg_a`, optionally `fg_b_baseline` generation support, then return produced windows.

#### C) Training ↔ Backfill
Use contract `NdrTrainingRemediationRequest.v1`:
```json
{
  "contract_version":"NdrTrainingRemediationRequest.v1",
  "consumer":"training",
  "project_name":"...",
  "feature_spec_version":"...",
  "run_id":"...",
  "training_window": {"start_ts_iso":"...","end_ts_iso":"..."},
  "evaluation_windows":[...],
  "missing_manifest": {...},
  "requested_families":["fg_a_15m","fg_b_daily"]
}
```
- **Training side:** submits exact missing manifest.
- **Backfill side:** executes selective remediation and returns completion manifest.
- **Training side:** reverify gate before train stage.

---

### 2) Detailed fix for S3 fallback gating bug

Current extractor falls back to S3 listing only if project inference fails; otherwise it errors on empty batch-index result.

#### Fix
- Change logic:
  - attempt batch-index,
  - if no rows (or lookup exception), attempt S3 listing regardless of project inference result,
  - if both empty, fail.
- Add provenance marker in manifest:
  - `source_mode: batch_index|s3_listing_fallback`.
- Add warning-level event when fallback path is used.

---

### 3) Detailed fix for hardcoded project inference

Current inference accepts only literal `fw_paloalto`.

#### Fix
- Prefer explicit `project_name` runtime parameter from Step Function.
- Keep prefix inference as optional fallback:
  - parse canonical path segment position from DPP routing config,
  - never hardcode project string.
- Validate inferred/declared project against DPP `project_name`.

---

### 4) FG-B windows absent in extractor manifest — fix + recommendation

Extractor currently emits empty `fg_b_baseline` ranges.

#### Two options
1. **Single-provider option:** backfill flow also orchestrates FG-B missing baseline generation.
2. **Split-provider option:** backfill handles 15m families only; monthly/training separately invoke FG-B pipeline.

### Recommendation
Given system goals (retro training + analytics + complete historical baseline metadata), recommend **Option 1 (single provider)** for consistency and lower orchestration drift:
- Backfill manifest should include `fg_b_baseline` ranges when required,
- Backfill SFN dispatches FG-B generation branch per range/reference,
- Return unified produced-manifest to consumer.

---

### 5) Detailed fix for backfill pipeline code URI placeholder issue

Historical extractor pipeline resolves code URI using placeholder defaults (`<required:...>`), risking wrong resolution at build time.

#### Fix
- Update builder signature to accept:
  - `project_name_for_contracts`,
  - `feature_spec_version_for_contracts`.
- Resolve code URI from those explicit values (same pattern as other pipeline builders).
- Add startup assertion: reject unresolved placeholder tokens.

---

## Additional retained high-severity flaw
Backfill SFN calls `${PipelineNameBackfill15m}` with `ArtifactFamily/RangeStartTsIso/RangeEndTsIso`, but available 15m streaming pipeline contract uses `MiniBatchId/RawParsedLogsS3Prefix/BatchStartTsIso/BatchEndTsIso`.
This must be reconciled by either:
- creating dedicated backfill-15m pipeline with range/family contract, or
- changing SFN to drive existing contract with a per-window expansion phase.

---

## Part B — Complete implementation plan (tasks in optimal execution order)

## Implementation principles (mandatory for every task)
1. **No partial contract alignment:** every producer/consumer pair must be updated together before task closure.
2. **DDB as source of truth:** runtime payload includes only run-varying values; behavior and policy come from DPP/MLP config.
3. **Provider-consumer coherence:** each task must include both sides of every changed interface.
4. **Idempotency + replay safety:** all writes, backfill calls, and publication calls must be safe under retries.
5. **Observability built-in:** every task adds structured logs, status fields, and failure reasons.
6. **Backward-safe rollout:** dual-read/dual-write only where needed; remove legacy path only after verification.

---

## Mandatory execution protocol for every task in this plan

To avoid a repeat of incomplete/imperfect implementation outcomes, every task must satisfy all rules below before closure.

### A) Required implementation outputs
Each task must produce:
1. **Contract update artifacts** (schema/validator updates where applicable).
2. **Producer changes** and **consumer changes** in the same task scope (no one-sided contract edits).
3. **Deterministic failure semantics** with explicit error codes/messages.
4. **Operational telemetry updates** (logs/metrics/events) for the changed path.
5. **Regression-safe tests** covering both happy path and failure path.

### B) Required validation evidence
Each task must include explicit evidence of:
1. Unit tests (or equivalent local checks) for changed modules.
2. Contract tests for altered interfaces.
3. Integration tests for affected orchestration handoffs.
4. Idempotency/retry behavior checks for externally-triggered actions.
5. Startup/cold-start behavior checks when relevant to the task.

### C) Prohibited closure conditions
A task is **not done** if any of the following is true:
- A contract changed but only one side (producer or consumer) was updated.
- Validation covers only success path and omits failure/retry path.
- New behavior depends on undocumented implicit defaults.
- Startup/bootstrap path was impacted but not explicitly tested.

### D) Required handoff package per task
Each completed task must append a short “handoff bundle”:
- changed files,
- contract diffs,
- tests run and pass/fail output,
- known risks,
- rollback steps.

---

## Integrated analysis-to-plan mapping (by flow, with no omissions)

This section integrates every discovered issue from Part A directly into the execution plan, first by flow (analysis view), then by task (implementation view).

### Flow 1 — Monthly baselines (integrated analysis)

#### Findings integrated from Part A
1. Machine inventory monthly completeness risk (incremental exclude + overwrite pattern can produce partial monthly snapshot).
2. Baseline FG-B stage should remain artifact-consumer only; raw-log fallback belongs upstream when FG-A prerequisites are missing.

#### Integrated fix path in plan
- **Task 1**: enforce full monthly snapshot semantics (Option A).
- **Task 2**: add FG-A readiness/remediation gate before FG-B.
- **Task 7**: if unified backfill provider is selected, include FG-B baseline family support for baseline consumers.
- **Task 11**: integration/rollback validation for monthly bootstrap and replay safety.

---

### Flow 2 — Real-time flow (integrated analysis)

#### Findings integrated from Part A
1. No explicit missing-range contract invocation before inference/publication.
2. No integrated DPP-driven Redshift fallback for ingestion gaps.
3. Fallback module exists but is not operationally wired in the main RT execution path.

#### Integrated fix path in plan
- **Task 3**: introduce shared RawInputResolver and DPP-driven Redshift fallback mechanics.
- **Task 4**: add RT missing-range detection and RT↔Backfill contract call/revalidation states.
- **Task 5**: reconcile backfill execution contract with executable pipeline interface.
- **Task 11**: production observability + retry/idempotency verification for RT cold-start and steady-state.

---

### Flow 3 — Training flow (integrated analysis)

#### Findings integrated from Part A
1. Missing explicit mode contract (`training` / `evaluation` / `production`) from trigger to runtime.
2. `ml_project_names` branch handling is inconsistent with pre-branch MLP config read.
3. Initial deployment remediation safety depends on contract coherence with backfill and FG-B providers.

#### Integrated fix path in plan
- **Task 8**: explicit mode parameter propagation and mode-aware runtime behavior.
- **Task 9**: per-branch MLP config lookup and branch-safe validation.
- **Task 5** + **Task 7**: remediate cross-contract dependencies consumed by training remediation.
- **Task 11**: training remediation and mode acceptance tests.

---

### Flow 4 — Backfill service-provider flow (integrated analysis)

#### Findings integrated from Part A
1. High-severity contract mismatch between Backfill SFN parameters and available 15m pipeline parameters.
2. Historical extractor disables useful S3 fallback in common initial-deployment conditions.
3. Project inference is hardcoded (`fw_paloalto`) and non-generic.
4. Extractor manifest omits FG-B baseline ranges.
5. Historical extractor pipeline code URI resolution relies on placeholder defaults at builder time.

#### Integrated fix path in plan
- **Task 5**: implement backfill pipeline contract that matches Backfill SFN invocation semantics.
- **Task 6**: historical extractor fallback and project-resolution hardening.
- **Task 7**: add FG-B baseline family planning/execution support.
- **Task 10**: pipeline code URI resolution hardening with concrete contract identities.
- **Task 11**: end-to-end contract and replay validation across all consumers.
- **Task 16**: remove unnecessary MLP coupling from backfill orchestration path.

---

## Integrated task coverage matrix (analysis findings → tasks)

| Finding ID | Summary | Primary tasks | Supporting tasks |
|---|---|---|---|
| F1.1 | Machine inventory monthly snapshot completeness risk | 1 | 11 |
| F1.2 | Monthly FG-B must gate on FG-A readiness/remediation | 2 | 7, 11 |
| F2.1 | RT missing-range remediation contract absent | 4 | 0, 11 |
| F2.2 | RT ingestion fallback not integrated, must be DPP-driven | 3 | 11 |
| F2.3 | Existing fallback module is non-operational from orchestration path | 3 | 4, 11 |
| F3.1 | Training mode contract absent end-to-end | 8 | 11 |
| F3.2 | `ml_project_names` pre-branch config read inconsistency | 9 | 8, 11 |
| F3.3 | Training remediation depends on backfill/FG-B contract coherence | 5 | 7, 11 |
| F4.1 | Backfill SFN ↔ pipeline parameter mismatch | 5 | 0, 11 |
| F4.2 | Extractor fallback gating bug | 6 | 11 |
| F4.3 | Hardcoded project inference | 6 | 11 |
| F4.4 | FG-B baseline omission in extractor family ranges | 7 | 2, 11 |
| F4.5 | Placeholder-based code URI resolution in extractor pipeline builder | 10 | 11 |
| F5.1 | Unnecessary MLP lookup in monthly/backfill orchestration introduces avoidable failures | 16 | 11 |

---

## Integrated initial-deployment capability report-to-plan mapping (new, explicit)

This section integrates the **initial deployment capability** findings (cold-start state with no baselines and no final features) into the fixes plan by flow and by task.

### Initial deployment finding ID set

- **IDP-1 (RT cold-start deadlock):** RT executes FG-C in the primary 15m chain, while FG-C fails fast if FG-B baselines are absent.
- **IDP-2 (RT missing-range remediation gap):** RT SFN has no explicit missing-range detection + backfill + revalidation gate before inference/publication.
- **IDP-3 (Monthly bootstrap fragility):** monthly FG-B baseline run lacks an FG-A readiness/remediation gate.
- **IDP-4 (Backfill execution contract mismatch):** Backfill SFN starts a pipeline with family/range parameters that do not match the available 15m pipeline parameter contract.
- **IDP-5 (Extractor bootstrap fragility):** extractor fallback gating/project inference/family omission prevent robust cold-start reconstruction.
- **IDP-6 (Training remediation contract drift):** training remediation invocation for FG-B uses runtime parameters inconsistent with FG-B pipeline contract.
- **IDP-7 (Raw-log fallback not integrated):** fallback utilities exist but are not integrated into main processing-orchestration path for deterministic cold-start recovery.
- **IDP-8 (Extractor pipeline code URI placeholder risk):** code URI resolution in historical extractor builder uses placeholder defaults at build-time.
- **IDP-9 (Non-ML flow MLP coupling risk):** monthly/backfill orchestrators depend on MLP lookup although no MLP-owned action is required.

### Flow-by-flow initial deployment integration

#### Flow 1 — Monthly (initial deployment)
- Integrated issues: **IDP-3**, **IDP-9**
- Required fix path: **Task 2**, **Task 7**, **Task 11**, **Task 16**
- Rationale: monthly must not assume FG-A history exists on day-0; it needs deterministic readiness/remediation before FG-B.

#### Flow 2 — Real-time (initial deployment)
- Integrated issues: **IDP-1**, **IDP-2**, **IDP-7**
- Required fix path: **Task 3**, **Task 4**, **Task 11**, plus new bootstrap tasks below.
- Rationale: RT must be able to process first batches without deadlocking on missing baselines/features.

#### Flow 3 — Training (initial deployment)
- Integrated issues: **IDP-6**
- Required fix path: **Task 8**, **Task 9**, **Task 11**
- Rationale: training-led remediation must use contract-compatible FG-B and backfill targets in empty-system startup conditions.

#### Flow 4 — Backfill (initial deployment)
- Integrated issues: **IDP-4**, **IDP-5**, **IDP-8**, **IDP-9**
- Required fix path: **Task 5**, **Task 6**, **Task 7**, **Task 10**, **Task 11**, **Task 16**
- Rationale: backfill is the service-provider for cold-start reconstruction and must be contract-coherent and source-resilient.

---

### Task 0 — Contract freeze for all three cross-flow remediation contracts
- **Goal:** finalize canonical payloads and response schemas:
  - `NdrBackfillRequest.v1` (RT consumer)
  - `NdrBaselineRemediationRequest.v1` (monthly baseline consumer)
  - `NdrTrainingRemediationRequest.v1` (training consumer)
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
  - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
  - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
  - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
  - `src/ndr/orchestration/backfill_contracts.py`
- **Scope:** interface schemas, idempotency keys, required fields, error codes.
- **Implementation instructions:**
  1. Add schema validators for all 3 request types and one unified response schema.
  2. Define deterministic `idempotency_key` recipe in one shared helper.
  3. Define required provenance fields (`source_mode`, `producer_flow`, `request_id`).
- **Deliverables:** schema module + tests + contract matrix table.
- **Acceptance tests:**
  - valid payloads pass
  - missing required fields fail with deterministic error
  - unknown contract versions fail-fast.

---

### Task 1 — Fix machine inventory monthly completeness (Option A)
- **Goal:** ensure full monthly snapshot semantics.
- **Mandatory reading:**
  - `src/ndr/processing/machine_inventory_unload_job.py`
  - `src/ndr/scripts/run_machine_inventory_unload.py`
- **Scope:** machine inventory unload logic only.
- **Implementation instructions:**
  1. Remove existing-IP exclusion temporary-table branch.
  2. Always unload full snapshot for month.
  3. Keep month partition overwrite behavior.
  4. Preserve historical partitions.
  5. Keep metadata fields needed for persistent/non-persistent and sufficiency checks.
- **Context-complementary instructions:**
  - Ensure non-persistent/persistent labeling by machine name prefix remains possible from stored data.
  - Ensure machine-history sufficiency can be computed retroactively across month partitions.
- **Acceptance tests:**
  - snapshot completeness against source query count
  - re-run idempotency test
  - month-over-month history presence test.

---

### Task 2 — Add FG-A readiness gate before monthly FG-B execution
- **Goal:** baseline flow should remediate missing upstream artifacts before FG-B.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
  - `src/ndr/processing/fg_b_builder_job.py`
  - backfill contract module from Task 0.
- **Scope:** monthly SFN + readiness checker Lambda/step + backfill call wiring.
- **Implementation instructions:**
  1. Insert `CheckBaselineDependencies` state before `StartFGBBaselinePipeline`.
  2. Build missing-range manifest for required FG-A windows.
  3. If missing, invoke backfill with `NdrBaselineRemediationRequest.v1`.
  4. Re-check readiness; proceed only on success.
- **Acceptance tests:**
  - no-missing path skips backfill
  - missing path invokes backfill once and then proceeds
  - unresolved missing path fails with explicit reason.

---

### Task 3 — Introduce reusable raw-input resolver with DPP-driven Redshift fallback
- **Goal:** implement exact fallback mechanics for missing ingestion rows.
- **Mandatory reading:**
  - `src/ndr/processing/base_runner.py`
  - `src/ndr/processing/backfill_redshift_fallback.py`
  - `src/ndr/processing/delta_builder_job.py`
  - DPP loader modules.
- **Scope:** shared resolver + integration points.
- **Implementation instructions:**
  1. Create `RawInputResolver` service:
     - completeness check,
     - DPP fallback contract read,
     - query rendering + UNLOAD execution,
     - output prefix selection,
     - provenance return object.
  2. Integrate into Delta runtime path.
  3. Integrate into backfill extraction path.
  4. Write provenance to Batch Index.
- **Context-complementary instructions:**
  - query text and all connection params must come from DPP, not env defaults.
  - fail-fast if required DPP fields missing while fallback enabled.
- **Acceptance tests:**
  - complete ingestion path uses `source_mode=ingestion`
  - missing ingestion path uses `source_mode=redshift_unload_fallback`
  - fallback-disabled path fails deterministically.

---

### Task 4 — Real-time SFN: add missing-range detection and RT↔Backfill contract call
- **Goal:** make RT flow contract-aware of missing baselines/features.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
  - backfill contract validators from Task 0.
- **Scope:** RT SFN branch logic between 15m and inference.
- **Implementation instructions:**
  1. Add `ComputeMissingRanges` state.
  2. Add choice state:
     - no missing → inference
     - missing → invoke backfill sync with `NdrBackfillRequest.v1`.
  3. Add post-backfill revalidation state.
  4. Persist backfill request ID and status in Batch Index.
- **Acceptance tests:**
  - branch behavior with/without missing ranges
  - backfill timeout/failure handling
  - retry idempotency via `idempotency_key`.

---

### Task 5 — Backfill SFN/pipeline contract reconciliation (high-severity mismatch fix)
- **Goal:** align `PipelineNameBackfill15m` interface with SFN.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
- **Scope:** choose and implement one consistent contract.
- **Implementation instructions:**
  1. Implement dedicated backfill 15m pipeline accepting:
     - `ArtifactFamily`, `RangeStartTsIso`, `RangeEndTsIso`, `ProjectName`, `FeatureSpecVersion`.
  2. Route to relevant family builders deterministically.
  3. Keep per-family dependency ordering in pipeline logic.
- **Acceptance tests:**
  - each family invocation path works
  - mixed-family ranges run deterministically
  - polling and completion metadata are correct.

---

### Task 6 — Historical windows extractor reliability fixes
- **Goal:** eliminate gating bug + hardcoded project inference.
- **Mandatory reading:**
  - `src/ndr/processing/historical_windows_extractor_job.py`
  - `src/ndr/processing/historical_windows_extractor_job.py` tests.
- **Scope:** extractor behavior.
- **Implementation instructions:**
  1. Try S3 listing fallback whenever batch-index rows are empty/unavailable.
  2. Add explicit runtime `project_name`; remove hardcoded `fw_paloalto` dependency.
  3. Add manifest `source_mode` + `resolution_reason`.
- **Acceptance tests:**
  - batch-index-only success
  - s3-fallback success when index empty
  - deterministic failure when both empty.

---

### Task 7 — Extend backfill family planning to FG-B baseline support (recommended Option 1)
- **Goal:** single service provider for missing features and baseline stats.
- **Mandatory reading:**
  - `src/ndr/orchestration/backfill_contracts.py`
  - `src/ndr/processing/historical_windows_extractor_job.py`
  - FG-B builder runtime contract.
- **Scope:** family planner + execution map + result manifest.
- **Implementation instructions:**
  1. Populate `fg_b_baseline` ranges in manifest when needed.
  2. Add backfill executor branch for FG-B range/reference handling.
  3. Include produced baseline metadata in completion payload.
- **Acceptance tests:**
  - FG-B-only backfill request
  - mixed FG-A + FG-B request
  - monthly/training consumers consume completion manifest successfully.

---

### Task 8 — Training mode refactor (`training` / `evaluation` / `production`)
- **Goal:** explicit mode semantics from trigger to runtime.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
  - `src/ndr/scripts/run_if_training.py`
  - `src/ndr/processing/if_training_job.py`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`
- **Scope:** SFN input contract, pipeline params, CLI args, runtime dispatch.
- **Implementation instructions:**
  1. Add `mode` runtime param in SFN and pipeline parameters.
  2. Add `--mode` CLI and runtime config field.
  3. Implement mode-dependent stage execution and validation rules.
  4. Ensure production mode publishes/deploys production model artifacts.
- **Acceptance tests:**
  - training mode skips evaluation and production deployment
  - evaluation mode runs evaluation artifacts only
  - production mode runs publish/attributes/deploy path with gates.

---

### Task 9 — Training MLP config lookup fix for `ml_project_names`
- **Goal:** eliminate singular-key preload bug.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_training_orchestrator.json`
- **Scope:** training SFN branching + per-branch MLP config validation.
- **Implementation instructions:**
  1. Normalize `ml_project_names` first.
  2. Move MLP DDB lookup into map branch.
  3. Validate reciprocal project linkage per branch.
- **Acceptance tests:**
  - list-only input works
  - mixed invalid branch fails predictably
  - branch-level diagnostics identify offending MLP name.

---

### Task 10 — Backfill extractor pipeline code URI resolution hardening
- **Goal:** remove placeholder-default code URI risk.
- **Mandatory reading:**
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_backfill_historical_extractor.py`
  - other pipeline definition files using explicit contract identities.
- **Scope:** pipeline builder signature + call sites + tests.
- **Implementation instructions:**
  1. Add explicit `project_name_for_contracts`, `feature_spec_version_for_contracts` args.
  2. Resolve step code URI only from concrete values.
  3. Guard against `<required:...>` values with explicit failure.
- **Acceptance tests:**
  - valid concrete values resolve correctly
  - placeholder defaults fail-fast in unit tests.

---

### Task 11 — End-to-end integration, gates, and rollout
- **Goal:** produce a complete working system with aligned provider/consumer contracts.
- **Mandatory reading:**
  - all changed SFNs/pipelines/jobs,
  - contract validators and tests.
- **Scope:** final hardening, operational readiness, rollout controls.
- **Implementation instructions:**
  1. Add integration tests for RT→Backfill, Monthly→Backfill, Training→Backfill.
  2. Add synthetic missing-data scenario tests (ingestion gap + baseline gap).
  3. Add CloudWatch metrics and alarms for fallback frequency, backfill latency, unresolved missing ranges.
  4. Add rollout playbook: canary project, staged enable flags, rollback switches.
- **Acceptance tests:**
  - all unit + contract + integration suites pass
  - replay/idempotency tests pass
  - rollback dry-run validated.
- **Integrated finding coverage:** validates and closes all findings F1.1–F5.1 under production gates.

---

### Task 12 — Initial deployment bootstrap orchestration (new, mandatory)
- **Goal:** establish a deterministic, production-ready day-0 bootstrap sequence when no baselines/features exist.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json`
  - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
  - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
  - `src/ndr/processing/fg_c_builder_job.py`
- **Scope:** orchestration of first-time environment bring-up only.
- **Implementation instructions:**
  1. Add explicit bootstrap runbook execution state machine (or equivalent orchestrated sequence):
     - seed machine inventory,
     - reconstruct required historical families via backfill provider,
     - build baseline statistics,
     - validate readiness manifest,
     - enable RT steady-state.
  2. Gate RT FG-C/inference/publication until bootstrap readiness is passed.
  3. Persist bootstrap status and checkpoints in deterministic control records.
- **Acceptance tests:**
  - brand-new environment transitions from empty state to ready without manual data patching,
  - RT first-batch run succeeds post-bootstrap,
  - rerun of bootstrap is idempotent and no-op safe.
- **Integrated initial-deployment coverage:** IDP-1, IDP-2, IDP-3.

---

### Task 13 — RT two-phase cold-start-safe execution split (new, mandatory)
- **Goal:** remove FG-C baseline dependency deadlock from RT initial runs.
- **Mandatory reading:**
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`
  - `src/ndr/processing/fg_c_builder_job.py`
  - RT SFN definition.
- **Scope:** RT pipeline decomposition and SFN branch structure.
- **Implementation instructions:**
  1. Split 15m execution into:
     - core artifacts phase (delta, fg_a, pair_counts),
     - dependent phase (fg_c, inference, publication).
  2. Insert readiness check + remediation between the two phases.
  3. Ensure first phase remains reusable by backfill and bootstrap orchestration.
- **Acceptance tests:**
  - initial run with no FG-B baselines completes core phase and triggers remediation,
  - dependent phase runs only after readiness success,
  - steady-state path remains efficient with remediation bypass.
- **Integrated initial-deployment coverage:** IDP-1, IDP-2.

---

### Task 14 — Initial-deployment contract conformance hardening (new, mandatory)
- **Goal:** enforce contract coherence specifically on startup-critical paths.
- **Mandatory reading:**
  - backfill SFN + pipeline definitions,
  - historical extractor job + pipeline builder,
  - training remediation invocation paths.
- **Scope:** startup-critical contract validators and fail-fast checks.
- **Implementation instructions:**
  1. Add startup contract validator suite for:
     - backfill SFN ↔ backfill pipeline parameter schema,
     - training remediation ↔ FG-B pipeline parameter schema,
     - extractor runtime inputs and generated manifest schema.
  2. Enforce CI gate: bootstrap contracts must be green before deploy.
  3. Add explicit error codes for every startup contract mismatch.
- **Acceptance tests:**
  - intentional contract mismatch fixtures fail with deterministic diagnostics,
  - valid startup contract matrix passes.
- **Integrated initial-deployment coverage:** IDP-4, IDP-6, IDP-8.

---

### Task 15 — Initial-deployment observability and rollback package (new, mandatory)
- **Goal:** make cold-start behavior operable and safe in production.
- **Mandatory reading:**
  - all startup path flows (RT/monthly/backfill/training),
  - task outputs from Tasks 12–14.
- **Scope:** metrics, alarms, dashboard, rollback procedures for bootstrap failures.
- **Implementation instructions:**
  1. Add metrics:
     - bootstrap duration,
     - remediation invocation count,
     - unresolved missing-range count,
     - fallback source mode distribution,
     - startup contract validation failures.
  2. Add alarms and runbooks for each failure class.
  3. Add rollback switch that reverts to pre-bootstrap operational mode safely.
- **Acceptance tests:**
  - synthetic startup failures emit expected metrics/alarms,
  - rollback drill restores stable operation without data corruption.
- **Integrated initial-deployment coverage:** IDP-2, IDP-5, IDP-7.

---

### Task 16 — Remove unnecessary MLP coupling from monthly/backfill orchestration (new, mandatory)
- **Goal:** eliminate non-essential MLP dependencies from DPP-owned monthly/backfill flows.
- **Mandatory reading:**
  - `docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json`
  - `docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json`
  - flow runtime validation logic for monthly/backfill.
- **Scope:** orchestration state definitions and parameter-validation contracts only.
- **Implementation instructions:**
  1. Remove unconditional `LoadMlpConfigFromDynamo` states from monthly/backfill, or route them through optional non-blocking branches.
  2. Ensure these flows validate only DPP-owned required runtime parameters.
  3. Add contract tests proving monthly/backfill execute with DPP-only defaults and no `ml_project_name`.
  4. Preserve MLP requirements in RT/training where they are functionally required.
- **Acceptance tests:**
  - monthly SFN succeeds without `ml_project_name` input/default.
  - backfill SFN succeeds without `ml_project_name` input/default.
  - RT/training still fail-fast when required MLP contracts are missing.
- **Integrated initial-deployment coverage:** IDP-9.

---

## Implementation order summary (optimal sequence)
1. Task 0
2. Task 1
3. Task 3
4. Task 6
5. Task 5
6. Task 7
7. Task 2
8. Task 4
9. Task 8
10. Task 9
11. Task 10
12. Task 11
13. Task 12
14. Task 13
15. Task 14
16. Task 15
17. Task 16

This ordering minimizes contract churn, fixes high-severity blockers early, and enforces provider/consumer alignment before rollout.
