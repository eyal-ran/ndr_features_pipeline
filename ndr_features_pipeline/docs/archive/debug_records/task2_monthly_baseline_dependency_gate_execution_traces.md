# Task 2 monthly FG-B dependency gate execution traces

## Trace A — No missing FG-A history (remediation skipped)
1. `InventoryPipelineStatusChoice` -> `BuildBaselineDependencyCheckManifest`.
2. `BuildBaselineDependencyCheckManifest` emits `baseline_dependency_manifest` with `missing_ranges=[]` and `check_status=ready`.
3. `CheckBaselineDependencies` takes the no-missing branch directly to `StartFGBBaselinePipeline`.
4. FG-B baseline pipeline runs without invoking remediation.

## Trace B — Missing FG-A history (remediation invoked, then proceed)
1. `InventoryPipelineStatusChoice` -> `BuildBaselineDependencyCheckManifest` with non-empty `missing_ranges`.
2. `CheckBaselineDependencies` detects missing ranges and routes to `BuildBaselineRemediationRequest` (cycle 0).
3. `BuildBaselineRemediationRequest` constructs `NdrBaselineRemediationRequest.v1` payload + min/max remediation window.
4. `InvokeBaselineDependencyRemediation` invokes `${BackfillStateMachineArn}` synchronously.
5. `RecheckBaselineDependencies` refreshes readiness manifest from `recheck_baseline_dependency_manifest` and increments cycle.
6. `CheckBaselineDependencies` sees no missing ranges and routes to `StartFGBBaselinePipeline`.

## Trace C — Missing FG-A history unresolved after remediation (explicit fail)
1. Steps 1-5 follow Trace B, but recheck still contains non-empty `missing_ranges`.
2. `CheckBaselineDependencies` falls through to `FailBaselineDependenciesUnresolved` (default branch when cycle is already 1).
3. Workflow fails with:
   - `Error`: `BaselineDependencyGateFailed`
   - `Cause`: `Unresolved FG-A dependency after remediation; dependency_missing:fg_a_history`

## Idempotency / retry / rollback considerations included
- Remediation call uses `states:startExecution.sync:2` with retries for throttling/execution-limit/task-failure conditions.
- `NdrBaselineRemediationRequest.v1` payload carries an `idempotency_key` field (caller-provided override supported).
- Gate cycles are explicit (`baseline_gate_cycle`), preventing infinite remediation loops.
- Rollback is straightforward: revert Task 2 state additions and restore direct inventory-success -> FG-B transition.
