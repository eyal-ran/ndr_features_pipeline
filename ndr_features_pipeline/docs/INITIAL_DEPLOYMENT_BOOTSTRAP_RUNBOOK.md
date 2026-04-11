# Initial Deployment Bootstrap Runbook (Task 12)

## Purpose
This runbook defines deterministic day-0 bootstrap orchestration for empty environments and establishes the readiness contract that enables RT steady-state activation.

## State machine expression semantics
- Bootstrap contract source (`docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json`) explicitly declares `QueryLanguage: JSONata` so `{% ... %}` expressions and `Assign` behavior are deterministic across deployment/runtime environments.

## Startup path dependency orientation (RT, Monthly, Backfill)
1. RT steady-state (`sfn_ndr_15m_features_inference`) now checks a persisted bootstrap control record before running feature/inference stages.
2. Bootstrap execution seeds machine inventory, invokes backfill reconstruction for required families, and invokes the monthly baseline **state machine** (`${MonthlyStateMachineArn}`) synchronously.
3. Bootstrap outputs publish `bootstrap_rt_activation.v1` readiness inputs consumed by RT gating.

### Responsibility boundary (authoritative)
- **Bootstrap SFN responsibilities:** readiness checkpointing in DDB, prerequisite sequencing, and day-0 activation gate transitions.
- **Monthly SFN responsibilities:** monthly inventory refresh, monthly dependency gate/remediation, FG-B baseline materialization, and monthly completion event emission.
- Bootstrap must not directly invoke the monthly FG-B SageMaker pipeline; monthly business execution always flows through the monthly SFN contract (`project_name`, `feature_spec_version`, `reference_month`).

## Deterministic checkpoints (authoritative)
Checkpoints are persisted in DDB control records (`job_name_version=bootstrap_control#v1`):
1. `seed_machine_inventory`
2. `reconstruct_historical_families`
3. `build_monthly_baseline`
4. `validate_readiness_manifest`
5. `activate_rt_steady_state`

## Readiness definition (measurable)
Bootstrap readiness is considered `READY` only when all checks pass:
- `missing_ranges_count == 0`
- `fg_b_baseline_ready == true`
- control record status persisted as `READY`

If measurable criteria are missing, bootstrap must fail fast with `TASK12_CONTRACT_VIOLATION`.

## Idempotency / retry / rollback behavior
- **Idempotency:** if control record status is already `READY`, bootstrap follows the no-op branch and returns the activation contract without recomputing.
- **Monthly invocation duplicate suppression:** bootstrap uses deterministic monthly execution naming (`bootstrap-monthly-<project>-<feature_spec_version>-<reference_month>`) and treats `StepFunctions.ExecutionAlreadyExists` as an idempotent duplicate.
- **Retry:** bootstrap state-machine invocations use bounded retry policies for Step Functions and SageMaker throttling/service errors.
- **Rollback/failure:** any unrecoverable error persists `bootstrap_status=FAILED` before terminating with `TASK12_BOOTSTRAP_FAILED`.

## Rollback guard (Task 5)
- Keep a deployment-level rollback switch to re-enable the prior bootstrap monthly handoff only during a controlled rollback window.
- Default posture stays on the monthly SFN path; legacy direct monthly pipeline invocation remains disabled to prevent regression.

## Partial-bootstrap recovery
If a prior run ended with a non-`READY` status, rerun bootstrap; persisted checkpoint status provides deterministic resume context and RT remains gated until `READY` is persisted.
