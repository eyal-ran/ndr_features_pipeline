# Task 9 — End-to-end hardening and rollout gate validation bundle

## Mandatory orientation completed
Reviewed:
- `docs/archive/debug_records/refactoring_fixes_plan_v2.md` (all Part A findings + Task 9 requirements).
- Task 0 contract foundations (`docs/contracts/flow_contract_matrix_v2.md`, `docs/contracts/flow_contract_matrix_v2.json`).
- Task 1 readiness gate contracts (`tests/test_task1_readiness_contracts.py`).
- Task 2/3 execution traces and fallback outputs (`docs/archive/debug_records/task2_backfill_provider_hardening_traces.md`, `docs/archive/debug_records/task3_pair_counts_raw_input_resolution_samples.md`).
- Task 4–7 integration surfaces in orchestration/runtime tests (`tests/test_task5_bootstrap_monthly_orchestration_contracts.py`, `tests/test_task7_training_readiness_recompute.py`, `tests/test_task74_backfill_batch_index_writes.py`).
- Task 8 outputs (`docs/archive/debug_records/task89_runtime_field_mapping_report.md`, `docs/archive/debug_records/task810_integration_gate_evidence.md`, `docs/archive/debug_records/task811_training_verification_bundle.md`, `docs/EXCEPTION_TABLE_CONTRACTS.md`).

## Inventory of touched contracts and integration points
1. **Release gate contract** (`task9_release_hardening_gate.v1`)
   - Scenario matrix schema.
   - Critical scenario threshold policy.
   - Producer/consumer contract-edge validation requirements.
   - Rollback drill + post-rollback validation requirements.
2. **Cross-flow integration paths validated in matrix**
   - Monthly, RT, backfill, training, and control-plane coherence.
   - Duplicate replay/idempotency and partial-failure retry behavior.
3. **Failure signaling**
   - `TASK9_CONTRACT_VIOLATION` for malformed/incomplete gate evidence.
   - `TASK9_RELEASE_GATE_RED` for no-go release decisions.

## Deliverables implemented
- Regression suite for Task 9 release gate semantics:
  - `tests/test_task9_release_gate.py`
- Release-gate validation logic:
  - `src/ndr/orchestration/contract_validators.py` (`evaluate_task9_release_hardening_gate`)
- Release gate config:
  - `docs/release/task9_release_gate_config.json`
- Rollout checklist:
  - `docs/release/task9_rollout_checklist.md`
- Rollback playbook:
  - `docs/release/task9_rollback_playbook.md`

## Scenario matrix coverage
Required scenarios covered and enforced:
- `normal`
- `missing_dependency`
- `fallback`
- `duplicate_replay`
- `partial_failure_retry`

Each scenario must pass:
- deterministic execution checks,
- failure-handling validation,
- standalone + full integration execution,
- all-flow coverage (`monthly`, `rt`, `backfill`, `training`, `control_plane`).

## Release gate policy
- Gate blocks when any critical scenario fails.
- Critical threshold is policy-driven and defaults to `1.0`.
- Gate blocks when any producer/consumer contract edge remains unresolved.
- Gate blocks when rollback drill is missing or post-rollback validation fails.

## Rollback readiness validation
- Rollback drill execution is mandatory before release approval.
- Recovery requires both state restoration and post-rollback validation pass.
- Rollback process is defined as idempotent with retry-safe behavior.

## Evidence commands
- `PYTHONPATH=src pytest -q tests/test_task9_release_gate.py`
- `PYTHONPATH=src pytest -q tests/test_task9_hardening_validators.py tests/test_task11_system_readiness_gate.py`
