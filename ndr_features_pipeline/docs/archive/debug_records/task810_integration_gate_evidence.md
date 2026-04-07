# Task 8.10 — Integration gate definitions and evidence

## Scope
- Lock integration order and cross-unit compatibility across Task 8.1–8.9 outputs.
- Add explicit dependency gates in IF training orchestration so composition failures fail fast with deterministic diagnostics.

## Integration gates added
- `IFTrainingJob._run_task8_integration_gate(stage, verification, history_plan)` now enforces and records Task 8 integration checks before `plan`, `remediate`, and `train` execution.
- Gate report artifact:
  - stage status key: `integration_gate`
  - contract version: `task8_integration_gate.v1`
  - output contains per-check records (`task_id`, `passed`, `detail`) and aggregate `status`.

## Dependency/order checks implemented
1. **8.1 + 8.9 runtime contract continuity**
   - Required runtime fields are present and non-empty before orchestration advances.
2. **8.6 branch context continuity**
   - `ml_project_name` must exist for branch-scoped execution.
3. **8.2 sequencing gate**
   - `plan` requires `verification.stage == verify`.
   - `remediate` requires `verification.stage == verify` lineage.
   - `train` requires `verification.stage == reverify`.
4. **8.3 manifest compatibility**
   - `missing_windows_manifest` is validated with canonical schema (`ensure_manifest`).
5. **8.4 selective-remediation compatibility**
   - Manifest entries must remain range-addressable (`ranges`) for selective execution.
6. **8.5 Batch Index readiness continuity (train gate)**
   - `train` requires `history_plan.batch_index_readiness.batch_index_evidence.selectors.pk`.

## Wiring points
- Planning stage now runs the integration gate immediately after loading verification status.
- Remediation stage runs the integration gate after loading verification + history plan.
- Train stage runs the integration gate after reverify hard-gate and history-plan load.

## Test evidence added
- `tests/test_task810_integration_gates.py`
  - plan gate rejects non-verify prerequisite lineage.
  - train gate fails when Batch Index readiness artifact is present but malformed.
  - train gate passes with integrated verification/history artifacts and deterministic check ordering.
