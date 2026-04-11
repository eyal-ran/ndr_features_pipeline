# Task 7 — v3 final release gate and acceptance enforcement

## Orientation completed before implementation
- Full `refactoring_fixes_plan_v3.md` reviewed end-to-end.
- Prior v3 task deliverables and validator/test surfaces reviewed:
  - Task 0 schema freeze (`tests/test_task0_v3_schema_freeze.py`).
  - Task 1–4 readiness/fallback gates (`tests/test_task1_readiness_contracts.py`, `tests/test_task2_monthly_baseline_dependency_gate_contracts.py`, `tests/test_task3_rt_readiness_checker_job.py`, `tests/test_task4_rt_raw_input_resolver_job.py`).
  - Task 5–6 producer/consumer alignment (`tests/test_task5_backfill_execution_contracts.py`, `tests/test_task6_cross_flow_contract_conformance.py`).
  - Task 8–9 deployment + release hardening (`tests/test_task9_release_gate.py`, `docs/release/task9_release_gate_config.json`).
- Startup validation scripts reviewed:
  - `src/ndr/scripts/run_startup_contract_gate.py`
  - `src/ndr/scripts/run_startup_observability_gate.py`
  - `src/ndr/scripts/check_contract_drift_v3.py`

## Implemented release gate hardening
1. Added strict final gate validator `evaluate_task7_v3_final_release_gate`:
   - Blocks if any correctness-critical class is missing/failed.
   - Blocks if any critical check is warning-only.
   - Blocks when targeted flow matrix is incomplete or failing.
   - Blocks when producer/consumer drift remains.
   - Blocks when startup/deployment-precondition gates are not `go`.
   - Blocks when rollback drill/replay determinism is not verified.
2. Added CLI runner `run_v3_final_release_gate.py` for deterministic gate execution from JSON evidence.
3. Added canonical evidence file `task7_v3_final_release_gate_evidence.json` for auditable pass/fail evaluation.
4. Added targeted tests to enforce strict red/no-go behavior for contract-critical defects.

## Final checklist and failure criteria (release blocking)
- QueryLanguage explicitly declared for JSONata SFNs.
- No readiness-from-input anti-pattern accepted.
- Backfill requested family scope enforced end-to-end.
- RT fallback contract failures block release.
- Producer/consumer contracts verified with no drift.
- Startup + deployment-precondition gates green.
- Rollback drill completed and deterministic replay safety confirmed.

## Validation commands
- `PYTHONPATH=src pytest -q tests/test_task7_v3_final_release_gate.py`
- `PYTHONPATH=src python -m ndr.scripts.run_v3_final_release_gate --evidence-path docs/archive/debug_records/task7_v3_final_release_gate_evidence.json`
