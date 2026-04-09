# Task 15 — Startup observability and rollback validation bundle

## Evidence artifacts
- Canonical bundle: `docs/archive/debug_records/task15_startup_observability_bundle.json`
- Orientation notes: `docs/archive/debug_records/task15_startup_failure_classes_and_remediation_paths.md`
- Dashboard definition: `docs/archive/debug_records/task15_startup_dashboard.json`
- Startup runbook: `docs/STARTUP_OBSERVABILITY_ROLLBACK_RUNBOOK.md`
- Test evidence: `docs/archive/debug_records/task15_startup_observability_test_evidence.md`
- Validator: `src/ndr/orchestration/contract_validators.py` (`evaluate_task15_initial_deployment_observability`)
- CLI gate: `src/ndr/scripts/run_startup_observability_gate.py`
- Tests: `tests/test_task15_startup_observability_and_rollback.py`

## Gate command
```bash
PYTHONPATH=src python -m ndr.scripts.run_startup_observability_gate \
  --evidence-path docs/archive/debug_records/task15_startup_observability_bundle.json
```

The command exits non-zero when startup observability/rollback readiness is not production-safe.
