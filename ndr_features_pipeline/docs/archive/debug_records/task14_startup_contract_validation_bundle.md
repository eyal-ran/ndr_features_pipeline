# Task 14 — Startup contract conformance validation bundle

## Evidence artifacts
- Canonical startup contract matrix: `docs/archive/debug_records/task14_startup_contract_matrix.json`
- Validator implementation: `src/ndr/orchestration/contract_validators.py` (`evaluate_task14_startup_contract_conformance`)
- Pre-deploy gate CLI: `src/ndr/scripts/run_startup_contract_gate.py`
- Contract tests (green + intentional mismatch fixtures): `tests/test_task14_startup_contract_conformance.py`

## CI/pre-deploy gate command
```bash
PYTHONPATH=src python -m ndr.scripts.run_startup_contract_gate \
  --evidence-path docs/archive/debug_records/task14_startup_contract_matrix.json
```

The command exits non-zero on startup contract red status to block release.
