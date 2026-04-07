# Task 9 hardening validation bundle

## Scope implemented
- Added cross-cutting Task 9 validators in `src/ndr/orchestration/contract_validators.py`:
  - `validate_no_business_fallback_markers(...)` to fail fast when runtime/business fields still contain placeholder or env-fallback markers.
  - `validate_targeted_recovery_manifest(...)` to enforce selective family/range execution for targeted-recovery flows.
- Added focused tests in `tests/test_task9_hardening_validators.py` to validate both positive and negative paths for these hardening rules.
- Corrected `fg_a_builder_job.py` import compatibility by removing unused `Window` import that broke test-module import in offline/stubbed environments.

## Regression hardening intent coverage
- **No placeholder/env business fallback remains (evidence):**
  - Validators reject `${...}`, `<required:...>`, `<placeholder...>`, `env_fallback`, and `code_default` markers in contract-critical values.
- **Targeted-recovery compatibility checks:**
  - Manifest validation enforces requested-family-only execution and non-empty range lists per family.

## Test commands executed for this patch
- `PYTHONPATH=src pytest -q tests/test_task9_hardening_validators.py`
- `PYTHONPATH=src pytest -q tests/test_task0_contract_matrix.py tests/test_task810_integration_gates.py tests/test_task89_runtime_contract_hygiene.py tests/test_task85_batch_index_readiness.py tests/test_task71_backfill_contracts.py tests/test_task72_backfill_manifest_map_wiring.py tests/test_task73_backfill_redshift_fallback.py tests/test_task74_backfill_batch_index_writes.py`

## Notes
- Full-suite Spark-heavy tests require a real PySpark runtime instead of the lightweight offline stubs used by many contract tests; those are tracked separately from this Task 9 hardening patch.
