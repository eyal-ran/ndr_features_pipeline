# Startup Observability + Rollback Runbook (Task 15)

## Scope
This runbook makes cold-start execution detectable, diagnosable, and rollback-safe across RT, monthly, backfill, and training startup paths.

## Startup health metrics (must emit)
- `bootstrap_duration_seconds`
- `startup_remediation_invocation_count`
- `startup_unresolved_missing_range_count`
- `startup_fallback_source_mode_count`
- `startup_contract_validation_failure_count`

## Dashboard
Use `docs/archive/debug_records/task15_startup_dashboard.json` dashboard definition (`ndr-startup-health`) for production monitoring.

## Alerting policy
| Alarm | Failure class | Severity | Noise budget | Runbook section |
|---|---|---|---:|---|
| `startup-missing-range-remediation-gap` | `missing_range_remediation_gap` | sev1 | 2/day | [Missing range remediation gap](#failure-class-missing-range-remediation-gap) |
| `startup-extractor-bootstrap-fragility` | `extractor_bootstrap_fragility` | sev2 | 4/day | [Extractor bootstrap fragility](#failure-class-extractor-bootstrap-fragility) |
| `startup-raw-fallback-integration-failure` | `raw_log_fallback_not_integrated` | sev1 | 1/day | [Raw-log fallback not integrated](#failure-class-raw-log-fallback-not-integrated) |
| `startup-contract-validation-failure` | `startup_contract_validation_failure` | sev2 | 3/day | [Startup contract validation failure](#failure-class-startup-contract-validation-failure) |

## Failure class: missing range remediation gap
1. Confirm alarm source metric `startup_unresolved_missing_range_count` > 0.
2. Check latest targeted backfill execution ID and missing-ranges manifest.
3. Re-run targeted backfill only for unresolved families/ranges.
4. Verify count returns to 0 before enabling RT dependent phase.

## Failure class: extractor bootstrap fragility
1. Confirm fallback distribution in `startup_fallback_source_mode_count` by source mode.
2. Validate extractor runtime input contract and manifest completeness.
3. Re-run extractor in deterministic startup mode with source hints from DDB config.
4. Confirm manifests include required artifact families before continuing bootstrap.

## Failure class: raw-log fallback not integrated
1. Confirm fallback source mode is selected but no reconstructed outputs were produced.
2. Verify fallback branch integration in backfill orchestration and raw-log S3 prefixes.
3. Execute targeted recovery for missing families/ranges.
4. Re-run startup contract gate and validate downstream consumers receive artifacts.

## Failure class: startup contract validation failure
1. Run startup contract gate (`run_startup_contract_gate.py`) and collect diagnostics.
2. Resolve producer/consumer contract mismatch from Task 14 matrix.
3. Re-run gate until status is green.
4. Resume startup flow only after green status.

## Rollback switch (DDB-first)
- Switch name: `BootstrapRollbackMode`
- Config source: DynamoDB control table (`bootstrap_rollback#v1`)
- Safe mode: `pre_bootstrap_rt_gate`

### Rollback procedure
1. Set rollback switch in DDB to safe mode.
2. Halt new startup remediation invocations.
3. Keep RT inference/publication on pre-bootstrap gating path.
4. Validate no data corruption (checkpoint hashes and manifest integrity).
5. After remediation fixes, disable rollback switch and run synthetic startup tests.

## Ownership and escalation
- Primary owner team: `NDR Platform`
- Escalation policy: `NDR-Startup-PagerDuty`
- Consumer comms: update startup readiness status and failure class every 15 minutes until recovery.

## Exception-table contracts (Task 8)
- Run exception-table preflight before RT/monthly/backfill/training startup:
  - `PYTHONPATH=src python -m ndr.scripts.run_exception_table_preflight --flow <rt|monthly|backfill|training>`.
- Non-retriable failures: `EXC_TABLE_SCHEMA_DRIFT`, `EXC_TABLE_MISSING`, `EXC_TABLE_ACCESS_DENIED`.
  - Stop startup and correct schema/provisioning/IAM before retry.
- Retriable failure: `EXC_TABLE_DDB_TRANSIENT`.
  - Retry with backoff; escalate if sustained.
- Lock cleanup safety:
  - inspect `processing_lock`/`publication_lock` by `pk/sk`,
  - only remove expired rows (`ttl_epoch` in the past),
  - record manual deletes in incident notes to preserve rollback/audit traceability.
