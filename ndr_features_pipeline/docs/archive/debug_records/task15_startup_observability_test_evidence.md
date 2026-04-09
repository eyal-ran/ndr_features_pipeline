# Task 15 test evidence

## Synthetic startup failure drills
- Injected unresolved missing ranges (`missing_range_remediation_gap`) and observed `startup-missing-range-remediation-gap` alarm trigger.
- Injected extractor fallback instability (`extractor_bootstrap_fragility`) and observed `startup-extractor-bootstrap-fragility` alarm trigger.
- Injected fallback integration miss (`raw_log_fallback_not_integrated`) and observed `startup-raw-fallback-integration-failure` alarm trigger.
- Injected startup contract mismatch and observed `startup-contract-validation-failure` alarm trigger.

## Rollback drill
- Enabled `BootstrapRollbackMode` with DDB key `bootstrap_rollback#v1`.
- Verified startup flow reverted to `pre_bootstrap_rt_gate` operation.
- Re-ran readiness checks and confirmed stable state restoration with no data corruption.
