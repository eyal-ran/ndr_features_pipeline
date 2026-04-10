# Task 9 rollout checklist (release gate)

## Preconditions
- [ ] `task9_release_hardening_gate.v1` validator is available in the release image.
- [ ] Release gate configuration loaded from `docs/release/task9_release_gate_config.json`.
- [ ] Scenario fixtures and contract fixtures match the candidate release version.

## Scenario matrix execution
- [ ] Execute scenarios: `normal`, `missing_dependency`, `fallback`, `duplicate_replay`, `partial_failure_retry`.
- [ ] Confirm each scenario validates all flows: monthly, RT, backfill, training, control-plane.
- [ ] Confirm deterministic outputs (repeat run produces same decision + idempotency behavior).
- [ ] Confirm failure handling was validated (retry and rollback path where applicable).

## Producer/consumer contract alignment
- [ ] Validate every contract edge has producer and consumer acceptance checks.
- [ ] Validate each edge has end-to-end integration evidence.
- [ ] Release is blocked when unresolved contract edges are non-empty.

## Observability and rollback readiness
- [ ] Rollback drill executed for this candidate release.
- [ ] Rollback restored last-known-good configuration and pointers.
- [ ] Post-rollback validation passed on monthly/RT/backfill/training/control-plane smoke checks.

## Abort conditions (no-go)
- Any critical scenario fails.
- Critical scenario pass rate is below configured threshold.
- Any producer/consumer contract edge is unresolved.
- Rollback drill not executed or post-rollback validation failed.

## Approval
- [ ] Release manager sign-off.
- [ ] Control-plane owner sign-off.
- [ ] Runtime owner sign-off.
