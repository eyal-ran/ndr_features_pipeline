# Task 2 backfill provider hardening traces

## Trace A — Mixed-family dispatch succeeds with deterministic dependency order
1. Executor receives `artifact_family=fg_c` and normalized `missing_entries` for all expanded families.
2. `BackfillFamilyDispatcher` normalizes entries and dispatches in canonical order: `delta -> fg_a -> pair_counts -> fg_b_baseline -> fg_c`.
3. Each family dispatch includes deterministic payload contract fields:
   - `project_name`, `feature_spec_version`, `family`, `window_ranges`, `batch_ids`,
   - `correlation_id`, `retry_attempt`.
4. `BackfillCompletionVerifier` receives the family handler outputs and emits:
   - `all_succeeded=true`, `failed_families=[]`, `unresolved_families=[]`, `verifier_code=BACKFILL_COMPLETION_OK`.
5. Backfill completion event path remains enabled only after verifier pass.

## Trace B — Unsupported family fails fast with explicit contract code
1. Dispatcher normalization receives `missing_entries[0].family=unknown_family`.
2. Dispatcher raises:
   - `BACKFILL_DISPATCH_CONTRACT_ERROR: unsupported family in missing_entries[0].family='unknown_family'`.
3. No family dispatch requests are submitted.

## Trace C — Duplicate replay is idempotent and completion-safe
1. Initial request dispatch starts with `correlation_id=bkf-3`.
2. Replay with same request/correlation id receives `ConflictException` from SageMaker start call.
3. Family result is marked `DuplicateRequest` with non-retriable `EXECUTION_ALREADY_EXISTS` reason.
4. Verifier treats duplicate replay as terminal success and still requires full requested-family coverage.

## Trace D — Completion event blocked on any family failure
1. Family handler outputs contain `fg_a=status=Failed`.
2. `BackfillCompletionVerifier` emits:
   - `all_succeeded=false`, `failed_families=['fg_a']`, `verifier_code=BACKFILL_COMPLETION_CONTRACT_ERROR`.
3. Completion publication is blocked; flow fails with `BackfillCompletionContractError`.
