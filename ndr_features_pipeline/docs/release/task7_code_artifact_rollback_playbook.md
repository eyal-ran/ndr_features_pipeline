# Task 7 code-artifact lifecycle rollback playbook

## Objective
Provide deterministic rollback for code-artifact promotion failures, including failed validation and failed smoke paths.

## Trigger classes
- **Validation failure (pre-promotion):** hash mismatch, missing entry script, invalid archive, or missing object.
- **Smoke failure (post-promotion candidate):** entry script execution/import failure, timeout, or runtime contract failure.
- **Promotion-state mismatch:** control-plane pointers diverge from expected artifact build id.

## Deterministic rollback actions by stage
1. **Validation failure (`failure_stage=validate`)**
   - Block promotion immediately.
   - Keep previous DDB step contract pointers unchanged.
   - Emit non-retriable alert with deterministic error code and artifact build id.
2. **Smoke failure (`failure_stage=smoke`)**
   - Roll back promoted contract pointers to last-known-good artifact build id.
   - Restore last-known-good immutable artifact URIs.
   - Open incident record with smoke error code and affected families.
3. **Promotion failure (`failure_stage=promotion`)**
   - Roll back promoted pointers.
   - Reconcile DDB contract state against previous successful release snapshot.
   - Open incident and require explicit operator confirmation before retry.

## Idempotency and retry guarantees
- All rollback operations must use deterministic execution IDs and source artifact references.
- Re-running rollback with identical inputs must be no-op safe.
- Retriable behavior is limited to transient infrastructure failures; contract violations are non-retriable and remain blocking.

## Post-rollback acceptance checks
- Validation + smoke reports for rollback target are `PASS`.
- Producer/consumer alignment remains green for all required families.
- Contract drift and notebook/pipeline gate checks remain green.
- Rollback event log includes operator identity, timestamps, and evidence paths.
