# Task 9 rollback playbook

## Objective
Restore the prior stable release when Task 9 release-gate failures are detected after rollout begins.

## Rollback triggers
- Critical scenario regression in production canary.
- Producer/consumer contract mismatch detected on live path.
- Retry/replay behavior diverges from idempotency contract.

## Procedure
1. **Stop forward rollout**
   - Freeze deployment promotion and disable new candidate traffic routing.
2. **Restore control-plane contracts/config**
   - Revert release gate config and contract pointers to last-known-good revision.
   - Revert DDB-owned orchestration target/config values to previous version.
3. **Restore artifact pointers**
   - Revert model/artifact/data pointers to last-known-good immutable versions.
   - Confirm pointer updates are atomic from consumer perspective.
4. **Re-run readiness checks**
   - Re-execute monthly/RT/training/backfill/control-plane smoke matrix against rollback revision.
5. **Post-rollback validation**
   - Confirm all critical scenarios pass with threshold 1.0.
   - Confirm no unresolved producer/consumer contract edges.
6. **Close incident**
   - Capture rollback execution ID, operator, timestamps, and evidence links.

## Idempotency + retry safeguards
- Rollback operations must use deterministic execution IDs.
- Re-running rollback with same execution ID must be safe (no duplicate side effects).
- Transient failures are retried; contract violations are non-retriable and require operator intervention.

## Success criteria
- Stable release revision restored.
- Post-rollback validation passed.
- Release gate status is `go` for rollback target.
