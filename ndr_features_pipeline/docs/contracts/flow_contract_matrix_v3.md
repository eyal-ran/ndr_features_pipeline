# Flow Contract Matrix v3 (Frozen)

Authoritative machine-readable source: `docs/contracts/flow_contract_matrix_v3.json`.

## Scope frozen in Task 0
- `monthly_fg_b_readiness.v3`
- `rt_artifact_readiness.v3`
- `NdrBackfillRequest.v2` (`requested_families` semantics)
- Step code artifact contract fields for deployment/runtime wiring

## Ownership invariants
- Every field has explicit producer and consumer owners.
- Producer-only and consumer-only fields are blocked unless marked as explicit metadata no-op.
- Drift checks fail fast on unknown/missing fields and on contract-version mismatch.
