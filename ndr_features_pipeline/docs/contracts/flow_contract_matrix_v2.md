# Flow Contract Matrix v2 (Frozen)

This is the authoritative v2 contract source for cross-flow interfaces. Machine-readable source: `docs/contracts/flow_contract_matrix_v2.json`.

## Scope
- Monthly
- RT (15m)
- Backfill
- Training
- Control-plane bootstrap

## Layers tracked per edge
1. SFN payload fields
2. SageMaker pipeline parameters
3. Runtime CLI args
4. Runtime fields
5. DDB keys/config dependencies

## Invariants
- Every field has explicit producer + consumer ownership, or explicit `validated_metadata_noop=true`.
- Required fields are fail-fast validated by the shared validator (`CONTRACT_REQUIRED_FIELD_MISSING`).
- Unknown fields fail-fast in CI/validator (`CONTRACT_UNDECLARED_FIELD`).
- Business configuration remains DDB-first (DPP/MLP/Batch Index keys are explicit in matrix).

## Operational semantics
- Idempotency: backfill contract optionally carries `idempotency_key`; consumers must treat duplicate keys as replay-safe.
- Retry: retriable vs non-retriable decisions are defined in `error_taxonomy_v2`.
- Rollback: control-plane and monthly consumers rely on DDB pointer rollback (for example `ip_machine_mapping_s3_prefix`) instead of runtime override fallbacks.

## Pre-merge drift check
Use `tests/test_task0_contract_matrix_v2.py` in CI to block undeclared drift between frozen matrix and implemented interfaces.
