# Task 6 — Cross-flow producer/consumer alignment hardening

## What was hardened
- Added a cross-flow compatibility matrix covering monthly, RT, backfill, bootstrap, training, and deployment interfaces (`task6_cross_flow_contract_matrix.json`).
- Added a strict conformance validator (`evaluate_task6_cross_flow_contract_conformance`) to enforce:
  - producer↔consumer field parity,
  - explicit producer/consumer mapping tables,
  - strict-controls posture for unknown/missing/version mismatch behavior,
  - idempotency/retry/rollback safeguards.
- Added strict runtime schema for RT raw-input resolution (`rt.raw_input_resolution.v1`) and validated resolver job output against it.

## Fail-fast semantics now asserted
- Unknown fields are rejected for readiness and RT raw-input-resolution contracts.
- Missing required fields are rejected for RT raw-input-resolution contracts.
- Contract version mismatches are rejected for backfill requests.

## Replay/idempotency verification
- Backfill request builder replay check confirms deterministic idempotency key generation for equivalent payloads.
