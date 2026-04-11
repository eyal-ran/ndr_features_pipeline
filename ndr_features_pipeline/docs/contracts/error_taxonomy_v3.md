# Error Taxonomy v3 (Deterministic)

Authoritative machine-readable source: `docs/contracts/error_taxonomy_v3.json`.

## Classification rules
- `retriable=false`: contract/schema violations, unsupported families, hash mismatches.
- `retriable=true`: missing readiness artifact due to eventual consistency/transient publication timing.

## Required usage
- Contract validators and orchestration gates must emit one declared taxonomy code.
- CI checks must fail on missing taxonomy declarations for new contract errors.
