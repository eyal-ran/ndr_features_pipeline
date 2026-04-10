# Error Taxonomy v2 (Deterministic)

Machine-readable source: `docs/contracts/error_taxonomy_v2.json`.

## Rules
- Error code selection is deterministic by failure class.
- Every contract/gate failure maps to one explicit code.
- `retriable=true` is only used for failures where replay can realistically succeed without changing contract inputs.

## Classification guidance
- **Non-retriable**: contract shape violations, missing required ownership, unsupported family, missing DDB key schema.
- **Retriable**: transient DDB throttling, temporarily missing readiness artifacts, temporary batch index lag, fallback empty due to eventual consistency.

## Required usage
- Validators and orchestration gates must emit one taxonomy code.
- CI drift checks must fail with contract domain codes when interfaces drift.
- New error classes require adding taxonomy entry before producer/consumer change merges.
