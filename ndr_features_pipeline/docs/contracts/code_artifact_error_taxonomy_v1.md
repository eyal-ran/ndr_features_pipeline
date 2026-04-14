# Code artifact lifecycle error taxonomy (`code_artifact_error_taxonomy.v1`)

This taxonomy freezes deterministic error-code semantics for Task 0 contract consumers and producers.

## Retriable errors
- `CODE_ARTIFACT_OBJECT_MISSING`: expected S3 artifact object not present yet.
- `CODE_ARTIFACT_DOWNLOAD_FAILED`: transient S3/network/service failure during artifact retrieval.
- `CODE_SMOKE_TIMEOUT`: smoke command timeout; safe to retry with same immutable artifact id.

## Non-retriable errors
- `CODE_ARTIFACT_HASH_MISMATCH`: immutable bytes diverge from declared hash; operator remediation required.
- `CODE_ARTIFACT_ARCHIVE_INVALID`: artifact payload is not a valid declared archive format.
- `CODE_ARTIFACT_ENTRY_SCRIPT_MISSING`: build manifest entry script cannot be resolved inside archive.
- `CODE_ARTIFACT_SCHEMA_INVALID`: validator reported frozen contract violation.
- `CODE_SMOKE_EXECUTION_FAILED`: entry script invocation failed deterministically.
- `CODE_SMOKE_IMPORT_FAILED`: import/runtime shape mismatch in artifact extraction environment.
- `CODE_SMOKE_SCHEMA_INVALID`: smoke report failed frozen schema checks.

## Idempotency, retry, and rollback notes
- Build/validate/smoke should key writes by `artifact_build_id` and never mutate prior immutable build IDs.
- Retried validation/smoke runs must emit the same contract shape and deterministic status/error code for the same artifact bytes.
- Rollback is achieved by promoting a previously validated `artifact_build_id` in DDB script contracts; no artifact rewrite required.
