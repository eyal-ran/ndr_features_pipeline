# Exception DynamoDB Table Contracts (Task 8)

This document promotes three operational DynamoDB tables to explicit control-plane contracts.

## Registry (source of truth)
Runtime contract registry: `src/ndr/config/exception_table_contracts.py`.

| Logical contract | Default table name | Key schema | Allowed operations | TTL attribute | Purpose |
|---|---|---|---|---|---|
| `routing` | `ml_projects_routing` | `org_key(HASH)` | `DescribeTable`, `GetItem`, `PutItem` | n/a | Resolve org path to project + ingestion prefix. |
| `processing_lock` | `processing_lock` | `pk(HASH)`, `sk(RANGE)` | `DescribeTable`, `PutItem`, `DeleteItem` | `ttl_epoch` | Enforce idempotent/replay-safe processing windows. |
| `publication_lock` | `publication_lock` | `pk(HASH)`, `sk(RANGE)` | `DescribeTable`, `PutItem`, `UpdateItem`, `DeleteItem` | `ttl_epoch` | Publication idempotency and terminal status transitions. |

## Preflight validator (SageMaker Processing-compatible)
Use `src/ndr/scripts/run_exception_table_preflight.py` as a preflight Processing entrypoint in flows that depend on these tables.

Example:

```bash
PYTHONPATH=src python -m ndr.scripts.run_exception_table_preflight --flow rt
```

Supported profiles:
- `rt`: routing + processing lock + publication lock
- `monthly`: processing lock
- `backfill`: processing lock
- `training`: processing lock
- `all`: all three

## Error semantics
- `EXC_TABLE_SCHEMA_DRIFT`: non-retriable contract violation (wrong key schema).
- `EXC_TABLE_MISSING`: non-retriable missing dependency.
- `EXC_TABLE_ACCESS_DENIED`: non-retriable IAM/access mismatch.
- `EXC_TABLE_DDB_TRANSIENT`: retriable transient outage/throttling.

## Observability standard
Preflight emits structured JSON logs with these fields:
- `table`, `logical_name`, `operation`, `result`, `error_code`
- metric events:
  - `exception_table_validation_success_count`
  - `exception_table_validation_failure_count`

Lock contention/staleness signals remain tracked from orchestration paths and should use the same table/logical-name fields.

## Deployment and smoke checks
- Include all three tables in deployment inventory/provisioning (`build_split_table_contracts` + deployment plan).
- Seed at least one routing record before RT activation.
- Run deploy-time smoke checks:
  1. preflight validation (`--flow all`),
  2. routing lookup (`LoadProjectRoutingFromDynamo` equivalent `GetItem`),
  3. lock acquire/release lifecycle probe for processing/publication lock tables.
