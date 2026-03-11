# Palo Alto Raw Partitioning + Batch Pointer Strategy (vNext)

## Scope

This document defines ML orchestration + builder-facing ingestion pointer behavior for the DPP/MLP refactor.

## Canonical path contract

The canonical runtime path shape is:

- `s3://<ingestion_bucket>/fw_paloalto/<org1>/<org2>/YYYY/MM/dd/<batch_id>/...`

Rules:

- `project_name` is the DPP id and is `fw_paloalto` in the canonical example.
- `data_source_name` must equal `project_name`.
- `batch_id` is the hashed folder name and canonical mini-batch identity.
- `raw_parsed_logs_s3_prefix` is the authoritative per-run pointer and must end with `/<batch_id>/`.

## Runtime source of truth

For 15m ingestion:

- Payload-provided `batch_id` and `raw_parsed_logs_s3_prefix` are authoritative.
- Orchestration resolves remaining runtime fields (`date_utc`, `hour_utc`, `slot15`, window timestamps) from payload timestamp.
- Base prefixes/contracts come from DynamoDB config; runtime composes only dynamic suffixes when needed.

## Delta + Pair Counts ingestion behavior (contract)

- Delta Builder primary input: `raw_parsed_logs_s3_prefix` (runtime pointer).
- Pair Counts Builder primary input: `raw_parsed_logs_s3_prefix` (runtime pointer for batch data selection).
- `raw_parsed_logs_s3_prefix` is canonical and required; legacy aliases (`batch_s3_prefix`, `mini_batch_s3_prefix`) are accepted only at ingress during P0.

## Slot derivation policy

`slot15` is derived from payload timestamp minute:

- `00-14 => 1`
- `15-29 => 2`
- `30-44 => 3`
- `45-59 => 4`

## Idempotency + indexing

Before starting 15m pipeline execution, Step Functions writes deterministic batch identity records to `ndr_batch_index` using idempotent Put/Update semantics.

This enables:

- replay-safe ingestion,
- reverse lookup by batch id via GSI,
- index-first non-RT lookup behavior in later tasks.

## Compatibility and migration

Task 7 finalization removes dual-mode behavior:

- canonical path parsing is required,
- `raw_parsed_logs_s3_prefix` is always required at canonical surfaces,
- no compatibility toggles are used in this contract.
