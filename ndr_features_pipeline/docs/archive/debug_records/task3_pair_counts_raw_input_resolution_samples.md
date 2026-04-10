# Task 3 — Pair-counts raw-input resolution metadata samples

This document captures the persisted metadata contract emitted by
`run_pair_counts_builder_from_runtime_config` after deterministic
raw-input resolution via `RawInputResolver`.

## Metadata artifact location

For each run, metadata is written to:

`<pair_counts_batch_output_prefix>/_metadata/raw_input_resolution.json`

where `<pair_counts_batch_output_prefix>` is the deterministic pair-counts
batch output prefix resolved from Batch Index `s3_prefixes.dpp.pair_counts`
(preferred) or the pair-counts JobSpec output prefix.

## Contract

```json
{
  "contract_version": "pair_counts.raw_input_resolution.v1",
  "project_name": "<project_name>",
  "feature_spec_version": "<feature_spec_version>",
  "mini_batch_id": "<mini_batch_id>",
  "batch_start_ts_iso": "<ISO8601>",
  "batch_end_ts_iso": "<ISO8601>",
  "source_mode": "ingestion|redshift_unload_fallback",
  "resolution_reason": "ingestion_rows_present|ingestion_rows_missing",
  "raw_input_s3_prefix": "<resolved input prefix>",
  "provenance": {
    "source_mode": "ingestion|redshift_unload_fallback",
    "producer_flow": "pair_counts_builder",
    "resolution_reason": "ingestion_rows_present|ingestion_rows_missing",
    "request_id": "<mini_batch_id>",
    "resolved_at": "<ISO8601 UTC>"
  }
}
```

## Sample: ingestion available

```json
{
  "contract_version": "pair_counts.raw_input_resolution.v1",
  "project_name": "proj",
  "feature_spec_version": "v1",
  "mini_batch_id": "mb-1",
  "batch_start_ts_iso": "2025-01-01T00:00:00Z",
  "batch_end_ts_iso": "2025-01-01T00:15:00Z",
  "source_mode": "ingestion",
  "resolution_reason": "ingestion_rows_present",
  "raw_input_s3_prefix": "s3://bucket/raw/mb-1/",
  "provenance": {
    "source_mode": "ingestion",
    "producer_flow": "pair_counts_builder",
    "resolution_reason": "ingestion_rows_present",
    "request_id": "mb-1",
    "resolved_at": "2025-01-01T00:16:00Z"
  }
}
```

## Sample: ingestion missing, fallback enabled

```json
{
  "contract_version": "pair_counts.raw_input_resolution.v1",
  "project_name": "proj",
  "feature_spec_version": "v1",
  "mini_batch_id": "mb-1",
  "batch_start_ts_iso": "2025-01-01T00:00:00Z",
  "batch_end_ts_iso": "2025-01-01T00:15:00Z",
  "source_mode": "redshift_unload_fallback",
  "resolution_reason": "ingestion_rows_missing",
  "raw_input_s3_prefix": "s3://bucket/fallback/descriptor=pair_counts/range_0000/attempt_01/",
  "provenance": {
    "source_mode": "redshift_unload_fallback",
    "producer_flow": "pair_counts_builder",
    "resolution_reason": "ingestion_rows_missing",
    "request_id": "mb-1",
    "resolved_at": "2025-01-01T00:16:00Z"
  }
}
```
