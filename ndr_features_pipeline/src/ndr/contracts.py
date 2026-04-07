"""Frozen machine-readable contracts for Task 1 refactor foundation."""

from __future__ import annotations

BATCH_INDEX_PK = "PK"
BATCH_INDEX_SK = "SK"

BATCH_LOOKUP_ITEM_KIND = "BATCH_LOOKUP"
DATE_LOOKUP_ITEM_KIND = "DATE_LOOKUP"

DPP_S3_PREFIX_KEYS: tuple[str, ...] = (
    "delta",
    "pair_counts",
    "fg_a",
    "fg_a_subpaths",
    "fg_c",
    "machine_inventory",
    "fg_b",
    "code",
    "code_metadata",
)

DPP_CODE_STEP_KEYS: tuple[str, ...] = (
    "delta_step",
    "fg_a_step",
    "pair_counts_step",
    "fg_c_step",
    "fg_b_step",
    "machine_inventory_unload_step",
    "backfill_extractor_step",
)

MLP_CODE_STEP_KEYS: tuple[str, ...] = ("inference_step", "join_step", "training_step")

CANONICAL_BATCH_INDEX_REQUIRED_FIELDS: tuple[str, ...] = (
    "PK",
    "SK",
    "batch_id",
    "date_partition",
    "hour",
    "within_hour_run_number",
    "etl_ts",
    "org1",
    "org2",
    "raw_parsed_logs_s3_prefix",
    "ml_project_names",
    "s3_prefixes",
)

CANONICAL_CODE_METADATA_FIELDS: tuple[str, ...] = (
    "artifact_mode",
    "artifact_build_id",
    "artifact_sha256",
)


def date_lookup_sk(*, date_partition: str, hour: str, within_hour_run_number: str) -> str:
    return f"{date_partition}#{hour}#{within_hour_run_number}"
