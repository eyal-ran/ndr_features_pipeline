from ndr.contracts import (
    CANONICAL_BATCH_INDEX_REQUIRED_FIELDS,
    CANONICAL_CODE_METADATA_FIELDS,
    DPP_CODE_STEP_KEYS,
    DPP_S3_PREFIX_KEYS,
    MLP_CODE_STEP_KEYS,
    date_lookup_sk,
)


def test_batch_index_contract_required_fields_are_frozen():
    assert CANONICAL_BATCH_INDEX_REQUIRED_FIELDS == (
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


def test_dpp_mlp_code_keys_and_metadata_fields_are_frozen():
    assert DPP_CODE_STEP_KEYS == (
        "delta_step",
        "fg_a_step",
        "pair_counts_step",
        "fg_c_step",
        "fg_b_step",
        "machine_inventory_unload_step",
        "backfill_extractor_step",
    )
    assert MLP_CODE_STEP_KEYS == ("inference_step", "join_step", "training_step")
    assert CANONICAL_CODE_METADATA_FIELDS == (
        "code_artifact_s3_uri",
        "artifact_build_id",
        "artifact_sha256",
        "artifact_format",
    )


def test_date_lookup_sk_shape_matches_dual_item_contract():
    assert date_lookup_sk(date_partition="2025/02/03", hour="14", within_hour_run_number="2") == "2025/02/03#14#2"


def test_dpp_prefix_ownership_keys_present():
    assert set(DPP_S3_PREFIX_KEYS) == {
        "delta",
        "pair_counts",
        "fg_a",
        "fg_a_subpaths",
        "fg_c",
        "machine_inventory",
        "fg_b",
        "code",
        "code_metadata",
    }
