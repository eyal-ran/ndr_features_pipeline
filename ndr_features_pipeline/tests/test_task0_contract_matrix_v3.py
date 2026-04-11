from __future__ import annotations

from ndr.contracts_v2 import (
    ContractDriftError,
    ErrorTaxonomyValidationError,
    assert_contract_surface,
    assert_error_codes_declared,
    assert_no_consumer_only_fields,
    assert_no_producer_only_fields,
    load_contract_matrix,
    load_error_taxonomy,
)


def test_contract_surface_positive_monthly_artifact() -> None:
    matrix = load_contract_matrix()
    assert_contract_surface(
        matrix=matrix,
        edge_id="monthly_readiness_artifact",
        layer="artifact",
        observed_fields={
            "contract_version",
            "project_name",
            "feature_spec_version",
            "reference_month",
            "ready",
            "required_families",
            "missing_ranges",
            "decision_code",
            "as_of_ts",
            "idempotency_key",
        },
    )


def test_contract_surface_rejects_unknown_field() -> None:
    matrix = load_contract_matrix()
    try:
        assert_contract_surface(
            matrix=matrix,
            edge_id="step_code_artifact",
            layer="ddb_step_spec",
            observed_fields={
                "code_prefix_s3",
                "code_artifact_s3_uri",
                "entry_script",
                "artifact_build_id",
                "artifact_sha256",
                "artifact_format",
                "future_field",
            },
        )
    except ContractDriftError as exc:
        assert exc.code == "CONTRACT_UNDECLARED_FIELD"
    else:
        raise AssertionError("Expected unknown-field drift validation failure")


def test_contract_surface_rejects_missing_required_field() -> None:
    matrix = load_contract_matrix()
    try:
        assert_contract_surface(
            matrix=matrix,
            edge_id="rt_readiness_artifact",
            layer="artifact",
            observed_fields={
                "contract_version",
                "project_name",
                "feature_spec_version",
                "ml_project_name",
                "mini_batch_id",
                "ready",
                "required_families",
                "missing_ranges",
                "decision_code",
                "as_of_ts",
            },
        )
    except ContractDriftError as exc:
        assert exc.code == "CONTRACT_REQUIRED_FIELD_MISSING"
    else:
        raise AssertionError("Expected missing-required-field drift validation failure")


def test_contract_matrix_disallows_producer_or_consumer_only_fields() -> None:
    matrix = load_contract_matrix()
    assert_no_producer_only_fields(matrix=matrix)
    assert_no_consumer_only_fields(matrix=matrix)


def test_error_taxonomy_declares_required_contract_codes() -> None:
    taxonomy = load_error_taxonomy()
    assert_error_codes_declared(
        taxonomy=taxonomy,
        required_codes={
            "CONTRACT_UNDECLARED_FIELD",
            "CONTRACT_REQUIRED_FIELD_MISSING",
            "CONTRACT_EDGE_NOT_FOUND",
            "CONTRACT_LAYER_NOT_FOUND",
            "CONTRACT_PRODUCER_ONLY_FIELD",
            "CONTRACT_CONSUMER_ONLY_FIELD",
            "ERROR_TAXONOMY_MISSING_CODE",
            "MONTHLY_READINESS_CONTRACT_INVALID",
            "RT_READINESS_CONTRACT_INVALID",
            "BACKFILL_REQUEST_CONTRACT_INVALID",
            "CODE_ARTIFACT_CONTRACT_INVALID",
        },
    )


def test_error_taxonomy_missing_code_fails_fast() -> None:
    taxonomy = load_error_taxonomy()
    try:
        assert_error_codes_declared(taxonomy=taxonomy, required_codes={"DOES_NOT_EXIST"})
    except ErrorTaxonomyValidationError as exc:
        assert exc.code == "ERROR_TAXONOMY_MISSING_CODE"
    else:
        raise AssertionError("Expected missing taxonomy code validation failure")
