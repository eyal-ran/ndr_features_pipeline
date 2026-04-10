from __future__ import annotations

from ndr.contracts_v2 import (
    ContractDriftError,
    ContractValidationError,
    ErrorTaxonomyValidationError,
    assert_contract_surface,
    assert_error_codes_declared,
    assert_no_producer_only_fields,
    load_contract_matrix,
    load_error_taxonomy,
)


def test_contract_surface_positive_rt_core_pipeline_params() -> None:
    matrix = load_contract_matrix()
    assert_contract_surface(
        matrix=matrix,
        edge_id="rt_15m_core",
        layer="pipeline_params",
        observed_fields={
            "ProjectName",
            "FeatureSpecVersion",
            "MiniBatchId",
            "BatchStartTsIso",
            "BatchEndTsIso",
            "RawParsedLogsS3Prefix",
        },
    )


def test_contract_surface_rejects_unknown_field() -> None:
    matrix = load_contract_matrix()
    try:
        assert_contract_surface(
            matrix=matrix,
            edge_id="rt_15m_core",
            layer="pipeline_params",
            observed_fields={
                "ProjectName",
                "FeatureSpecVersion",
                "MiniBatchId",
                "BatchStartTsIso",
                "BatchEndTsIso",
                "RawParsedLogsS3Prefix",
                "UnexpectedField",
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
            edge_id="training_if_pipeline",
            layer="pipeline_params",
            observed_fields={
                "ProjectName",
                "FeatureSpecVersion",
                "MlProjectName",
                "RunId",
                "ExecutionTsIso",
                "DppConfigTableName",
                "MlpConfigTableName",
            },
        )
    except ContractDriftError as exc:
        assert exc.code == "CONTRACT_REQUIRED_FIELD_MISSING"
    else:
        raise AssertionError("Expected missing-required-field drift validation failure")


def test_contract_matrix_disallows_producer_only_fields() -> None:
    matrix = load_contract_matrix()
    assert_no_producer_only_fields(matrix=matrix)


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
            "ERROR_TAXONOMY_MISSING_CODE",
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


def test_drift_detection_ci_guard_blocks_undeclared_interface_change() -> None:
    matrix = load_contract_matrix()
    observed_with_drift = {
        "ProjectName",
        "FeatureSpecVersion",
        "ArtifactFamily",
        "RangeStartTsIso",
        "RangeEndTsIso",
        "UndeclaredFutureField",
    }
    try:
        assert_contract_surface(
            matrix=matrix,
            edge_id="backfill_reprocessing",
            layer="pipeline_params",
            observed_fields=observed_with_drift,
        )
    except ContractDriftError as exc:
        assert exc.code == "CONTRACT_UNDECLARED_FIELD"
    else:
        raise AssertionError("Expected CI guard to fail on undeclared drift")
