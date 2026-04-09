import json
from pathlib import Path

import pytest

from ndr.orchestration.remediation_contracts import (
    BACKFILL_REQUEST_VERSION,
    BASELINE_REQUEST_VERSION,
    REMEDIATION_RESPONSE_VERSION,
    TRAINING_REQUEST_VERSION,
    ContractValidationError,
    build_backfill_request,
    build_baseline_remediation_request,
    build_default_provenance,
    build_idempotency_key,
    build_remediation_response,
    build_training_remediation_request,
    load_schema,
    validate_contract_payload,
)

ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = ROOT / "docs" / "archive" / "debug_records" / "task0_remediation_contract_matrix.json"


def _provenance() -> dict[str, str]:
    return build_default_provenance(
        producer_flow="if_training",
        source_mode="batch_index",
        request_id="req-001",
    )


def test_task0_machine_readable_schemas_are_available_for_all_contracts():
    for version in [
        BACKFILL_REQUEST_VERSION,
        BASELINE_REQUEST_VERSION,
        TRAINING_REQUEST_VERSION,
        REMEDIATION_RESPONSE_VERSION,
    ]:
        schema = load_schema(version)
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"


def test_task0_positive_validation_for_all_three_requests_and_unified_response():
    backfill = build_backfill_request(
        project_name="fw_paloalto",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        batch_id="batch-123",
        requested_families=["delta", "fg_a"],
        missing_ranges=[
            {
                "family": "delta",
                "start_ts_iso": "2024-04-01T00:00:00Z",
                "end_ts_iso": "2024-04-01T00:15:00Z",
                "reason_code": "missing_feature_window",
            }
        ],
        provenance=_provenance(),
        source_hint={"raw_parsed_logs_s3_prefix": "s3://bucket/path/"},
    )
    assert backfill["contract_version"] == BACKFILL_REQUEST_VERSION

    baseline = build_baseline_remediation_request(
        project_name="fw_paloalto",
        feature_spec_version="v1",
        reference_month="2024/04",
        required_families=["delta", "fg_a", "fg_b_baseline"],
        missing_ranges=[
            {
                "family": "fg_b_baseline",
                "start_ts_iso": "2024-04-01T00:00:00Z",
                "end_ts_iso": "2024-04-01T23:59:59Z",
                "reason_code": "baseline_missing_for_reference",
            }
        ],
        provenance=build_default_provenance(
            producer_flow="monthly_fg_b_baselines",
            source_mode="batch_index",
            request_id="req-002",
        ),
    )
    assert baseline["contract_version"] == BASELINE_REQUEST_VERSION

    training = build_training_remediation_request(
        project_name="fw_paloalto",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        run_id="run-abc",
        training_window={"start_ts_iso": "2024-03-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:00:00Z"},
        evaluation_windows=[{"start_ts_iso": "2024-02-15T00:00:00Z", "end_ts_iso": "2024-02-29T00:00:00Z"}],
        missing_manifest={"contract_version": "if_training_missing_windows.v1", "entries": []},
        requested_families=["fg_a_15m", "fg_b_daily", "delta"],
        missing_ranges=[
            {
                "family": "delta",
                "start_ts_iso": "2024-04-01T00:00:00Z",
                "end_ts_iso": "2024-04-01T00:15:00Z",
                "reason_code": "missing_training_15m_window",
            }
        ],
        provenance=_provenance(),
    )
    assert training["contract_version"] == TRAINING_REQUEST_VERSION

    response = build_remediation_response(
        request_contract_version=TRAINING_REQUEST_VERSION,
        consumer="training",
        request_id=training["provenance"]["request_id"],
        idempotency_key=training["idempotency_key"],
        producer_flow="backfill_reprocessing",
        source_mode="batch_index",
        status="completed",
        produced_ranges=[
            {
                "family": "delta",
                "start_ts_iso": "2024-04-01T00:00:00Z",
                "end_ts_iso": "2024-04-01T00:15:00Z",
            }
        ],
        errors=[],
    )
    assert response["contract_version"] == REMEDIATION_RESPONSE_VERSION


def test_task0_version_gate_and_missing_unknown_field_behavior():
    with pytest.raises(ContractValidationError, match="CONTRACT_VERSION_UNSUPPORTED"):
        validate_contract_payload({"contract_version": "NdrTrainingRemediationRequest.v0"})

    valid = build_training_remediation_request(
        project_name="fw_paloalto",
        feature_spec_version="v1",
        run_id="run-abc",
        training_window={"start_ts_iso": "2024-03-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:00:00Z"},
        evaluation_windows=[],
        missing_manifest={"contract_version": "if_training_missing_windows.v1", "entries": []},
        requested_families=["delta"],
        missing_ranges=[
            {
                "family": "delta",
                "start_ts_iso": "2024-04-01T00:00:00Z",
                "end_ts_iso": "2024-04-01T00:15:00Z",
                "reason_code": "missing_training_15m_window",
            }
        ],
        provenance=_provenance(),
    )

    missing = dict(valid)
    missing.pop("run_id")
    with pytest.raises(ContractValidationError, match="CONTRACT_MISSING_REQUIRED_FIELD"):
        validate_contract_payload(missing)

    unknown = dict(valid)
    unknown["surprise"] = "not-allowed"
    with pytest.raises(ContractValidationError, match="CONTRACT_UNKNOWN_FIELD"):
        validate_contract_payload(unknown)


def test_task0_idempotency_recipe_is_deterministic_and_enforced():
    payload = {
        "contract_version": BACKFILL_REQUEST_VERSION,
        "consumer": "realtime",
        "project_name": "fw_paloalto",
        "feature_spec_version": "v1",
        "requested_families": ["delta"],
        "missing_ranges": [
            {
                "family": "delta",
                "start_ts_iso": "2024-04-01T00:00:00Z",
                "end_ts_iso": "2024-04-01T00:15:00Z",
                "reason_code": "missing",
            }
        ],
        "provenance": build_default_provenance(
            producer_flow="rt_15m_inference",
            source_mode="ingestion",
            request_id="req-rt-1",
        ),
    }
    key_a = build_idempotency_key(contract_version=BACKFILL_REQUEST_VERSION, payload_without_idempotency_key=payload)
    key_b = build_idempotency_key(contract_version=BACKFILL_REQUEST_VERSION, payload_without_idempotency_key=dict(payload))
    assert key_a == key_b

    with_key = dict(payload)
    with_key["idempotency_key"] = key_a
    validate_contract_payload(with_key, expected_contract_version=BACKFILL_REQUEST_VERSION)

    with_key["idempotency_key"] = "f" * 64
    with pytest.raises(ContractValidationError, match="CONTRACT_IDEMPOTENCY_KEY_MISMATCH"):
        validate_contract_payload(with_key, expected_contract_version=BACKFILL_REQUEST_VERSION)


def test_task0_contract_matrix_has_consumer_alignment_for_every_producer_field():
    matrix = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    assert matrix["contract_bundle_version"] == "task0.remediation_contract_matrix.v1"
    assert len(matrix["contracts"]) == 3

    for contract in matrix["contracts"]:
        field_map = contract["producer_to_consumer_field_map"]
        assert field_map
        for consumer_path in field_map.values():
            assert consumer_path
