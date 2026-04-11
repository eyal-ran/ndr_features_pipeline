import copy
import json
from pathlib import Path

import pytest

from ndr.orchestration.contract_schemas_v3 import validate_payload
from ndr.orchestration.contract_validators import (
    TASK6_GATE_ERROR_CODE,
    evaluate_task6_cross_flow_contract_conformance,
)
from ndr.orchestration.remediation_contracts import (
    BACKFILL_REQUEST_VERSION,
    build_backfill_request,
    build_default_provenance,
    validate_contract_payload,
)


MATRIX_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "archive"
    / "debug_records"
    / "task6_cross_flow_contract_matrix.json"
)


def _base_evidence() -> dict:
    return json.loads(MATRIX_PATH.read_text(encoding="utf-8"))


def test_task6_cross_flow_contract_matrix_is_green():
    report = evaluate_task6_cross_flow_contract_conformance(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["failed_checks"] == []
    assert report["contract_version"] == "task6_cross_flow_contract_conformance.v1"


def test_task6_fails_fast_when_a_consumer_field_is_unmapped():
    evidence = _base_evidence()
    rt = next(item for item in evidence["interfaces"] if item["interface_id"] == "rt_readiness_artifact")
    rt["consumer_fields"] = [field for field in rt["consumer_fields"] if field != "decision_code"]

    with pytest.raises(ValueError, match=TASK6_GATE_ERROR_CODE):
        evaluate_task6_cross_flow_contract_conformance(evidence=evidence)


def test_task6_fails_fast_when_unknown_fields_not_rejected():
    evidence = _base_evidence()
    evidence["strict_controls"]["reject_unknown_fields"] = False

    with pytest.raises(ValueError, match="TASK6_UNKNOWN_FIELDS_NOT_REJECTED"):
        evaluate_task6_cross_flow_contract_conformance(evidence=evidence)


def test_task6_monthly_readiness_schema_rejects_unknown_fields():
    payload = {
        "contract_version": "monthly_fg_b_readiness.v3",
        "project_name": "proj",
        "feature_spec_version": "v3",
        "reference_month": "2026-01",
        "ready": True,
        "required_families": ["fg_a"],
        "missing_ranges": [],
        "decision_code": "READY",
        "as_of_ts": "2026-01-01T00:00:00Z",
        "idempotency_key": "k" * 64,
        "extra": "not-allowed",
    }

    with pytest.raises(ValueError, match="CONTRACT_UNKNOWN_FIELD"):
        validate_payload(payload, contract_version="monthly_fg_b_readiness.v3")


def test_task6_rejects_backfill_contract_version_mismatch():
    request = build_backfill_request(
        project_name="proj",
        feature_spec_version="v3",
        requested_families=["delta"],
        missing_ranges=[{"family": "delta", "start_ts_iso": "2026-01-01T00:00:00Z", "end_ts_iso": "2026-01-01T00:15:00Z", "reason_code": "dependency_missing"}],
        provenance=build_default_provenance(producer_flow="rt", source_mode="ingestion", request_id="req-1"),
    )

    with pytest.raises(ValueError, match="CONTRACT_VERSION_MISMATCH"):
        validate_contract_payload(request, expected_contract_version=BACKFILL_REQUEST_VERSION.replace("v2", "v1"))


def test_task6_rt_raw_input_resolution_schema_rejects_missing_required_fields():
    payload = {
        "contract_version": "rt.raw_input_resolution.v1",
        "project_name": "proj",
        "feature_spec_version": "v3",
        "mini_batch_id": "b1",
        "batch_start_ts_iso": "2026-01-01T00:00:00Z",
        "batch_end_ts_iso": "2026-01-01T00:15:00Z",
        "raw_input_s3_prefix": "s3://bucket/raw/",
        "source_mode": "ingestion",
        "resolution_reason": "ingestion_available",
    }

    with pytest.raises(ValueError, match="CONTRACT_MISSING_REQUIRED_FIELD"):
        validate_payload(payload, contract_version="rt.raw_input_resolution.v1")


def test_task6_replay_idempotency_is_stable_for_backfill_requests():
    args = {
        "project_name": "proj",
        "feature_spec_version": "v3",
        "requested_families": ["delta"],
        "missing_ranges": [{"family": "delta", "start_ts_iso": "2026-01-01T00:00:00Z", "end_ts_iso": "2026-01-01T00:15:00Z", "reason_code": "dependency_missing"}],
        "provenance": build_default_provenance(producer_flow="rt", source_mode="ingestion", request_id="req-1"),
    }
    a = build_backfill_request(**copy.deepcopy(args))
    b = build_backfill_request(**copy.deepcopy(args))

    assert a["idempotency_key"] == b["idempotency_key"]
