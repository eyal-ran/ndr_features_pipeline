import pytest

from ndr.orchestration.contract_schemas_v3 import load_schema, validate_payload
from ndr.orchestration.remediation_contracts import ContractValidationError


def test_task0_v3_schema_positive_paths():
    monthly = {
        "contract_version": "monthly_fg_b_readiness.v3",
        "project_name": "proj",
        "feature_spec_version": "v1",
        "reference_month": "2026/04",
        "ready": False,
        "required_families": ["fg_a"],
        "missing_ranges": [{"family": "fg_a", "start_ts_iso": "2026-04-01T00:00:00Z", "end_ts_iso": "2026-04-01T00:15:00Z", "reason_code": "missing"}],
        "decision_code": "MISSING_DEPENDENCIES",
        "as_of_ts": "2026-04-11T00:00:00Z",
        "idempotency_key": "a" * 64,
    }
    validate_payload(monthly, contract_version="monthly_fg_b_readiness.v3")

    rt = {
        "contract_version": "rt_artifact_readiness.v3",
        "project_name": "proj",
        "feature_spec_version": "v1",
        "ml_project_name": "ml",
        "mini_batch_id": "mb-1",
        "ready": True,
        "required_families": ["fg_c"],
        "missing_ranges": [],
        "decision_code": "READY",
        "as_of_ts": "2026-04-11T00:00:00Z",
        "idempotency_key": "b" * 64,
    }
    validate_payload(rt, contract_version="rt_artifact_readiness.v3")


def test_task0_v3_schema_unknown_and_missing_and_version_mismatch_fail_fast():
    valid = {
        "contract_version": "NdrBackfillRequest.v2",
        "consumer": "realtime",
        "project_name": "proj",
        "feature_spec_version": "v1",
        "requested_families": ["delta"],
        "missing_ranges": [{"family": "delta", "start_ts_iso": "2026-04-01T00:00:00Z", "end_ts_iso": "2026-04-01T00:15:00Z", "reason_code": "missing"}],
        "provenance": {"producer_flow": "rt", "source_mode": "batch_index", "request_id": "r1"},
        "idempotency_key": "f" * 64,
    }
    validate_payload(valid, contract_version="NdrBackfillRequest.v2")

    missing = dict(valid)
    missing.pop("requested_families")
    with pytest.raises(ContractValidationError, match="CONTRACT_MISSING_REQUIRED_FIELD"):
        validate_payload(missing, contract_version="NdrBackfillRequest.v2")

    unknown = dict(valid)
    unknown["x"] = 1
    with pytest.raises(ContractValidationError, match="CONTRACT_UNKNOWN_FIELD"):
        validate_payload(unknown, contract_version="NdrBackfillRequest.v2")

    with pytest.raises(ContractValidationError, match="CONTRACT_VERSION_UNSUPPORTED"):
        load_schema("NdrBackfillRequest.v999")
