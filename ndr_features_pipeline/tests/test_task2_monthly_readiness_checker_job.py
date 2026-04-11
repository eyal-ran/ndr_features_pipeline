import hashlib

from ndr.processing.monthly_readiness_checker_job import (
    build_monthly_readiness_artifact_key,
    build_monthly_readiness_idempotency_key,
    compute_and_validate_monthly_readiness_artifact,
)


def test_ready_path_produces_v3_contract_with_no_missing_ranges():
    payload = compute_and_validate_monthly_readiness_artifact(
        project_name="proj",
        feature_spec_version="v1",
        reference_month="2026/03",
        readiness_cycle=0,
        required_families=["delta", "fg_a"],
        missing_ranges=[],
    )
    assert payload["contract_version"] == "monthly_fg_b_readiness.v3"
    assert payload["ready"] is True
    assert payload["decision_code"] == "READY"
    assert payload["missing_ranges"] == []


def test_missing_path_and_idempotency_key_are_deterministic_per_cycle():
    payload = compute_and_validate_monthly_readiness_artifact(
        project_name="proj",
        feature_spec_version="v1",
        reference_month="2026/03",
        readiness_cycle=1,
        required_families=["delta", "fg_a"],
        missing_ranges=[
            {
                "family": "delta",
                "start_ts_iso": "2026-03-01T00:00:00Z",
                "end_ts_iso": "2026-03-02T00:00:00Z",
                "reason_code": "dependency_missing",
            }
        ],
    )
    expected = hashlib.sha256("proj|v1|2026/03|1".encode("utf-8")).hexdigest()
    assert payload["idempotency_key"] == expected
    assert payload["ready"] is False
    assert payload["decision_code"] == "MISSING_DEPENDENCIES"


def test_artifact_key_is_deterministic_and_cycle_scoped():
    key = build_monthly_readiness_artifact_key(
        project_name="proj",
        feature_spec_version="v1",
        reference_month="2026/03",
        readiness_cycle=2,
    )
    assert key == "orchestration/readiness/monthly_fg_b_readiness/v3/proj/v1/2026/03/cycle=2/manifest.json"


def test_idempotency_key_changes_by_cycle_only_when_inputs_match():
    cycle_0 = build_monthly_readiness_idempotency_key(
        project_name="proj",
        feature_spec_version="v1",
        reference_month="2026/03",
        readiness_cycle=0,
    )
    cycle_1 = build_monthly_readiness_idempotency_key(
        project_name="proj",
        feature_spec_version="v1",
        reference_month="2026/03",
        readiness_cycle=1,
    )
    assert cycle_0 != cycle_1
