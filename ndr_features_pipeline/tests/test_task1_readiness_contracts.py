import pytest

from ndr.orchestration.readiness_contracts import (
    ReadinessContractError,
    compute_monthly_fg_b_readiness_v2,
    compute_rt_artifact_readiness_v2,
)


def test_monthly_ready_path_is_deterministic_and_canonical():
    payload = compute_monthly_fg_b_readiness_v2(required_families=["fg_a"], missing_ranges=[], as_of_ts="2026-04-10T00:00:00Z")
    assert payload == {
        "contract_version": "monthly_fg_b_readiness.v2",
        "ready": True,
        "required_families": ["fg_a"],
        "missing_ranges": [],
        "unresolved_count": 0,
        "decision_code": "READY",
        "as_of_ts": "2026-04-10T00:00:00Z",
    }


def test_monthly_missing_path_emits_missing_dependencies_decision():
    payload = compute_monthly_fg_b_readiness_v2(
        required_families=["fg_a", "delta"],
        missing_ranges=[
            {"family": "fg_a", "start_ts_iso": "2026-04-01T00:00:00Z", "end_ts_iso": "2026-04-01T00:15:00Z", "reason_code": "dependency_missing:fg_a"}
        ],
        as_of_ts="2026-04-10T00:00:00Z",
    )
    assert payload["ready"] is False
    assert payload["decision_code"] == "MISSING_DEPENDENCIES"
    assert payload["unresolved_count"] == 1


def test_rt_requires_idempotency_key_and_rejects_invalid_contract_payload():
    with pytest.raises(ReadinessContractError, match="RT_READINESS_MISSING_IDEMPOTENCY_KEY"):
        compute_rt_artifact_readiness_v2(required_families=["fg_c"], missing_ranges=[], idempotency_key="")


def test_stale_external_manifest_cannot_override_recomputed_missing_ranges():
    stale_external = {"ready": True, "missing_ranges": []}
    recomputed = compute_rt_artifact_readiness_v2(
        required_families=["fg_c"],
        missing_ranges=[{"family": "fg_c", "start_ts_iso": "2026-04-10T00:00:00Z", "end_ts_iso": "2026-04-10T00:15:00Z"}],
        idempotency_key="abc123",
        as_of_ts="2026-04-10T00:01:00Z",
    )
    assert stale_external["ready"] is True
    assert recomputed["ready"] is False
    assert recomputed["unresolved_count"] == 1
