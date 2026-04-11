from ndr.processing.rt_readiness_checker_job import (
    build_rt_readiness_artifact_key,
    build_rt_readiness_idempotency_key,
    compute_and_validate_rt_readiness_artifact,
)


def test_compute_rt_readiness_artifact_ready_path() -> None:
    payload = compute_and_validate_rt_readiness_artifact(
        project_name="proj",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        mini_batch_id="batch-001",
        readiness_cycle=0,
        required_families=["delta", "fg_a"],
        missing_ranges=[],
    )

    assert payload["contract_version"] == "rt_artifact_readiness.v3"
    assert payload["ready"] is True
    assert payload["decision_code"] == "READY"
    assert payload["required_families"] == ["delta", "fg_a"]
    assert payload["missing_ranges"] == []


def test_compute_rt_readiness_artifact_missing_path() -> None:
    payload = compute_and_validate_rt_readiness_artifact(
        project_name="proj",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        mini_batch_id="batch-001",
        readiness_cycle=1,
        required_families=["delta", "fg_a"],
        missing_ranges=[
            {
                "family": "delta",
                "start_ts_iso": "2026-03-01T00:00:00Z",
                "end_ts_iso": "2026-03-01T00:15:00Z",
                "reason_code": "artifact_missing",
            }
        ],
    )

    assert payload["ready"] is False
    assert payload["decision_code"] == "MISSING_DEPENDENCIES"
    assert payload["missing_ranges"][0]["family"] == "delta"


def test_rt_readiness_artifact_key_uses_cycle_scope() -> None:
    key = build_rt_readiness_artifact_key(
        project_name="proj",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        mini_batch_id="batch-001",
        readiness_cycle=2,
    )
    assert key == "orchestration/readiness/rt_artifact_readiness/v3/proj/v1/ml-a/batch-001/cycle=2/manifest.json"


def test_rt_readiness_idempotency_key_is_deterministic_per_cycle() -> None:
    cycle_0 = build_rt_readiness_idempotency_key(
        project_name="proj",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        mini_batch_id="batch-001",
        readiness_cycle=0,
    )
    cycle_1 = build_rt_readiness_idempotency_key(
        project_name="proj",
        feature_spec_version="v1",
        ml_project_name="ml-a",
        mini_batch_id="batch-001",
        readiness_cycle=1,
    )

    assert cycle_0 != cycle_1
    assert len(cycle_0) == 64
    assert len(cycle_1) == 64
