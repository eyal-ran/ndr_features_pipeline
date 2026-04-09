import pytest

from ndr.orchestration.backfill_execution_contract import build_execution_request
from ndr.orchestration.contract_validators import (
    TASK11_CONTRACT_ERROR_CODE,
    TASK11_GATE_ERROR_CODE,
    evaluate_task11_system_readiness_gate,
)


_REQUIRED_METRICS = [
    "fallback_frequency",
    "backfill_latency_seconds",
    "unresolved_missing_ranges_count",
]

_REQUIRED_ALARMS = [
    "fallback_frequency_alarm",
    "backfill_latency_alarm",
    "unresolved_missing_ranges_alarm",
]



def _base_evidence() -> dict:
    return {
        "flows": {
            "rt_backfill_green": True,
            "monthly_backfill_green": True,
            "training_backfill_green": True,
            "backfill_interactions_green": True,
        },
        "replay": {
            "idempotency_verified": True,
            "retry_verified": True,
        },
        "rollback": {
            "status": "passed",
            "dry_run_execution_id": "rb-001",
        },
        "producer_consumer": {
            "interfaces_verified": True,
            "drift_detected": False,
        },
        "observability": {
            "metrics": list(_REQUIRED_METRICS),
            "alarms": list(_REQUIRED_ALARMS),
            "signal_checks": {
                "fallback_frequency": True,
                "backfill_latency_seconds": True,
                "unresolved_missing_ranges_count": True,
            },
        },
        "finding_closure": {
            "F1.1": True,
            "F1.2": True,
            "F2.1": True,
            "F2.2": True,
            "F2.3": True,
            "F3.1": True,
            "F3.2": True,
            "F3.3": True,
            "F4.1": True,
            "F4.2": True,
            "F4.3": True,
            "F4.4": True,
            "F4.5": True,
            "F5.1": True,
        },
    }



def test_task11_go_no_go_gate_passes_when_all_cross_flow_checks_are_green():
    report = evaluate_task11_system_readiness_gate(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["failed_checks"] == []
    assert report["contract_version"] == "task11_system_readiness_gate.v1"



def test_task11_replay_idempotency_is_deterministic_for_backfill_contract_inputs():
    request_a = build_execution_request(
        project_name="ndr-prod",
        feature_spec_version="v1",
        artifact_family="fg_c",
        range_start_ts_iso="2025-01-01T00:00:00Z",
        range_end_ts_iso="2025-01-01T00:15:00Z",
    )
    request_b = build_execution_request(
        project_name="ndr-prod",
        feature_spec_version="v1",
        artifact_family="fg_c",
        range_start_ts_iso="2025-01-01T00:00:00Z",
        range_end_ts_iso="2025-01-01T00:15:00Z",
    )

    assert request_a.idempotency_key == request_b.idempotency_key



def test_task11_synthetic_failure_paths_fail_fast_for_ingestion_and_baseline_gaps():
    evidence = _base_evidence()
    evidence["flows"]["rt_backfill_green"] = False
    evidence["flows"]["monthly_backfill_green"] = False

    with pytest.raises(ValueError, match=TASK11_GATE_ERROR_CODE):
        evaluate_task11_system_readiness_gate(evidence=evidence)



def test_task11_monitoring_signal_contract_fails_when_required_metric_or_alarm_missing():
    evidence = _base_evidence()
    evidence["observability"]["metrics"] = ["fallback_frequency", "backfill_latency_seconds"]

    with pytest.raises(ValueError, match=TASK11_GATE_ERROR_CODE):
        evaluate_task11_system_readiness_gate(evidence=evidence)



def test_task11_contract_fails_fast_when_required_sections_are_missing():
    evidence = _base_evidence()
    evidence.pop("rollback")

    with pytest.raises(ValueError, match=TASK11_CONTRACT_ERROR_CODE):
        evaluate_task11_system_readiness_gate(evidence=evidence)



def test_task11_no_go_when_contract_drift_remains_between_producer_consumer_interfaces():
    evidence = _base_evidence()
    evidence["producer_consumer"]["drift_detected"] = True

    with pytest.raises(ValueError, match=TASK11_GATE_ERROR_CODE):
        evaluate_task11_system_readiness_gate(evidence=evidence)



def test_task11_requires_full_finding_closure_matrix_before_go_decision():
    evidence = _base_evidence()
    evidence["finding_closure"]["F5.1"] = False

    with pytest.raises(ValueError, match=TASK11_GATE_ERROR_CODE):
        evaluate_task11_system_readiness_gate(evidence=evidence)
