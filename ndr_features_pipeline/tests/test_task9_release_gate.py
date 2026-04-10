import pytest

from ndr.orchestration.contract_validators import (
    TASK9_CONTRACT_ERROR_CODE,
    TASK9_GATE_ERROR_CODE,
    evaluate_task9_release_hardening_gate,
)


def _scenario(scenario_id: str, *, critical: bool = True, status: str = "passed") -> dict:
    return {
        "scenario_id": scenario_id,
        "critical": critical,
        "status": status,
        "deterministic": True,
        "failure_handling_validated": True,
        "standalone_passed": True,
        "integration_passed": True,
        "flows_covered": ["monthly", "rt", "backfill", "training", "control_plane"],
    }


def _base_evidence() -> dict:
    return {
        "scenario_matrix": [
            _scenario("normal"),
            _scenario("missing_dependency"),
            _scenario("fallback"),
            _scenario("duplicate_replay"),
            _scenario("partial_failure_retry"),
        ],
        "release_gate": {
            "critical_pass_threshold": 1.0,
            "block_release_on_critical_failure": True,
        },
        "producer_consumer": {
            "contract_edges": [
                {
                    "edge_id": "monthly_fg_b_to_fg_c",
                    "producer_contract_valid": True,
                    "consumer_contract_valid": True,
                    "integration_validated": True,
                },
                {
                    "edge_id": "rt_pair_counts_to_fg_c",
                    "producer_contract_valid": True,
                    "consumer_contract_valid": True,
                    "integration_validated": True,
                },
            ]
        },
        "rollback": {
            "drill_executed": True,
            "restore_successful": True,
            "post_rollback_validation_passed": True,
        },
    }


def test_task9_release_gate_passes_when_matrix_and_rollback_are_green():
    report = evaluate_task9_release_hardening_gate(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["critical_pass_rate"] == 1.0
    assert report["failed_checks"] == []



def test_task9_release_gate_fails_when_required_scenario_missing():
    evidence = _base_evidence()
    evidence["scenario_matrix"] = [s for s in evidence["scenario_matrix"] if s["scenario_id"] != "fallback"]

    with pytest.raises(ValueError, match=TASK9_GATE_ERROR_CODE):
        evaluate_task9_release_hardening_gate(evidence=evidence)



def test_task9_release_gate_fails_when_critical_threshold_not_met():
    evidence = _base_evidence()
    evidence["scenario_matrix"][0]["status"] = "failed"

    with pytest.raises(ValueError, match=TASK9_GATE_ERROR_CODE):
        evaluate_task9_release_hardening_gate(evidence=evidence)



def test_task9_release_gate_fails_when_producer_consumer_edge_not_validated():
    evidence = _base_evidence()
    evidence["producer_consumer"]["contract_edges"][1]["consumer_contract_valid"] = False

    with pytest.raises(ValueError, match=TASK9_GATE_ERROR_CODE):
        evaluate_task9_release_hardening_gate(evidence=evidence)



def test_task9_release_gate_fails_when_rollback_drill_not_ready():
    evidence = _base_evidence()
    evidence["rollback"]["post_rollback_validation_passed"] = False

    with pytest.raises(ValueError, match=TASK9_GATE_ERROR_CODE):
        evaluate_task9_release_hardening_gate(evidence=evidence)



def test_task9_release_gate_fails_fast_on_contract_shape_violation():
    with pytest.raises(ValueError, match=TASK9_CONTRACT_ERROR_CODE):
        evaluate_task9_release_hardening_gate(
            evidence={
                "release_gate": {},
                "producer_consumer": {},
                "rollback": {},
            }
        )
