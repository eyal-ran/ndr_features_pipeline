import pytest

from ndr.orchestration.contract_validators import (
    TASK12_CONTRACT_ERROR_CODE,
    TASK12_GATE_ERROR_CODE,
    evaluate_task12_initial_deployment_bootstrap,
)


def _base_evidence() -> dict:
    return {
        "startup_paths": {
            "rt_reviewed": True,
            "monthly_reviewed": True,
            "backfill_reviewed": True,
        },
        "control_record": {
            "control_key": "bootstrap#ndr-prod#v1",
            "status": "READY",
            "updated_at": "2026-04-09T00:00:00Z",
        },
        "bootstrap_manifest": {
            "checkpoints": {
                "seed_machine_inventory": {"passed": True},
                "reconstruct_historical_families": {"passed": True},
                "build_monthly_baseline": {"passed": True},
                "validate_readiness_manifest": {"passed": True},
                "activate_rt_steady_state": {"passed": True},
            },
            "readiness_criteria": [
                {
                    "metric": "missing_ranges_count",
                    "expected": 0,
                    "actual": 0,
                    "passed": True,
                },
                {
                    "metric": "fg_b_baseline_ready",
                    "expected": True,
                    "actual": True,
                    "passed": True,
                },
            ],
        },
        "rt_activation": {
            "contract_version": "bootstrap_rt_activation.v1",
            "bootstrap_ready": True,
            "bootstrap_control_key": "bootstrap#ndr-prod#v1",
        },
        "recovery": {
            "rerun_no_op_verified": True,
            "partial_recovery_verified": True,
            "retry_strategy_verified": True,
            "rollback_strategy_verified": True,
        },
    }


def test_task12_bootstrap_gate_passes_for_empty_system_bootstrap_success():
    report = evaluate_task12_initial_deployment_bootstrap(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["failed_checks"] == []
    assert report["contract_version"] == "task12_initial_deployment_bootstrap.v1"


def test_task12_bootstrap_gate_fails_fast_when_readiness_definition_is_not_measurable():
    evidence = _base_evidence()
    evidence["bootstrap_manifest"]["readiness_criteria"] = []

    with pytest.raises(ValueError, match=TASK12_CONTRACT_ERROR_CODE):
        evaluate_task12_initial_deployment_bootstrap(evidence=evidence)


def test_task12_bootstrap_gate_enforces_rerun_idempotency_and_partial_recovery():
    evidence = _base_evidence()
    evidence["recovery"]["rerun_no_op_verified"] = False

    with pytest.raises(ValueError, match=TASK12_GATE_ERROR_CODE):
        evaluate_task12_initial_deployment_bootstrap(evidence=evidence)

    evidence = _base_evidence()
    evidence["recovery"]["partial_recovery_verified"] = False
    with pytest.raises(ValueError, match=TASK12_GATE_ERROR_CODE):
        evaluate_task12_initial_deployment_bootstrap(evidence=evidence)


def test_task12_bootstrap_gate_requires_authoritative_rt_activation_contract():
    evidence = _base_evidence()
    evidence["rt_activation"]["contract_version"] = "legacy"

    with pytest.raises(ValueError, match=TASK12_GATE_ERROR_CODE):
        evaluate_task12_initial_deployment_bootstrap(evidence=evidence)
