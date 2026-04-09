import copy
import json
from pathlib import Path

import pytest

from ndr.orchestration.contract_validators import (
    TASK15_CONTRACT_ERROR_CODE,
    TASK15_GATE_ERROR_CODE,
    evaluate_task15_initial_deployment_observability,
)
from ndr.scripts.run_startup_observability_gate import main as run_startup_observability_gate_main


BUNDLE_PATH = Path(__file__).resolve().parents[1] / "docs" / "archive" / "debug_records" / "task15_startup_observability_bundle.json"


def _base_evidence() -> dict:
    return json.loads(BUNDLE_PATH.read_text(encoding="utf-8"))


def test_task15_startup_observability_bundle_is_go_and_release_ready():
    report = evaluate_task15_initial_deployment_observability(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["failed_checks"] == []
    assert report["contract_version"] == "task15_initial_deployment_observability.v1"


def test_task15_fails_fast_when_incident_ownership_or_escalation_model_is_missing():
    evidence = _base_evidence()
    evidence["observability"]["incident_model"]["escalation_policy"] = ""

    with pytest.raises(ValueError, match="TASK15_OWNERSHIP_MODEL_UNDEFINED"):
        evaluate_task15_initial_deployment_observability(evidence=evidence)


def test_task15_synthetic_startup_failures_must_trigger_alarms_for_each_failure_class():
    evidence = _base_evidence()
    evidence["validation"]["synthetic_failures"][0]["alarm_triggered"] = False

    with pytest.raises(ValueError, match=TASK15_GATE_ERROR_CODE):
        evaluate_task15_initial_deployment_observability(evidence=evidence)


def test_task15_gate_fails_when_rollback_switch_is_not_ddb_first_or_safety_not_verified():
    evidence = _base_evidence()
    evidence["rollback"]["switch"]["config_source"] = "env"

    with pytest.raises(ValueError, match=TASK15_GATE_ERROR_CODE):
        evaluate_task15_initial_deployment_observability(evidence=evidence)

    evidence = _base_evidence()
    evidence["rollback"]["safety"]["retry_verified"] = False
    with pytest.raises(ValueError, match=TASK15_GATE_ERROR_CODE):
        evaluate_task15_initial_deployment_observability(evidence=evidence)


def test_task15_contract_fails_fast_when_sections_are_missing_or_malformed():
    evidence = _base_evidence()
    evidence.pop("observability")

    with pytest.raises(ValueError, match=TASK15_CONTRACT_ERROR_CODE):
        evaluate_task15_initial_deployment_observability(evidence=evidence)

    evidence = _base_evidence()
    evidence["observability"]["alarms"] = {"bad": "shape"}

    with pytest.raises(ValueError, match=TASK15_CONTRACT_ERROR_CODE):
        evaluate_task15_initial_deployment_observability(evidence=evidence)


def test_task15_cli_gate_emits_go_report_for_valid_bundle(capsys):
    rc = run_startup_observability_gate_main(["--evidence-path", str(BUNDLE_PATH)])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["status"] == "go"
