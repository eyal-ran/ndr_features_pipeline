import json
from pathlib import Path

import pytest

from ndr.orchestration.contract_validators import (
    TASK7_CONTRACT_ERROR_CODE,
    TASK7_GATE_ERROR_CODE,
    evaluate_task7_v3_final_release_gate,
)
from ndr.scripts.run_v3_final_release_gate import main as run_v3_final_release_gate_main


EVIDENCE_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "archive"
    / "debug_records"
    / "task7_v3_final_release_gate_evidence.json"
)


def _base_evidence() -> dict:
    return json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))


def test_task7_v3_final_release_gate_is_release_blocking_and_green():
    report = evaluate_task7_v3_final_release_gate(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["failed_checks"] == []
    assert report["contract_version"] == "task7_v3_final_release_gate.v1"


def test_task7_fails_when_critical_check_is_warning_only():
    evidence = _base_evidence()
    evidence["correctness_critical_checks"][0]["warning_only"] = True

    with pytest.raises(ValueError, match=TASK7_GATE_ERROR_CODE):
        evaluate_task7_v3_final_release_gate(evidence=evidence)


def test_task7_fails_when_startup_or_deployment_prerequisite_gate_is_not_go():
    evidence = _base_evidence()
    evidence["startup_gates"]["deployment_precondition_gate"]["status"] = "red"

    with pytest.raises(ValueError, match=TASK7_GATE_ERROR_CODE):
        evaluate_task7_v3_final_release_gate(evidence=evidence)


def test_task7_fails_when_v3_contract_drift_check_does_not_pass():
    evidence = _base_evidence()
    evidence["contract_drift"]["check_contract_drift_v3_passed"] = False

    with pytest.raises(ValueError, match=TASK7_GATE_ERROR_CODE):
        evaluate_task7_v3_final_release_gate(evidence=evidence)


def test_task7_fails_fast_on_contract_shape_violation():
    evidence = _base_evidence()
    evidence.pop("targeted_pytest_matrix")

    with pytest.raises(ValueError, match=TASK7_CONTRACT_ERROR_CODE):
        evaluate_task7_v3_final_release_gate(evidence=evidence)


def test_task7_cli_gate_emits_go_report_for_valid_evidence(capsys):
    rc = run_v3_final_release_gate_main(["--evidence-path", str(EVIDENCE_PATH)])

    assert rc == 0
    out = capsys.readouterr().out
    assert '"status": "go"' in out
