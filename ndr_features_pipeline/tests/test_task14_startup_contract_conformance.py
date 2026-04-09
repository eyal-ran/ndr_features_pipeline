import copy
import json
from pathlib import Path

import pytest

from ndr.orchestration.contract_validators import (
    TASK14_BACKFILL_SFN_TO_EXECUTOR_CONSUMER_MISMATCH,
    TASK14_BACKFILL_SFN_TO_EXECUTOR_PRODUCER_MISMATCH,
    TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_CONSUMER_MISMATCH,
    TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_PRODUCER_MISMATCH,
    TASK14_GATE_ERROR_CODE,
    TASK14_TRAINING_TO_FGB_CONSUMER_MISMATCH,
    TASK14_TRAINING_TO_FGB_PRODUCER_MISMATCH,
    evaluate_task14_startup_contract_conformance,
)
from ndr.scripts.run_startup_contract_gate import main as run_startup_contract_gate_main


MATRIX_PATH = Path(__file__).resolve().parents[1] / "docs" / "archive" / "debug_records" / "task14_startup_contract_matrix.json"


def _base_evidence() -> dict:
    return json.loads(MATRIX_PATH.read_text(encoding="utf-8"))


def _interface(evidence: dict, interface_id: str) -> dict:
    return next(item for item in evidence["interfaces"] if item["interface_id"] == interface_id)


def test_task14_startup_contract_matrix_is_green_and_release_blocking():
    report = evaluate_task14_startup_contract_conformance(evidence=_base_evidence())

    assert report["status"] == "go"
    assert report["failed_checks"] == []
    assert report["contract_version"] == "task14_startup_contract_conformance.v1"


@pytest.mark.parametrize(
    ("interface_id", "expected_code"),
    [
        ("backfill_sfn_to_backfill_executor", TASK14_BACKFILL_SFN_TO_EXECUTOR_PRODUCER_MISMATCH),
        ("training_remediation_to_fgb_pipeline", TASK14_TRAINING_TO_FGB_PRODUCER_MISMATCH),
        ("extractor_runtime_to_manifest", TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_PRODUCER_MISMATCH),
    ],
)
def test_task14_producer_to_consumer_mismatch_has_deterministic_error_codes(interface_id: str, expected_code: str):
    evidence = _base_evidence()
    interface = _interface(evidence, interface_id)
    interface["consumer_fields"] = [field for field in interface["consumer_fields"] if field != interface["producer_fields"][-1]]

    with pytest.raises(ValueError, match=TASK14_GATE_ERROR_CODE) as exc:
        evaluate_task14_startup_contract_conformance(evidence=evidence)

    assert expected_code in str(exc.value)


@pytest.mark.parametrize(
    ("interface_id", "expected_code"),
    [
        ("backfill_sfn_to_backfill_executor", TASK14_BACKFILL_SFN_TO_EXECUTOR_CONSUMER_MISMATCH),
        ("training_remediation_to_fgb_pipeline", TASK14_TRAINING_TO_FGB_CONSUMER_MISMATCH),
        ("extractor_runtime_to_manifest", TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_CONSUMER_MISMATCH),
    ],
)
def test_task14_consumer_to_producer_mismatch_has_deterministic_error_codes(interface_id: str, expected_code: str):
    evidence = _base_evidence()
    interface = _interface(evidence, interface_id)
    interface["producer_fields"] = [field for field in interface["producer_fields"] if field != interface["consumer_fields"][-1]]

    with pytest.raises(ValueError, match=TASK14_GATE_ERROR_CODE) as exc:
        evaluate_task14_startup_contract_conformance(evidence=evidence)

    assert expected_code in str(exc.value)


def test_task14_release_is_blocked_when_startup_contract_status_is_red():
    evidence = copy.deepcopy(_base_evidence())
    evidence["release_gate"]["status"] = "red"

    with pytest.raises(ValueError, match="TASK14_RELEASE_GATE_RED"):
        evaluate_task14_startup_contract_conformance(evidence=evidence)


def test_task14_gate_fails_when_idempotency_retry_or_rollback_guards_are_missing():
    evidence = copy.deepcopy(_base_evidence())
    evidence["safeguards"]["idempotency_verified"] = False

    with pytest.raises(ValueError, match="TASK14_IDEMPOTENCY_GUARD_MISSING"):
        evaluate_task14_startup_contract_conformance(evidence=evidence)


def test_task14_cli_gate_emits_go_report_for_valid_startup_matrix(capsys):
    rc = run_startup_contract_gate_main(["--evidence-path", str(MATRIX_PATH)])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["status"] == "go"
