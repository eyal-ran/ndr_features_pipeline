import json
from pathlib import Path


STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json")


def _load_states() -> dict:
    return json.loads(STEP_FUNCTION_PATH.read_text(encoding="utf-8"))["States"]


def test_no_missing_path_skips_remediation_and_starts_fgb():
    states = _load_states()

    inventory_success = next(
        choice
        for choice in states["InventoryPipelineStatusChoice"]["Choices"]
        if choice["Condition"] == "{% $inventory_pipeline_status = 'Succeeded' %}"
    )
    assert inventory_success["Next"] == "BuildBaselineDependencyCheckManifest"

    check = states["CheckBaselineDependencies"]
    no_missing = next(choice for choice in check["Choices"] if choice["Condition"] == "{% $count($baseline_dependency_manifest.missing_ranges) = 0 %}")
    assert no_missing["Next"] == "StartFGBBaselinePipeline"


def test_missing_path_invokes_remediation_then_rechecks_gate():
    states = _load_states()

    check = states["CheckBaselineDependencies"]
    first_cycle_missing = next(choice for choice in check["Choices"] if choice["Condition"] == "{% $baseline_gate_cycle = 0 %}")
    assert first_cycle_missing["Next"] == "BuildBaselineRemediationRequest"

    assert states["BuildBaselineRemediationRequest"]["Next"] == "InvokeBaselineDependencyRemediation"

    remediation = states["InvokeBaselineDependencyRemediation"]
    assert remediation["Resource"] == "arn:aws:states:::states:startExecution.sync:2"
    assert remediation["Arguments"]["StateMachineArn"] == "${BackfillStateMachineArn}"
    assert remediation["Next"] == "RecheckBaselineDependencies"

    assert states["RecheckBaselineDependencies"]["Next"] == "CheckBaselineDependencies"


def test_unresolved_path_fails_with_explicit_reason_code():
    states = _load_states()

    check = states["CheckBaselineDependencies"]
    assert check["Default"] == "FailBaselineDependenciesUnresolved"

    fail_state = states["FailBaselineDependenciesUnresolved"]
    assert fail_state["Type"] == "Fail"
    assert fail_state["Error"] == "BaselineDependencyGateFailed"
    assert "dependency_missing:fg_a_history" in fail_state["Cause"]


def test_remediation_contract_and_readiness_manifest_fields_align_with_consumer_contract():
    states = _load_states()

    readiness_expr = states["BuildBaselineDependencyCheckManifest"]["Assign"]["baseline_dependency_manifest"]
    assert "'required_families': ['fg_a']" in readiness_expr
    assert "missing_ranges" in readiness_expr
    assert "start_ts_iso" in states["BuildBaselineRemediationRequest"]["Assign"]["baseline_remediation_window"]
    assert "end_ts_iso" in states["BuildBaselineRemediationRequest"]["Assign"]["baseline_remediation_window"]

    request_expr = states["BuildBaselineRemediationRequest"]["Assign"]["baseline_remediation_request"]
    assert "NdrBaselineRemediationRequest.v1" in request_expr
    assert "required_families':['delta','fg_a']" in request_expr

    remediation_input = states["InvokeBaselineDependencyRemediation"]["Arguments"]["Input"]
    assert remediation_input["start_ts"] == "{% $baseline_remediation_window.start_ts %}"
    assert remediation_input["end_ts"] == "{% $baseline_remediation_window.end_ts %}"
    assert remediation_input["required_families"] == "{% $baseline_remediation_request.required_families %}"
    assert remediation_input["missing_ranges"] == "{% $baseline_remediation_request.missing_ranges %}"
