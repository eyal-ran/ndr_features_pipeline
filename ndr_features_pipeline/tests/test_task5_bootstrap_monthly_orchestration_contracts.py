import json
from pathlib import Path


BOOTSTRAP_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json")
MONTHLY_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json")
DEPLOYMENT_PLAN_PATH = Path("src/ndr/deployment/canonical_end_to_end_deployment_plan.md")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_bootstrap_invokes_monthly_state_machine_with_minimal_contract_payload():
    bootstrap_states = _load_json(BOOTSTRAP_STEP_FUNCTION_PATH)["States"]
    monthly_start = bootstrap_states["StartMonthlyBaselineOrchestration"]

    assert monthly_start["Resource"] == "arn:aws:states:::states:startExecution.sync:2"
    assert monthly_start["Arguments"]["StateMachineArn"] == "${MonthlyStateMachineArn}"

    payload_expr = monthly_start["Arguments"]["Input"]
    assert "project_name" in payload_expr
    assert "feature_spec_version" in payload_expr
    assert "reference_month" in payload_expr
    assert "ml_project_name" not in payload_expr
    assert "raw_parsed_logs_s3_prefix" not in payload_expr

    bootstrap_text = BOOTSTRAP_STEP_FUNCTION_PATH.read_text(encoding="utf-8")
    assert "${PipelineNameMonthlyFgBBaselines}" not in bootstrap_text



def test_bootstrap_monthly_invocation_has_deterministic_duplicate_suppression_controls():
    bootstrap_states = _load_json(BOOTSTRAP_STEP_FUNCTION_PATH)["States"]
    monthly_start = bootstrap_states["StartMonthlyBaselineOrchestration"]

    name_expr = monthly_start["Arguments"]["Name"]
    assert "bootstrap-monthly-" in name_expr
    assert "$states.input.project_name" in name_expr
    assert "$states.input.feature_spec_version" in name_expr
    assert "$states.input.reference_month" in name_expr

    execution_exists = next(
        c
        for c in monthly_start["Catch"]
        if "StepFunctions.ExecutionAlreadyExists" in c["ErrorEquals"]
    )
    assert execution_exists["Next"] == "PersistCheckpointReadiness"



def test_monthly_contract_violation_is_rejected_before_monthly_state_machine_invocation():
    bootstrap_states = _load_json(BOOTSTRAP_STEP_FUNCTION_PATH)["States"]
    validate = bootstrap_states["ValidateMonthlyBootstrapInvocation"]

    assert validate["Type"] == "Choice"
    assert validate["Default"] == "StartMonthlyBaselineOrchestration"

    missing_reference_month = next(
        choice
        for choice in validate["Choices"]
        if "reference_month" in choice["Condition"]
    )
    assert missing_reference_month["Next"] == "FailMissingReferenceMonthForMonthlyBootstrap"

    fail_state = bootstrap_states["FailMissingReferenceMonthForMonthlyBootstrap"]
    assert fail_state["Type"] == "Fail"
    assert fail_state["Error"] == "BootstrapMonthlyContractViolation"
    assert "BOOTSTRAP_MONTHLY_INPUT_CONTRACT_VIOLATION" in fail_state["Cause"]



def test_monthly_state_machine_still_declares_required_entry_contract_fields():
    monthly_states = _load_json(MONTHLY_STEP_FUNCTION_PATH)["States"]
    validate = monthly_states["ValidateResolvedRuntimeParams"]

    conditions = [choice["Condition"] for choice in validate["Choices"]]
    assert '{% $project_name = "" %}' in conditions
    assert '{% $feature_spec_version = "" %}' in conditions
    assert '{% $reference_month = "" %}' in conditions



def test_deployment_plan_includes_monthly_state_machine_wiring_and_start_execution_permissions():
    deployment_text = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")

    assert "MonthlyStateMachineArn" in deployment_text
    assert "states:StartExecution" in deployment_text
    assert "states:DescribeExecution" in deployment_text
    assert "states:StopExecution" in deployment_text
    assert "disallow `sagemaker:StartPipelineExecution` for `${PipelineNameMonthlyFgBBaselines}`" in deployment_text
