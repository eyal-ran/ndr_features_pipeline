import json
from pathlib import Path


BOOTSTRAP_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_initial_deployment_bootstrap.json")
RT_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json")


def _load_bootstrap_states() -> dict:
    return json.loads(BOOTSTRAP_STEP_FUNCTION_PATH.read_text(encoding="utf-8"))["States"]


def _load_rt_states() -> dict:
    doc = json.loads(RT_STEP_FUNCTION_PATH.read_text(encoding="utf-8"))
    return doc["States"]["RunPerMlProjectBranch"]["ItemProcessor"]


def test_bootstrap_state_machine_runs_deterministic_day0_sequence_with_checkpoint_persistence():
    states = _load_bootstrap_states()

    assert "SeedMachineInventoryPipeline" in states
    assert states["SeedMachineInventoryPipeline"]["Next"] == "PersistCheckpointBackfill"
    assert states["InvokeBackfillReconstruction"]["Next"] == "PersistCheckpointBaseline"
    assert states["StartMonthlyBaselineOrchestration"]["Next"] == "PersistCheckpointReadiness"
    assert states["PersistBootstrapReady"]["Next"] == "BootstrapSucceeded"

    update_expr = states["PersistBootstrapReady"]["Arguments"]["UpdateExpression"]
    assert "bootstrap_status" in update_expr
    assert "bootstrap_checkpoint" in update_expr


def test_bootstrap_rerun_is_idempotent_when_control_record_is_already_ready():
    states = _load_bootstrap_states()
    choice = states["BootstrapAlreadyReadyChoice"]

    ready_branch = next(c for c in choice["Choices"] if "bootstrap_status.S = 'READY'" in c["Condition"])
    assert ready_branch["Next"] == "BuildReadyBootstrapOutput"


def test_rt_flow_gates_steady_state_execution_on_bootstrap_readiness_contract():
    item_processor = _load_rt_states()
    states = item_processor["States"]

    assert item_processor["StartAt"] == "LoadBootstrapControlRecord"
    assert states["LoadBootstrapControlRecord"]["Next"] == "BuildBootstrapReadinessManifest"

    gate = states["CheckBootstrapReadiness"]
    ready_branch = next(c for c in gate["Choices"] if c["Condition"] == "{% $bootstrap_readiness_manifest.bootstrap_ready = true %}")
    assert ready_branch["Next"] == "Start15mFeaturesPipeline"
    assert gate["Default"] == "UpdateBootstrapCheckpointRequested"

    invoke = states["StartBootstrapOrchestration"]
    assert invoke["Arguments"]["StateMachineArn"] == "${BootstrapStateMachineArn}"
    assert "bootstrap_rt_activation.v1" in states["BuildBootstrapReadinessManifest"]["Assign"]["bootstrap_readiness_manifest"]
