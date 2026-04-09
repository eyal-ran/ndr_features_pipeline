import json
from pathlib import Path


STEP_FUNCTIONS_DIR = Path(__file__).resolve().parents[1] / "docs" / "step_functions_jsonata"


def _load_training_doc() -> dict:
    return json.loads((STEP_FUNCTIONS_DIR / "sfn_ndr_training_orchestrator.json").read_text(encoding="utf-8"))


def test_training_mlp_lookup_executes_inside_branch_map_only():
    doc = _load_training_doc()
    top_states = doc["States"]

    assert top_states["LoadDppConfigFromDynamo"]["Next"] == "ResolvePipelineRuntimeParams"

    branch_states = top_states["RunTrainingPerMlProject"]["ItemProcessor"]["States"]
    assert top_states["RunTrainingPerMlProject"]["ItemProcessor"]["StartAt"] == "LoadMlpConfigFromDynamo"
    assert branch_states["LoadMlpConfigFromDynamo"]["Arguments"]["Key"]["ml_project_name"]["S"] == "{% $states.input.ml_project_name %}"


def test_training_branch_reciprocal_mapping_validation_is_fail_fast():
    doc = _load_training_doc()
    branch_states = doc["States"]["RunTrainingPerMlProject"]["ItemProcessor"]["States"]

    assert "ValidateMlpBranchReciprocalMapping" in branch_states
    choice = branch_states["ValidateMlpBranchReciprocalMapping"]
    assert choice["Default"] == "StartTrainingPipeline"

    next_states = {entry["Next"] for entry in choice["Choices"]}
    assert {"FailMissingMlpBranchConfig", "FailMlpBranchReciprocalMismatch"}.issubset(next_states)

    assert branch_states["FailMissingMlpBranchConfig"]["Error"] == "MlpBranchConfigValidationError"
    assert branch_states["FailMlpBranchReciprocalMismatch"]["Error"] == "MlpBranchConfigValidationError"


def test_training_branch_context_is_propagated_unchanged_to_lookup_and_pipeline():
    doc = _load_training_doc()
    map_state = doc["States"]["RunTrainingPerMlProject"]

    assert map_state["ItemSelector"]["ml_project_name"] == "{% $states.context.Map.Item.Value %}"

    branch_states = map_state["ItemProcessor"]["States"]
    lookup_key = branch_states["LoadMlpConfigFromDynamo"]["Arguments"]["Key"]["ml_project_name"]["S"]
    assert lookup_key == "{% $states.input.ml_project_name %}"

    pipeline_params = branch_states["StartTrainingPipeline"]["Arguments"]["PipelineParameters"]
    mlp_param = next(item for item in pipeline_params if item["Name"] == "MlProjectName")
    assert mlp_param["Value"] == "{% $states.input.ml_project_name %}"


def test_training_list_only_ml_project_names_path_is_normalized_and_validated_before_map():
    doc = _load_training_doc()
    states = doc["States"]

    assert states["NormalizeMlProjectBranches"]["Assign"]["ml_project_branches"].startswith("{% $distinct(")
    assert states["NormalizeMlProjectBranches"]["Next"] == "ValidateNormalizedMlProjectBranches"
    assert states["ValidateNormalizedMlProjectBranches"]["Default"] == "RunTrainingPerMlProject"
    assert states["RunTrainingPerMlProject"]["Items"] == "{% $ml_project_branches %}"
