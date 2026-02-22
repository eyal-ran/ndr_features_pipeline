import json
from pathlib import Path


STEP_FUNCTIONS_DIR = Path(__file__).resolve().parents[1] / "docs" / "step_functions_jsonata"


def test_step_functions_load_project_parameters_from_dynamodb():
    for path in STEP_FUNCTIONS_DIR.glob("sfn_ndr_*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        states = data["States"]
        assert "LoadProjectParametersFromDynamo" in states, f"{path.name} missing DynamoDB parameter load state"
        load_state = states["LoadProjectParametersFromDynamo"]
        assert load_state["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:getItem"


def test_step_functions_no_hard_coded_project_defaults():
    for path in STEP_FUNCTIONS_DIR.glob("sfn_ndr_*.json"):
        text = path.read_text(encoding="utf-8")
        assert "ndr-project" not in text
        assert "'v1'" not in text


def test_priority_item23_required_runtime_validation_states_present():
    required = {
        "sfn_ndr_15m_features_inference.json": {
            "ValidateResolvedRuntimeParams",
            "FailMissingProjectName",
            "FailMissingFeatureSpecVersion",
            "FailMissingMiniBatchId",
            "FailMissingBatchStartTsIso",
            "FailMissingBatchEndTsIso",
            "FailMissingMiniBatchS3Prefix",
        },
        "sfn_ndr_monthly_fg_b_baselines.json": {
            "ValidateResolvedRuntimeParams",
            "FailMissingProjectName",
            "FailMissingFeatureSpecVersion",
            "FailMissingReferenceMonthIso",
            "FailMissingReferenceTimeIso",
        },
        "sfn_ndr_backfill_reprocessing.json": {
            "ValidateResolvedRuntimeParams",
            "FailMissingProjectName",
            "FailMissingFeatureSpecVersion",
            "FailMissingStartTs",
            "FailMissingEndTs",
            "FailInvalidStartTsFormat",
            "FailInvalidEndTsFormat",
            "FailInvalidBackfillWindow",
        },
        "sfn_ndr_training_orchestrator.json": {
            "ValidateResolvedRuntimeParams",
            "FailMissingProjectName",
            "FailMissingFeatureSpecVersion",
            "FailMissingRunId",
            "FailMissingTrainingStartTs",
            "FailMissingTrainingEndTs",
            "FailMissingEvalStartTs",
            "FailMissingEvalEndTs",
        },
    }

    for filename, expected_states in required.items():
        doc = json.loads((STEP_FUNCTIONS_DIR / filename).read_text(encoding="utf-8"))
        states = doc["States"]
        missing = expected_states - set(states)
        assert not missing, f"{filename} missing runtime validation states: {sorted(missing)}"


def test_priority_item23_backfill_has_no_static_date_literals():
    text = (STEP_FUNCTIONS_DIR / "sfn_ndr_backfill_reprocessing.json").read_text(encoding="utf-8")
    assert "2025-01-01T00:00:00Z" not in text
    assert "2025-01-02T00:00:00Z" not in text
