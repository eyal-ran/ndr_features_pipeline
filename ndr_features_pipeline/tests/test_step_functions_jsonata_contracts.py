import json
from pathlib import Path


STEP_FUNCTIONS_DIR = Path(__file__).resolve().parents[1] / "docs" / "step_functions_jsonata"


def _collect_states(state_machine: dict) -> dict:
    collected = {}

    def _visit(states: dict):
        for name, state in states.items():
            collected[name] = state
            if isinstance(state, dict) and state.get("Type") == "Map" and "ItemProcessor" in state:
                _visit(state["ItemProcessor"]["States"])

    _visit(state_machine["States"])
    return collected


def test_step_functions_load_required_control_plane_configs_from_dynamodb():
    dpp_required = {
        "sfn_ndr_15m_features_inference.json",
        "sfn_ndr_backfill_reprocessing.json",
        "sfn_ndr_monthly_fg_b_baselines.json",
        "sfn_ndr_prediction_publication.json",
        "sfn_ndr_training_orchestrator.json",
    }
    mlp_required = {
        "sfn_ndr_15m_features_inference.json",
        "sfn_ndr_prediction_publication.json",
        "sfn_ndr_training_orchestrator.json",
    }

    for path in STEP_FUNCTIONS_DIR.glob("sfn_ndr_*.json"):
        if path.name not in dpp_required:
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        states = _collect_states(data)
        assert "LoadDppConfigFromDynamo" in states, f"{path.name} missing DPP load state"
        assert states["LoadDppConfigFromDynamo"]["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:getItem"
        if path.name in mlp_required:
            assert "LoadMlpConfigFromDynamo" in states, f"{path.name} missing MLP load state"
            assert states["LoadMlpConfigFromDynamo"]["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:getItem"


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
            "FailMissingRawParsedLogsS3Prefix",
        },
        "sfn_ndr_monthly_fg_b_baselines.json": {
            "ValidateResolvedRuntimeParams",
            "FailMissingProjectName",
            "FailMissingFeatureSpecVersion",
            "FailMissingReferenceMonth",
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


def test_training_orchestrator_passes_control_plane_table_params_to_if_pipeline():
    data = json.loads((STEP_FUNCTIONS_DIR / "sfn_ndr_training_orchestrator.json").read_text(encoding="utf-8"))
    params = data["States"]["RunTrainingPerMlProject"]["ItemProcessor"]["States"]["StartTrainingPipeline"]["Arguments"]["PipelineParameters"]
    names = {item["Name"] for item in params}
    assert {"DppConfigTableName", "MlpConfigTableName", "BatchIndexTableName"}.issubset(names)


def test_step_functions_do_not_source_table_names_from_eventbridge_payload():
    for path in STEP_FUNCTIONS_DIR.glob("sfn_ndr_*.json"):
        text = path.read_text(encoding="utf-8")
        assert 'states.input.DppConfigTableName' not in text
        assert 'states.input.MlpConfigTableName' not in text
        assert 'states.input.BatchIndexTableName' not in text
        assert 'states.input.table_name' not in text
        assert 'states.input.TableName' not in text
