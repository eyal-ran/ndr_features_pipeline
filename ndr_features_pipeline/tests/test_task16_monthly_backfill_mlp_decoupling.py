import json
from pathlib import Path


STEP_FUNCTIONS_DIR = Path(__file__).resolve().parents[1] / "docs" / "step_functions_jsonata"


def _load(name: str) -> dict:
    return json.loads((STEP_FUNCTIONS_DIR / name).read_text(encoding="utf-8"))


def test_monthly_flow_is_dpp_only_and_does_not_require_mlp_lookup():
    doc = _load("sfn_ndr_monthly_fg_b_baselines.json")
    states = doc["States"]

    assert states["LoadDppConfigFromDynamo"]["Next"] == "ResolvePipelineRuntimeParams"
    assert "LoadMlpConfigFromDynamo" not in states

    validation_branches = {choice["Next"] for choice in states["ValidateResolvedRuntimeParams"]["Choices"]}
    assert "FailMissingMlProjectName" not in validation_branches


def test_backfill_flow_is_dpp_only_and_does_not_require_mlp_lookup():
    doc = _load("sfn_ndr_backfill_reprocessing.json")
    states = doc["States"]

    assert states["LoadDppConfigFromDynamo"]["Next"] == "ResolvePipelineRuntimeParams"
    assert "LoadMlpConfigFromDynamo" not in states

    validation_branches = {choice["Next"] for choice in states["ValidateResolvedRuntimeParams"]["Choices"]}
    assert "FailMissingMlProjectName" not in validation_branches


def test_rt_flow_still_fails_fast_when_mlp_contract_is_missing():
    doc = _load("sfn_ndr_15m_features_inference.json")
    states = doc["States"]

    assert "ValidateMlProjectNamesBeforeMlpRead" in states
    assert states["ValidateMlProjectNamesBeforeMlpRead"]["Default"] == "LoadMlpConfigFromDynamo"
    assert states["ValidateMlProjectNamesBeforeMlpRead"]["Choices"][0]["Next"] == "FailMissingMlProjectNames"
    assert states["FailMissingMlProjectNames"]["Error"] == "RuntimeParameterValidationError"


def test_training_flow_still_requires_explicit_mlp_contract_selection():
    doc = _load("sfn_ndr_training_orchestrator.json")
    states = doc["States"]

    choice = states["ValidateResolvedRuntimeParams"]
    targets = {entry["Next"] for entry in choice["Choices"]}

    assert "FailInvalidMlProjectSelection" in targets
    assert states["FailInvalidMlProjectSelection"]["Error"] == "RuntimeParameterValidationError"
