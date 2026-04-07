import json
from pathlib import Path

import pytest

from ndr.orchestration.contract_validators import (
    normalize_ml_project_names,
    validate_pipeline_parameter_alignment,
)


ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = ROOT / "docs" / "archive" / "debug_records" / "task0_contract_compatibility_matrix.json"
SF_DIR = ROOT / "docs" / "step_functions_jsonata"


def _matrix():
    return json.loads(MATRIX_PATH.read_text(encoding="utf-8"))


def _state_machine(name: str):
    return json.loads((SF_DIR / name).read_text(encoding="utf-8"))


def _collect_states(state_machine: dict) -> dict:
    collected = {}

    def _visit(states: dict):
        for name, state in states.items():
            collected[name] = state
            if isinstance(state, dict) and state.get("Type") == "Map" and "ItemProcessor" in state:
                _visit(state["ItemProcessor"]["States"])

    _visit(state_machine["States"])
    return collected


def test_task0_matrix_has_explicit_producer_consumer_mappings():
    matrix = _matrix()
    assert matrix["policy"]["business_parameter_sources"] == ["ddb", "flow_payload"]
    assert "environment_placeholder_defaults" in matrix["policy"]["forbidden_sources"]
    for flow in matrix["flows"]:
        mapping = flow["producer_consumer_field_map"]
        assert mapping
        assert set(mapping) == set(flow["runtime_config_fields"])
        for _, consumer in mapping.items():
            assert "->" in consumer


def test_rt_15m_ordering_normalizes_ml_project_array_before_mlp_read():
    doc = _state_machine("sfn_ndr_15m_features_inference.json")
    states = doc["States"]
    assert states["LoadDppConfigFromDynamo"]["Next"] == "NormalizeMlProjectNames"
    assert states["NormalizeMlProjectNames"]["Next"] == "ValidateMlProjectNamesBeforeMlpRead"
    assert states["ValidateMlProjectNamesBeforeMlpRead"]["Default"] == "LoadMlpConfigFromDynamo"
    assert states["LoadMlpConfigFromDynamo"]["Arguments"]["Key"]["ml_project_name"]["S"] == "{% $ml_project_names[0] %}"


def test_rt_15m_no_environment_placeholder_business_fallback():
    text = (SF_DIR / "sfn_ndr_15m_features_inference.json").read_text(encoding="utf-8")
    assert "${DefaultFeatureSpecVersion}" not in text


def test_task0_pipeline_parameter_alignment_matrix_entries():
    matrix = _matrix()
    for flow in matrix["flows"]:
        doc = _state_machine(flow["step_function"])
        states = _collect_states(doc)
        params = {
            item["Name"]
            for item in states[flow["step_function_state"]]["Arguments"]["PipelineParameters"]
        }
        validate_pipeline_parameter_alignment(
            declared_parameters=flow["declared_pipeline_parameters"],
            passed_parameters=params,
        )


def test_normalize_ml_project_names_array_first_and_negative_cases():
    assert normalize_ml_project_names(["ml-a", "ml-b", "ml-a"], None) == ["ml-a", "ml-b"]
    assert normalize_ml_project_names(None, "ml-a") == ["ml-a"]

    with pytest.raises(ValueError, match="non-empty"):
        normalize_ml_project_names([], None)
    with pytest.raises(ValueError, match="empty"):
        normalize_ml_project_names(["ml-a", ""], None)


def test_parameter_alignment_negative_cases():
    with pytest.raises(ValueError, match="undeclared"):
        validate_pipeline_parameter_alignment(
            declared_parameters=["A", "B"],
            passed_parameters=["A", "B", "C"],
        )
    with pytest.raises(ValueError, match="declared but not passed"):
        validate_pipeline_parameter_alignment(
            declared_parameters=["A", "B"],
            passed_parameters=["A"],
        )
