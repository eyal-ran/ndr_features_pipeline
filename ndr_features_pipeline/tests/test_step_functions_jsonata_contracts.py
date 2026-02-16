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
