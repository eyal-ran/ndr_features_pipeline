import json
from pathlib import Path

import pytest

from ndr.orchestration.step_functions_validation import (
    StepFunctionDefinitionError,
    ensure_required_substitutions,
    render_and_validate_template,
    validate_state_machine_definition,
)


def test_validate_real_templates_pass_structural_validation():
    for path in sorted(Path("docs/step_functions_jsonata").glob("*.json")):
        text = path.read_text(encoding="utf-8")
        validate_state_machine_definition(text)


def test_validate_state_machine_definition_rejects_broken_next_target():
    bad = {
        "StartAt": "A",
        "States": {
            "A": {"Type": "Pass", "Next": "B"},
            "C": {"Type": "Succeed"},
        },
    }
    with pytest.raises(StepFunctionDefinitionError, match="Next target 'B' not found"):
        validate_state_machine_definition(json.dumps(bad))


def test_required_substitutions_reject_missing_or_empty_values():
    with pytest.raises(StepFunctionDefinitionError, match="Missing required substitutions"):
        ensure_required_substitutions({"A": "x"}, ["A", "B"])

    with pytest.raises(StepFunctionDefinitionError, match="Empty required substitutions"):
        ensure_required_substitutions({"A": "x", "B": ""}, ["A", "B"])


def test_render_and_validate_template_rejects_unresolved_placeholder():
    template = '{"StartAt":"A","States":{"A":{"Type":"Succeed"}},"Comment":"${Missing}"}'
    with pytest.raises(StepFunctionDefinitionError, match="Missing substitution"):
        render_and_validate_template(template, substitutions={})
