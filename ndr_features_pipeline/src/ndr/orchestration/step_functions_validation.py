from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


@dataclass(frozen=True)
class ValidationIssue:
    path: str
    message: str


class StepFunctionDefinitionError(ValueError):
    pass


def ensure_required_substitutions(substitutions: dict[str, Any], required_keys: list[str]) -> None:
    missing = [key for key in required_keys if key not in substitutions]
    if missing:
        raise StepFunctionDefinitionError(f"Missing required substitutions: {missing}")
    empty = [key for key in required_keys if str(substitutions.get(key, "")).strip() == ""]
    if empty:
        raise StepFunctionDefinitionError(f"Empty required substitutions: {empty}")


def render_and_validate_template(template_text: str, substitutions: dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in substitutions:
            raise StepFunctionDefinitionError(f"Missing substitution for placeholder: {key}")
        return str(substitutions[key])

    rendered = PLACEHOLDER_RE.sub(repl, template_text)
    unresolved = sorted(set(PLACEHOLDER_RE.findall(rendered)))
    if unresolved:
        raise StepFunctionDefinitionError(f"Unresolved placeholders remain: {unresolved}")

    validate_state_machine_definition(rendered)
    return rendered


def validate_state_machine_definition(definition_text: str) -> dict[str, Any]:
    try:
        document = json.loads(definition_text)
    except json.JSONDecodeError as exc:
        raise StepFunctionDefinitionError(f"Invalid JSON: {exc}") from exc

    issues: list[ValidationIssue] = []
    _walk_scope(document, path="/", issues=issues)
    if issues:
        joined = "; ".join(f"{i.path}: {i.message}" for i in issues)
        raise StepFunctionDefinitionError(f"Invalid Step Functions definition: {joined}")
    return document


def _walk_scope(node: dict[str, Any], path: str, issues: list[ValidationIssue]) -> None:
    states = node.get("States")
    if not isinstance(states, dict):
        issues.append(ValidationIssue(path, "missing or invalid States object"))
        return

    start_at = node.get("StartAt")
    if not isinstance(start_at, str) or start_at not in states:
        issues.append(ValidationIssue(path, f"StartAt '{start_at}' is not a valid state in this scope"))

    for state_name, state_def in states.items():
        state_path = f"{path}States/{state_name}"
        if not isinstance(state_def, dict):
            issues.append(ValidationIssue(state_path, "state definition must be an object"))
            continue

        state_type = state_def.get("Type")
        if not isinstance(state_type, str):
            issues.append(ValidationIssue(state_path, "state Type is required"))
            continue

        _validate_transitions(state_path, state_type, state_def, states, issues)

        if state_type == "Map":
            item_processor = state_def.get("ItemProcessor")
            iterator = state_def.get("Iterator")
            if isinstance(item_processor, dict):
                _walk_scope(item_processor, state_path + "/ItemProcessor/", issues)
            if isinstance(iterator, dict):
                _walk_scope(iterator, state_path + "/Iterator/", issues)
        elif state_type == "Parallel":
            branches = state_def.get("Branches")
            if not isinstance(branches, list) or not branches:
                issues.append(ValidationIssue(state_path, "Parallel state must include non-empty Branches"))
            else:
                for idx, branch in enumerate(branches):
                    if isinstance(branch, dict):
                        _walk_scope(branch, state_path + f"/Branches[{idx}]/", issues)
                    else:
                        issues.append(ValidationIssue(state_path, f"Branch at index {idx} must be an object"))


def _validate_transitions(
    state_path: str,
    state_type: str,
    state_def: dict[str, Any],
    states: dict[str, Any],
    issues: list[ValidationIssue],
) -> None:
    terminal = state_type in {"Succeed", "Fail"} or state_def.get("End") is True

    if not terminal and "Next" not in state_def and state_type not in {"Choice", "Parallel", "Map"}:
        issues.append(ValidationIssue(state_path, "non-terminal state must define Next or End"))

    if "Next" in state_def:
        next_target = state_def.get("Next")
        if not isinstance(next_target, str) or next_target not in states:
            issues.append(ValidationIssue(state_path, f"Next target '{next_target}' not found in scope"))

    if state_type == "Choice":
        choices = state_def.get("Choices")
        if not isinstance(choices, list) or not choices:
            issues.append(ValidationIssue(state_path, "Choice state must include non-empty Choices"))
        else:
            for idx, choice in enumerate(choices):
                if not isinstance(choice, dict):
                    issues.append(ValidationIssue(state_path, f"Choice at index {idx} must be an object"))
                    continue
                target = choice.get("Next")
                if not isinstance(target, str) or target not in states:
                    issues.append(ValidationIssue(state_path, f"Choices[{idx}].Next target '{target}' not found in scope"))
        if "Default" in state_def:
            default_target = state_def.get("Default")
            if not isinstance(default_target, str) or default_target not in states:
                issues.append(ValidationIssue(state_path, f"Default target '{default_target}' not found in scope"))
