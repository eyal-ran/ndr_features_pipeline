"""Reusable contract validators for orchestration and interface hardening."""

from __future__ import annotations

from typing import Iterable, Sequence


def normalize_ml_project_names(
    ml_project_names: Sequence[str] | None,
    ml_project_name: str | None = None,
) -> list[str]:
    """Normalize to array-first ml_project_names and enforce deterministic ordering."""
    normalized: list[str] = []
    if ml_project_names:
        for value in ml_project_names:
            candidate = str(value).strip()
            if not candidate:
                raise ValueError("ml_project_names cannot contain empty values")
            if candidate not in normalized:
                normalized.append(candidate)
    elif ml_project_name:
        candidate = str(ml_project_name).strip()
        if not candidate:
            raise ValueError("ml_project_name cannot be blank when provided")
        normalized = [candidate]

    if not normalized:
        raise ValueError("ml_project_names must resolve to a non-empty list")
    return normalized


def validate_pipeline_parameter_alignment(
    *,
    declared_parameters: Iterable[str],
    passed_parameters: Iterable[str],
) -> None:
    """Fail fast when SF->pipeline interfaces drift."""
    declared = set(declared_parameters)
    passed = set(passed_parameters)
    undeclared = sorted(passed - declared)
    if undeclared:
        raise ValueError(f"Pipeline parameters passed but undeclared: {undeclared}")
    missing = sorted(declared - passed)
    if missing:
        raise ValueError(f"Pipeline parameters declared but not passed: {missing}")
