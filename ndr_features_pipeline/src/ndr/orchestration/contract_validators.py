"""Reusable contract validators for orchestration and interface hardening."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


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


_FORBIDDEN_BUSINESS_FALLBACK_MARKERS = (
    "<required:",
    "<placeholder",
    "${",
    "env_fallback",
    "code_default",
)


def validate_no_business_fallback_markers(*, values: Mapping[str, Any], context: str) -> None:
    """Reject runtime/business contract values that still contain fallback placeholders.

    Task-9 hardening requires concrete DDB-resolved values for orchestration ownership.
    This validator intentionally checks for marker substrings rather than exact values so
    contract checks fail fast even when payload shapes evolve.
    """

    offending: list[str] = []
    for key, value in values.items():
        text = str(value).strip()
        if not text:
            offending.append(f"{key}=<blank>")
            continue
        lowered = text.lower()
        if any(marker in lowered for marker in _FORBIDDEN_BUSINESS_FALLBACK_MARKERS):
            offending.append(f"{key}={text}")
    if offending:
        raise ValueError(
            f"{context}: fallback/placeholder business markers are not allowed: {offending}"
        )


def validate_targeted_recovery_manifest(
    *,
    manifest_entries: Sequence[Mapping[str, Any]],
    requested_families: Sequence[str],
) -> None:
    """Ensure targeted-recovery execution remains selective and deterministic."""

    requested = {str(f).strip() for f in requested_families if str(f).strip()}
    if not requested:
        raise ValueError("requested_families must contain at least one artifact family")

    if not manifest_entries:
        raise ValueError("manifest_entries cannot be empty for targeted recovery")

    planned: set[str] = set()
    for idx, entry in enumerate(manifest_entries):
        family = str(entry.get("artifact_family") or "").strip()
        ranges = entry.get("ranges")
        if not family:
            raise ValueError(f"manifest_entries[{idx}] is missing artifact_family")
        if family not in requested:
            raise ValueError(
                f"manifest_entries[{idx}] requests unsupported family '{family}' "
                f"(requested_families={sorted(requested)})"
            )
        if not isinstance(ranges, list) or not ranges:
            raise ValueError(f"manifest_entries[{idx}] for family '{family}' must include non-empty ranges")
        planned.add(family)

    missing = sorted(requested - planned)
    if missing:
        raise ValueError(f"targeted recovery manifest is missing requested families: {missing}")
