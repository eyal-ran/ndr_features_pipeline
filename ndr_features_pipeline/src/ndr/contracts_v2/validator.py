"""Validator helpers for frozen flow contracts and deterministic error taxonomy (Task 0)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_CONTRACT_MATRIX_PATH = Path("docs/contracts/flow_contract_matrix_v2.json")
DEFAULT_ERROR_TAXONOMY_PATH = Path("docs/contracts/error_taxonomy_v2.json")


@dataclass(frozen=True)
class ContractValidationError(ValueError):
    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class ContractDriftError(ContractValidationError):
    """Raised when observed producer/consumer interfaces drift from frozen matrix."""


class ErrorTaxonomyValidationError(ContractValidationError):
    """Raised when required error codes are not declared in taxonomy."""


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_contract_matrix(path: Path = DEFAULT_CONTRACT_MATRIX_PATH) -> dict[str, Any]:
    matrix = _load_json(path)
    if matrix.get("contract_version") != "flow_contract_matrix.v2":
        raise ContractValidationError(
            code="CONTRACT_MATRIX_VERSION_MISMATCH",
            message=f"Unsupported contract version: {matrix.get('contract_version')}",
        )
    return matrix


def load_error_taxonomy(path: Path = DEFAULT_ERROR_TAXONOMY_PATH) -> dict[str, Any]:
    taxonomy = _load_json(path)
    if taxonomy.get("taxonomy_version") != "error_taxonomy.v2":
        raise ContractValidationError(
            code="ERROR_TAXONOMY_VERSION_MISMATCH",
            message=f"Unsupported taxonomy version: {taxonomy.get('taxonomy_version')}",
        )
    return taxonomy


def _find_edge(matrix: dict[str, Any], edge_id: str) -> dict[str, Any]:
    for edge in matrix.get("edges", []):
        if edge.get("edge_id") == edge_id:
            return edge
    raise ContractValidationError(
        code="CONTRACT_EDGE_NOT_FOUND",
        message=f"edge_id={edge_id} not found in frozen matrix",
    )


def assert_contract_surface(*, matrix: dict[str, Any], edge_id: str, layer: str, observed_fields: Iterable[str]) -> None:
    edge = _find_edge(matrix, edge_id=edge_id)
    layer_def = edge.get("layers", {}).get(layer)
    if not isinstance(layer_def, dict):
        raise ContractValidationError(
            code="CONTRACT_LAYER_NOT_FOUND",
            message=f"edge_id={edge_id} has no layer={layer}",
        )

    declared_required = {str(v) for v in layer_def.get("required", [])}
    declared_optional = {str(v) for v in layer_def.get("optional", [])}
    declared = declared_required | declared_optional
    observed = {str(v) for v in observed_fields}

    undeclared = sorted(observed - declared)
    if undeclared:
        raise ContractDriftError(
            code="CONTRACT_UNDECLARED_FIELD",
            message=f"edge={edge_id} layer={layer} has undeclared fields {undeclared}",
        )

    missing = sorted(declared_required - observed)
    if missing:
        raise ContractDriftError(
            code="CONTRACT_REQUIRED_FIELD_MISSING",
            message=f"edge={edge_id} layer={layer} missing required fields {missing}",
        )


def assert_no_producer_only_fields(*, matrix: dict[str, Any]) -> None:
    violations: list[str] = []
    for field in matrix.get("field_ownership", []):
        producer = str(field.get("producer_owner") or "").strip()
        consumer = str(field.get("consumer_owner") or "").strip()
        metadata_noop = bool(field.get("validated_metadata_noop", False))
        if producer and not consumer and not metadata_noop:
            violations.append(str(field.get("field_path") or "<unknown>"))

    if violations:
        raise ContractValidationError(
            code="CONTRACT_PRODUCER_ONLY_FIELD",
            message=f"Producer-only fields without consumer/no-op declaration: {sorted(violations)}",
        )


def assert_error_codes_declared(*, taxonomy: dict[str, Any], required_codes: Iterable[str]) -> None:
    declared = {
        str(entry.get("error_code"))
        for entry in taxonomy.get("errors", [])
        if str(entry.get("error_code") or "").strip()
    }
    missing = sorted({str(code) for code in required_codes} - declared)
    if missing:
        raise ErrorTaxonomyValidationError(
            code="ERROR_TAXONOMY_MISSING_CODE",
            message=f"Missing error taxonomy codes: {missing}",
        )
