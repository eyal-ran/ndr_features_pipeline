"""Frozen artifact lifecycle contracts (Task 0).

This module defines machine-validated producer/consumer contracts for:
- code bundle build output
- artifact validation report
- artifact smoke report
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ndr.orchestration.remediation_contracts import ContractValidationError, _resolve_ref, _validate_object

CODE_BUNDLE_BUILD_OUTPUT_VERSION = "code_bundle_build_output.v1"
CODE_ARTIFACT_VALIDATE_REPORT_VERSION = "code_artifact_validate_report.v1"
CODE_SMOKE_VALIDATE_REPORT_VERSION = "code_smoke_validate_report.v1"

SCHEMA_FILE_BY_VERSION: dict[str, str] = {
    CODE_BUNDLE_BUILD_OUTPUT_VERSION: "code_bundle_build_output_v1.schema.json",
    CODE_ARTIFACT_VALIDATE_REPORT_VERSION: "code_artifact_validate_report_v1.schema.json",
    CODE_SMOKE_VALIDATE_REPORT_VERSION: "code_smoke_validate_report_v1.schema.json",
}


def load_schema(contract_version: str) -> dict[str, Any]:
    try:
        filename = SCHEMA_FILE_BY_VERSION[contract_version]
    except KeyError as exc:
        raise ContractValidationError(
            code="CONTRACT_VERSION_UNSUPPORTED",
            message=f"Unsupported contract version '{contract_version}'",
        ) from exc
    schema_path = Path(__file__).parent / "schemas" / filename
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_payload(
    payload: dict[str, Any],
    *,
    expected_contract_version: str,
) -> dict[str, Any]:
    payload_version = str(payload.get("contract_version") or "").strip()
    if not payload_version:
        raise ContractValidationError(
            code="CONTRACT_MISSING_REQUIRED_FIELD",
            message="Missing required field: $.contract_version",
        )
    if payload_version != expected_contract_version:
        raise ContractValidationError(
            code="CONTRACT_VERSION_MISMATCH",
            message=(
                f"Contract version mismatch: expected '{expected_contract_version}', "
                f"got '{payload_version}'"
            ),
        )

    schema = load_schema(expected_contract_version)
    _validate_object(payload=payload, schema=_resolve_ref(schema, schema), path="$", root_schema=schema)
    return payload


def validate_code_bundle_build_output(payload: dict[str, Any]) -> dict[str, Any]:
    return validate_payload(payload, expected_contract_version=CODE_BUNDLE_BUILD_OUTPUT_VERSION)


def validate_code_artifact_validate_report(payload: dict[str, Any]) -> dict[str, Any]:
    return validate_payload(payload, expected_contract_version=CODE_ARTIFACT_VALIDATE_REPORT_VERSION)


def validate_code_smoke_validate_report(payload: dict[str, Any]) -> dict[str, Any]:
    return validate_payload(payload, expected_contract_version=CODE_SMOKE_VALIDATE_REPORT_VERSION)
