"""Machine-validated Task-0 frozen v3 schemas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ndr.orchestration.remediation_contracts import ContractValidationError, _resolve_ref, _validate_object

SCHEMA_FILE_BY_VERSION = {
    "monthly_fg_b_readiness.v3": "monthly_fg_b_readiness_v3.schema.json",
    "rt_artifact_readiness.v3": "rt_artifact_readiness_v3.schema.json",
    "rt.raw_input_resolution.v1": "rt_raw_input_resolution_v1.schema.json",
    "NdrBackfillRequest.v2": "ndr_backfill_request_v2.schema.json",
    "step_code_artifact_contract.v1": "step_code_artifact_contract_v1.schema.json",
}


def load_schema(contract_version: str) -> dict[str, Any]:
    try:
        filename = SCHEMA_FILE_BY_VERSION[contract_version]
    except KeyError as exc:
        raise ContractValidationError(
            code="CONTRACT_VERSION_UNSUPPORTED",
            message=f"Unsupported contract version '{contract_version}'",
        ) from exc
    path = Path(__file__).parent / "schemas" / filename
    return json.loads(path.read_text(encoding="utf-8"))


def validate_payload(payload: dict[str, Any], *, contract_version: str) -> dict[str, Any]:
    schema = load_schema(contract_version)
    _validate_object(payload=payload, schema=_resolve_ref(schema, schema), path="$", root_schema=schema)
    return payload
