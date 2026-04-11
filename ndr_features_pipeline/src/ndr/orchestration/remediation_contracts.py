"""Canonical remediation contracts, schemas, and validators (Task 0)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

ISO_UTC_PATTERN = "%Y-%m-%dT%H:%M:%SZ"

BACKFILL_REQUEST_VERSION = "NdrBackfillRequest.v2"
BASELINE_REQUEST_VERSION = "NdrBaselineRemediationRequest.v1"
TRAINING_REQUEST_VERSION = "NdrTrainingRemediationRequest.v1"
REMEDIATION_RESPONSE_VERSION = "NdrRemediationResponse.v1"

SUPPORTED_REQUEST_VERSIONS = {
    BACKFILL_REQUEST_VERSION,
    BASELINE_REQUEST_VERSION,
    TRAINING_REQUEST_VERSION,
}

SCHEMA_FILE_BY_VERSION = {
    BACKFILL_REQUEST_VERSION: "ndr_backfill_request_v2.schema.json",
    BASELINE_REQUEST_VERSION: "ndr_baseline_remediation_request_v1.schema.json",
    TRAINING_REQUEST_VERSION: "ndr_training_remediation_request_v1.schema.json",
    REMEDIATION_RESPONSE_VERSION: "ndr_remediation_response_v1.schema.json",
}


@dataclass
class ContractValidationError(ValueError):
    """Typed validation failure with deterministic code and message."""

    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def load_schema(contract_version: str) -> dict[str, Any]:
    """Load machine-readable JSON schema for the provided contract version."""
    try:
        filename = SCHEMA_FILE_BY_VERSION[contract_version]
    except KeyError as exc:
        raise ContractValidationError(
            code="CONTRACT_VERSION_UNSUPPORTED",
            message=f"Unsupported contract version '{contract_version}'",
        ) from exc
    path = Path(__file__).parent / "schemas" / filename
    return json.loads(path.read_text(encoding="utf-8"))


def build_idempotency_key(*, contract_version: str, payload_without_idempotency_key: dict[str, Any]) -> str:
    """Build deterministic idempotency key via canonical JSON SHA-256."""
    canonical = {
        "contract_version": contract_version,
        "payload": payload_without_idempotency_key,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_contract_payload(
    payload: dict[str, Any],
    *,
    expected_contract_version: str | None = None,
) -> dict[str, Any]:
    """Validate remediation request/response payloads against canonical contracts."""
    contract_version = str(payload.get("contract_version") or "").strip()
    if not contract_version:
        raise ContractValidationError(
            code="CONTRACT_MISSING_REQUIRED_FIELD",
            message="Missing required field: contract_version",
        )
    if expected_contract_version and contract_version != expected_contract_version:
        raise ContractValidationError(
            code="CONTRACT_VERSION_MISMATCH",
            message=(
                f"Contract version mismatch: expected '{expected_contract_version}', "
                f"got '{contract_version}'"
            ),
        )

    schema = load_schema(contract_version)
    _validate_object(payload=payload, schema=schema, path="$", root_schema=schema)

    _validate_range_order(payload=payload)
    _validate_idempotency_key(payload=payload)
    return payload


def build_training_remediation_request(
    *,
    project_name: str,
    feature_spec_version: str,
    run_id: str,
    training_window: dict[str, str],
    evaluation_windows: list[dict[str, str]],
    missing_manifest: dict[str, Any],
    requested_families: list[str],
    missing_ranges: list[dict[str, str]],
    provenance: dict[str, str],
    ml_project_name: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "contract_version": TRAINING_REQUEST_VERSION,
        "consumer": "training",
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "run_id": run_id,
        "training_window": training_window,
        "evaluation_windows": evaluation_windows,
        "missing_manifest": missing_manifest,
        "requested_families": requested_families,
        "missing_ranges": missing_ranges,
        "provenance": provenance,
    }
    if ml_project_name:
        payload["ml_project_name"] = ml_project_name
    payload["idempotency_key"] = build_idempotency_key(
        contract_version=TRAINING_REQUEST_VERSION,
        payload_without_idempotency_key=payload,
    )
    return validate_contract_payload(payload, expected_contract_version=TRAINING_REQUEST_VERSION)


def build_backfill_request(
    *,
    project_name: str,
    feature_spec_version: str,
    requested_families: list[str],
    missing_ranges: list[dict[str, str]],
    provenance: dict[str, str],
    ml_project_name: str | None = None,
    batch_id: str | None = None,
    source_hint: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "contract_version": BACKFILL_REQUEST_VERSION,
        "consumer": "realtime",
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "requested_families": requested_families,
        "missing_ranges": missing_ranges,
        "provenance": provenance,
    }
    if ml_project_name:
        payload["ml_project_name"] = ml_project_name
    if batch_id:
        payload["batch_id"] = batch_id
    if source_hint:
        payload["source_hint"] = source_hint

    payload["idempotency_key"] = build_idempotency_key(
        contract_version=BACKFILL_REQUEST_VERSION,
        payload_without_idempotency_key=payload,
    )
    return validate_contract_payload(payload, expected_contract_version=BACKFILL_REQUEST_VERSION)


def build_baseline_remediation_request(
    *,
    project_name: str,
    feature_spec_version: str,
    reference_month: str,
    required_families: list[str],
    missing_ranges: list[dict[str, str]],
    provenance: dict[str, str],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "contract_version": BASELINE_REQUEST_VERSION,
        "consumer": "monthly_baseline",
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "reference_month": reference_month,
        "required_families": required_families,
        "missing_ranges": missing_ranges,
        "provenance": provenance,
    }
    payload["idempotency_key"] = build_idempotency_key(
        contract_version=BASELINE_REQUEST_VERSION,
        payload_without_idempotency_key=payload,
    )
    return validate_contract_payload(payload, expected_contract_version=BASELINE_REQUEST_VERSION)


def build_remediation_response(
    *,
    request_contract_version: str,
    consumer: str,
    request_id: str,
    idempotency_key: str,
    producer_flow: str,
    source_mode: str,
    status: str,
    produced_ranges: list[dict[str, Any]] | None = None,
    errors: list[dict[str, str]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "contract_version": REMEDIATION_RESPONSE_VERSION,
        "request_contract_version": request_contract_version,
        "consumer": consumer,
        "status": status,
        "request_id": request_id,
        "idempotency_key": idempotency_key,
        "provenance": {
            "producer_flow": producer_flow,
            "source_mode": source_mode,
            "request_id": request_id,
        },
        "produced_ranges": produced_ranges or [],
        "errors": errors or [],
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return validate_contract_payload(payload, expected_contract_version=REMEDIATION_RESPONSE_VERSION)


def build_default_provenance(*, producer_flow: str, source_mode: str, request_id: str | None = None) -> dict[str, str]:
    return {
        "producer_flow": producer_flow,
        "source_mode": source_mode,
        "request_id": request_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
    }


def _validate_idempotency_key(*, payload: dict[str, Any]) -> None:
    if "idempotency_key" not in payload:
        return
    if payload.get("contract_version") == REMEDIATION_RESPONSE_VERSION:
        return
    provided = payload["idempotency_key"]
    if not isinstance(provided, str) or not provided:
        raise ContractValidationError(
            code="CONTRACT_INVALID_FIELD",
            message="idempotency_key must be a non-empty sha256 hex string",
        )
    base = {k: v for k, v in payload.items() if k != "idempotency_key"}
    expected = build_idempotency_key(
        contract_version=str(payload["contract_version"]),
        payload_without_idempotency_key=base,
    )
    if provided != expected:
        raise ContractValidationError(
            code="CONTRACT_IDEMPOTENCY_KEY_MISMATCH",
            message="idempotency_key does not match deterministic contract recipe",
        )


def _validate_range_order(*, payload: dict[str, Any]) -> None:
    ranges = payload.get("missing_ranges") or []
    for idx, row in enumerate(ranges):
        start = _parse_ts(row["start_ts_iso"])
        end = _parse_ts(row["end_ts_iso"])
        if start >= end:
            raise ContractValidationError(
                code="CONTRACT_INVALID_TIME_RANGE",
                message=f"missing_ranges[{idx}] start_ts_iso must be < end_ts_iso",
            )
    if payload.get("training_window"):
        start = _parse_ts(payload["training_window"]["start_ts_iso"])
        end = _parse_ts(payload["training_window"]["end_ts_iso"])
        if start >= end:
            raise ContractValidationError(
                code="CONTRACT_INVALID_TIME_RANGE",
                message="training_window.start_ts_iso must be < training_window.end_ts_iso",
            )


def _parse_ts(value: str) -> datetime:
    try:
        return datetime.strptime(value, ISO_UTC_PATTERN).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ContractValidationError(
            code="CONTRACT_INVALID_FIELD",
            message=f"Invalid UTC timestamp value: '{value}'",
        ) from exc


def _validate_object(*, payload: Any, schema: dict[str, Any], path: str, root_schema: dict[str, Any]) -> None:
    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(payload, dict):
            raise ContractValidationError(code="CONTRACT_INVALID_TYPE", message=f"{path} must be an object")
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for key in required:
            if key not in payload:
                raise ContractValidationError(
                    code="CONTRACT_MISSING_REQUIRED_FIELD",
                    message=f"Missing required field: {path}.{key}",
                )

        if schema.get("additionalProperties") is False:
            unknown = sorted(set(payload) - set(properties))
            if unknown:
                raise ContractValidationError(
                    code="CONTRACT_UNKNOWN_FIELD",
                    message=f"Unknown field(s) at {path}: {unknown}",
                )
        for key, value in payload.items():
            subschema = properties.get(key)
            if subschema is None:
                continue
            _validate_object(
                payload=value,
                schema=_resolve_ref(subschema, root_schema),
                path=f"{path}.{key}",
                root_schema=root_schema,
            )

    elif expected_type == "array":
        if not isinstance(payload, list):
            raise ContractValidationError(code="CONTRACT_INVALID_TYPE", message=f"{path} must be an array")
        min_items = schema.get("minItems")
        if min_items is not None and len(payload) < min_items:
            raise ContractValidationError(
                code="CONTRACT_MISSING_REQUIRED_FIELD",
                message=f"{path} must contain at least {min_items} item(s)",
            )
        if schema.get("uniqueItems") and len(payload) != len(set(json.dumps(i, sort_keys=True) for i in payload)):
            raise ContractValidationError(code="CONTRACT_INVALID_FIELD", message=f"{path} must contain unique items")
        item_schema = _resolve_ref(schema.get("items", {}), root_schema)
        for idx, item in enumerate(payload):
            _validate_object(payload=item, schema=item_schema, path=f"{path}[{idx}]", root_schema=root_schema)

    elif expected_type == "string":
        if not isinstance(payload, str):
            raise ContractValidationError(code="CONTRACT_INVALID_TYPE", message=f"{path} must be a string")
        min_length = schema.get("minLength")
        if min_length is not None and len(payload) < min_length:
            raise ContractValidationError(code="CONTRACT_INVALID_FIELD", message=f"{path} must be non-empty")
        pattern = schema.get("pattern")
        if pattern and re.match(pattern, payload) is None:
            raise ContractValidationError(
                code="CONTRACT_INVALID_FIELD",
                message=f"{path} does not match required pattern",
            )
        enum = schema.get("enum")
        if enum and payload not in enum:
            raise ContractValidationError(
                code="CONTRACT_INVALID_FIELD",
                message=f"{path} must be one of {enum}",
            )
        const = schema.get("const")
        if const and payload != const:
            raise ContractValidationError(code="CONTRACT_VERSION_UNSUPPORTED", message=f"{path} must equal '{const}'")

    else:
        const = schema.get("const")
        if const is not None and payload != const:
            raise ContractValidationError(code="CONTRACT_INVALID_FIELD", message=f"{path} must equal '{const}'")
        enum = schema.get("enum")
        if enum and payload not in enum:
            raise ContractValidationError(code="CONTRACT_INVALID_FIELD", message=f"{path} must be one of {enum}")


def _resolve_ref(schema: dict[str, Any], root_schema: dict[str, Any]) -> dict[str, Any]:
    ref = schema.get("$ref")
    if not ref:
        return schema
    if not ref.startswith("#/definitions/"):
        raise ContractValidationError(
            code="CONTRACT_INVALID_SCHEMA",
            message=f"Unsupported schema reference '{ref}'",
        )
    key = ref.split("/")[-1]
    try:
        return root_schema["definitions"][key]
    except KeyError as exc:
        raise ContractValidationError(
            code="CONTRACT_INVALID_SCHEMA",
            message=f"Missing schema definition '{key}'",
        ) from exc
