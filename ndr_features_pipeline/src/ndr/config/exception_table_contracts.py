"""Control-plane contracts for exception DynamoDB tables (Task 8)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


TRANSIENT_DDB_ERROR_CODES = {
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
    "InternalServerError",
    "ServiceUnavailable",
}


class ExceptionTableContractError(RuntimeError):
    """Typed fail-fast error for exception-table contract violations."""

    def __init__(self, *, code: str, message: str, retriable: bool = False) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retriable = retriable


@dataclass(frozen=True)
class ExceptionTableContract:
    logical_name: str
    default_table_name: str
    key_schema: tuple[tuple[str, str], ...]
    allowed_operations: tuple[str, ...]
    ttl_attribute: str | None = None


EXCEPTION_TABLE_CONTRACTS: dict[str, ExceptionTableContract] = {
    "routing": ExceptionTableContract(
        logical_name="routing",
        default_table_name="ml_projects_routing",
        key_schema=(("org_key", "HASH"),),
        allowed_operations=("DescribeTable", "GetItem", "PutItem"),
    ),
    "processing_lock": ExceptionTableContract(
        logical_name="processing_lock",
        default_table_name="processing_lock",
        key_schema=(("pk", "HASH"), ("sk", "RANGE")),
        ttl_attribute="ttl_epoch",
        allowed_operations=("DescribeTable", "PutItem", "DeleteItem"),
    ),
    "publication_lock": ExceptionTableContract(
        logical_name="publication_lock",
        default_table_name="publication_lock",
        key_schema=(("pk", "HASH"), ("sk", "RANGE")),
        ttl_attribute="ttl_epoch",
        allowed_operations=("DescribeTable", "PutItem", "UpdateItem", "DeleteItem"),
    ),
}


def get_exception_table_contract(logical_name: str) -> ExceptionTableContract:
    try:
        return EXCEPTION_TABLE_CONTRACTS[logical_name]
    except KeyError as exc:
        raise ExceptionTableContractError(
            code="EXC_TABLE_UNKNOWN",
            message=f"Unknown exception-table contract '{logical_name}'.",
            retriable=False,
        ) from exc


def assert_operation_allowed(*, logical_name: str, operation: str) -> None:
    contract = get_exception_table_contract(logical_name)
    if operation not in contract.allowed_operations:
        raise ExceptionTableContractError(
            code="EXC_TABLE_OPERATION_FORBIDDEN",
            message=(
                f"Operation '{operation}' is not allowed for '{logical_name}'. "
                f"Allowed operations={list(contract.allowed_operations)}"
            ),
            retriable=False,
        )


def _normalized_key_schema(table_description: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    key_schema = table_description.get("KeySchema") or []
    normalized: list[tuple[str, str]] = []
    for item in key_schema:
        normalized.append((str(item.get("AttributeName")), str(item.get("KeyType"))))
    return tuple(normalized)


def validate_table_schema(*, logical_name: str, table_name: str, table_description: Mapping[str, Any]) -> None:
    contract = get_exception_table_contract(logical_name)
    observed = _normalized_key_schema(table_description)
    if observed != contract.key_schema:
        raise ExceptionTableContractError(
            code="EXC_TABLE_SCHEMA_DRIFT",
            message=(
                f"{logical_name} table '{table_name}' key schema mismatch. "
                f"expected={list(contract.key_schema)} observed={list(observed)}"
            ),
            retriable=False,
        )


def classify_ddb_error(*, logical_name: str, table_name: str, error: Exception) -> ExceptionTableContractError:
    response = getattr(error, "response", None) or {}
    details = response.get("Error", {}) if isinstance(response, Mapping) else {}
    error_code = str(details.get("Code") or error.__class__.__name__)
    if error_code == "ResourceNotFoundException":
        return ExceptionTableContractError(
            code="EXC_TABLE_MISSING",
            message=f"{logical_name} table '{table_name}' does not exist.",
            retriable=False,
        )
    if error_code in {"AccessDeniedException", "UnauthorizedOperation", "UnrecognizedClientException"}:
        return ExceptionTableContractError(
            code="EXC_TABLE_ACCESS_DENIED",
            message=f"Access denied for {logical_name} table '{table_name}' ({error_code}).",
            retriable=False,
        )
    if error_code in TRANSIENT_DDB_ERROR_CODES:
        return ExceptionTableContractError(
            code="EXC_TABLE_DDB_TRANSIENT",
            message=f"Transient DDB error for {logical_name} table '{table_name}' ({error_code}).",
            retriable=True,
        )
    return ExceptionTableContractError(
        code="EXC_TABLE_VALIDATION_FAILED",
        message=f"Unexpected DDB error for {logical_name} table '{table_name}' ({error_code}).",
        retriable=False,
    )
