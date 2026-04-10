"""SageMaker Processing preflight validator for exception control-plane tables."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import boto3

from ndr.config.exception_table_contracts import (
    EXCEPTION_TABLE_CONTRACTS,
    assert_operation_allowed,
    classify_ddb_error,
    validate_table_schema,
)

_FLOW_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "rt": ("routing", "processing_lock", "publication_lock"),
    "monthly": ("processing_lock",),
    "backfill": ("processing_lock",),
    "training": ("processing_lock",),
    "all": ("routing", "processing_lock", "publication_lock"),
}


def _table_name_for(logical_name: str, explicit: str | None = None) -> str:
    if explicit:
        return explicit
    env_map = {
        "routing": "ML_PROJECTS_ROUTING_TABLE_NAME",
        "processing_lock": "PROCESSING_LOCK_TABLE_NAME",
        "publication_lock": "PUBLICATION_LOCK_TABLE_NAME",
    }
    return os.getenv(env_map[logical_name]) or EXCEPTION_TABLE_CONTRACTS[logical_name].default_table_name


def _structured_log(**payload: Any) -> None:
    print(json.dumps(payload, sort_keys=True))


def _validate_table(ddb_client: Any, *, logical_name: str, table_name: str) -> None:
    assert_operation_allowed(logical_name=logical_name, operation="DescribeTable")
    try:
        desc = ddb_client.describe_table(TableName=table_name)["Table"]
    except Exception as exc:  # pragma: no cover - boto client behavior
        raise classify_ddb_error(logical_name=logical_name, table_name=table_name, error=exc) from exc

    validate_table_schema(logical_name=logical_name, table_name=table_name, table_description=desc)
    _structured_log(
        table=table_name,
        logical_name=logical_name,
        operation="DescribeTable",
        result="ok",
        error_code="",
        metric_name="exception_table_validation_success_count",
        metric_value=1,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate exception DynamoDB table contracts before flow startup.")
    parser.add_argument("--region", default=None, help="AWS region. Defaults to environment/provider chain.")
    parser.add_argument(
        "--flow",
        choices=tuple(_FLOW_DEPENDENCIES.keys()),
        default="all",
        help="Flow dependency profile to validate.",
    )
    parser.add_argument("--routing-table-name", default=None)
    parser.add_argument("--processing-lock-table-name", default=None)
    parser.add_argument("--publication-lock-table-name", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logical_tables = _FLOW_DEPENDENCIES[args.flow]
    resolved = {
        "routing": _table_name_for("routing", args.routing_table_name),
        "processing_lock": _table_name_for("processing_lock", args.processing_lock_table_name),
        "publication_lock": _table_name_for("publication_lock", args.publication_lock_table_name),
    }

    ddb_client = boto3.client("dynamodb", region_name=args.region)
    for logical_name in logical_tables:
        table_name = resolved[logical_name]
        _validate_table(ddb_client, logical_name=logical_name, table_name=table_name)

    _structured_log(event="exception_table_preflight", flow=args.flow, status="ok", tables=logical_tables)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI behavior
        _structured_log(
            event="exception_table_preflight",
            status="failed",
            error_code=getattr(exc, "code", "EXC_TABLE_PREFLIGHT_FAILURE"),
            retriable=bool(getattr(exc, "retriable", False)),
            message=str(exc),
            metric_name="exception_table_validation_failure_count",
            metric_value=1,
        )
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
