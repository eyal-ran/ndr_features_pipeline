"""Write helpers for the greenfield dual-item Batch Index contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import boto3

from ndr.config.project_parameters_loader import resolve_batch_index_table_name
from ndr.contracts import BATCH_INDEX_PK, BATCH_INDEX_SK, date_lookup_sk


@dataclass(frozen=True)
class BatchIndexWriteRequest:
    project_name: str
    batch_id: str
    date_partition: str
    hour: str
    within_hour_run_number: str
    etl_ts: str
    org1: str
    org2: str
    raw_parsed_logs_s3_prefix: str
    ml_project_names: list[str]
    s3_prefixes: dict[str, Any]
    rt_flow_status: str = "PENDING"
    backfill_status: str = "NOT_STARTED"
    source_mode: str = "ingestion"


class BatchIndexWriter:
    def __init__(self, table_name: str | None = None) -> None:
        self._ddb = boto3.resource("dynamodb")
        self._table = self._ddb.Table(resolve_batch_index_table_name(table_name))

    def upsert_dual_items(self, write_request: BatchIndexWriteRequest) -> None:
        batch_item = self._build_batch_lookup_item(write_request)
        date_item = self._build_date_lookup_item(write_request)
        self._idempotent_put(item=batch_item)
        self._idempotent_put(item=date_item)

    # Compatibility method retained until flow migrations move to upsert_dual_items.
    def upsert(self, write_request: BatchIndexWriteRequest) -> None:
        self.upsert_dual_items(write_request)

    def update_status(
        self,
        *,
        project_name: str,
        batch_id: str,
        rt_flow_status: str | None = None,
        backfill_status: str | None = None,
    ) -> None:
        if rt_flow_status is None and backfill_status is None:
            raise ValueError("At least one status field must be provided")
        updates: list[str] = ["last_updated_at = :last_updated_at"]
        values: dict[str, Any] = {":last_updated_at": _now_iso()}
        if rt_flow_status is not None:
            updates.append("rt_flow_status = :rt_flow_status")
            values[":rt_flow_status"] = rt_flow_status
        if backfill_status is not None:
            updates.append("backfill_status = :backfill_status")
            values[":backfill_status"] = backfill_status

        self._table.update_item(
            Key={BATCH_INDEX_PK: project_name, BATCH_INDEX_SK: batch_id},
            UpdateExpression=f"SET {', '.join(updates)}",
            ExpressionAttributeValues=values,
            ConditionExpression=f"attribute_exists({BATCH_INDEX_PK}) AND attribute_exists({BATCH_INDEX_SK})",
        )

    def get_item(self, *, project_name: str, sort_key: str) -> dict[str, Any] | None:
        response = self._table.get_item(Key={BATCH_INDEX_PK: project_name, BATCH_INDEX_SK: sort_key})
        item = response.get("Item")
        return item if isinstance(item, dict) else None

    def _build_batch_lookup_item(self, request: BatchIndexWriteRequest) -> dict[str, Any]:
        return {
            BATCH_INDEX_PK: request.project_name,
            BATCH_INDEX_SK: request.batch_id,
            "item_kind": "BATCH_LOOKUP",
            "batch_id": request.batch_id,
            "date_partition": request.date_partition,
            "hour": request.hour,
            "within_hour_run_number": request.within_hour_run_number,
            "etl_ts": request.etl_ts,
            "org1": request.org1,
            "org2": request.org2,
            "raw_parsed_logs_s3_prefix": request.raw_parsed_logs_s3_prefix,
            "ml_project_names": request.ml_project_names,
            "s3_prefixes": request.s3_prefixes,
            "rt_flow_status": request.rt_flow_status,
            "backfill_status": request.backfill_status,
            "source_mode": request.source_mode,
            "last_updated_at": _now_iso(),
        }

    def _build_date_lookup_item(self, request: BatchIndexWriteRequest) -> dict[str, Any]:
        return {
            BATCH_INDEX_PK: request.project_name,
            BATCH_INDEX_SK: date_lookup_sk(
                date_partition=request.date_partition,
                hour=request.hour,
                within_hour_run_number=request.within_hour_run_number,
            ),
            "item_kind": "DATE_LOOKUP",
            "batch_id": request.batch_id,
            "batch_lookup_sk": request.batch_id,
            "date_partition": request.date_partition,
            "hour": request.hour,
            "within_hour_run_number": request.within_hour_run_number,
            "etl_ts": request.etl_ts,
            "org1": request.org1,
            "org2": request.org2,
            "raw_parsed_logs_s3_prefix": request.raw_parsed_logs_s3_prefix,
            "last_updated_at": _now_iso(),
        }

    def _idempotent_put(self, *, item: dict[str, Any]) -> None:
        key = {BATCH_INDEX_PK: item[BATCH_INDEX_PK], BATCH_INDEX_SK: item[BATCH_INDEX_SK]}
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression=f"attribute_not_exists({BATCH_INDEX_PK}) AND attribute_not_exists({BATCH_INDEX_SK})",
            )
            return
        except Exception as exc:
            error_code = (
                getattr(exc, "response", {}).get("Error", {}).get("Code")
                if hasattr(exc, "response")
                else None
            )
            if error_code != "ConditionalCheckFailedException":
                raise

        immutable_fields = {
            k: v
            for k, v in item.items()
            if k not in {BATCH_INDEX_PK, BATCH_INDEX_SK, "rt_flow_status", "backfill_status", "last_updated_at"}
        }
        assignments = [f"{k} = if_not_exists({k}, :{k})" for k in sorted(immutable_fields)]
        assignments.append("last_updated_at = :last_updated_at")
        expression_values = {f":{k}": v for k, v in immutable_fields.items()}
        expression_values[":last_updated_at"] = _now_iso()
        self._table.update_item(
            Key=key,
            UpdateExpression=f"SET {', '.join(assignments)}",
            ExpressionAttributeValues=expression_values,
            ConditionExpression=f"attribute_exists({BATCH_INDEX_PK}) AND attribute_exists({BATCH_INDEX_SK})",
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
