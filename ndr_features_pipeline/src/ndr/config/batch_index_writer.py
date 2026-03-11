"""Write helpers for deterministic idempotent batch_index upserts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ndr.config.project_parameters_loader import resolve_batch_index_table_name


@dataclass(frozen=True)
class BatchIndexWriteRequest:
    pk: str
    sk: str
    project_name: str
    data_source_name: str
    version: str
    date_utc: str
    hour_utc: str
    slot15: int
    batch_id: str
    raw_parsed_logs_s3_prefix: str
    event_ts_utc: str
    org1: str
    org2: str
    status: str
    gsi1pk: str
    gsi1sk: str
    ml_project_name: str = ""
    ml_project_names_json: str = ""
    ttl_epoch: int | None = None


class BatchIndexWriter:
    """Persists index records with the vNext idempotent write contract."""

    def __init__(self, table_name: str | None = None) -> None:
        self._ddb = boto3.resource("dynamodb")
        self._table = self._ddb.Table(resolve_batch_index_table_name(table_name))

    def upsert(self, write_request: BatchIndexWriteRequest) -> None:
        item = self._build_item(write_request)
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
            )
            return
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise

        self._table.update_item(
            Key={"pk": write_request.pk, "sk": write_request.sk},
            UpdateExpression=(
                "SET raw_parsed_logs_s3_prefix = :raw_parsed_logs_s3_prefix, "
                "event_ts_utc = :event_ts_utc, "
                "ingested_at_utc = :ingested_at_utc, "
                "#status = :status, "
                "ml_project_name = if_not_exists(ml_project_name, :ml_project_name), "
                "ml_project_names_json = if_not_exists(ml_project_names_json, :ml_project_names_json), "
                "GSI1PK = :gsi1pk, "
                "GSI1SK = :gsi1sk"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":raw_parsed_logs_s3_prefix": write_request.raw_parsed_logs_s3_prefix,
                ":event_ts_utc": write_request.event_ts_utc,
                ":ingested_at_utc": _now_iso(),
                ":status": write_request.status,
                ":ml_project_name": write_request.ml_project_name,
                ":ml_project_names_json": write_request.ml_project_names_json,
                ":gsi1pk": write_request.gsi1pk,
                ":gsi1sk": write_request.gsi1sk,
            },
            ConditionExpression="attribute_exists(pk) AND attribute_exists(sk)",
        )

    def _build_item(self, write_request: BatchIndexWriteRequest) -> dict[str, Any]:
        item: dict[str, Any] = {
            "pk": write_request.pk,
            "sk": write_request.sk,
            "project_name": write_request.project_name,
            "data_source_name": write_request.data_source_name,
            "version": write_request.version,
            "date_utc": write_request.date_utc,
            "hour_utc": write_request.hour_utc,
            "slot15": write_request.slot15,
            "batch_id": write_request.batch_id,
            "raw_parsed_logs_s3_prefix": write_request.raw_parsed_logs_s3_prefix,
            "event_ts_utc": write_request.event_ts_utc,
            "org1": write_request.org1,
            "org2": write_request.org2,
            "ingested_at_utc": _now_iso(),
            "status": write_request.status,
            "GSI1PK": write_request.gsi1pk,
            "GSI1SK": write_request.gsi1sk,
        }
        if write_request.ml_project_name:
            item["ml_project_name"] = write_request.ml_project_name
        if write_request.ml_project_names_json:
            item["ml_project_names_json"] = write_request.ml_project_names_json
        if write_request.ttl_epoch is not None:
            item["ttl_epoch"] = write_request.ttl_epoch
        return item


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
