"""Read access helpers for the ndr_batch_index table."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from ndr.config.project_parameters_loader import resolve_batch_index_table_name


@dataclass(frozen=True)
class BatchIndexRecord:
    project_name: str
    data_source_name: str
    version: str
    batch_id: str
    batch_s3_prefix: str
    event_ts_utc: str


class BatchIndexLoader:
    """Load deterministic non-RT batch pointers from the batch index table."""

    def __init__(self, table_name: str | None = None) -> None:
        self._ddb = boto3.resource("dynamodb")
        self._table = self._ddb.Table(resolve_batch_index_table_name(table_name))

    def lookup_forward(
        self,
        *,
        project_name: str,
        data_source_name: str,
        version: str,
        start_ts_iso: str,
        end_ts_iso: str,
    ) -> list[BatchIndexRecord]:
        """Forward lookup rows by project/data-source/version and UTC range."""
        start_ts = _parse_iso8601(start_ts_iso)
        end_ts = _parse_iso8601(end_ts_iso)
        if end_ts <= start_ts:
            raise ValueError("end_ts_iso must be greater than start_ts_iso")

        records: list[BatchIndexRecord] = []
        for date_utc in _iter_dates(start_ts, end_ts):
            pk = _compose_pk(
                project_name=project_name,
                data_source_name=data_source_name,
                version=version,
                date_utc=date_utc,
            )
            last_evaluated_key: dict[str, Any] | None = None
            while True:
                query_kwargs: dict[str, Any] = {
                    "KeyConditionExpression": Key("pk").eq(pk),
                }
                if last_evaluated_key:
                    query_kwargs["ExclusiveStartKey"] = last_evaluated_key
                response = self._table.query(**query_kwargs)
                items = response.get("Items", [])
                for item in items:
                    event_ts = _parse_iso8601(str(item.get("event_ts_utc", "")))
                    if start_ts <= event_ts < end_ts:
                        records.append(_record_from_item(item))

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

        return sorted(records, key=lambda r: (r.event_ts_utc, r.batch_id))

    def lookup_reverse(
        self,
        *,
        project_name: str,
        data_source_name: str,
        version: str,
        batch_id: str,
    ) -> BatchIndexRecord | None:
        """Reverse lookup latest row by batch identity using GSI1."""
        gsi1pk = f"{project_name}#{data_source_name}#{version}#{batch_id}"
        response = self._table.query(
            IndexName="GSI1",
            KeyConditionExpression=Key("GSI1PK").eq(gsi1pk),
            ScanIndexForward=False,
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            return None
        return _record_from_item(items[0])


def _compose_pk(*, project_name: str, data_source_name: str, version: str, date_utc: str) -> str:
    return f"{project_name}#{data_source_name}#{version}#{date_utc}"


def _record_from_item(item: dict[str, Any]) -> BatchIndexRecord:
    return BatchIndexRecord(
        project_name=str(item["project_name"]),
        data_source_name=str(item["data_source_name"]),
        version=str(item["version"]),
        batch_id=str(item["batch_id"]),
        batch_s3_prefix=str(item["batch_s3_prefix"]),
        event_ts_utc=str(item["event_ts_utc"]),
    )


def _iter_dates(start_ts: datetime, end_ts: datetime) -> list[str]:
    cursor = start_ts.date()
    end_date = (end_ts - timedelta(seconds=1)).date()
    out: list[str] = []
    while cursor <= end_date:
        out.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return out


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
