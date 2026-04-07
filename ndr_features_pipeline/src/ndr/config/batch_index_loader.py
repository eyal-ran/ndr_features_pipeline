"""Read access helpers for the greenfield dual-item batch_index contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from ndr.config.project_parameters_loader import resolve_batch_index_table_name
from ndr.contracts import BATCH_INDEX_PK, BATCH_INDEX_SK


@dataclass(frozen=True)
class BatchIndexRecord:
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


class BatchIndexLoader:
    """Loader for direct, reverse-date, and MLP-branch lookups."""

    def __init__(self, table_name: str | None = None) -> None:
        self._ddb = boto3.resource("dynamodb")
        self._table = self._ddb.Table(resolve_batch_index_table_name(table_name))

    def get_batch(self, *, project_name: str, batch_id: str) -> BatchIndexRecord | None:
        response = self._table.get_item(Key={BATCH_INDEX_PK: project_name, BATCH_INDEX_SK: batch_id})
        item = response.get("Item")
        if not item:
            return None
        return _record_from_item(item)

    def lookup_by_date_partition(self, *, project_name: str, date_partition: str) -> list[dict[str, str]]:
        response = self._table.query(
            KeyConditionExpression=Key(BATCH_INDEX_PK).eq(project_name)
            & Key(BATCH_INDEX_SK).begins_with(f"{date_partition}#"),
        )
        pointers: list[dict[str, str]] = []
        for item in response.get("Items", []):
            pointers.append(
                {
                    "batch_id": str(item["batch_id"]),
                    "batch_lookup_sk": str(item.get("batch_lookup_sk", item["batch_id"])),
                    "date_partition": str(item["date_partition"]),
                    "hour": str(item["hour"]),
                    "within_hour_run_number": str(item["within_hour_run_number"]),
                    "etl_ts": str(item["etl_ts"]),
                }
            )
        pointers.sort(key=lambda x: (x["date_partition"], x["hour"], x["within_hour_run_number"], x["batch_id"]))
        return pointers

    def resolve_mlp_branch_prefixes(
        self,
        *,
        project_name: str,
        batch_id: str,
        ml_project_name: str,
    ) -> dict[str, Any]:
        record = self.get_batch(project_name=project_name, batch_id=batch_id)
        if record is None:
            raise KeyError(f"BatchIndex item not found: project_name={project_name}, batch_id={batch_id}")
        mlp = record.s3_prefixes.get("mlp", {})
        if ml_project_name not in mlp:
            raise KeyError(f"ml_project_name={ml_project_name} missing under s3_prefixes.mlp")
        return mlp[ml_project_name]

    # Backward-compatible method names retained for existing callers.
    def lookup_reverse(self, *, project_name: str, data_source_name: str, version: str, batch_id: str) -> BatchIndexRecord | None:
        del data_source_name, version
        return self.get_batch(project_name=project_name, batch_id=batch_id)

    def lookup_forward(
        self,
        *,
        project_name: str,
        data_source_name: str,
        version: str,
        start_ts_iso: str,
        end_ts_iso: str,
    ) -> list[BatchIndexRecord]:
        del data_source_name, version
        start_ts = _parse_iso8601(start_ts_iso)
        end_ts = _parse_iso8601(end_ts_iso)
        if end_ts <= start_ts:
            raise ValueError("end_ts_iso must be greater than start_ts_iso")

        rows: list[BatchIndexRecord] = []
        for day in _iter_dates(start_ts, end_ts):
            for pointer in self.lookup_by_date_partition(project_name=project_name, date_partition=day):
                record = self.get_batch(project_name=project_name, batch_id=pointer["batch_lookup_sk"])
                if record is None:
                    continue
                event_ts = _parse_iso8601(record.etl_ts)
                if start_ts <= event_ts < end_ts:
                    rows.append(record)
        rows.sort(key=lambda r: (r.etl_ts, r.batch_id))
        return rows


def _iter_dates(start_ts: datetime, end_ts: datetime) -> list[str]:
    cursor = start_ts.date()
    end_date = (end_ts - timedelta(seconds=1)).date()
    out: list[str] = []
    while cursor <= end_date:
        out.append(cursor.strftime("%Y/%m/%d"))
        cursor += timedelta(days=1)
    return out


def _record_from_item(item: dict[str, Any]) -> BatchIndexRecord:
    return BatchIndexRecord(
        project_name=str(item[BATCH_INDEX_PK]),
        batch_id=str(item["batch_id"]),
        date_partition=str(item["date_partition"]),
        hour=str(item["hour"]),
        within_hour_run_number=str(item["within_hour_run_number"]),
        etl_ts=str(item["etl_ts"]),
        org1=str(item["org1"]),
        org2=str(item["org2"]),
        raw_parsed_logs_s3_prefix=str(item["raw_parsed_logs_s3_prefix"]),
        ml_project_names=[str(v) for v in item.get("ml_project_names", [])],
        s3_prefixes=dict(item.get("s3_prefixes", {})),
    )


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
