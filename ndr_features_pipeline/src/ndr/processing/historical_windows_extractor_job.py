"""Historical mini-batch window extractor for backfill orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from urllib.parse import urlparse

import boto3

from ndr.config.project_parameters_loader import resolve_feature_spec_version
from ndr.orchestration.palo_alto_batch_utils import (
    parse_batch_path_from_s3_key,
    derive_window_bounds,
    to_iso_z,
)


@dataclass
class HistoricalWindowsExtractorRuntimeConfig:
    input_s3_prefix: str
    output_s3_prefix: str
    start_ts_iso: str
    end_ts_iso: str
    window_floor_minutes: list[int]
    preferred_feature_spec_version: str | None = None


class HistoricalWindowsExtractorJob:
    """Enumerates historical mini-batches and emits backfill execution units."""

    def __init__(self, runtime_config: HistoricalWindowsExtractorRuntimeConfig) -> None:
        self.runtime_config = runtime_config
        self._s3 = boto3.client("s3")

    def run(self) -> str:
        rows = self._extract_rows()
        return self._write_rows(rows)

    def _extract_rows(self) -> list[dict[str, str]]:
        in_bucket, in_prefix = _split_s3_uri(self.runtime_config.input_s3_prefix)
        start_ts = _parse_iso8601(self.runtime_config.start_ts_iso)
        end_ts = _parse_iso8601(self.runtime_config.end_ts_iso)

        grouped: dict[tuple[str, str], dict[str, object]] = {}
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=in_bucket, Prefix=in_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"].astimezone(timezone.utc)
                if not (start_ts <= last_modified < end_ts):
                    continue

                suffix_key = key[len(in_prefix):].lstrip("/") if key.startswith(in_prefix) else key
                parsed = parse_batch_path_from_s3_key(suffix_key)
                mini_batch_prefix = (
                    f"s3://{in_bucket}/{in_prefix.rstrip('/')}/"
                    f"{parsed.org1}/{parsed.org2}/{parsed.project_name}/"
                    f"{parsed.year}/{parsed.month}/{parsed.day}/{parsed.mini_batch_id}/"
                )
                group_key = (parsed.project_name, parsed.mini_batch_id)
                existing = grouped.get(group_key)
                if existing is None or last_modified > existing["last_modified"]:
                    grouped[group_key] = {
                        "project_name": parsed.project_name,
                        "mini_batch_id": parsed.mini_batch_id,
                        "mini_batch_s3_prefix": mini_batch_prefix,
                        "last_modified": last_modified,
                    }

        rows: list[dict[str, str]] = []
        for item in grouped.values():
            project_name = str(item["project_name"])
            source_ts = item["last_modified"]
            batch_start_ts_iso, batch_end_ts_iso = derive_window_bounds(source_ts, self.runtime_config.window_floor_minutes)
            feature_spec_version = resolve_feature_spec_version(
                project_name=project_name,
                preferred_feature_spec_version=self.runtime_config.preferred_feature_spec_version,
            )

            rows.append(
                {
                    "project_name": project_name,
                    "feature_spec_version": feature_spec_version,
                    "mini_batch_id": str(item["mini_batch_id"]),
                    "mini_batch_s3_prefix": str(item["mini_batch_s3_prefix"]),
                    "batch_start_ts_iso": batch_start_ts_iso,
                    "batch_end_ts_iso": batch_end_ts_iso,
                    "source_last_modified_ts_iso": to_iso_z(source_ts),
                }
            )

        rows.sort(key=lambda r: (r["project_name"], r["batch_start_ts_iso"], r["mini_batch_id"]))
        return rows

    def _write_rows(self, rows: list[dict[str, str]]) -> str:
        out_bucket, out_prefix = _split_s3_uri(self.runtime_config.output_s3_prefix)
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{out_prefix.rstrip('/')}/historical_windows/{now}.jsonl"
        body = "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else "")
        self._s3.put_object(Bucket=out_bucket, Key=key, Body=body.encode("utf-8"))
        return f"s3://{out_bucket}/{key}"


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
