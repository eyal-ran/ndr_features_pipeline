"""Historical mini-batch window extractor for backfill orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from urllib.parse import urlparse

import boto3

from ndr.config.batch_index_loader import BatchIndexLoader
from ndr.config.project_parameters_loader import load_project_parameters, resolve_feature_spec_version
from ndr.orchestration.backfill_contracts import build_execution_manifest, build_family_range_plan
from ndr.orchestration.palo_alto_batch_utils import (
    parse_batch_path_from_s3_key,
    derive_window_bounds,
    to_iso_z,
)
from ndr.processing.raw_input_resolver import RawInputResolver


@dataclass
class HistoricalWindowsExtractorRuntimeConfig:
    input_s3_prefix: str
    output_s3_prefix: str
    start_ts_iso: str
    end_ts_iso: str
    window_floor_minutes: list[int]
    project_name: str | None = None
    preferred_feature_spec_version: str | None = None
    dpp_config_table_name: str | None = None
    requested_families: list[str] | None = None


class HistoricalWindowsExtractorJob:
    """Enumerates historical mini-batches and emits backfill execution units."""

    def __init__(self, runtime_config: HistoricalWindowsExtractorRuntimeConfig) -> None:
        self.runtime_config = runtime_config
        self._s3 = boto3.client("s3")
        self._batch_index_loader: BatchIndexLoader | None = None

    def run(self) -> str:
        rows = self._extract_rows()
        return self._write_rows(rows)

    def _extract_rows(self) -> list[dict[str, str]]:
        index_lookup_error: Exception | None = None
        s3_lookup_error: Exception | None = None
        try:
            index_rows = self._extract_rows_from_batch_index()
        except Exception as exc:  # index path is best-effort before fallback resolution
            index_rows = []
            index_lookup_error = exc

        s3_rows: list[dict[str, str]] = []
        if not index_rows:
            try:
                s3_rows = self._extract_rows_from_s3_listing()
            except Exception as exc:  # preserve deterministic failure if both resolvers fail/empty
                s3_lookup_error = exc

        project_name = self._resolve_project_name(index_rows=index_rows, s3_rows=s3_rows)
        if index_rows and project_name:
            feature_spec_version = index_rows[0]["feature_spec_version"]
            dpp_spec = load_project_parameters(
                project_name,
                feature_spec_version,
                dpp_table_name=self.runtime_config.dpp_config_table_name,
            )
            fallback = dpp_spec.get("backfill_redshift_fallback") or {}
            allow_redshift_fallback = bool(fallback) and bool(fallback.get("enabled", True))
            resolution = RawInputResolver().resolve(
                ingestion_rows=index_rows,
                allow_redshift_fallback=allow_redshift_fallback,
                dpp_spec=dpp_spec,
                artifact_family="delta",
                range_start_ts=self.runtime_config.start_ts_iso,
                range_end_ts=self.runtime_config.end_ts_iso,
                producer_flow="historical_windows_extractor",
            )
            if resolution.source_mode == "ingestion":
                for row in index_rows:
                    row["source_mode"] = resolution.source_mode
                    row["resolution_reason"] = resolution.resolution_reason
                return index_rows

        if s3_rows:
            if project_name:
                self._assert_rows_match_project(s3_rows, project_name)
            for row in s3_rows:
                row.setdefault("source_mode", "ingestion")
                row.setdefault("resolution_reason", "s3_listing_fallback")
            return s3_rows

        if project_name:
            feature_spec_version = resolve_feature_spec_version(
                project_name=project_name,
                preferred_feature_spec_version=self.runtime_config.preferred_feature_spec_version,
            )
            dpp_spec = load_project_parameters(
                project_name,
                feature_spec_version,
                dpp_table_name=self.runtime_config.dpp_config_table_name,
            )
            fallback = dpp_spec.get("backfill_redshift_fallback") or {}
            allow_redshift_fallback = bool(fallback) and bool(fallback.get("enabled", True))
            resolution = RawInputResolver().resolve(
                ingestion_rows=[],
                allow_redshift_fallback=allow_redshift_fallback,
                dpp_spec=dpp_spec,
                artifact_family="delta",
                range_start_ts=self.runtime_config.start_ts_iso,
                range_end_ts=self.runtime_config.end_ts_iso,
                producer_flow="historical_windows_extractor",
            )
            return [
                {
                    "project_name": project_name,
                    "feature_spec_version": feature_spec_version,
                    "mini_batch_id": _fallback_batch_id(
                        self.runtime_config.start_ts_iso,
                        self.runtime_config.end_ts_iso,
                    ),
                    "raw_parsed_logs_s3_prefix": resolution.raw_input_s3_prefix,
                    "batch_start_ts_iso": self.runtime_config.start_ts_iso,
                    "batch_end_ts_iso": self.runtime_config.end_ts_iso,
                    "source_last_modified_ts_iso": _now_iso(),
                    "source_mode": resolution.source_mode,
                    "resolution_reason": resolution.resolution_reason,
                }
            ]

        raise RuntimeError(
            "HWE_NO_ROWS_RESOLVED: No batch-index rows, no S3 rows, and no resolvable project_name for Redshift fallback"
        ) from (s3_lookup_error or index_lookup_error)


    def _extract_rows_from_batch_index(self) -> list[dict[str, str]]:
        start_ts = _parse_iso8601(self.runtime_config.start_ts_iso)
        end_ts = _parse_iso8601(self.runtime_config.end_ts_iso)
        project_name = self._resolve_runtime_project_name()
        if not project_name:
            return []
        feature_spec_version = resolve_feature_spec_version(
            project_name=project_name,
            preferred_feature_spec_version=self.runtime_config.preferred_feature_spec_version,
        )
        if self._batch_index_loader is None:
            self._batch_index_loader = BatchIndexLoader()
        records = self._batch_index_loader.lookup_forward(
            project_name=project_name,
            data_source_name=project_name,
            version=feature_spec_version,
            start_ts_iso=to_iso_z(start_ts),
            end_ts_iso=to_iso_z(end_ts),
        )
        rows: list[dict[str, str]] = []
        for record in records:
            source_ts = _parse_iso8601(record.event_ts_utc)
            batch_start_ts_iso, batch_end_ts_iso = derive_window_bounds(source_ts, self.runtime_config.window_floor_minutes)
            rows.append(
                {
                    "project_name": project_name,
                    "feature_spec_version": feature_spec_version,
                    "mini_batch_id": record.batch_id,
                    "raw_parsed_logs_s3_prefix": record.raw_parsed_logs_s3_prefix,
                    "batch_start_ts_iso": batch_start_ts_iso,
                    "batch_end_ts_iso": batch_end_ts_iso,
                    "source_last_modified_ts_iso": to_iso_z(source_ts),
                }
            )
        rows.sort(key=lambda r: (r["project_name"], r["batch_start_ts_iso"], r["mini_batch_id"]))
        return rows

    def _extract_rows_from_s3_listing(self) -> list[dict[str, str]]:
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
                    f"{parsed.project_name}/{parsed.org1}/{parsed.org2}/"
                    f"{parsed.year}/{parsed.month}/{parsed.day}/{parsed.mini_batch_id}/"
                )
                group_key = (parsed.project_name, parsed.mini_batch_id)
                existing = grouped.get(group_key)
                if existing is None or last_modified > existing["last_modified"]:
                    grouped[group_key] = {
                        "project_name": parsed.project_name,
                        "mini_batch_id": parsed.mini_batch_id,
                        "raw_parsed_logs_s3_prefix": mini_batch_prefix,
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
                    "raw_parsed_logs_s3_prefix": str(item["raw_parsed_logs_s3_prefix"]),
                    "batch_start_ts_iso": batch_start_ts_iso,
                    "batch_end_ts_iso": batch_end_ts_iso,
                    "source_last_modified_ts_iso": to_iso_z(source_ts),
                }
            )

        rows.sort(key=lambda r: (r["project_name"], r["batch_start_ts_iso"], r["mini_batch_id"]))
        return rows

    def _infer_project_name_from_input_prefix(self) -> str | None:
        _bucket, input_prefix = _split_s3_uri(self.runtime_config.input_s3_prefix)
        parts = [p for p in input_prefix.strip('/').split('/') if p]
        if len(parts) < 2:
            return None
        return parts[-1]

    def _resolve_runtime_project_name(self) -> str | None:
        explicit = (self.runtime_config.project_name or "").strip()
        if explicit:
            return explicit
        inferred = (self._infer_project_name_from_input_prefix() or "").strip()
        return inferred or None

    def _resolve_project_name(self, *, index_rows: list[dict[str, str]], s3_rows: list[dict[str, str]]) -> str | None:
        explicit = (self.runtime_config.project_name or "").strip()
        if explicit:
            self._assert_rows_match_project(index_rows, explicit)
            self._assert_rows_match_project(s3_rows, explicit)
            return explicit
        index_project_names = {row["project_name"] for row in index_rows if row.get("project_name")}
        if len(index_project_names) == 1:
            return next(iter(index_project_names))
        s3_project_names = {row["project_name"] for row in s3_rows if row.get("project_name")}
        if len(s3_project_names) == 1:
            return next(iter(s3_project_names))
        return None

    @staticmethod
    def _assert_rows_match_project(rows: list[dict[str, str]], expected_project_name: str) -> None:
        mismatched = sorted({row.get("project_name", "") for row in rows if row.get("project_name") != expected_project_name})
        if mismatched:
            raise RuntimeError(
                "HWE_PROJECT_MISMATCH: runtime project_name does not match resolved rows "
                f"(project_name={expected_project_name}, row_projects={mismatched})"
            )

    def _write_rows(self, rows: list[dict[str, str]]) -> str:
        out_bucket, out_prefix = _split_s3_uri(self.runtime_config.output_s3_prefix)
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base_prefix = out_prefix.rstrip('/')
        key = f"{base_prefix}/historical_windows/{now}.json"
        latest_key = f"{base_prefix}/historical_windows/latest_manifest.json"
        project_name = rows[0]["project_name"] if rows else (self._resolve_runtime_project_name() or "")
        feature_spec_version = rows[0]["feature_spec_version"] if rows else (
            resolve_feature_spec_version(
                project_name=project_name,
                preferred_feature_spec_version=self.runtime_config.preferred_feature_spec_version,
            ) if project_name else (self.runtime_config.preferred_feature_spec_version or "")
        )
        family_ranges = {
            "delta": [{"start_ts": row["batch_start_ts_iso"], "end_ts": row["batch_end_ts_iso"]} for row in rows],
            "fg_a": [{"start_ts": row["batch_start_ts_iso"], "end_ts": row["batch_end_ts_iso"]} for row in rows],
            "pair_counts": [{"start_ts": row["batch_start_ts_iso"], "end_ts": row["batch_end_ts_iso"]} for row in rows],
            "fg_b_baseline": _derive_fg_b_reference_ranges(rows),
            "fg_c": [{"start_ts": row["batch_start_ts_iso"], "end_ts": row["batch_end_ts_iso"]} for row in rows],
        }
        source_mode = rows[0].get("source_mode", "ingestion") if rows else "ingestion"
        resolution_reason = rows[0].get("resolution_reason", "batch_index") if rows else "empty"
        requested_families = self.runtime_config.requested_families or []
        manifest = build_execution_manifest(
            project_name=project_name,
            feature_spec_version=feature_spec_version,
            planner_mode="caller_guided" if requested_families else "self_detect",
            source="historical_windows_extractor",
            family_plan=build_family_range_plan(
                family_ranges=family_ranges,
                requested_families=requested_families or None,
            ),
            requested_families=requested_families,
        )
        manifest["rows"] = rows
        manifest["source_mode"] = source_mode
        manifest["resolution_reason"] = resolution_reason
        body = json.dumps(manifest, sort_keys=True) + "\n"
        encoded_body = body.encode("utf-8")
        self._s3.put_object(Bucket=out_bucket, Key=key, Body=encoded_body)
        self._s3.put_object(Bucket=out_bucket, Key=latest_key, Body=encoded_body)
        return f"s3://{out_bucket}/{key}"


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _fallback_batch_id(start_ts_iso: str, end_ts_iso: str) -> str:
    start = start_ts_iso.replace("-", "").replace(":", "")
    end = end_ts_iso.replace("-", "").replace(":", "")
    return f"fallback-{start}-{end}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _derive_fg_b_reference_ranges(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Derive deterministic FG-B daily reference windows from extracted batch rows.

    Policy:
    - one FG-B range per distinct UTC reference day,
    - reference day inferred from batch_end_ts_iso,
    - range spans [day_start, day_end] in UTC.
    """
    day_starts: set[datetime] = set()
    for row in rows:
        batch_end = _parse_iso8601(row["batch_end_ts_iso"])
        day_start = batch_end.replace(hour=0, minute=0, second=0, microsecond=0)
        day_starts.add(day_start)

    ranges: list[dict[str, str]] = []
    for day_start in sorted(day_starts):
        day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=0)
        ranges.append(
            {
                "start_ts": to_iso_z(day_start),
                "end_ts": to_iso_z(day_end),
            }
        )
    return ranges
