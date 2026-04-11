"""RT pre-delta raw-input resolver job.

Runs as a SageMaker Processing step and enforces deterministic
ingestion/fallback resolution before delta processing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

import boto3

from ndr.config.project_parameters_loader import load_project_parameters, resolve_batch_index_table_name
from ndr.processing.raw_input_resolver import RawInputResolver


@dataclass(frozen=True)
class RtRawInputResolverRuntimeConfig:
    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str
    raw_parsed_logs_s3_prefix: str = ""
    batch_index_table_name: str | None = None
    dpp_config_table_name: str | None = None
    request_id: str | None = None


def run_rt_raw_input_resolver(runtime_config: RtRawInputResolverRuntimeConfig) -> dict[str, Any]:
    """Resolve canonical raw input and persist provenance/status to Batch Index."""
    ingestion_rows: list[dict[str, str]] = []
    if runtime_config.raw_parsed_logs_s3_prefix.strip():
        ingestion_rows.append({"raw_parsed_logs_s3_prefix": runtime_config.raw_parsed_logs_s3_prefix.strip()})

    project_parameters = load_project_parameters(
        runtime_config.project_name,
        runtime_config.feature_spec_version,
        dpp_table_name=runtime_config.dpp_config_table_name,
    )
    fallback = project_parameters.get("backfill_redshift_fallback") or {}
    allow_redshift_fallback = bool(fallback.get("enabled", False))

    resolution = RawInputResolver().resolve(
        ingestion_rows=ingestion_rows,
        allow_redshift_fallback=allow_redshift_fallback,
        dpp_spec=project_parameters,
        artifact_family="delta",
        range_start_ts=runtime_config.batch_start_ts_iso,
        range_end_ts=runtime_config.batch_end_ts_iso,
        producer_flow="rt_pre_delta_raw_input_resolver",
        request_id=runtime_config.request_id or runtime_config.mini_batch_id,
    )

    _write_batch_index_resolution(
        table_name=resolve_batch_index_table_name(runtime_config.batch_index_table_name),
        project_name=runtime_config.project_name,
        mini_batch_id=runtime_config.mini_batch_id,
        resolved_prefix=resolution.raw_input_s3_prefix,
        provenance=resolution.provenance,
    )

    return {
        "contract_version": "rt.raw_input_resolution.v1",
        "project_name": runtime_config.project_name,
        "feature_spec_version": runtime_config.feature_spec_version,
        "mini_batch_id": runtime_config.mini_batch_id,
        "batch_start_ts_iso": runtime_config.batch_start_ts_iso,
        "batch_end_ts_iso": runtime_config.batch_end_ts_iso,
        "raw_input_s3_prefix": resolution.raw_input_s3_prefix,
        "source_mode": resolution.source_mode,
        "resolution_reason": resolution.resolution_reason,
        "provenance": resolution.provenance,
    }


def _write_batch_index_resolution(
    *,
    table_name: str,
    project_name: str,
    mini_batch_id: str,
    resolved_prefix: str,
    provenance: dict[str, str],
) -> None:
    source_mode = str(provenance.get("source_mode", "")).strip()
    resolution_reason = str(provenance.get("resolution_reason", "")).strip()
    if not source_mode or not resolution_reason:
        raise RuntimeError(
            "RT_RAW_INPUT_RESOLUTION_CONTRACT_VIOLATION: source_mode and resolution_reason are required"
        )

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    table = boto3.resource("dynamodb").Table(table_name)
    table.update_item(
        Key={"PK": project_name, "SK": mini_batch_id},
        UpdateExpression=(
            "SET raw_parsed_logs_s3_prefix = :raw_prefix, "
            "source_mode = :source_mode, "
            "raw_input_resolution_reason = :resolution_reason, "
            "raw_input_resolution_provenance = :provenance, "
            "rt_flow_status = :rt_flow_status, "
            "last_updated_at = :updated_at"
        ),
        ExpressionAttributeValues={
            ":raw_prefix": resolved_prefix,
            ":source_mode": source_mode,
            ":resolution_reason": resolution_reason,
            ":provenance": json.loads(json.dumps(provenance)),
            ":rt_flow_status": "RAW_INPUT_RESOLVED",
            ":updated_at": now_iso,
        },
        ConditionExpression="attribute_exists(PK) AND attribute_exists(SK)",
    )
