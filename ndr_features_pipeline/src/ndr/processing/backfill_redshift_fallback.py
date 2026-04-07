"""Deterministic Redshift UNLOAD fallback executor for backfill ranges (Task 7.3)."""

from __future__ import annotations

from dataclasses import dataclass
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import boto3


@dataclass(frozen=True)
class BackfillRange:
    """Canonical backfill time-range contract."""

    start_ts: str
    end_ts: str


@dataclass(frozen=True)
class BackfillFallbackRedshiftConfig:
    """Data API + unload configuration resolved from DPP-owned contracts."""

    cluster_identifier: str
    database: str
    secret_arn: str
    region: str
    iam_role: str
    unload_s3_prefix: str
    db_user: str | None = None


@dataclass(frozen=True)
class BackfillFlowQuerySpec:
    """Flow-specific query descriptor for Redshift fallback execution."""

    descriptor_id: str
    sql_template: str


@dataclass(frozen=True)
class BackfillRangeExecutionResult:
    """Deterministic execution metadata for one range."""

    range_start_ts: str
    range_end_ts: str
    unload_s3_prefix: str
    local_stage_dir: str
    local_files: tuple[str, ...]
    statement_id: str
    attempt: int


def resolve_backfill_source_mode(*, ingestion_rows: list[dict[str, Any]], allow_redshift_fallback: bool) -> str:
    """Resolve source mode deterministically when ingestion rows are missing."""
    if ingestion_rows:
        return "ingestion"
    if allow_redshift_fallback:
        return "redshift_unload_fallback"
    raise RuntimeError("No ingestion rows resolved and Redshift fallback is disabled")


def load_backfill_fallback_contract(*, dpp_spec: dict[str, Any], artifact_family: str) -> tuple[BackfillFallbackRedshiftConfig, BackfillFlowQuerySpec]:
    """Load flow-specific fallback config/query from DPP-owned spec payload.

    Generic-query fallbacks are intentionally rejected by contract.
    """
    fallback = dpp_spec.get("backfill_redshift_fallback") or {}
    redshift = fallback.get("redshift") or {}
    queries = fallback.get("queries") or {}

    required_redshift_keys = ("cluster_identifier", "database", "secret_arn", "region", "iam_role", "unload_s3_prefix")
    missing = [k for k in required_redshift_keys if not str(redshift.get(k, "")).strip()]
    if missing:
        raise ValueError(f"Missing backfill_redshift_fallback.redshift keys: {missing}")

    if artifact_family not in queries:
        raise ValueError(
            "Missing flow-specific fallback query for artifact family "
            f"'{artifact_family}'. Generic query fallback is not allowed by contract."
        )

    family_query = queries[artifact_family] or {}
    sql_template = str(family_query.get("sql_template", "")).strip()
    descriptor_id = str(family_query.get("descriptor_id", artifact_family)).strip()
    if not sql_template:
        raise ValueError(f"backfill_redshift_fallback.queries.{artifact_family}.sql_template is required")

    return (
        BackfillFallbackRedshiftConfig(
            cluster_identifier=str(redshift["cluster_identifier"]),
            database=str(redshift["database"]),
            secret_arn=str(redshift["secret_arn"]),
            region=str(redshift["region"]),
            iam_role=str(redshift["iam_role"]),
            unload_s3_prefix=str(redshift["unload_s3_prefix"]),
            db_user=str(redshift.get("db_user")) if redshift.get("db_user") is not None else None,
        ),
        BackfillFlowQuerySpec(descriptor_id=descriptor_id, sql_template=sql_template),
    )


def execute_backfill_redshift_fallback(
    *,
    config: BackfillFallbackRedshiftConfig,
    query_spec: BackfillFlowQuerySpec,
    ranges: list[BackfillRange],
    max_attempts: int = 3,
    retry_backoff_seconds: float = 0.0,
    local_staging_root: str | None = None,
) -> list[BackfillRangeExecutionResult]:
    """Execute deterministic multi-range UNLOAD fallback with per-range retries and local staging."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    ordered_ranges = sorted(ranges, key=lambda r: (r.start_ts, r.end_ts))
    data_api = boto3.client("redshift-data", region_name=config.region)
    s3_client = boto3.client("s3")

    if local_staging_root:
        os.makedirs(local_staging_root, exist_ok=True)
        staging_root = local_staging_root
    else:
        staging_root = tempfile.mkdtemp(prefix="ndr-backfill-redshift-")

    results: list[BackfillRangeExecutionResult] = []
    for idx, time_range in enumerate(ordered_ranges):
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            unload_prefix = (
                f"{config.unload_s3_prefix.rstrip('/')}/"
                f"descriptor={query_spec.descriptor_id}/"
                f"range_{idx:04d}/attempt_{attempt:02d}/"
            )
            try:
                range_query = render_flow_query(
                    query_spec.sql_template,
                    start_ts=time_range.start_ts,
                    end_ts=time_range.end_ts,
                )
                statement_id = _execute_unload_statement(
                    data_api=data_api,
                    config=config,
                    unload_sql=build_unload_sql(
                        select_sql=range_query,
                        unload_s3_prefix=unload_prefix,
                        iam_role=config.iam_role,
                    ),
                )
                local_dir = Path(staging_root) / f"range_{idx:04d}"
                local_files = stage_unload_parquet_to_local(
                    s3_client=s3_client,
                    unload_s3_prefix=unload_prefix,
                    local_dir=str(local_dir),
                )
                results.append(
                    BackfillRangeExecutionResult(
                        range_start_ts=time_range.start_ts,
                        range_end_ts=time_range.end_ts,
                        unload_s3_prefix=unload_prefix,
                        local_stage_dir=str(local_dir),
                        local_files=tuple(local_files),
                        statement_id=statement_id,
                        attempt=attempt,
                    )
                )
                break
            except Exception as exc:
                last_error = exc
                if attempt < max_attempts and retry_backoff_seconds > 0:
                    time.sleep(retry_backoff_seconds)
        else:
            raise RuntimeError(
                "Redshift fallback UNLOAD failed after deterministic retries for "
                f"range_start={time_range.start_ts}, range_end={time_range.end_ts}"
            ) from last_error

    return results


def render_flow_query(sql_template: str, *, start_ts: str, end_ts: str) -> str:
    """Render flow query using explicit timestamp placeholders."""
    return sql_template.replace("{{start_ts}}", start_ts).replace("{{end_ts}}", end_ts)


def build_unload_sql(*, select_sql: str, unload_s3_prefix: str, iam_role: str) -> str:
    escaped = select_sql.replace("'", "''")
    return (
        "UNLOAD ('{query}') "
        "TO '{prefix}' "
        "IAM_ROLE '{iam_role}' "
        "FORMAT AS PARQUET ALLOWOVERWRITE PARALLEL OFF"
    ).format(query=escaped, prefix=unload_s3_prefix, iam_role=iam_role)


def stage_unload_parquet_to_local(*, s3_client: Any, unload_s3_prefix: str, local_dir: str) -> list[str]:
    """Download UNLOAD parquet output to local FS deterministically."""
    bucket, key_prefix = _split_s3_uri(unload_s3_prefix)
    os.makedirs(local_dir, exist_ok=True)

    paginator = s3_client.get_paginator("list_objects_v2")
    files: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            if not key.lower().endswith(".parquet"):
                continue
            destination = Path(local_dir) / Path(key).name
            s3_client.download_file(bucket, key, str(destination))
            files.append(str(destination))
    files.sort()
    return files


def _execute_unload_statement(*, data_api: Any, config: BackfillFallbackRedshiftConfig, unload_sql: str) -> str:
    params: dict[str, Any] = {
        "ClusterIdentifier": config.cluster_identifier,
        "Database": config.database,
        "SecretArn": config.secret_arn,
        "Sql": unload_sql,
    }
    if config.db_user:
        params["DbUser"] = config.db_user
    response = data_api.execute_statement(**params)
    statement_id = response["Id"]

    while True:
        detail = data_api.describe_statement(Id=statement_id)
        status = detail.get("Status")
        if status in {"FAILED", "ABORTED"}:
            raise RuntimeError(f"Redshift Data API statement {statement_id} failed: {detail.get('Error')}")
        if status == "FINISHED":
            return statement_id
        time.sleep(1.0)


def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    rest = uri[5:]
    if "/" not in rest:
        return rest, ""
    bucket, key = rest.split("/", 1)
    return bucket, key
