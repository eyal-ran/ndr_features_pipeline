"""NDR machine inventory unload job module."""

from __future__ import annotations


import time
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ndr.config.job_spec_loader import load_job_spec
from ndr.logging.logger import get_logger
from ndr.processing.base_runner import BaseRunner


LOGGER = get_logger(__name__)


@dataclass
class MachineInventoryUnloadRuntimeConfig:
    """Runtime configuration for the machine inventory unload job."""

    project_name: str
    feature_spec_version: str
    reference_month: str


@dataclass(frozen=True)
class RedshiftDataApiConfig:
    """Data container for RedshiftDataApiConfig."""
    cluster_identifier: str
    database: str
    secret_arn: str
    region: str
    iam_role: str
    db_user: Optional[str] = None


@dataclass(frozen=True)
class MachineInventoryQuerySpec:
    """Data container for MachineInventoryQuerySpec."""
    sql: Optional[str]
    descriptor_id: str
    ip_column: str
    name_column: str
    active_filter: Optional[str]
    additional_filters: List[str]


@dataclass(frozen=True)
class MachineInventoryOutputSpec:
    """Data container for MachineInventoryOutputSpec."""
    s3_prefix: str
    output_format: str
    partitioning: List[str]
    ip_output_column: str = "ip_address"
    name_output_column: str = "machine_name"


class MachineInventoryUnloadJob(BaseRunner):
    """Unload full active machine inventory snapshots from Redshift to S3."""

    def __init__(
        self,
        spark: SparkSession,
        job_spec: Dict[str, Any],
        runtime_config: MachineInventoryUnloadRuntimeConfig,
    ) -> None:
        """Initialize the instance with required clients and runtime configuration."""
        super().__init__()
        self.spark = spark
        self.job_spec = job_spec
        self.runtime_config = runtime_config

    def run(self) -> None:
        """Execute the full workflow for this job runner."""
        redshift_cfg, query_spec, output_spec = parse_machine_inventory_spec(self.job_spec)
        snapshot_month = build_snapshot_month(self.runtime_config.reference_month)
        partition_prefix = build_snapshot_output_prefix(output_spec.s3_prefix, snapshot_month)
        unload_prefix = build_unload_staging_prefix(
            output_spec.s3_prefix,
            snapshot_month,
            datetime.utcnow().strftime("%Y%m%d%H%M%S"),
        )

        data_api = boto3.client("redshift-data", region_name=redshift_cfg.region)

        source_row_count, unloaded_rows = self._unload_from_redshift(
            data_api=data_api,
            redshift_cfg=redshift_cfg,
            query_spec=query_spec,
            output_spec=output_spec,
            output_prefix=unload_prefix,
        )
        if source_row_count != unloaded_rows:
            raise RuntimeError(
                "MACHINE_INVENTORY_SNAPSHOT_PARITY_FAILED: "
                f"source_row_count={source_row_count}, unloaded_rows={unloaded_rows}, "
                f"snapshot_month={snapshot_month}, descriptor_id={query_spec.descriptor_id}"
            )
        self._stage_unload_output_locally_and_write_partition(
            unload_prefix=unload_prefix,
            partition_prefix=partition_prefix,
            output_spec=output_spec,
            snapshot_month=snapshot_month,
        )
        self._delete_prefix(*parse_s3_uri(unload_prefix))
        self.logger.info(
            "Redshift UNLOAD completed for snapshot_month=%s, descriptor_id=%s, source_rows=%s, unloaded_rows=%s.",
            snapshot_month,
            query_spec.descriptor_id,
            source_row_count,
            unloaded_rows,
        )

    def _stage_unload_output_locally_and_write_partition(
        self,
        *,
        unload_prefix: str,
        partition_prefix: str,
        output_spec: MachineInventoryOutputSpec,
        snapshot_month: str,
    ) -> None:
        """Stage Redshift UNLOAD data to local FS then publish canonical monthly partition."""
        with tempfile.TemporaryDirectory(prefix="ndr-machine-unload-") as local_dir:
            downloaded_paths = self._download_unload_objects(
                unload_prefix=unload_prefix,
                local_dir=local_dir,
            )
            if not downloaded_paths:
                self.logger.info("No UNLOAD parquet objects found at %s.", unload_prefix)
                return
            local_df = self.spark.read.parquet(*downloaded_paths)
            write_df = (
                local_df.dropna(subset=[output_spec.ip_output_column])
                .dropDuplicates([output_spec.ip_output_column, output_spec.name_output_column])
                .withColumn("snapshot_month", F.lit(snapshot_month))
                .select(
                    output_spec.ip_output_column,
                    output_spec.name_output_column,
                    "snapshot_month",
                )
            )
            write_df.write.mode("overwrite").parquet(partition_prefix)

    def _download_unload_objects(self, *, unload_prefix: str, local_dir: str) -> List[str]:
        """Download UNLOAD parquet objects to local filesystem for deterministic staging."""
        bucket, key_prefix = parse_s3_uri(unload_prefix)
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        downloaded: List[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                source_key = obj["Key"]
                if not source_key.lower().endswith(".parquet"):
                    continue
                destination = Path(local_dir) / Path(source_key).name
                s3.download_file(bucket, source_key, str(destination))
                downloaded.append(str(destination))
        return downloaded

    def _delete_prefix(self, bucket: str, key_prefix: str) -> None:
        """Execute the delete prefix stage of the workflow."""
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})

    def _unload_from_redshift(
        self,
        data_api: Any,
        redshift_cfg: RedshiftDataApiConfig,
        query_spec: MachineInventoryQuerySpec,
        output_spec: MachineInventoryOutputSpec,
        output_prefix: str,
    ) -> Tuple[int, int]:
        """Execute the unload from redshift stage of the workflow."""
        source_query = build_source_query(query_spec)
        filtered_query = (
            "SELECT DISTINCT src.ip_address, src.machine_name "
            "FROM ({source_query}) AS src"
        ).format(source_query=source_query)
        source_row_count = self._get_query_row_count(
            data_api=data_api,
            config=redshift_cfg,
            query=filtered_query,
        )

        unload_sql = build_unload_sql(
            filtered_query=filtered_query,
            output_prefix=output_prefix,
            output_format=output_spec.output_format,
            iam_role=redshift_cfg.iam_role,
        )
        statement_id = self._execute_statement(data_api, redshift_cfg, unload_sql)
        unloaded_rows = self._get_unload_row_count(data_api, statement_id)
        return source_row_count, unloaded_rows

    def _get_query_row_count(self, data_api: Any, config: RedshiftDataApiConfig, query: str) -> int:
        """Execute the source query row-count stage of the workflow."""
        count_sql = "SELECT COUNT(*) AS row_count FROM ({query}) AS src".format(query=query)
        statement_id = self._execute_statement(data_api, config, count_sql)
        result = data_api.get_statement_result(Id=statement_id)
        records = result.get("Records", [])
        if not records or not records[0]:
            raise RuntimeError(
                "MACHINE_INVENTORY_SOURCE_COUNT_FAILED: Empty row-count result from Redshift Data API."
            )
        scalar = records[0][0]
        if "longValue" in scalar:
            return int(scalar["longValue"])
        if "stringValue" in scalar:
            return int(scalar["stringValue"])
        raise RuntimeError(
            "MACHINE_INVENTORY_SOURCE_COUNT_FAILED: Unsupported COUNT(*) scalar format."
        )

    def _execute_statement(self, data_api: Any, config: RedshiftDataApiConfig, sql: str) -> str:
        """Execute the execute statement stage of the workflow."""
        params: Dict[str, Any] = {
            "ClusterIdentifier": config.cluster_identifier,
            "Database": config.database,
            "SecretArn": config.secret_arn,
            "Sql": sql,
        }
        if config.db_user:
            params["DbUser"] = config.db_user
        response = data_api.execute_statement(**params)
        statement_id = response["Id"]
        self._wait_for_statement(data_api, statement_id)
        return statement_id

    def _wait_for_statement(self, data_api: Any, statement_id: str) -> None:
        """Execute the wait for statement stage of the workflow."""
        while True:
            description = data_api.describe_statement(Id=statement_id)
            status = description["Status"]
            if status in {"FAILED", "ABORTED"}:
                raise RuntimeError(
                    f"Redshift Data API statement {statement_id} failed: "
                    f"{description.get('Error')}"
                )
            if status == "FINISHED":
                return
            time.sleep(1.0)

    def _get_unload_row_count(self, data_api: Any, statement_id: str) -> int:
        """Execute the get unload row count stage of the workflow."""
        description = data_api.describe_statement(Id=statement_id)
        return int(description.get("ResultRows", 0))

def parse_machine_inventory_spec(
    job_spec: Dict[str, Any]
) -> Tuple[RedshiftDataApiConfig, MachineInventoryQuerySpec, MachineInventoryOutputSpec]:
    """Execute the parse machine inventory spec stage of the workflow."""
    if not job_spec:
        raise ValueError("Job spec payload is empty.")

    redshift_payload = job_spec.get("redshift", {})
    required_redshift = ["cluster_identifier", "database", "secret_arn", "region", "iam_role"]
    missing = [k for k in required_redshift if not redshift_payload.get(k)]
    if missing:
        raise ValueError(f"Missing redshift config keys: {missing}")
    redshift_cfg = RedshiftDataApiConfig(
        cluster_identifier=redshift_payload["cluster_identifier"],
        database=redshift_payload["database"],
        secret_arn=redshift_payload["secret_arn"],
        region=redshift_payload["region"],
        iam_role=redshift_payload["iam_role"],
        db_user=redshift_payload.get("db_user"),
    )

    query_payload = job_spec.get("query", {})
    query_spec = MachineInventoryQuerySpec(
        sql=query_payload.get("sql"),
        descriptor_id=query_payload.get("descriptor_id", "machine_inventory_monthly"),
        ip_column=query_payload.get("ip_column", "ip_address"),
        name_column=query_payload.get("name_column", "machine_name"),
        active_filter=query_payload.get("active_filter"),
        additional_filters=query_payload.get("additional_filters", []),
    )

    if not query_spec.sql:
        raise ValueError("query.sql is required for UNLOAD-only machine inventory flow.")

    output_payload = job_spec.get("output", {})
    output_spec = MachineInventoryOutputSpec(
        s3_prefix=output_payload.get("s3_prefix", ""),
        output_format=output_payload.get("output_format", "PARQUET"),
        partitioning=output_payload.get("partitioning", ["snapshot_month"]),
        ip_output_column=output_payload.get("ip_output_column", "ip_address"),
        name_output_column=output_payload.get("name_output_column", "machine_name"),
    )
    if not output_spec.s3_prefix:
        raise ValueError("output.s3_prefix is required for machine inventory unload.")
    if output_spec.output_format.upper() != "PARQUET":
        raise ValueError("Only PARQUET output_format is supported.")
    if "snapshot_month" not in output_spec.partitioning:
        raise ValueError("output.partitioning must include 'snapshot_month'.")

    return redshift_cfg, query_spec, output_spec


def build_snapshot_month(reference_month: str) -> str:
    """Execute the build snapshot month stage of the workflow."""
    try:
        normalized = datetime.strptime(reference_month, "%Y/%m")
    except ValueError as exc:
        raise ValueError("reference_month must use YYYY/MM format.") from exc
    return normalized.strftime("%Y-%m")


def build_snapshot_output_prefix(base_prefix: str, snapshot_month: str) -> str:
    """Execute the build snapshot output prefix stage of the workflow."""
    if not base_prefix.endswith("/"):
        base_prefix = f"{base_prefix}/"
    return f"{base_prefix}snapshot_month={snapshot_month}/"


def build_unload_staging_prefix(base_prefix: str, snapshot_month: str, run_id: str) -> str:
    """Execute the build temp output prefix stage of the workflow."""
    partition_prefix = build_snapshot_output_prefix(base_prefix, snapshot_month)
    if not partition_prefix.endswith("/"):
        partition_prefix = f"{partition_prefix}/"
    return f"{partition_prefix}_tmp_run_id={run_id}/"


def build_source_query(query_spec: MachineInventoryQuerySpec) -> str:
    """Execute the build source query stage of the workflow."""
    base_query = query_spec.sql.strip().rstrip(";")
    return f"SELECT {query_spec.ip_column} AS ip_address, {query_spec.name_column} AS machine_name FROM ({base_query}) AS base"


def build_unload_sql(
    filtered_query: str,
    output_prefix: str,
    output_format: str,
    iam_role: str,
) -> str:
    """Execute the build unload sql stage of the workflow."""
    credentials = f"IAM_ROLE '{iam_role}'" if iam_role else ""
    format_clause = output_format.upper()
    return (
        "UNLOAD ('{query}') TO '{prefix}' {credentials} "
        "FORMAT AS {format_clause};"
    ).format(
        query=filtered_query.replace("'", "''"),
        prefix=output_prefix,
        credentials=credentials,
        format_clause=format_clause,
    )


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Execute the parse s3 uri stage of the workflow."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    path = uri.replace("s3://", "", 1)
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def run_machine_inventory_unload_from_runtime_config(
    runtime_config: MachineInventoryUnloadRuntimeConfig,
) -> None:
    """Execute the run machine inventory unload from runtime config stage of the workflow."""
    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="machine_inventory_unload",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    spark = (
        SparkSession.builder.appName("machine_inventory_unload")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        job = MachineInventoryUnloadJob(
            spark=spark,
            job_spec=job_spec,
            runtime_config=runtime_config,
        )
        job.run()
    finally:
        spark.stop()
