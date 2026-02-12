"""NDR machine inventory unload job module."""

from __future__ import annotations


import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from pyspark.sql import DataFrame, SparkSession
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
    reference_month_iso: str


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
    schema: Optional[str]
    table: Optional[str]
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
    """Unload active machine inventory from Redshift to S3, avoiding duplicates."""

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
        snapshot_month = build_snapshot_month(self.runtime_config.reference_month_iso)
        partition_prefix = build_snapshot_output_prefix(output_spec.s3_prefix, snapshot_month)
        output_prefix = self._select_unload_prefix(
            output_spec.s3_prefix,
            partition_prefix,
            snapshot_month,
        )

        existing_ips = self._load_existing_ips(
            output_spec.s3_prefix,
            output_spec.ip_output_column,
        )
        self.logger.info(
            "Loaded %s existing IPs from %s.",
            len(existing_ips),
            output_spec.s3_prefix,
        )

        data_api = boto3.client("redshift-data", region_name=redshift_cfg.region)

        try:
            unloaded_rows = self._unload_from_redshift(
                data_api=data_api,
                redshift_cfg=redshift_cfg,
                query_spec=query_spec,
                existing_ips=existing_ips,
                output_spec=output_spec,
                output_prefix=output_prefix,
            )
            if output_prefix != partition_prefix:
                self._copy_unload_objects(
                    source_prefix=output_prefix,
                    target_prefix=partition_prefix,
                )
            self.logger.info(
                "Redshift UNLOAD completed for snapshot_month=%s, rows=%s.",
                snapshot_month,
                unloaded_rows,
            )
        except Exception as exc:
            self.logger.exception(
                "Redshift UNLOAD failed; falling back to Data API query + Spark write.",
                extra={"error": str(exc)},
            )
            self._fallback_query_and_write(
                data_api=data_api,
                redshift_cfg=redshift_cfg,
                query_spec=query_spec,
                output_spec=output_spec,
                output_prefix=output_prefix,
                existing_ips=existing_ips,
                snapshot_month=snapshot_month,
            )

    def _load_existing_ips(self, s3_prefix: str, ip_column: str) -> List[str]:
        """Execute the load existing ips stage of the workflow."""
        if not s3_prefix:
            return []
        if not s3_prefix.startswith("s3://"):
            raise ValueError(f"Invalid S3 prefix: {s3_prefix}")

        bucket, key_prefix = parse_s3_uri(s3_prefix)
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
        if response.get("KeyCount", 0) == 0:
            return []

        df = self.spark.read.parquet(s3_prefix)
        if ip_column not in df.columns:
            raise ValueError(
                f"Expected IP column '{ip_column}' in existing inventory data. "
                f"Found columns: {df.columns}"
            )
        return [
            row[ip_column]
            for row in df.select(ip_column).dropna().distinct().collect()
        ]

    def _select_unload_prefix(
        self,
        base_prefix: str,
        partition_prefix: str,
        snapshot_month: str,
    ) -> str:
        """Execute the select unload prefix stage of the workflow."""
        if not self._s3_prefix_has_objects(partition_prefix):
            return partition_prefix
        run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        temp_prefix = build_temp_output_prefix(base_prefix, snapshot_month, run_id)
        self.logger.info(
            "Partition prefix already populated. Using temporary UNLOAD prefix %s.",
            temp_prefix,
        )
        return temp_prefix

    def _s3_prefix_has_objects(self, s3_prefix: str) -> bool:
        """Execute the s3 prefix has objects stage of the workflow."""
        bucket, key_prefix = parse_s3_uri(s3_prefix)
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
        return response.get("KeyCount", 0) > 0

    def _copy_unload_objects(self, source_prefix: str, target_prefix: str) -> None:
        """Execute the copy unload objects stage of the workflow."""
        if source_prefix == target_prefix:
            return
        source_bucket, source_key_prefix = parse_s3_uri(source_prefix)
        target_bucket, target_key_prefix = parse_s3_uri(target_prefix)
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_key_prefix):
            for obj in page.get("Contents", []):
                source_key = obj["Key"]
                relative_key = source_key[len(source_key_prefix):].lstrip("/")
                target_key = f"{target_key_prefix.rstrip('/')}/{relative_key}"
                objects.append((source_key, target_key))
        if not objects:
            self.logger.info("No objects to copy from temporary UNLOAD prefix.")
            return
        copied = 0
        for source_key, target_key in objects:
            s3.copy_object(
                Bucket=target_bucket,
                Key=target_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
            )
            copied += 1
        self.logger.info(
            "Copied %s UNLOAD objects from %s to %s.",
            copied,
            source_prefix,
            target_prefix,
        )
        if self._confirm_copied_objects(target_bucket, target_key_prefix, objects):
            self._write_success_marker(target_bucket, target_key_prefix)
            self._delete_prefix(source_bucket, source_key_prefix)
        else:
            self.logger.warning(
                "Copy verification failed; keeping temporary UNLOAD prefix %s.",
                source_prefix,
            )

    def _confirm_copied_objects(
        self,
        bucket: str,
        target_key_prefix: str,
        expected_objects: List[Tuple[str, str]],
    ) -> bool:
        """Execute the confirm copied objects stage of the workflow."""
        expected_keys = {target_key for _, target_key in expected_objects}
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        found_keys = set()
        for page in paginator.paginate(Bucket=bucket, Prefix=target_key_prefix):
            for obj in page.get("Contents", []):
                found_keys.add(obj["Key"])
        missing = expected_keys - found_keys
        if missing:
            self.logger.error(
                "Missing %s copied objects under %s.",
                len(missing),
                target_key_prefix,
            )
            return False
        return True

    def _write_success_marker(self, bucket: str, key_prefix: str) -> None:
        """Execute the write success marker stage of the workflow."""
        s3 = boto3.client("s3")
        marker_key = f"{key_prefix.rstrip('/')}/_SUCCESS"
        s3.put_object(Bucket=bucket, Key=marker_key, Body=b"")

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
        existing_ips: List[str],
        output_spec: MachineInventoryOutputSpec,
        output_prefix: str,
    ) -> int:
        """Execute the unload from redshift stage of the workflow."""
        source_query = build_source_query(query_spec)
        if existing_ips:
            self._execute_statement(
                data_api,
                redshift_cfg,
                "CREATE TEMP TABLE tmp_existing_ips (ip_address VARCHAR(128))",
            )
            self._insert_existing_ips(data_api, redshift_cfg, existing_ips)
            filtered_query = (
                "SELECT DISTINCT src.ip_address, src.machine_name "
                "FROM ({source_query}) AS src "
                "LEFT JOIN tmp_existing_ips ex "
                "ON src.ip_address = ex.ip_address "
                "WHERE ex.ip_address IS NULL"
            ).format(source_query=source_query)
        else:
            filtered_query = (
                "SELECT DISTINCT src.ip_address, src.machine_name "
                "FROM ({source_query}) AS src"
            ).format(source_query=source_query)

        unload_sql = build_unload_sql(
            filtered_query=filtered_query,
            output_prefix=output_prefix,
            output_format=output_spec.output_format,
            iam_role=redshift_cfg.iam_role,
        )
        statement_id = self._execute_statement(data_api, redshift_cfg, unload_sql)
        return self._get_unload_row_count(data_api, statement_id)

    def _fallback_query_and_write(
        self,
        data_api: Any,
        redshift_cfg: RedshiftDataApiConfig,
        query_spec: MachineInventoryQuerySpec,
        output_spec: MachineInventoryOutputSpec,
        output_prefix: str,
        existing_ips: List[str],
        snapshot_month: str,
    ) -> None:
        """Execute the fallback query and write stage of the workflow."""
        source_query = build_source_query(query_spec)
        statement_id = self._execute_statement(data_api, redshift_cfg, source_query)
        rows = self._fetch_statement_results(data_api, statement_id)

        if not rows:
            self.logger.info("No rows returned from Redshift source query.")
            return

        df = self.spark.createDataFrame(
            rows,
            schema=[output_spec.ip_output_column, output_spec.name_output_column],
        ).dropna(subset=[output_spec.ip_output_column])

        if existing_ips:
            existing_df = self.spark.createDataFrame(
                [(ip,) for ip in existing_ips],
                schema=[output_spec.ip_output_column],
            )
            df = df.join(existing_df, on=output_spec.ip_output_column, how="left_anti")

        df = df.withColumn("snapshot_month", F.lit(snapshot_month))
        write_df = df.select(
            output_spec.ip_output_column,
            output_spec.name_output_column,
            "snapshot_month",
        )

        output_count = write_df.count()
        write_df.write.mode("append").partitionBy("snapshot_month").parquet(output_spec.s3_prefix)

        self.logger.info(
            "Fallback Spark write completed. Rows=%s, prefix=%s",
            output_count,
            output_spec.s3_prefix,
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

    def _insert_existing_ips(
        self,
        data_api: Any,
        config: RedshiftDataApiConfig,
        existing_ips: List[str],
    ) -> None:
        """Execute the insert existing ips stage of the workflow."""
        chunk_size = 500
        for chunk in chunked(existing_ips, chunk_size):
            parameters = [
                [{"name": "ip_address", "value": {"stringValue": ip}}]
                for ip in chunk
            ]
            params: Dict[str, Any] = {
                "ClusterIdentifier": config.cluster_identifier,
                "Database": config.database,
                "SecretArn": config.secret_arn,
                "Sql": "INSERT INTO tmp_existing_ips (ip_address) VALUES (:ip_address)",
                "ParameterSets": parameters,
            }
            if config.db_user:
                params["DbUser"] = config.db_user
            response = data_api.batch_execute_statement(**params)
            self._wait_for_statement(data_api, response["Id"])

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

    def _fetch_statement_results(self, data_api: Any, statement_id: str) -> List[Tuple[str, str]]:
        """Execute the fetch statement results stage of the workflow."""
        rows: List[Tuple[str, str]] = []
        next_token = None
        while True:
            params: Dict[str, Any] = {"Id": statement_id}
            if next_token:
                params["NextToken"] = next_token
            response = data_api.get_statement_result(**params)
            for record in response.get("Records", []):
                ip_value = record[0].get("stringValue")
                name_value = record[1].get("stringValue")
                rows.append((ip_value, name_value))
            next_token = response.get("NextToken")
            if not next_token:
                break
        return rows


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
        schema=query_payload.get("schema"),
        table=query_payload.get("table"),
        ip_column=query_payload.get("ip_column", "ip_address"),
        name_column=query_payload.get("name_column", "machine_name"),
        active_filter=query_payload.get("active_filter"),
        additional_filters=query_payload.get("additional_filters", []),
    )

    if not query_spec.sql and not (query_spec.schema and query_spec.table):
        raise ValueError("Either query.sql or query.schema/table must be provided.")

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


def build_snapshot_month(reference_month_iso: str) -> str:
    """Execute the build snapshot month stage of the workflow."""
    reference = datetime.fromisoformat(reference_month_iso.replace("Z", "+00:00"))
    return reference.strftime("%Y-%m")


def build_snapshot_output_prefix(base_prefix: str, snapshot_month: str) -> str:
    """Execute the build snapshot output prefix stage of the workflow."""
    if not base_prefix.endswith("/"):
        base_prefix = f"{base_prefix}/"
    return f"{base_prefix}snapshot_month={snapshot_month}/"


def build_temp_output_prefix(base_prefix: str, snapshot_month: str, run_id: str) -> str:
    """Execute the build temp output prefix stage of the workflow."""
    partition_prefix = build_snapshot_output_prefix(base_prefix, snapshot_month)
    if not partition_prefix.endswith("/"):
        partition_prefix = f"{partition_prefix}/"
    return f"{partition_prefix}_tmp_run_id={run_id}/"


def build_source_query(query_spec: MachineInventoryQuerySpec) -> str:
    """Execute the build source query stage of the workflow."""
    if query_spec.sql:
        base_query = query_spec.sql.strip().rstrip(";")
        return f"SELECT {query_spec.ip_column} AS ip_address, {query_spec.name_column} AS machine_name FROM ({base_query}) AS base"

    filters = []
    if query_spec.active_filter:
        filters.append(f"({query_spec.active_filter})")
    if query_spec.additional_filters:
        filters.extend([f"({f})" for f in query_spec.additional_filters])
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    return (
        "SELECT {ip_col} AS ip_address, {name_col} AS machine_name "
        "FROM {schema}.{table} {where_clause}"
    ).format(
        ip_col=query_spec.ip_column,
        name_col=query_spec.name_column,
        schema=query_spec.schema,
        table=query_spec.table,
        where_clause=where_clause,
    )


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


def chunked(items: Iterable[str], size: int) -> Iterable[List[str]]:
    """Execute the chunked stage of the workflow."""
    chunk: List[str] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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
