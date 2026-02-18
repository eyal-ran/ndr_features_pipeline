"""NDR delta builder job module."""
from dataclasses import dataclass

from typing import Dict, Any

import boto3
import json
from pyspark.sql import SparkSession, DataFrame, functions as F

from ndr.config.job_spec_models import JobSpec
from ndr.config.job_spec_loader import JobSpecLoader
from ndr.processing.base_runner import BaseProcessingRunner, RuntimeParams
from ndr.processing import delta_builder_operators as ops
from ndr.logging.logger import get_logger
from ndr.catalog.schema_manifest import build_delta_manifest
from ndr.processing.schema_enforcement import enforce_schema
from ndr.processing.raw_traffic_fields import (
    normalize_raw_traffic_fields,
    REQUIRED_DELTA_TRAFFIC_FIELDS,
)


class DeltaBuilderRunner(BaseProcessingRunner):
    """Concrete runner that builds 15m delta tables from Palo Alto logs."""

    def __init__(self, spark: SparkSession, job_spec: JobSpec, runtime: RuntimeParams):
        """Initialize the instance with required clients and runtime configuration."""
        super().__init__(spark, job_spec, runtime)
        self._s3_client = boto3.client("s3")
        self.logger = get_logger("DeltaBuilderRunner")  # override name
        self._pair_context_df: DataFrame | None = None


    def _read_input(self) -> DataFrame:
        """Read raw mini-batch data and normalize input columns using JobSpec mapping."""
        df = super()._read_input()
        input_spec = self.job_spec.input
        return normalize_raw_traffic_fields(
            df,
            field_mapping=input_spec.field_mapping,
            required_canonical_fields=REQUIRED_DELTA_TRAFFIC_FIELDS,
            context_name="delta_builder.input",
        )

    def _apply_data_quality(self, df: DataFrame) -> DataFrame:
        """Apply Palo Alto specific DQ cleaning rules."""
        dq_spec = self.job_spec.dq

        if dq_spec.filter_null_bytes_ports:
            df = df.filter(
                ~(
                    F.col("source_bytes").isNull()
                    & F.col("destination_bytes").isNull()
                    & F.col("source_port").isNull()
                    & F.col("destination_port").isNull()
                )
            )

        df = df.withColumn(
            "duration_sec",
            F.when(
                F.col("event_end").isNotNull() & F.col("event_start").isNotNull(),
                F.col("event_end") - F.col("event_start"),
            ).otherwise(F.lit(0.0)),
        )

        if dq_spec.duration_non_negative:
            df = df.withColumn(
                "duration_sec",
                F.when(F.col("duration_sec") < 0, F.lit(0.0)).otherwise(F.col("duration_sec")),
            )

        if dq_spec.bytes_non_negative:
            df = df.withColumn(
                "source_bytes",
                F.when(F.col("source_bytes") < 0, None).otherwise(F.col("source_bytes")),
            ).withColumn(
                "destination_bytes",
                F.when(F.col("destination_bytes") < 0, None).otherwise(F.col("destination_bytes")),
            )

        if self.runtime.slice_start_ts and self.runtime.slice_end_ts:
            start_ts = self.runtime.slice_start_ts
            end_ts = self.runtime.slice_end_ts
            df = df.filter(
                (F.col("event_start") >= F.to_timestamp(F.lit(start_ts)))
                & (F.col("event_start") < F.to_timestamp(F.lit(end_ts)))
            )

        return df

    def _load_port_sets(self) -> Dict[str, Any]:
        """Load port sets from the enrichment spec, if configured."""
        uri = self.job_spec.enrichment.port_sets_location
        if not uri:
            return {}
        if not uri.startswith("s3://"):
            self.logger.warning("Port sets location is not an s3:// URI: %s", uri)
            return {}
        bucket, key = uri[5:].split("/", 1)
        obj = self._s3_client.get_object(Bucket=bucket, Key=key)
        text = obj["Body"].read().decode("utf-8")
        return json.loads(text)

    def _build_dataframe(self, df: DataFrame) -> DataFrame:
        """Build the final delta DataFrame from cleaned events."""
        if self.runtime.slice_start_ts:
            df = df.withColumn(
                "slice_start_ts", F.to_timestamp(F.lit(self.runtime.slice_start_ts))
            )
        else:
            df = df.withColumn("slice_start_ts", F.col("event_start"))

        if self.runtime.slice_end_ts:
            df = df.withColumn(
                "slice_end_ts", F.to_timestamp(F.lit(self.runtime.slice_end_ts))
            )
        else:
            max_end = df.agg(F.max("event_end").alias("max_end")).collect()[0]["max_end"]
            df = df.withColumn("slice_end_ts", F.lit(max_end))

        df = df.withColumn("dt", F.date_format(F.col("slice_start_ts"), "yyyy-MM-dd"))
        df = df.withColumn("hh", F.date_format(F.col("slice_start_ts"), "HH"))
        df = df.withColumn("mm", F.date_format(F.col("slice_start_ts"), "mm"))

        port_sets = self._load_port_sets()

        role_dfs = []
        for role_spec in self.job_spec.roles:
            role_name = role_spec.name
            self.logger.info("Building role view for %s", role_name)

            role_df = (
                df.withColumn("host_ip", F.col(role_spec.host_ip))
                .withColumn("peer_ip", F.col(role_spec.peer_ip))
                .withColumn("peer_port", F.col(role_spec.peer_port).cast("int"))
                .withColumn("bytes_sent", F.col(role_spec.bytes_sent).cast("double"))
                .withColumn("bytes_recv", F.col(role_spec.bytes_recv).cast("double"))
                .withColumn("role", F.lit(role_name))
            )

            pair_context = (
                role_df.groupBy(
                    "host_ip",
                    "role",
                    "peer_ip",
                    "peer_port",
                    "slice_start_ts",
                    "slice_end_ts",
                    "dt",
                    "hh",
                    "mm",
                )
                .agg(
                    F.count(F.lit(1)).alias("sessions_cnt"),
                    F.max("event_end").alias("event_ts"),
                )
                .withColumn("pair_key", F.concat_ws("|", F.col("peer_ip"), F.col("peer_port").cast("string")))
            )

            base = ops.apply_base_counts_and_sums(role_df, params={})
            quants = ops.apply_quantiles(role_df, params={})
            port_counts = ops.apply_port_category_counters(
                role_df,
                params={"port_sets": port_sets},
            )
            burst = ops.apply_burst_metrics(role_df, params={})

            join_keys = ["host_ip", "role", "slice_start_ts", "slice_end_ts", "dt", "hh", "mm"]
            tmp = base.join(quants, on=join_keys, how="left")
            tmp = tmp.join(port_counts, on=join_keys, how="left")
            tmp = tmp.join(burst, on=join_keys, how="left")

            if self._pair_context_df is None:
                self._pair_context_df = pair_context
            else:
                self._pair_context_df = self._pair_context_df.unionByName(pair_context, allowMissingColumns=True)

            role_dfs.append(tmp)

        if not role_dfs:
            return df.limit(0)

        result = role_dfs[0]
        for other in role_dfs[1:]:
            result = result.unionByName(other, allowMissingColumns=True)

        return result

    def _write_output(self, df: DataFrame) -> None:
        """Execute the write output stage of the workflow."""
        port_sets = self._load_port_sets()
        manifest = build_delta_manifest(port_set_names=sorted(port_sets.keys()))
        df = enforce_schema(df, manifest, "delta", self.logger)
        super()._write_output(df)
        pair_context_output = self.job_spec.pair_context_output
        if not pair_context_output or self._pair_context_df is None:
            return

        pair_context = (
            self._pair_context_df.withColumn("mini_batch_id", F.lit(self.runtime.run_id))
            .withColumn("feature_spec_version", F.lit(self.runtime.feature_spec_version))
        )
        self.writer.write_parquet_partitioned(
            df=pair_context,
            base_prefix=pair_context_output.s3_prefix,
            partition_cols=pair_context_output.partition_keys,
            mode=pair_context_output.write_mode,
        )


def run_delta_builder(spark: SparkSession, job_spec: JobSpec, runtime: RuntimeParams) -> None:
    """Module-level entrypoint used by the CLI wrapper."""
    runner = DeltaBuilderRunner(spark=spark, job_spec=job_spec, runtime=runtime)
    runner.run()


@dataclass
class DeltaBuilderJobRuntimeConfig:
    """Runtime config passed from Step Functions / Pipeline to Delta Builder."""

    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str


def run_delta_builder_from_runtime_config(runtime_config: DeltaBuilderJobRuntimeConfig) -> None:
    """Run Delta Builder from a typed runtime config."""
    loader = JobSpecLoader()
    job_spec = loader.load(
        project_name=runtime_config.project_name,
        job_name="delta_builder",
        feature_spec_version=runtime_config.feature_spec_version,
    )

    runtime = RuntimeParams(
        project_name=runtime_config.project_name,
        job_name="delta_builder",
        mini_batch_s3_prefix=job_spec.input.s3_prefix,
        feature_spec_version=runtime_config.feature_spec_version,
        run_id=runtime_config.mini_batch_id,
        slice_start_ts=runtime_config.batch_start_ts_iso,
        slice_end_ts=runtime_config.batch_end_ts_iso,
    )

    spark = (
        SparkSession.builder.appName("delta_builder")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        run_delta_builder(spark=spark, job_spec=job_spec, runtime=runtime)
    finally:
        spark.stop()
