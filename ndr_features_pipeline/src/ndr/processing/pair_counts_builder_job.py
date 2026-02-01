
"""Pair-counts builder job for NDR pipeline.

This job computes per-15m pair-counts from raw Palo Alto traffic logs for:

    (src_ip, dst_ip, dst_port, event_ts, sessions_cnt)

It is intended as a ProcessingStep in the 15m streaming pipeline, after FG-A builder.
The output is later consumed by the FG-B baseline builder for pair rarity baselines.
"""

from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ndr.logging.logger import get_logger
from ndr.config.job_spec_loader import load_job_spec
from ndr.processing.base_runner import BaseRunner
from ndr.io.s3_writer import S3Writer
from ndr.processing.output_paths import build_batch_output_prefix
from ndr.processing.segment_utils import add_segment_id


LOGGER = get_logger(__name__)


@dataclass
class PairCountsJobRuntimeConfig:
    """Runtime config for Pair-Counts builder.

    Attributes
    ----------
    project_name : str
        Logical NDR project identifier (used for JobSpec lookup).
    feature_spec_version : str
        Feature spec version, used for JobSpec lookup and partitioning.
    mini_batch_id : str
        Identifier of the 15m ETL mini-batch (aligns with integration bucket folder).
    batch_start_ts_iso : str
        ISO8601 string of the batch start time (inclusive).
    batch_end_ts_iso : str
        ISO8601 string of the batch end time (exclusive) - used as event_ts.
    """

    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str


class PairCountsBuilderJob(BaseRunner):
    """Pair-counts builder job.

    The JobSpec for this job (job_name="pair_counts_builder") is expected to define:

    - traffic_input:
        - s3_prefix: base prefix for parsed Palo Alto integration logs
        - layout: "batch_folder" (for now)
    - pair_counts_output:
        - s3_prefix: base prefix for pair-counts dataset
    - filters (optional):
        - require_nonnull_ips: bool
        - require_destination_port: bool
    """

    def __init__(self, runtime_config: PairCountsJobRuntimeConfig) -> None:
        super().__init__()
        self.runtime_config = runtime_config
        self.spark: Optional[SparkSession] = None
        self.s3_writer = S3Writer()
        self.job_spec: Optional[Dict[str, Any]] = None

    # -------------------------------------------------------------- #
    # Entry point                                                    #
    # -------------------------------------------------------------- #
    def run(self) -> None:
        LOGGER.info("Pair-Counts builder job started.")
        try:
            self.job_spec = load_job_spec(
                project_name=self.runtime_config.project_name,
                job_name="pair_counts_builder",
                feature_spec_version=self.runtime_config.feature_spec_version,
            )
            LOGGER.info(
                "Loaded JobSpec for pair_counts_builder.",
                extra={"job_spec_keys": list(self.job_spec.keys())},
            )

            self.spark = self._build_spark_session()

            # 1) Read raw traffic for this mini-batch
            traffic_df = self._read_traffic_for_batch()

            # 2) Apply basic filters (non-null IPs, valid port, etc.)
            traffic_df = self._apply_filters(traffic_df)

            # 3) Aggregate to pair-counts
            pair_df = self._compute_pair_counts(traffic_df)

            # 4) Write out as Parquet partitioned by dt/hh/mm
            self._write_pair_counts(pair_df)

            LOGGER.info("Pair-Counts builder job completed successfully.")
        except Exception as exc:
            LOGGER.error("Pair-Counts builder job failed: %s", exc, exc_info=True)
            traceback.print_exc(file=sys.stderr)
            raise
        finally:
            if self.spark is not None:
                self.spark.stop()
                LOGGER.info("SparkSession stopped.")

    # -------------------------------------------------------------- #
    # Spark & IO helpers                                             #
    # -------------------------------------------------------------- #
    def _build_spark_session(self) -> SparkSession:
        LOGGER.info("Building SparkSession for Pair-Counts builder.")
        spark = (
            SparkSession.builder.appName("pair_counts_builder")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        return spark

    def _read_traffic_for_batch(self) -> DataFrame:
        """Read parsed Palo Alto traffic logs for this mini-batch.

        This implementation assumes:

        - Parsed integration logs live under traffic_input.s3_prefix
        - Each mini-batch has its own prefix:
              s3_prefix / mini_batch_id / *.gz
        - Files are JSON Lines, compressed with gzip, as per your integration bucket.
        """
        traffic_cfg = self.job_spec.get("traffic_input", {})
        base_prefix = traffic_cfg.get("s3_prefix")
        layout = traffic_cfg.get("layout", "batch_folder")

        if not base_prefix:
            raise ValueError("traffic_input.s3_prefix must be configured for pair_counts_builder.")

        mini_batch_id = self.runtime_config.mini_batch_id

        if layout == "batch_folder":
            path = f"{base_prefix.rstrip('/')}/{mini_batch_id}/"
        else:
            # Future: manifest-based or time-based layouts
            raise ValueError(f"Unsupported traffic_input.layout: {layout}")

        LOGGER.info(
            "Reading traffic logs for pair-counts.",
            extra={"path": path, "mini_batch_id": mini_batch_id},
        )

        # Spark handles .gz JSON Lines automatically
        df = self.spark.read.json(path)

        # Ensure expected columns exist; schema enforcement is done upstream,
        # but we defensively check for presence of the core fields.
        required_cols = [
            "source_ip",
            "destination_ip",
            "destination_port",
            "event_start",
            "event_end",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in traffic input: {missing}")

        return df

    def _apply_filters(self, df: DataFrame) -> DataFrame:
        """Filter out non-traffic records or malformed entries.

        Current logic:
        - Drop rows with null source_ip or destination_ip.
        - Drop rows with null or non-positive destination_port, if configured.
        - Optionally, restrict to known traffic event types (e.g., event_name == 'TRAFFIC').

        Additional filters can be encoded via JobSpec.filters.
        """
        filters_cfg = self.job_spec.get("filters", {})
        require_nonnull_ips = bool(filters_cfg.get("require_nonnull_ips", True))
        require_destination_port = bool(filters_cfg.get("require_destination_port", True))

        if require_nonnull_ips:
            df = df.filter(
                F.col("source_ip").isNotNull() & F.col("destination_ip").isNotNull()
            )

        if require_destination_port:
            df = df.filter(
                F.col("destination_port").isNotNull()
                & (F.col("destination_port") > F.lit(0))
            )

        # If event_name exists, restrict to TRAFFIC events (ignore THREAT-only entries)
        if "event_name" in df.columns:
            df = df.filter(F.col("event_name") == F.lit("TRAFFIC"))

        return df

    # -------------------------------------------------------------- #
    # Core pair-counts computation                                   #
    # -------------------------------------------------------------- #
    def _compute_pair_counts(self, df: DataFrame) -> DataFrame:
        """Aggregate traffic into pair-counts for this 15m batch.

        Output schema (before partition columns are added):

            src_ip: string
            dst_ip: string
            dst_port: int
            segment_id: string
            event_ts: timestamp  (slice end time per mini-batch)
            sessions_cnt: long
            mini_batch_id: string
            feature_spec_version: string
        """
        batch_end_ts_iso = self.runtime_config.batch_end_ts_iso
        mini_batch_id = self.runtime_config.mini_batch_id
        feature_spec_version = self.runtime_config.feature_spec_version

        # Use batch_end_ts as the representative timestamp for the 15m slice.
        event_ts_lit = F.to_timestamp(F.lit(batch_end_ts_iso))

        seg_cfg = self.job_spec.get("segment_mapping", {})
        df = add_segment_id(df=df, ip_col="source_ip", segment_mapping=seg_cfg)

        grouped = (
            df.groupBy("source_ip", "destination_ip", "destination_port", "segment_id")
            .agg(F.count(F.lit(1)).alias("sessions_cnt"))
            .withColumn("src_ip", F.col("source_ip"))
            .withColumn("dst_ip", F.col("destination_ip"))
            .withColumn("dst_port", F.col("destination_port").cast(T.IntegerType()))
            .drop("source_ip", "destination_ip", "destination_port")
            .withColumn("event_ts", event_ts_lit)
            .withColumn("mini_batch_id", F.lit(mini_batch_id))
            .withColumn("feature_spec_version", F.lit(feature_spec_version))
        )

        # Derive partition columns
        grouped = (
            grouped.withColumn("dt", F.to_date("event_ts"))
            .withColumn("hh", F.date_format("event_ts", "HH"))
            .withColumn("mm", F.date_format("event_ts", "mm"))
        )

        return grouped

    # -------------------------------------------------------------- #
    # Output writing                                                 #
    # -------------------------------------------------------------- #
    def _write_pair_counts(self, df: DataFrame) -> None:
        """Write pair-counts output as partitioned Parquet to S3."""
        output_cfg = self.job_spec.get("pair_counts_output", {})
        base_prefix = output_cfg.get("s3_prefix")

        if not base_prefix:
            raise ValueError(
                "pair_counts_output.s3_prefix must be configured for pair_counts_builder."
            )
        base_prefix = build_batch_output_prefix(
            base_prefix=base_prefix,
            dataset="pair_counts",
            batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
            batch_id=self.runtime_config.mini_batch_id,
        )

        LOGGER.info(
            "Writing pair-counts dataset.",
            extra={"base_prefix": base_prefix},
        )

        self.s3_writer.write_parquet(
            df=df,
            base_path=base_prefix,
            partition_cols=["dt", "hh", "mm", "feature_spec_version"],
            mode="append",
        )


def run_pair_counts_builder_from_runtime_config(
    runtime_config: PairCountsJobRuntimeConfig,
) -> None:
    """Helper to run PairCountsBuilderJob from a typed runtime config."""
    job = PairCountsBuilderJob(runtime_config=runtime_config)
    job.run()
