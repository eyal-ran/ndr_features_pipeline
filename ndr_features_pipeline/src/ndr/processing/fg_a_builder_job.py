
"""FG-A builder job.

This module implements the FG-A (current-behaviour) feature builder on top
of the 15-minute host-level delta tables produced by the delta builder.

High-level responsibilities
---------------------------
1. Read the delta table Parquet dataset for the most recent mini-batch.
2. Determine the anchor time (window_end_ts) as the maximum slice_end_ts
   present in that mini-batch.
3. For each configured window (15m, 30m, 1h, 8h, 24h) and for each role
   (outbound, inbound):
   - Aggregate composable metrics across the relevant deltas.
   - Derive secondary metrics (ratios, means, asymmetry, risk flags).
4. Assemble a wide FG-A row per (host_ip, window_label, window_end_ts)
   containing:
   - outbound_* features (unprefixed base metrics + derived metrics)
   - inbound_* features (prefixed with 'in_')
   - contextual time-of-day / weekday features.
5. Write the FG-A dataset to S3 as partitioned Parquet.

This implementation only depends on the delta tables and does *not* compute
baseline or correlation features. Those are handled by FG-B and FG-C builder
jobs respectively. If baseline / correlation columns are required in FG-A for
downstream compatibility, they can be added via a join from FG-A to FG-B/FG-C
outputs in a later step.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from ndr.processing.base_runner import BaseProcessingJobRunner
from ndr.config.job_spec_loader import load_job_spec
from ndr.processing.output_paths import build_batch_output_prefix
from ndr.model.fg_a_schema import (
    FG_A_WINDOWS_MINUTES,
    OUTBOUND_BASE_METRICS,
    INBOUND_BASE_METRICS,
    DERIVED_METRICS,
    build_feature_name,
)
from ndr.catalog.schema_manifest import build_fg_a_manifest
from ndr.processing.schema_enforcement import enforce_schema
from ndr.processing.segment_utils import add_segment_id


logger = logging.getLogger(__name__)


@dataclass
class FGABuilderConfig:
    """Configuration for the FG-A builder job.

    This is populated from Processing environment variables / arguments
    (for example, via Step Functions -> Pipeline -> Processing Step).

    Attributes
    ----------
    project_name:
        Logical project identifier, used mainly for logging and tagging.
    delta_s3_prefix:
        S3 prefix where the 15m host-level delta Parquet files are stored.
        Example: s3://my-ndr-bucket/deltas/
    output_s3_prefix:
        S3 prefix where FG-A Parquet files will be written.
        Example: s3://my-ndr-bucket/fg_a/
    mini_batch_id:
        Identifier of the ETL mini-batch that triggered this job.
    batch_start_ts_iso:
        ISO8601 batch start timestamp used in output prefix construction.
    feature_spec_version:
        Version string of the FG-A spec (e.g. "fga_v1").
    """

    project_name: str
    delta_s3_prefix: str
    output_s3_prefix: str
    mini_batch_id: str
    batch_start_ts_iso: str
    feature_spec_version: str
    pair_context_s3_prefix: Optional[str] = None
    pair_context_output_prefix: Optional[str] = None
    lookback30d_s3_prefix: Optional[str] = None
    lookback30d_thresholds: Optional[Dict[str, int]] = None
    high_risk_segments: Optional[List[str]] = None
    segment_mapping: Optional[Dict[str, Any]] = None


@dataclass
class FGABuilderJobRuntimeConfig:
    """Runtime config passed from Step Functions / Pipeline to FG-A builder."""

    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str


class FGABuilderJob(BaseProcessingJobRunner):
    """FG-A feature builder job runner.

    This class orchestrates reading the delta tables, aggregating features
    over multiple windows, and writing the resulting FG-A dataset.
    """

    def __init__(self, spark: SparkSession, config: FGABuilderConfig) -> None:
        """Initialize the instance with required clients and runtime configuration."""
        super().__init__(spark)
        self.config = config

    # ------------------------------------------------------------------
    # BaseProcessingJobRunner API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Entry point for the Processing job.

        This wraps the internal implementation with high-level error
        handling and logging so that the Processing container exits
        with a clear status and CloudWatch has sufficient diagnostics.
        """
        logger.info("Starting FG-A builder job for project=%s, mini_batch_id=%s", self.config.project_name, self.config.mini_batch_id)

        try:
            self._run_impl()
        except Exception:  # pylint: disable=broad-except
            logger.exception("FG-A builder job failed")
            # In a production system you might emit a custom CloudWatch metric here.
            raise

        logger.info("FG-A builder job completed successfully")

    # ------------------------------------------------------------------
    # Internal implementation
    # ------------------------------------------------------------------

    def _run_impl(self) -> None:
        # 1. Read delta tables for the relevant mini-batch.
        """Execute the run impl stage of the workflow."""
        delta_df = self._read_delta_table()

        if delta_df.rdd.isEmpty():
            logger.warning("Delta DataFrame for mini_batch_id=%s is empty; nothing to do", self.config.mini_batch_id)
            return

        pair_context_df = self._load_pair_context()
        lookback_df = self._load_lookback_table()

        # 2. Determine anchor time (window_end_ts) = max slice_end_ts in this batch.
        anchor_ts = self._compute_anchor_ts(delta_df)

        logger.info("Anchor (window_end_ts) for FG-A computation is %s", anchor_ts.isoformat())

        # 3. Build outbound and inbound aggregates for each window.
        outbound_df = self._build_direction_features(
            df=delta_df,
            role_value="outbound",
            prefix="",
            base_metrics=OUTBOUND_BASE_METRICS,
            anchor_ts=anchor_ts,
            pair_context_df=pair_context_df,
            lookback_df=lookback_df,
        )

        inbound_df = self._build_direction_features(
            df=delta_df,
            role_value="inbound",
            prefix="in_",
            base_metrics=INBOUND_BASE_METRICS,
            anchor_ts=anchor_ts,
            pair_context_df=pair_context_df,
            lookback_df=lookback_df,
        )

        # 4. Join outbound + inbound features on host_ip + window_label + window_start/end.
        fga_df = self._join_directions(outbound_df, inbound_df)

        # 5. Add metadata + time-of-day / weekday context features.
        fga_df = self._add_metadata_and_time_context(fga_df, anchor_ts)

        # 6. Write to S3 as Parquet.
        self._write_to_s3(fga_df)

        if pair_context_df is not None:
            self._write_pair_context(pair_context_df, anchor_ts)

    # ------------------------------------------------------------------
    # Helpers: IO
    # ------------------------------------------------------------------

    def _read_delta_table(self) -> DataFrame:
        """Read the delta Parquet dataset for the configured mini-batch.

        This method assumes that the delta builder wrote partitions that
        can be filtered by "mini_batch_id". If your delta schema uses a
        different partitioning strategy, adjust the filter accordingly.
        """
        spark = self.spark

        logger.info(
            "Reading delta table from %s for mini_batch_id=%s",
            self.config.delta_s3_prefix,
            self.config.mini_batch_id,
        )

        df = (
            spark.read
            .option("mergeSchema", "true")
            .parquet(self.config.delta_s3_prefix)
        )

        if "mini_batch_id" in df.columns:
            df = df.filter(F.col("mini_batch_id") == self.config.mini_batch_id)
        else:
            logger.warning("Delta DataFrame has no mini_batch_id column; using all rows under prefix")

        rename_map = {}
        if "bytes_src_sum" not in df.columns and "bytes_sent_sum" in df.columns:
            rename_map["bytes_sent_sum"] = "bytes_src_sum"
        if "bytes_dst_sum" not in df.columns and "bytes_recv_sum" in df.columns:
            rename_map["bytes_recv_sum"] = "bytes_dst_sum"
        if "deny_cnt" not in df.columns and "drop_cnt" in df.columns:
            rename_map["drop_cnt"] = "deny_cnt"
        for src, dest in rename_map.items():
            df = df.withColumnRenamed(src, dest)

        # Basic sanity check for required columns.
        required_cols = {"host_ip", "role", "slice_start_ts", "slice_end_ts"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Delta DataFrame is missing required columns: {sorted(missing)}")

        # Ensure timestamps are proper TimestampType.
        for col in ["slice_start_ts", "slice_end_ts"]:
            if not isinstance(df.schema[col].dataType, TimestampType):
                df = df.withColumn(col, F.to_timestamp(F.col(col)))

        return df

    def _load_pair_context(self) -> Optional[DataFrame]:
        """Execute the load pair context stage of the workflow."""
        if not self.config.pair_context_s3_prefix:
            return None
        logger.info("Reading pair context from %s", self.config.pair_context_s3_prefix)
        df = self.spark.read.parquet(self.config.pair_context_s3_prefix)
        if "feature_spec_version" in df.columns:
            df = df.filter(F.col("feature_spec_version") == self.config.feature_spec_version)
        if "mini_batch_id" in df.columns:
            df = df.filter(F.col("mini_batch_id") == self.config.mini_batch_id)
        return df

    def _load_lookback_table(self) -> Optional[DataFrame]:
        """Execute the load lookback table stage of the workflow."""
        if not self.config.lookback30d_s3_prefix:
            return None
        logger.info("Reading lookback30d table from %s", self.config.lookback30d_s3_prefix)
        df = self.spark.read.parquet(self.config.lookback30d_s3_prefix)
        if "feature_spec_version" in df.columns:
            df = df.filter(F.col("feature_spec_version") == self.config.feature_spec_version)
        return df


    def _write_to_s3(self, df: DataFrame) -> None:
        """Write FG-A dataset to S3 in Parquet format.

        The output is partitioned by feature_spec_version and dt (YYYY-MM-DD)
        for efficient downstream reading.
        """
        if df.rdd.isEmpty():
            logger.warning("FG-A DataFrame is empty; nothing to write to S3")
            return

        # Derive dt partition column from window_end_ts.
        df = df.withColumn("dt", F.date_format(F.col("window_end_ts"), "yyyy-MM-dd"))
        df = enforce_schema(df, build_fg_a_manifest(), "fg_a", logger)

        output_path = build_batch_output_prefix(
            base_prefix=self.config.output_s3_prefix,
            dataset="fg_a",
            batch_start_ts_iso=self.config.batch_start_ts_iso,
            batch_id=self.config.mini_batch_id,
        )
        logger.info("Writing FG-A Parquet to %s", output_path)

        (
            df.repartition("feature_spec_version", "dt")
            .write.mode("overwrite")
            .partitionBy("feature_spec_version", "dt")
            .parquet(output_path)
        )

    def _write_pair_context(self, df: DataFrame, anchor_ts: datetime) -> None:
        """Execute the write pair context stage of the workflow."""
        if not self.config.pair_context_output_prefix:
            return
        if df.rdd.isEmpty():
            logger.warning("Pair context DataFrame is empty; skipping write.")
            return

        output_path = build_batch_output_prefix(
            base_prefix=self.config.pair_context_output_prefix,
            dataset="fg_a_pair_context",
            batch_start_ts_iso=self.config.batch_start_ts_iso,
            batch_id=self.config.mini_batch_id,
        )

        window_frames: List[DataFrame] = []
        for window_label, minutes in FG_A_WINDOWS_MINUTES.items():
            window_start = anchor_ts - timedelta(minutes=minutes)
            filtered = df.filter(
                (F.col("slice_end_ts") > F.lit(window_start)) & (F.col("slice_end_ts") <= F.lit(anchor_ts))
            )
            if filtered.rdd.isEmpty():
                continue
            grouped = (
                filtered.groupBy("host_ip", "role", "peer_ip", "peer_port")
                .agg(
                    F.sum("sessions_cnt").alias("sessions_cnt"),
                    F.max("event_ts").alias("event_ts"),
                )
                .withColumn("window_label", F.lit(window_label))
                .withColumn("window_end_ts", F.lit(anchor_ts))
                .withColumn("window_start_ts", F.lit(window_start))
                .withColumnRenamed("peer_ip", "dst_ip")
                .withColumnRenamed("peer_port", "dst_port")
            )
            window_frames.append(grouped)

        if not window_frames:
            logger.warning("No pair context rows found for configured windows.")
            return

        pair_context = window_frames[0]
        for frame in window_frames[1:]:
            pair_context = pair_context.unionByName(frame, allowMissingColumns=True)

        pair_context = (
            pair_context.withColumn("mini_batch_id", F.lit(self.config.mini_batch_id))
            .withColumn("feature_spec_version", F.lit(self.config.feature_spec_version))
            .withColumn("dt", F.date_format(F.col("window_end_ts"), "yyyy-MM-dd"))
        )

        (
            pair_context.repartition("feature_spec_version", "dt")
            .write.mode("overwrite")
            .partitionBy("feature_spec_version", "dt")
            .parquet(output_path)
        )

    # ------------------------------------------------------------------
    # Helpers: core computation
    # ------------------------------------------------------------------

    def _compute_anchor_ts(self, delta_df: DataFrame) -> datetime:
        """Compute the anchor timestamp for FG-A windows.

        Anchor is defined as the maximum slice_end_ts within the mini-batch.
        All windows are computed as lookbacks ending at this point.
        """
        max_row = delta_df.agg(F.max("slice_end_ts").alias("max_ts")).collect()[0]
        max_ts = max_row["max_ts"]
        if max_ts is None:
            raise ValueError("slice_end_ts is null in the delta DataFrame")

        # Assume timestamps are in UTC. Adjust here if that is not the case.
        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        return max_ts

    def _build_direction_features(
        self,
        df: DataFrame,
        role_value: str,
        prefix: str,
        base_metrics: List[str],
        anchor_ts: datetime,
        pair_context_df: Optional[DataFrame],
        lookback_df: Optional[DataFrame],
    ) -> DataFrame:
        """Build FG-A features for a specific role (outbound or inbound).

        Parameters
        ----------
        df:
            Delta DataFrame filtered to the current mini-batch.
        role_value:
            Role value to filter on (e.g. "outbound" or "inbound").
        prefix:
            Prefix to apply to feature names ("" or "in_").
        base_metrics:
            List of base metric names expected to exist as columns in df.
        anchor_ts:
            Anchor (window_end_ts) used to define lookback windows.

        Returns
        -------
        DataFrame
            DataFrame with columns:
                - host_ip
                - window_label
                - window_start_ts
                - window_end_ts
                - prefixed feature columns per window.
        """
        spark = self.spark

        logger.info("Building %s features for role=%s", "outbound" if not prefix else "inbound", role_value)

        role_df = df.filter(F.col("role") == role_value)

        if role_df.rdd.isEmpty():
            logger.warning("No rows for role=%s in delta DataFrame", role_value)
            # Return empty DF with the expected schema.
            schema = spark.createDataFrame([], schema="host_ip STRING, window_label STRING, window_start_ts TIMESTAMP, window_end_ts TIMESTAMP").schema
            return spark.createDataFrame([], schema=schema)

        # For each configured window, build an aggregate DataFrame and then union them.
        window_frames: List[DataFrame] = []

        for window_label, minutes in FG_A_WINDOWS_MINUTES.items():
            window_frames.append(
                self._aggregate_for_window(
                    df=role_df,
                    prefix=prefix,
                    base_metrics=base_metrics,
                    window_label=window_label,
                    window_minutes=minutes,
                    anchor_ts=anchor_ts,
                    role_value=role_value,
                    pair_context_df=pair_context_df,
                    lookback_df=lookback_df,
                )
            )

        # Union all window frames, allowing for missing columns in case
        # some metrics are not available for a given role or schema version.
        fga_role_df: Optional[DataFrame] = None
        for frame in window_frames:
            if fga_role_df is None:
                fga_role_df = frame
            else:
                fga_role_df = fga_role_df.unionByName(frame, allowMissingColumns=True)

        assert fga_role_df is not None
        return fga_role_df

    def _aggregate_for_window(
        self,
        df: DataFrame,
        prefix: str,
        base_metrics: List[str],
        window_label: str,
        window_minutes: int,
        anchor_ts: datetime,
        role_value: str,
        pair_context_df: Optional[DataFrame],
        lookback_df: Optional[DataFrame],
    ) -> DataFrame:
        """Aggregate deltas for a single window and direction.

        This function sums composable metrics across all 15m slices whose
        slice_end_ts lies in (anchor_ts - window_minutes, anchor_ts].
        It then derives secondary metrics (ratios, means, etc.).
        """
        window_start = anchor_ts - timedelta(minutes=window_minutes)

        # Restrict to slices inside the window.
        filtered = df.filter(
            (F.col("slice_end_ts") > F.lit(window_start)) & (F.col("slice_end_ts") <= F.lit(anchor_ts))
        )

        if filtered.rdd.isEmpty():
            logger.info("No delta rows in window %s (%d minutes); returning empty frame", window_label, window_minutes)
            spark = self.spark
            schema = spark.createDataFrame(
                [],
                schema="host_ip STRING, window_label STRING, window_start_ts TIMESTAMP, window_end_ts TIMESTAMP",
            ).schema
            return spark.createDataFrame([], schema=schema)

        agg_exprs = []

        # Always carry host_ip as grouping key.
        group_by_cols = ["host_ip"]

        # Sum composable base metrics where available.
        for metric in base_metrics:
            if metric not in df.columns:
                logger.warning("Metric %s not present in delta DataFrame; skipping for window %s", metric, window_label)
                continue

            col_name = build_feature_name(prefix, metric, window_label)
            agg_exprs.append(F.sum(F.col(metric)).alias(col_name))

        # Include sessions_cnt explicitly if available, for derived metrics.
        sessions_col = build_feature_name(prefix, "sessions_cnt", window_label)

        aggregated = filtered.groupBy(*group_by_cols).agg(*agg_exprs)

        # Add window metadata columns.
        aggregated = (
            aggregated.withColumn("window_label", F.lit(window_label))
            .withColumn("window_start_ts", F.lit(window_start))
            .withColumn("window_end_ts", F.lit(anchor_ts))
        )

        # Derived metrics ------------------------------------------------
        eps = F.lit(1e-9)

        # duration_mean
        if build_feature_name(prefix, "duration_sum", window_label) in aggregated.columns and sessions_col in aggregated.columns:
            aggregated = aggregated.withColumn(
                build_feature_name(prefix, "duration_mean", window_label),
                F.col(build_feature_name(prefix, "duration_sum", window_label)) / (F.col(sessions_col) + eps),
            )

        # deny_ratio = deny_cnt / sessions_cnt
        deny_col = build_feature_name(prefix, "deny_cnt", window_label)
        if deny_col in aggregated.columns and sessions_col in aggregated.columns:
            aggregated = aggregated.withColumn(
                build_feature_name(prefix, "deny_ratio", window_label),
                F.col(deny_col) / (F.col(sessions_col) + eps),
            )

        # bytes_asymmetry_ratio = bytes_src_sum / bytes_dst_sum
        src_bytes_col = build_feature_name(prefix, "bytes_src_sum", window_label)
        dst_bytes_col = build_feature_name(prefix, "bytes_dst_sum", window_label)
        if src_bytes_col in aggregated.columns and dst_bytes_col in aggregated.columns:
            aggregated = aggregated.withColumn(
                build_feature_name(prefix, "bytes_asymmetry_ratio", window_label),
                F.col(src_bytes_col) / (F.col(dst_bytes_col) + eps),
            )

        # transport ratios: tcp_cnt / sessions_cnt, etc.
        for transport in ("tcp", "udp", "icmp"):
            base_name = f"{transport}_cnt"
            metric_col = build_feature_name(prefix, base_name, window_label)
            if metric_col in aggregated.columns and sessions_col in aggregated.columns:
                ratio_name = build_feature_name(prefix, f"transport_{transport}_ratio", window_label)
                aggregated = aggregated.withColumn(
                    ratio_name,
                    F.col(metric_col) / (F.col(sessions_col) + eps),
                )

        # has_high_risk_port_activity flag
        hr_col = build_feature_name(prefix, "high_risk_port_sessions_cnt", window_label)
        if hr_col in aggregated.columns:
            aggregated = aggregated.withColumn(
                build_feature_name(prefix, "has_high_risk_port_activity", window_label),
                (F.col(hr_col) > F.lit(0)).cast("int"),
            )

        aggregated = self._attach_novelty_features(
            aggregated=aggregated,
            pair_context_df=pair_context_df,
            lookback_df=lookback_df,
            role_value=role_value,
            window_label=window_label,
            window_start=window_start,
            anchor_ts=anchor_ts,
            prefix=prefix,
        )

        aggregated = self._attach_high_risk_segment_features(
            aggregated=aggregated,
            pair_context_df=pair_context_df,
            role_value=role_value,
            window_label=window_label,
            window_start=window_start,
            anchor_ts=anchor_ts,
            prefix=prefix,
        )

        return aggregated

    def _attach_novelty_features(
        self,
        aggregated: DataFrame,
        pair_context_df: Optional[DataFrame],
        lookback_df: Optional[DataFrame],
        role_value: str,
        window_label: str,
        window_start: datetime,
        anchor_ts: datetime,
        prefix: str,
    ) -> DataFrame:
        """Execute the attach novelty features stage of the workflow."""
        if pair_context_df is None or lookback_df is None:
            return aggregated

        role_pairs = pair_context_df.filter(F.col("role") == F.lit(role_value))
        required_pair_cols = {"host_ip", "peer_ip", "peer_port", "slice_end_ts"}
        missing_pair_cols = required_pair_cols - set(pair_context_df.columns)
        if missing_pair_cols:
            raise ValueError(f"Pair context missing required columns: {sorted(missing_pair_cols)}")
        role_pairs = role_pairs.filter(
            (F.col("slice_end_ts") > F.lit(window_start)) & (F.col("slice_end_ts") <= F.lit(anchor_ts))
        )
        if role_pairs.rdd.isEmpty():
            return aggregated

        role_pairs = role_pairs.withColumn(
            "pair_key", F.concat_ws("|", F.col("peer_ip"), F.col("peer_port").cast("string"))
        )

        current_sets = role_pairs.groupBy("host_ip").agg(
            F.collect_set("peer_ip").alias("current_peer_set"),
            F.collect_set("peer_port").alias("current_port_set"),
            F.collect_set("pair_key").alias("current_pair_set"),
        )

        thresholds = self.config.lookback30d_thresholds or {}
        peer_threshold = int(thresholds.get("peer", 1))
        port_threshold = int(thresholds.get("port", 1))
        pair_threshold = int(thresholds.get("pair", 1))

        required_lookback_cols = {
            "host_ip",
            "peer_ip",
            "peer_port",
            "pair_key",
            "peer_seen_count",
            "port_seen_count",
            "pair_seen_count",
        }
        missing_lookback = required_lookback_cols - set(lookback_df.columns)
        if missing_lookback:
            raise ValueError(f"Lookback table missing required columns: {sorted(missing_lookback)}")

        lookback_peers = lookback_df.groupBy("host_ip").agg(
            F.collect_set("peer_ip").alias("known_peer_set"),
            F.collect_set(F.when(F.col("peer_seen_count") <= F.lit(peer_threshold), F.col("peer_ip"))).alias(
                "rare_peer_set"
            ),
            F.collect_set("peer_port").alias("known_port_set"),
            F.collect_set(F.when(F.col("port_seen_count") <= F.lit(port_threshold), F.col("peer_port"))).alias(
                "rare_port_set"
            ),
            F.collect_set("pair_key").alias("known_pair_set"),
            F.collect_set(F.when(F.col("pair_seen_count") <= F.lit(pair_threshold), F.col("pair_key"))).alias(
                "rare_pair_set"
            ),
        )

        joined = aggregated.join(current_sets, on="host_ip", how="left").join(
            lookback_peers, on="host_ip", how="left"
        )

        def safe_size(col_name: str) -> F.Column:
            """Execute the safe size stage of the workflow."""
            return F.when(F.col(col_name).isNull(), F.lit(0)).otherwise(F.size(F.col(col_name)))

        def array_except_size(left: str, right: str) -> F.Column:
            """Execute the array except size stage of the workflow."""
            return F.when(
                F.col(left).isNull(),
                F.lit(0),
            ).otherwise(
                F.size(F.array_except(F.col(left), F.coalesce(F.col(right), F.expr("array()"))))
            )

        def array_intersect_size(left: str, right: str) -> F.Column:
            """Execute the array intersect size stage of the workflow."""
            return F.when(
                F.col(left).isNull(),
                F.lit(0),
            ).otherwise(
                F.size(F.array_intersect(F.col(left), F.coalesce(F.col(right), F.expr("array()"))))
            )

        new_peer_cnt = array_except_size("current_peer_set", "known_peer_set")
        peer_total = safe_size("current_peer_set")
        new_peer_ratio = F.when(peer_total > F.lit(0), new_peer_cnt / peer_total).otherwise(F.lit(0.0))
        rare_peer_cnt = array_intersect_size("current_peer_set", "rare_peer_set")

        new_port_cnt = array_except_size("current_port_set", "known_port_set")
        rare_port_cnt = array_intersect_size("current_port_set", "rare_port_set")

        new_pair_cnt = array_except_size("current_pair_set", "known_pair_set")
        rare_pair_cnt = array_intersect_size("current_pair_set", "rare_pair_set")

        joined = joined.withColumn(
            build_feature_name(prefix, "new_peer_cnt_lookback30d", window_label), new_peer_cnt
        ).withColumn(
            build_feature_name(prefix, "rare_peer_cnt_lookback30d", window_label), rare_peer_cnt
        ).withColumn(
            build_feature_name(prefix, "new_peer_ratio_lookback30d", window_label), new_peer_ratio
        ).withColumn(
            build_feature_name(prefix, "new_dst_port_cnt_lookback30d", window_label), new_port_cnt
        ).withColumn(
            build_feature_name(prefix, "rare_dst_port_cnt_lookback30d", window_label), rare_port_cnt
        ).withColumn(
            build_feature_name(prefix, "new_pair_cnt_lookback30d", window_label), new_pair_cnt
        ).withColumn(
            build_feature_name(prefix, "new_src_dst_port_cnt_lookback30d", window_label), new_pair_cnt
        )

        return joined.drop(
            "current_peer_set",
            "current_port_set",
            "current_pair_set",
            "known_peer_set",
            "rare_peer_set",
            "known_port_set",
            "rare_port_set",
            "known_pair_set",
            "rare_pair_set",
        )

    def _attach_high_risk_segment_features(
        self,
        aggregated: DataFrame,
        pair_context_df: Optional[DataFrame],
        role_value: str,
        window_label: str,
        window_start: datetime,
        anchor_ts: datetime,
        prefix: str,
    ) -> DataFrame:
        """Execute the attach high risk segment features stage of the workflow."""
        if pair_context_df is None:
            return aggregated
        high_risk_segments = self.config.high_risk_segments or []
        if not high_risk_segments:
            return aggregated

        role_pairs = pair_context_df.filter(F.col("role") == F.lit(role_value))
        required_pair_cols = {"host_ip", "peer_ip", "slice_end_ts", "sessions_cnt"}
        missing_pair_cols = required_pair_cols - set(pair_context_df.columns)
        if missing_pair_cols:
            raise ValueError(f"Pair context missing required columns: {sorted(missing_pair_cols)}")
        role_pairs = role_pairs.filter(
            (F.col("slice_end_ts") > F.lit(window_start)) & (F.col("slice_end_ts") <= F.lit(anchor_ts))
        )
        if role_pairs.rdd.isEmpty():
            return aggregated

        role_pairs = add_segment_id(
            df=role_pairs,
            ip_col="peer_ip",
            segment_mapping=self.config.segment_mapping,
        )
        high_risk_rows = role_pairs.filter(F.col("segment_id").isin(high_risk_segments))
        if high_risk_rows.rdd.isEmpty():
            return aggregated

        high_risk = high_risk_rows.groupBy("host_ip").agg(
            F.sum("sessions_cnt").alias("high_risk_segment_sessions_cnt"),
            F.countDistinct("peer_ip").alias("high_risk_segment_unique_dsts"),
        )

        joined = aggregated.join(high_risk, on="host_ip", how="left")
        joined = joined.withColumn(
            build_feature_name(prefix, "high_risk_segment_sessions_cnt", window_label),
            F.coalesce(F.col("high_risk_segment_sessions_cnt"), F.lit(0)),
        ).withColumn(
            build_feature_name(prefix, "high_risk_segment_unique_dsts", window_label),
            F.coalesce(F.col("high_risk_segment_unique_dsts"), F.lit(0)),
        ).withColumn(
            build_feature_name(prefix, "has_high_risk_segment_interaction", window_label),
            (F.coalesce(F.col("high_risk_segment_sessions_cnt"), F.lit(0)) > F.lit(0)).cast("int"),
        )

        return joined.drop("high_risk_segment_sessions_cnt", "high_risk_segment_unique_dsts")

    def _join_directions(self, outbound_df: DataFrame, inbound_df: DataFrame) -> DataFrame:
        """Join outbound + inbound per (host_ip, window_label, window_start/end)."""

        # Use full outer join so that hosts that only appear in one role
        # are still represented.
        join_keys = ["host_ip", "window_label", "window_start_ts", "window_end_ts"]

        joined = outbound_df.join(inbound_df, on=join_keys, how="outer")

        return joined

    def _add_metadata_and_time_context(self, df: DataFrame, anchor_ts: datetime) -> DataFrame:
        """Add metadata and time-of-day/week context features.

        Context features include:
            - hour_of_day (0-23)
            - is_working_hours (07:00-17:00)
            - is_off_hours (17:00-22:00)
            - is_night_hours (22:00-07:00)
            - is_weekend (Friday/Saturday)
        """
        if df.rdd.isEmpty():
            return df

        # Add mini_batch_id and feature_spec_version.
        df = (
            df.withColumn("mini_batch_id", F.lit(self.config.mini_batch_id))
            .withColumn("feature_spec_version", F.lit(self.config.feature_spec_version))
        )

        # Use window_end_ts as the reference time for context features.
        # Assuming timestamps are in UTC; convert to local time zone as needed.
        tz = "Asia/Jerusalem"
        local_ts = F.from_utc_timestamp(F.col("window_end_ts"), tz)

        df = df.withColumn("hour_of_day", F.hour(local_ts))
        df = df.withColumn("day_of_week", F.date_format(local_ts, "E"))  # Mon, Tue, ...

        # Working hours: 07:00 <= hour < 17:00
        df = df.withColumn(
            "is_working_hours",
            ((F.col("hour_of_day") >= 7) & (F.col("hour_of_day") < 17)).cast("int"),
        )

        # Off hours: 17:00 <= hour < 22:00
        df = df.withColumn(
            "is_off_hours",
            ((F.col("hour_of_day") >= 17) & (F.col("hour_of_day") < 22)).cast("int"),
        )

        # Night hours: 22:00 <= hour or hour < 7
        df = df.withColumn(
            "is_night_hours",
            ((F.col("hour_of_day") >= 22) | (F.col("hour_of_day") < 7)).cast("int"),
        )

        # Weekend: Friday or Saturday in Israel
        df = df.withColumn(
            "is_weekend",
            F.when(F.date_format(local_ts, "u").isin("5", "6"), F.lit(1)).otherwise(F.lit(0)),
        )

        # Build a stable record_id: host_ip|window_label|window_end_ts
        df = df.withColumn(
            "record_id",
            F.concat_ws("|", F.col("host_ip"), F.col("window_label"), F.col("window_end_ts")),
        )

        return df

def run_fg_a_builder_from_runtime_config(runtime_config: FGABuilderJobRuntimeConfig) -> None:
    """Run FG-A builder from a typed runtime config."""
    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="fg_a_builder",
        feature_spec_version=runtime_config.feature_spec_version,
    )

    delta_prefix = (
        job_spec.get("delta_input", {}).get("s3_prefix")
        or job_spec.get("delta_s3_prefix")
    )
    output_prefix = (
        job_spec.get("fg_a_output", {}).get("s3_prefix")
        or job_spec.get("output_s3_prefix")
    )
    if not delta_prefix or not output_prefix:
        raise ValueError("fg_a_builder JobSpec must define delta and output prefixes.")

    pair_context_prefix = job_spec.get("pair_context_input", {}).get("s3_prefix")
    pair_context_output = job_spec.get("pair_context_output", {}).get("s3_prefix")
    lookback_cfg = job_spec.get("lookback30d", {})
    lookback_prefix = lookback_cfg.get("s3_prefix")
    lookback_thresholds = lookback_cfg.get("rare_thresholds", {})

    config = FGABuilderConfig(
        project_name=runtime_config.project_name,
        delta_s3_prefix=delta_prefix,
        output_s3_prefix=output_prefix,
        mini_batch_id=runtime_config.mini_batch_id,
        batch_start_ts_iso=runtime_config.batch_start_ts_iso,
        feature_spec_version=runtime_config.feature_spec_version,
        pair_context_s3_prefix=pair_context_prefix,
        pair_context_output_prefix=pair_context_output,
        lookback30d_s3_prefix=lookback_prefix,
        lookback30d_thresholds=lookback_thresholds,
        high_risk_segments=job_spec.get("high_risk_segments", []),
        segment_mapping=job_spec.get("segment_mapping"),
    )

    spark = (
        SparkSession.builder.appName("fg_a_builder")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        job = FGABuilderJob(spark=spark, config=config)
        job.run()
    finally:
        spark.stop()
