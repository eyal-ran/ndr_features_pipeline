
"""FG-B Baseline Builder Job.

This module implements the FG-B builder, which computes robust baselines over FG-A
time-windowed features for different horizons (7d, 30d) with safety gaps and
segment-based slice-level anomaly capping.

It is designed to be run as a SageMaker Processing job. It relies on:

- ndr.config.job_spec_loader: to load JobSpec from DynamoDB
- ndr.io.s3_reader / s3_writer: to read/write Parquet from S3
- ndr.logging.logger: structured logging to CloudWatch-compatible logs
- PySpark: for scalable aggregation
"""

from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ndr.logging.logger import get_logger
from ndr.config.job_spec_loader import load_job_spec
from ndr.processing.base_runner import BaseRunner
from ndr.io.s3_reader import S3Reader
from ndr.io.s3_writer import S3Writer
from ndr.processing.output_paths import build_batch_output_prefix


LOGGER = get_logger(__name__)


@dataclass
class FGBaselineJobRuntimeConfig:
    """Runtime config passed from Step Functions / Pipeline to FG-B builder.

    Attributes
    ----------
    project_name : str
        Logical NDR project identifier (used for JobSpec lookup, logging, etc.).
    feature_spec_version : str
        Version string for the feature specification (FG-A/B/C schema version).
    reference_time_iso : str
        Reference time for baseline computation (ISO8601, e.g. 2025-12-31T00:00:00Z).
    mode : str
        Either "REGULAR" or "BACKFILL". In BACKFILL, explicit time windows may be
        provided in JobSpec for historical processing.
    batch_id : str
        Identifier for the baseline batch output prefix.
    """

    project_name: str
    feature_spec_version: str
    reference_time_iso: str
    mode: str = "REGULAR"
    batch_id: str = "baseline"


class FGBaselineBuilderJob(BaseRunner):
    """FG-B baseline builder job.

    This job:
    1. Loads JobSpec from DynamoDB using project_name and job_name "fg_b_builder".
    2. Builds a SparkSession.
    3. Reads FG-A features from S3 for the configured horizons and safety gaps.
    4. Derives segment_id from host_ip using a configurable mapping rule.
    5. Computes segment-level baselines for key metrics to derive slice-level robust z-scores.
    6. Applies segment-based slice-level anomaly capping (down-weight).
    7. Computes host-level baselines (median, P25/75/95/99, MAD, IQR, support_count, cold_start_flag).
    8. Computes segment-level baselines (for fallback).
    9. Optionally computes pair-level rarity baselines from pair-count datasets.
    10. Writes FG-B baselines as partitioned Parquet tables to S3.

    The JobSpec is expected to provide:
    - fg_a_input:
        - s3_prefix
        - format (parquet)
    - fg_b_output:
        - s3_prefix
    - horizons: ["7d", "30d"]
    - safety_gaps:
        - "7d": {"tail_days": 2, "head_days": 2}
        - "30d": {"tail_days": 7, "head_days": 7}
    - anomaly_capping:
        - key_metrics: [...]
        - z_max: float (e.g. 6.0)
        - w_anom: float (e.g. 0.1)
    - support_min:
        - metric_name -> int (minimum number of slices for stable baseline)
    - segment_mapping:
        - strategy: "ipv4_prefix"
        - prefix_length: 24
    - pair_counts (optional):
        - enabled: bool
        - s3_prefix: str
        - rarity_threshold: float
    """

    def __init__(self, runtime_config: FGBaselineJobRuntimeConfig) -> None:
        super().__init__()
        self.runtime_config = runtime_config
        self.spark: Optional[SparkSession] = None
        self.s3_reader: Optional[S3Reader] = None
        self.s3_writer = S3Writer()
        self.job_spec: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------ #
    # Entry point                                                        #
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """Execute the FG-B builder job.

        Orchestration flow:
        1. Load JobSpec from configuration source.
        2. Initialize Spark.
        3. For each horizon in JobSpec:
           a. Compute effective [baseline_start_ts, baseline_end_ts) using reference_time and safety gaps.
           b. Read FG-A slices for that horizon.
           c. Derive segment_id.
           d. Compute segment-level stats for anomaly capping.
           e. Compute slice-level z-scores and anomaly weights.
           f. Compute host-level baselines.
           g. Compute segment-level baselines.
           h. If configured, compute pair-level rarity baselines.
           i. Write results to S3.
        4. Stop Spark.
        """
        LOGGER.info("FG-B baseline builder job started.")
        try:
            self.job_spec = load_job_spec(
                project_name=self.runtime_config.project_name,
                job_name="fg_b_builder",
                feature_spec_version=self.runtime_config.feature_spec_version,
            )
            LOGGER.info("Loaded JobSpec for FG-B builder.", extra={"job_spec_keys": list(self.job_spec.keys())})

            self.spark = self._build_spark_session()
            self.s3_reader = S3Reader(self.spark)

            horizons = self.job_spec.get("horizons", ["7d", "30d"])
            for horizon in horizons:
                self._process_horizon(horizon)

            LOGGER.info("FG-B baseline builder job completed successfully.")
        except Exception as exc:
            LOGGER.error("FG-B baseline builder job failed: %s", exc, exc_info=True)
            traceback.print_exc(file=sys.stderr)
            raise
        finally:
            if self.spark is not None:
                self.spark.stop()
                LOGGER.info("SparkSession stopped.")

    # ------------------------------------------------------------------ #
    # Spark & time utilities                                             #
    # ------------------------------------------------------------------ #
    def _build_spark_session(self) -> SparkSession:
        """Create or get a SparkSession for the Processing job.

        Spark configuration can be extended here (e.g., shuffle partitions, dynamic allocation).
        """
        LOGGER.info("Building SparkSession for FG-B builder.")
        spark = (
            SparkSession.builder.appName("fg_b_baseline_builder")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        return spark

    def _compute_horizon_bounds(self, horizon: str) -> Dict[str, Any]:
        """Compute [baseline_start_ts, baseline_end_ts) for a given horizon.

        Uses JobSpec safety_gaps and the runtime reference_time_iso. Returns a dict with:
        - baseline_start_ts (string ISO)
        - baseline_end_ts (string ISO)
        - baseline_horizon (string, same as input)
        """
        from datetime import datetime, timedelta, timezone

        ref = datetime.fromisoformat(self.runtime_config.reference_time_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        safety_gaps = self.job_spec.get("safety_gaps", {})
        gap_cfg = safety_gaps.get(horizon, {})

        tail_days = int(gap_cfg.get("tail_days", 0))
        head_days = int(gap_cfg.get("head_days", 0))

        if horizon == "7d":
            eff_days = 7
        elif horizon == "30d":
            eff_days = 30
        else:
            raise ValueError(f"Unsupported baseline_horizon: {horizon}")

        baseline_end = ref - timedelta(days=head_days)
        baseline_start = baseline_end - timedelta(days=eff_days + tail_days)

        return {
            "baseline_start_ts": baseline_start.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "baseline_end_ts": baseline_end.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "baseline_horizon": horizon,
        }

    # ------------------------------------------------------------------ #
    # Horizon processing                                                 #
    # ------------------------------------------------------------------ #
    def _process_horizon(self, horizon: str) -> None:
        """Process one baseline_horizon (e.g. "7d" or "30d")."""
        bounds = self._compute_horizon_bounds(horizon)
        LOGGER.info(
            "Processing FG-B horizon.",
            extra={
                "horizon": horizon,
                "baseline_start_ts": bounds["baseline_start_ts"],
                "baseline_end_ts": bounds["baseline_end_ts"],
            },
        )

        fg_a_cfg = self.job_spec.get("fg_a_input", {})
        fg_a_prefix = fg_a_cfg.get("s3_prefix")
        if not fg_a_prefix:
            raise ValueError("fg_a_input.s3_prefix must be configured in JobSpec for FG-B.")

        # Read FG-A Parquet within time range
        fg_a_df = self._read_fg_a_for_horizon(fg_a_prefix, bounds)

        # Derive segment_id (prefix-based default)
        fg_a_seg_df = self._add_segment_id(fg_a_df)

        # Compute segment-level stats for key metrics (for anomaly capping)
        key_metrics_cfg = self.job_spec.get("anomaly_capping", {}).get("key_metrics", [])
        z_max = float(self.job_spec.get("anomaly_capping", {}).get("z_max", 6.0))
        w_anom = float(self.job_spec.get("anomaly_capping", {}).get("w_anom", 0.1))

        fg_a_with_weights = self._apply_segment_anomaly_capping(
            df=fg_a_seg_df,
            key_metrics=key_metrics_cfg,
            z_max=z_max,
            w_anom=w_anom,
        )

        # Build host-level baselines
        host_baselines = self._build_host_baselines(fg_a_with_weights, horizon)

        # Build segment-level baselines
        segment_baselines = self._build_segment_baselines(fg_a_with_weights, horizon)

        # Optionally build pair-level rarity baselines
        pair_baselines: Optional[DataFrame] = None
        pair_cfg = self.job_spec.get("pair_counts", {})
        if pair_cfg.get("enabled", False):
            pair_baselines = self._build_pair_rarity_baselines(horizon, bounds, pair_cfg)

        # Write out FG-B baselines
        self._write_fg_b_outputs(
            host_baselines=host_baselines,
            segment_baselines=segment_baselines,
            pair_baselines=pair_baselines,
            horizon=horizon,
        )

    # ------------------------------------------------------------------ #
    # Data loading                                                       #
    # ------------------------------------------------------------------ #
    def _read_fg_a_for_horizon(self, s3_prefix: str, bounds: Dict[str, Any]) -> DataFrame:
        """Read FG-A Parquet rows within the horizon bounds.

        Assumes FG-A is partitioned by dt (YYYY-MM-DD) and possibly other keys.
        Filters by window_end_ts between baseline_start_ts and baseline_end_ts.
        """
        LOGGER.info("Reading FG-A data for horizon from S3.", extra={"prefix": s3_prefix})
        df = self.spark.read.parquet(s3_prefix)

        # Filter by window_end_ts (timestamp column in FG-A)
        start_ts = bounds["baseline_start_ts"]
        end_ts = bounds["baseline_end_ts"]

        df = df.filter((F.col("window_end_ts") >= F.lit(start_ts)) & (F.col("window_end_ts") < F.lit(end_ts)))

        return df

    # ------------------------------------------------------------------ #
    # Segment ID derivation                                              #
    # ------------------------------------------------------------------ #
    def _add_segment_id(self, df: DataFrame) -> DataFrame:
        """Add segment_id column based on host_ip.

        Default implementation uses IPv4 prefix grouping (e.g. /24). For IPv6,
        this function can be extended or replaced via JobSpec configuration.
        """
        seg_cfg = self.job_spec.get("segment_mapping", {})
        strategy = seg_cfg.get("strategy", "ipv4_prefix")
        prefix_len = int(seg_cfg.get("prefix_length", 24))

        if strategy != "ipv4_prefix":
            LOGGER.warning("Unsupported segment_mapping.strategy '%s', falling back to ipv4_prefix.", strategy)

        def ipv4_prefix(ip: str) -> str:
            if ip is None:
                return "SEG_UNKNOWN"
            parts = ip.split(".")
            if len(parts) != 4:
                return "SEG_UNKNOWN"
            try:
                octets = [int(p) for p in parts]
            except ValueError:
                return "SEG_UNKNOWN"
            # Simple /24 prefix: first 3 octets, configurable if needed
            if prefix_len == 24:
                return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
            elif prefix_len == 16:
                return f"{octets[0]}.{octets[1]}.0.0/16"
            else:
                # Fallback: keep first 3 octets
                return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"

        ipv4_prefix_udf = F.udf(ipv4_prefix, T.StringType())

        return df.withColumn("segment_id", ipv4_prefix_udf(F.col("host_ip")))

    # ------------------------------------------------------------------ #
    # Segment anomaly capping                                            #
    # ------------------------------------------------------------------ #
    def _apply_segment_anomaly_capping(
        self,
        df: DataFrame,
        key_metrics: List[str],
        z_max: float,
        w_anom: float,
    ) -> DataFrame:
        """Compute segment-based robust z-scores and assign anomaly weights.

        For each key metric M, we compute segment-level median and MAD:

            segment_median_M = median(M_slice)
            segment_MAD_M    = median(|M_slice - segment_median_M|)

        Then for each slice row:

            z_slice_M = (M_slice - segment_median_M) / (segment_MAD_M + eps)

        If max_M |z_slice_M| > z_max for any key metric, we assign anomaly_weight = w_anom,
        else anomaly_weight = 1.0.

        We return the DataFrame with columns:

            - segment_id
            - anomaly_weight
            - (optional) max_abs_z_slice for debugging
        """
        if not key_metrics:
            LOGGER.warning("No key_metrics configured for anomaly_capping; assigning weight 1.0 to all slices.")
            return df.withColumn("anomaly_weight", F.lit(1.0))

        eps = 1e-6

        # 1) Compute segment-level median and MAD per metric
        seg_group_cols = ["segment_id", "role", "time_band", "window_label"]
        seg_stats_exprs = []
        for m in key_metrics:
            seg_stats_exprs.append(F.expr(f"percentile_approx({m}, 0.5, 10000)").alias(f"{m}_segment_median"))

        seg_medians = df.groupBy(*seg_group_cols).agg(*seg_stats_exprs)

        # Join medians back and compute |M_slice - median|
        joined = df.join(seg_medians, on=seg_group_cols, how="left")

        for m in key_metrics:
            joined = joined.withColumn(
                f"{m}_abs_dev", F.abs(F.col(m) - F.col(f"{m}_segment_median"))
            )

        # Compute MAD per segment for each metric
        seg_mad_exprs = []
        for m in key_metrics:
            seg_mad_exprs.append(
                F.expr(f"percentile_approx({m}_abs_dev, 0.5, 10000)").alias(f"{m}_segment_mad")
            )
        seg_mads = joined.groupBy(*seg_group_cols).agg(*seg_mad_exprs)

        # Join MADs back
        joined = joined.join(seg_mads, on=seg_group_cols, how="left")

        # Compute z-scores and max |z|
        max_abs_z_expr = None
        for m in key_metrics:
            z_col = F.when(
                F.col(f"{m}_segment_mad") > 0,
                (F.col(m) - F.col(f"{m}_segment_median")) / (F.col(f"{m}_segment_mad") + eps),
            ).otherwise(F.lit(0.0))
            z_col_name = f"{m}_z_slice"
            joined = joined.withColumn(z_col_name, z_col)
            abs_z = F.abs(z_col)
            max_abs_z_expr = abs_z if max_abs_z_expr is None else F.greatest(max_abs_z_expr, abs_z)

        joined = joined.withColumn("max_abs_z_slice", max_abs_z_expr)

        # Assign anomaly_weight based on threshold
        joined = joined.withColumn(
            "anomaly_weight",
            F.when(F.col("max_abs_z_slice") > F.lit(z_max), F.lit(float(w_anom))).otherwise(F.lit(1.0)),
        )

        return joined

    # ------------------------------------------------------------------ #
    # Host-level baselines                                               #
    # ------------------------------------------------------------------ #
    def _build_host_baselines(self, df: DataFrame, horizon: str) -> DataFrame:
        """Compute host-level baselines for all non-quantile FG-A metrics.

        We approximate down-weighting by computing distribution statistics on slices
        with anomaly_weight == 1.0, while support_count includes all slices with
        non-null metric values.

        Grouping keys:
            (host_ip, role, segment_id, time_band, window_label)

        Outputs:
            - *_median, *_p25, *_p75, *_p95, *_p99, *_mad, *_iqr
            - *_support_count, *_cold_start_flag
        """
        support_cfg = self.job_spec.get("support_min", {})
        group_cols = ["host_ip", "role", "segment_id", "time_band", "window_label"]

        metrics = self.job_spec.get("baseline_metrics", [])
        if not metrics:
            # Fallback: derive metrics from schema if not explicitly configured
            # Example: all columns starting with known prefixes
            candidate_cols = [c for c in df.columns if c not in group_cols + ["anomaly_weight", "max_abs_z_slice"]]
            metrics = candidate_cols

        # Filter good slices for quantiles
        good_df = df.filter(F.col("anomaly_weight") >= 1.0)

        agg_exprs_good = []
        agg_exprs_support = []

        for m in metrics:
            # Distribution stats on good slices only
            agg_exprs_good.extend([
                F.expr(f"percentile_approx({m}, 0.5, 10000)").alias(f"{m}_median"),
                F.expr(f"percentile_approx({m}, 0.25, 10000)").alias(f"{m}_p25"),
                F.expr(f"percentile_approx({m}, 0.75, 10000)").alias(f"{m}_p75"),
                F.expr(f"percentile_approx({m}, 0.95, 10000)").alias(f"{m}_p95"),
                F.expr(f"percentile_approx({m}, 0.99, 10000)").alias(f"{m}_p99"),
            ])
            # Support count across all slices with non-null m
            agg_exprs_support.append(
                F.sum(F.when(F.col(m).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(f"{m}_support_count")
            )

        good_aggs = good_df.groupBy(*group_cols).agg(*agg_exprs_good)
        support_aggs = df.groupBy(*group_cols).agg(*agg_exprs_support)

        # Join good stats and support counts
        joined = good_aggs.join(support_aggs, on=group_cols, how="outer")

        # Compute MAD and IQR
        for m in metrics:
            joined = joined.withColumn(
                f"{m}_iqr", F.col(f"{m}_p75") - F.col(f"{m}_p25")
            )
            # Approx MAD: median(|x - median|) over good slices
            # For simplicity, compute via second pass using window and join back if needed.
            # Here we use IQR as a proxy if MAD isn't materialized.
            mad_col = F.col(f"{m}_iqr") / F.lit(1.349)  # approximate MAD from IQR
            joined = joined.withColumn(f"{m}_mad", mad_col)

            support_min = int(support_cfg.get(m, 50))
            joined = joined.withColumn(
                f"{m}_cold_start_flag",
                F.when(F.col(f"{m}_support_count") < F.lit(support_min), F.lit(1)).otherwise(F.lit(0)),
            )

        # Add horizon and metadata columns
        bounds = self._compute_horizon_bounds(horizon)
        joined = (
            joined
            .withColumn("baseline_horizon", F.lit(bounds["baseline_horizon"]))
            .withColumn("baseline_start_ts", F.lit(bounds["baseline_start_ts"]))
            .withColumn("baseline_end_ts", F.lit(bounds["baseline_end_ts"]))
        )

        # record_id
        joined = joined.withColumn(
            "record_id",
            F.concat_ws(
                "|",
                F.col("host_ip"),
                F.col("role"),
                F.col("segment_id"),
                F.col("time_band"),
                F.col("window_label"),
                F.col("baseline_horizon"),
            ),
        )

        return joined

    # ------------------------------------------------------------------ #
    # Segment-level baselines                                            #
    # ------------------------------------------------------------------ #
    def _build_segment_baselines(self, df: DataFrame, horizon: str) -> DataFrame:
        """Compute segment-level baselines for use as fallbacks.

        Grouping keys:
            (segment_id, role, time_band, window_label)

        Metrics:
            same as host-level baseline metrics, with prefix segment_*
        """
        group_cols = ["segment_id", "role", "time_band", "window_label"]
        metrics = self.job_spec.get("baseline_metrics", [])

        if not metrics:
            candidate_cols = [c for c in df.columns if c not in group_cols + ["anomaly_weight", "max_abs_z_slice", "host_ip"]]
            metrics = candidate_cols

        good_df = df.filter(F.col("anomaly_weight") >= 1.0)

        agg_exprs = []
        for m in metrics:
            agg_exprs.extend([
                F.expr(f"percentile_approx({m}, 0.5, 10000)").alias(f"segment_{m}_median"),
                F.expr(f"percentile_approx(ABS({m} - segment_{m}_median), 0.5, 10000)").alias(f"segment_{m}_mad_temp"),
                F.sum(F.when(F.col(m).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(f"segment_{m}_support_count"),
            ])

        # For simplicity, compute only medians + support_count for segment baselines; MAD derived approximately later if needed.
        seg_aggs = good_df.groupBy(*group_cols).agg(*[
            F.expr(f"percentile_approx({m}, 0.5, 10000)").alias(f"segment_{m}_median")
            for m in metrics
        ])

        support_aggs = df.groupBy(*group_cols).agg(*[
            F.sum(F.when(F.col(m).isNotNull(), F.lit(1))).alias(f"segment_{m}_support_count")
            for m in metrics
        ])

        joined = seg_aggs.join(support_aggs, on=group_cols, how="outer")

        bounds = self._compute_horizon_bounds(horizon)
        joined = (
            joined
            .withColumn("baseline_horizon", F.lit(bounds["baseline_horizon"]))
            .withColumn("baseline_start_ts", F.lit(bounds["baseline_start_ts"]))
            .withColumn("baseline_end_ts", F.lit(bounds["baseline_end_ts"]))
        )

        return joined

    # ------------------------------------------------------------------ #
    # Pair-level rarity baselines                                        #
    # ------------------------------------------------------------------ #
    def _build_pair_rarity_baselines(
        self,
        horizon: str,
        bounds: Dict[str, Any],
        pair_cfg: Dict[str, Any],
    ) -> DataFrame:
        """Build pair-level rarity baselines from pair-counts dataset.

        Expects a pre-aggregated dataset with at least:
            - src_ip
            - dst_ip
            - dst_port
            - event_ts (or day)
            - sessions_cnt

        Uses the same horizon bounds as host baselines.
        """
        s3_prefix = pair_cfg.get("s3_prefix")
        rarity_threshold = float(pair_cfg.get("rarity_threshold", 0.5))

        if not s3_prefix:
            raise ValueError("pair_counts.s3_prefix must be configured when pair_counts.enabled is true.")

        LOGGER.info(
            "Reading pair-counts data for rarity baselines.",
            extra={"prefix": s3_prefix, "horizon": horizon},
        )

        df = self.spark.read.parquet(s3_prefix)
        start_ts = bounds["baseline_start_ts"]
        end_ts = bounds["baseline_end_ts"]

        df = df.filter((F.col("event_ts") >= F.lit(start_ts)) & (F.col("event_ts") < F.lit(end_ts)))

        grouped = df.groupBy("src_ip", "dst_ip", "dst_port").agg(
            F.sum("sessions_cnt").alias("pair_seen_count"),
            F.max("event_ts").alias("pair_last_seen_ts"),
            F.countDistinct(F.to_date("event_ts")).alias("active_days"),
        )

        grouped = grouped.withColumn(
            "pair_daily_avg",
            F.col("pair_seen_count") / F.when(F.col("active_days") > 0, F.col("active_days")).otherwise(F.lit(1.0)),
        )

        grouped = grouped.withColumn(
            "pair_rarity_score",
            1.0 / (1.0 + F.col("pair_seen_count").cast("double")),
        )

        grouped = grouped.withColumn(
            "is_new_pair_flag", F.when(F.col("pair_seen_count") == 0, F.lit(1)).otherwise(F.lit(0))
        )

        grouped = grouped.withColumn(
            "is_rare_pair_flag",
            F.when(F.col("pair_rarity_score") >= F.lit(rarity_threshold), F.lit(1)).otherwise(F.lit(0)),
        )

        grouped = grouped.withColumn("baseline_horizon", F.lit(horizon))

        grouped = grouped.withColumn(
            "pair_key",
            F.concat_ws("|", F.col("src_ip"), F.col("dst_ip"), F.col("dst_port"), F.col("baseline_horizon")),
        )

        return grouped

    # ------------------------------------------------------------------ #
    # Output writing                                                     #
    # ------------------------------------------------------------------ #
    def _write_fg_b_outputs(
        self,
        host_baselines: DataFrame,
        segment_baselines: DataFrame,
        pair_baselines: Optional[DataFrame],
        horizon: str,
    ) -> None:
        """Write FG-B outputs (host, segment, pair baselines) to S3."""
        fg_b_cfg = self.job_spec.get("fg_b_output", {})
        s3_prefix = fg_b_cfg.get("s3_prefix")
        if not s3_prefix:
            raise ValueError("fg_b_output.s3_prefix must be configured in JobSpec for FG-B.")
        base_prefix = build_batch_output_prefix(
            base_prefix=s3_prefix,
            dataset="fg_b",
            batch_start_ts_iso=self.runtime_config.reference_time_iso,
            batch_id=self.runtime_config.batch_id,
        )

        feature_spec_version = self.runtime_config.feature_spec_version

        LOGGER.info(
            "Writing FG-B host-level baselines.",
            extra={"prefix": base_prefix, "horizon": horizon, "feature_spec_version": feature_spec_version},
        )

        host_out = host_baselines.withColumn("feature_spec_version", F.lit(feature_spec_version))
        self.s3_writer.write_parquet(
            df=host_out,
            base_path=f"{base_prefix}/host/",
            partition_cols=["feature_spec_version", "baseline_horizon"],
            mode="overwrite",
        )

        LOGGER.info(
            "Writing FG-B segment-level baselines.",
            extra={"prefix": base_prefix, "horizon": horizon, "feature_spec_version": feature_spec_version},
        )

        segment_out = segment_baselines.withColumn("feature_spec_version", F.lit(feature_spec_version))
        self.s3_writer.write_parquet(
            df=segment_out,
            base_path=f"{base_prefix}/segment/",
            partition_cols=["feature_spec_version", "baseline_horizon"],
            mode="overwrite",
        )

        if pair_baselines is not None:
            LOGGER.info(
                "Writing FG-B pair-level rarity baselines.",
                extra={"prefix": base_prefix, "horizon": horizon, "feature_spec_version": feature_spec_version},
            )
            pair_out = pair_baselines.withColumn("feature_spec_version", F.lit(feature_spec_version))
            self.s3_writer.write_parquet(
                df=pair_out,
                base_path=f"{base_prefix}/pair/",
                partition_cols=["feature_spec_version", "baseline_horizon"],
                mode="overwrite",
            )


def run_fg_b_builder_from_runtime_config(runtime_config: FGBaselineJobRuntimeConfig) -> None:
    """Helper function to run FG-B builder from a typed runtime config."""
    job = FGBaselineBuilderJob(runtime_config=runtime_config)
    job.run()
