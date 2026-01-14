"""
FG-C correlation feature builder job for NDR pipeline.

FG-C computes correlation / comparison features between "current behaviour"
(FG-A) and baselines (FG-B). It is executed as a Spark-based SageMaker
Processing job, typically as a step in the 15m streaming pipeline.

Design highlights
-----------------
* Runtime parameters (project_name, feature_spec_version, mini_batch_id,
  batch_start_ts_iso, batch_end_ts_iso) are passed via the CLI wrapper
  (run_fg_c_builder.py) from the SageMaker Pipeline / Step Functions.

* Structural configuration (S3 prefixes, list of metrics, join keys,
  horizons, thresholds, etc.) is loaded from the JobSpec table in DynamoDB
  via `ndr.config.job_spec_loader.load_job_spec` with job_name="fg_c_builder".

* FG-C reads FG-A (current) and FG-B (baseline) Parquet datasets from S3,
  joins them on configurable keys, and derives correlation features such as:
    - z-scores (MAD-based)
    - ratio vs. median
    - absolute deviation vs. MAD / IQR
    - magnifier transforms (clipped z, signed power, log-ratio, etc.)
  for each configured baseline horizon (e.g. 7d, 30d).

* Output is written as a wide Parquet table keyed by
    (host_ip, window_label, window_end_ts, baseline_horizon)
  and partitioned by (feature_spec_version, baseline_horizon, dt) where
  dt = date(window_end_ts). The table is suitable for registration as the
  offline store of an FG-C feature group in SageMaker Feature Store.
"""

from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ndr.logging.logger import get_logger
from ndr.config.job_spec_loader import load_job_spec
from ndr.processing.base_runner import BaseRunner


LOGGER = get_logger(__name__)


@dataclass
class FGCorrJobRuntimeConfig:
    """Runtime config passed from Step Functions / Pipeline to FG-C builder.

    Attributes
    ----------
    project_name : str
        Logical NDR project identifier (used for JobSpec lookup, logging).
    feature_spec_version : str
        Version string for the FG schema (FG-A/B/C).
    mini_batch_id : str
        ETL mini-batch identifier (15m slice, aligns with delta + FG-A).
    batch_start_ts_iso : str
        Batch window start time, ISO8601 (e.g. "2025-12-31T12:00:00Z").
    batch_end_ts_iso : str
        Batch window end time, ISO8601 (exclusive, same format).
    """

    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str


class FGCorrBuilderJob(BaseRunner):
    """FG-C correlation feature builder.

    High-level flow
    ----------------
    1. Load JobSpec for job_name="fg_c_builder".
    2. Build SparkSession.
    3. For each configured baseline_horizon (e.g. "7d", "30d"):
       a. Read FG-A current features for the given batch window.
       b. Read FG-B baselines for the same feature_spec_version & horizon.
       c. Join FG-A and FG-B on configured join keys (e.g. host_ip, window_label).
       d. For each configured metric:
          - compute MAD-based z-score,
          - ratio_vs_median,
          - abs_dev_over_mad,
          - clipped z and signed-power magnifier,
          - log-ratio magnifier.
       e. Add metadata (record_id, mini_batch_id, baseline_horizon,
          baseline_start_ts, baseline_end_ts).
       f. Write FG-C rows to S3 as Parquet.
    4. Stop Spark.
    """

    def __init__(self, runtime_config: FGCorrJobRuntimeConfig) -> None:
        super().__init__()
        self.runtime_config = runtime_config
        self.spark: Optional[SparkSession] = None
        self.job_spec: Dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # BaseRunner entry point                                             #
    # ------------------------------------------------------------------ #
    def run(self) -> None:  # type: ignore[override]
        """Execute the FG-C builder job with logging and error handling."""
        LOGGER.info(
            "FG-C correlation builder job started.",
            extra={
                "project_name": self.runtime_config.project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "mini_batch_id": self.runtime_config.mini_batch_id,
                "batch_start_ts_iso": self.runtime_config.batch_start_ts_iso,
                "batch_end_ts_iso": self.runtime_config.batch_end_ts_iso,
            },
        )
        try:
            # 1. Load JobSpec from DynamoDB / config source
            self.job_spec = load_job_spec(
                project_name=self.runtime_config.project_name,
                job_name="fg_c_builder",
                feature_spec_version=self.runtime_config.feature_spec_version,
            )
            LOGGER.info("Loaded JobSpec for FG-C builder.", extra={"job_spec_keys": list(self.job_spec.keys())})

            # 2. Build SparkSession
            self.spark = self._build_spark_session()

            # 3. Core processing: compute FG-C for each horizon
            self._process_all_horizons()

            LOGGER.info("FG-C correlation builder job completed successfully.")
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.error("FG-C correlation builder job failed: %s", exc, exc_info=True)
            traceback.print_exc(file=sys.stderr)
            raise
        finally:
            if self.spark is not None:
                self.spark.stop()
                LOGGER.info("SparkSession stopped.")

    # ------------------------------------------------------------------ #
    # Spark & config helpers                                             #
    # ------------------------------------------------------------------ #
    def _build_spark_session(self) -> SparkSession:
        """Create or get a SparkSession for the Processing job."""
        LOGGER.info("Building SparkSession for FG-C builder.")
        spark = (
            SparkSession.builder.appName("fg_c_correlation_builder")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        return spark

    def _get_horizons(self) -> List[str]:
        """Return list of baseline horizons to process (e.g. ["7d", "30d"])."""
        horizons = self.job_spec.get("horizons")
        if not horizons:
            horizons = ["7d", "30d"]
        return horizons

    def _get_join_keys(self) -> List[str]:
        """Return join keys between FG-A and FG-B.

        By default we use (host_ip, window_label). Additional keys (e.g.
        role, segment_id, time_band) can be configured in JobSpec under
        join_keys if the FG-B baselines are more granular.
        """
        default_keys = ["host_ip", "window_label"]
        join_keys = self.job_spec.get("join_keys", default_keys)
        return join_keys

    def _get_metrics_to_compare(self, fg_a_df: DataFrame) -> List[str]:
        """Return the list of FG-A metrics that should be compared to baselines.

        The JobSpec may explicitly define metrics under "metrics". If not,
        a heuristic fallback is used: all non-key numeric columns that have
        a corresponding *_median baseline in FG-B.
        """
        metrics = self.job_spec.get("metrics", [])
        if metrics:
            return metrics

        # Fallback: pick numeric FG-A columns that look like metrics
        key_cols = {"host_ip", "window_label", "window_start_ts", "window_end_ts", "record_id"}
        candidates: List[str] = []
        for name, dtype in fg_a_df.dtypes:
            if name in key_cols:
                continue
            if dtype not in ("double", "bigint", "long", "int", "float"):
                continue
            # Heuristic: ignore obvious flags, they can still be added explicitly via JobSpec
            if name.endswith("_flag"):
                continue
            candidates.append(name)

        LOGGER.info(
            "Inferred FG-C metrics from FG-A schema (can be overridden via JobSpec.metrics).",
            extra={"metric_count": len(candidates)},
        )
        return candidates

    # ------------------------------------------------------------------ #
    # Core horizon processing                                            #
    # ------------------------------------------------------------------ #
    def _process_all_horizons(self) -> None:
        """Compute FG-C features for all configured baseline horizons."""
        horizons = self._get_horizons()
        for horizon in horizons:
            LOGGER.info("Processing FG-C for horizon '%s'.", horizon)
            fg_c_df = self._build_fg_c_for_horizon(horizon)
            self._write_fg_c_for_horizon(fg_c_df, horizon)

    def _build_fg_c_for_horizon(self, horizon: str) -> DataFrame:
        """Build FG-C features for a single baseline horizon.

        Parameters
        ----------
        horizon : str
            Baseline horizon label, e.g. "7d" or "30d".

        Returns
        -------
        DataFrame
            Wide FG-C feature frame partition-ready for S3 write.
        """
        assert self.spark is not None, "SparkSession must be initialized before building FG-C."

        fg_a_cfg = self.job_spec.get("fg_a_input", {})
        fg_b_cfg = self.job_spec.get("fg_b_input", {})
        if not fg_a_cfg or not fg_b_cfg:
            raise ValueError("JobSpec for fg_c_builder must define 'fg_a_input' and 'fg_b_input' sections.")

        fg_a_prefix = fg_a_cfg.get("s3_prefix")
        fg_b_prefix = fg_b_cfg.get("s3_prefix")
        if not fg_a_prefix or not fg_b_prefix:
            raise ValueError("fg_a_input.s3_prefix and fg_b_input.s3_prefix must be configured in JobSpec.")

        feature_spec_version = self.runtime_config.feature_spec_version

        # ------------------------------------------------------------------
        # 1. Read FG-A (current) rows for this batch window
        # ------------------------------------------------------------------
        LOGGER.info(
            "Reading FG-A data.",
            extra={
                "prefix": fg_a_prefix,
                "feature_spec_version": feature_spec_version,
                "batch_start_ts_iso": self.runtime_config.batch_start_ts_iso,
                "batch_end_ts_iso": self.runtime_config.batch_end_ts_iso,
            },
        )
        fg_a_df = (
            self.spark.read.parquet(fg_a_prefix)
            .filter(F.col("feature_spec_version") == F.lit(feature_spec_version))
        )

        # Filter by window_end_ts in [batch_start, batch_end)
        start_ts = self.runtime_config.batch_start_ts_iso
        end_ts = self.runtime_config.batch_end_ts_iso
        fg_a_df = fg_a_df.filter(
            (F.col("window_end_ts") >= F.lit(start_ts)) & (F.col("window_end_ts") < F.lit(end_ts))
        )

        if fg_a_df.rdd.isEmpty():
            LOGGER.warning("No FG-A rows found for batch window; FG-C will be empty for horizon '%s'.", horizon)
            # Build an empty DataFrame with the expected key columns so the write step is safe
            empty_schema = T.StructType(
                [
                    T.StructField("host_ip", T.StringType(), True),
                    T.StructField("window_label", T.StringType(), True),
                    T.StructField("window_start_ts", T.TimestampType(), True),
                    T.StructField("window_end_ts", T.TimestampType(), True),
                    T.StructField("baseline_horizon", T.StringType(), True),
                    T.StructField("record_id", T.StringType(), True),
                    T.StructField("mini_batch_id", T.StringType(), True),
                    T.StructField("feature_spec_version", T.StringType(), True),
                ]
            )
            return self.spark.createDataFrame([], schema=empty_schema)

        join_keys = self._get_join_keys()

        # ------------------------------------------------------------------
        # 2. Read FG-B baselines and filter by horizon
        # ------------------------------------------------------------------
        LOGGER.info(
            "Reading FG-B baselines.",
            extra={
                "prefix": fg_b_prefix,
                "feature_spec_version": feature_spec_version,
                "horizon": horizon,
            },
        )
        fg_b_df = (
            self.spark.read.parquet(fg_b_prefix)
            .filter(F.col("feature_spec_version") == F.lit(feature_spec_version))
            .filter(F.col("baseline_horizon") == F.lit(horizon))
        )

        if fg_b_df.rdd.isEmpty():
            LOGGER.warning("No FG-B baselines found for horizon '%s'; FG-C will be empty.", horizon)
            empty_schema = T.StructType(
                [
                    T.StructField("host_ip", T.StringType(), True),
                    T.StructField("window_label", T.StringType(), True),
                    T.StructField("window_start_ts", T.TimestampType(), True),
                    T.StructField("window_end_ts", T.TimestampType(), True),
                    T.StructField("baseline_horizon", T.StringType(), True),
                    T.StructField("record_id", T.StringType(), True),
                    T.StructField("mini_batch_id", T.StringType(), True),
                    T.StructField("feature_spec_version", T.StringType(), True),
                ]
            )
            return self.spark.createDataFrame([], schema=empty_schema)

        # ------------------------------------------------------------------
        # 3. Join FG-A and FG-B
        # ------------------------------------------------------------------
        LOGGER.info("Joining FG-A and FG-B on keys: %s", join_keys)
        joined = fg_a_df.join(fg_b_df, on=join_keys, how="left")

        # ------------------------------------------------------------------
        # 4. Build correlation features for configured metrics
        # ------------------------------------------------------------------
        metrics = self._get_metrics_to_compare(fg_a_df)
        joined = self._compute_correlation_features(joined, metrics)

        # Add baseline meta columns (use FG-B values if present)
        joined = joined.withColumn("baseline_horizon", F.lit(horizon))
        if "baseline_start_ts" in fg_b_df.columns:
            joined = joined.withColumn("baseline_start_ts", F.col("baseline_start_ts"))
        else:
            joined = joined.withColumn("baseline_start_ts", F.lit(None).cast("timestamp"))

        if "baseline_end_ts" in fg_b_df.columns:
            joined = joined.withColumn("baseline_end_ts", F.col("baseline_end_ts"))
        else:
            joined = joined.withColumn("baseline_end_ts", F.lit(None).cast("timestamp"))

        # Record id for FG-C
        joined = joined.withColumn(
            "record_id",
            F.concat_ws(
                "|",
                F.col("host_ip"),
                F.col("window_label"),
                F.col("window_end_ts"),
                F.col("baseline_horizon"),
            ),
        )

        # Attach mini_batch_id and feature_spec_version
        joined = joined.withColumn("mini_batch_id", F.lit(self.runtime_config.mini_batch_id))
        joined = joined.withColumn("feature_spec_version", F.lit(feature_spec_version))

        return joined

    def _compute_correlation_features(self, df: DataFrame, metrics: List[str]) -> DataFrame:
        """Compute FG-C correlation features for each metric.

        For each metric m in `metrics`, the following features are derived
        (assuming FG-B baselines provide m_median, m_mad, m_iqr):

            diff_m          = m - m_median
            ratio_m         = m / (m_median + eps)
            z_mad_m         = diff_m / (m_mad + eps)
            abs_dev_over_mad_m = |diff_m| / (m_mad + eps)
            z_mad_clipped_m = clip(z_mad_m, -z_max, +z_max)
            z_mad_signed_pow3_m = sign(z_mad_clipped_m) * |z_mad_clipped_m|^3
            log_ratio_m     = log(ratio_m + eps)

        These correspond to families described in the FG-C specification:
        - z-scores
        - ratios / relative deviation
        - magnifiers (bounded transforms)
        """
        eps = float(self.job_spec.get("eps", 1e-6))
        z_max = float(self.job_spec.get("z_max", 6.0))

        for m in metrics:
            median_col = f"{m}_median"
            mad_col = f"{m}_mad"
            iqr_col = f"{m}_iqr"

            if median_col not in df.columns:
                LOGGER.warning("Baseline column '%s' missing; skipping metric '%s' for FG-C.", median_col, m)
                continue

            # Current value
            cur = F.col(m)

            # Median and MAD/IQR from baseline
            median = F.col(median_col)
            mad = F.when(F.col(mad_col).isNotNull() & (F.col(mad_col) > 0), F.col(mad_col)).otherwise(None)
            iqr = F.when(F.col(iqr_col).isNotNull() & (F.col(iqr_col) > 0), F.col(iqr_col)).otherwise(None)

            diff = cur - median
            ratio = cur / (median + F.lit(eps))

            # Primary z-score based on MAD; if MAD is null/zero, fall back to IQR
            z_mad = F.when(mad.isNotNull(), diff / (mad + F.lit(eps))).otherwise(
                F.when(iqr.isNotNull(), diff / (iqr + F.lit(eps))).otherwise(F.lit(0.0))
            )

            # Derived correlation & magnifier features
            df = df.withColumn(f"{m}_diff", diff)
            df = df.withColumn(f"{m}_ratio", ratio)
            df = df.withColumn(f"{m}_z_mad", z_mad)
            df = df.withColumn(f"{m}_abs_dev_over_mad", F.abs(diff) / (mad + F.lit(eps)))

            z_clipped = F.when(z_mad > F.lit(z_max), F.lit(z_max)).when(
                z_mad < F.lit(-z_max), F.lit(-z_max)
            ).otherwise(z_mad)
            df = df.withColumn(f"{m}_z_mad_clipped", z_clipped)

            signed_pow3 = F.signum(z_clipped) * F.pow(F.abs(z_clipped), F.lit(3.0))
            df = df.withColumn(f"{m}_z_mad_signed_pow3", signed_pow3)

            df = df.withColumn(f"{m}_log_ratio", F.log(ratio + F.lit(eps)))

        return df

    # ------------------------------------------------------------------ #
    # Write results                                                      #
    # ------------------------------------------------------------------ #
    def _write_fg_c_for_horizon(self, df: DataFrame, horizon: str) -> None:
        """Write FG-C features for a single horizon to S3 as Parquet.

        Output layout
        -------------
        base_path = JobSpec["fg_c_output"]["s3_prefix"]

        Partition columns:
            - feature_spec_version
            - baseline_horizon
            - dt = date(window_end_ts)

        The job uses overwrite mode for the horizon + dt partitions of this
        mini-batch. If stronger idempotency is required, consider including
        mini_batch_id into the partitioning scheme as well.
        """
        fg_c_cfg = self.job_spec.get("fg_c_output", {})
        base_path = fg_c_cfg.get("s3_prefix")
        if not base_path:
            raise ValueError("fg_c_output.s3_prefix must be configured in JobSpec for fg_c_builder.")

        if df.rdd.isEmpty():
            LOGGER.warning("FG-C DataFrame is empty for horizon '%s'; nothing to write.", horizon)
            return

        # Derive dt partition from window_end_ts
        df = df.withColumn("dt", F.to_date(F.col("window_end_ts")))
        df = df.withColumn("baseline_horizon", F.lit(horizon))

        LOGGER.info(
            "Writing FG-C features.",
            extra={
                "output_path": base_path,
                "horizon": horizon,
                "partitions": ["feature_spec_version", "baseline_horizon", "dt"],
            },
        )

        (
            df.write.mode("overwrite")
            .partitionBy("feature_spec_version", "baseline_horizon", "dt")
            .parquet(base_path)
        )


def run_fg_c_builder_from_runtime_config(runtime_config: FGCorrJobRuntimeConfig) -> None:
    """Helper to run FG-C builder from a typed runtime config."""
    job = FGCorrBuilderJob(runtime_config=runtime_config)
    job.run()
