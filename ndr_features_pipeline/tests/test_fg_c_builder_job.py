"""
Unit tests for FG-C correlation builder.

These tests are intentionally minimal and focus on the core correlation logic
implemented in `FGCorrBuilderJob._compute_correlation_features`. They use a
local SparkSession and synthetic data to validate the math for a small set of
metrics, and do not touch S3 / DynamoDB.
"""

from __future__ import annotations

import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ndr.processing.fg_c_builder_job import FGCorrBuilderJob, FGCorrJobRuntimeConfig


class TestFGCCorrelationBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("fgc_test")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_compute_correlation_features_basic(self) -> None:
        """Validate basic MAD-based z-score and ratio logic for one metric."""
        # Synthetic FG-A current values and FG-B baseline stats for a single metric "m"
        schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("window_start_ts", T.TimestampType(), True),
                T.StructField("window_end_ts", T.TimestampType(), True),
                T.StructField("m", T.DoubleType(), True),
                T.StructField("m_median", T.DoubleType(), True),
                T.StructField("m_mad", T.DoubleType(), True),
                T.StructField("m_iqr", T.DoubleType(), True),
            ]
        )

        rows = [
            ("10.0.0.1", "w_15m", None, None, 20.0, 10.0, 5.0, 10.0),  # clear deviation
        ]

        df = self.spark.createDataFrame(rows, schema=schema)

        # Minimal runtime config & job to access _compute_correlation_features
        runtime_cfg = FGCorrJobRuntimeConfig(
            project_name="test-project",
            feature_spec_version="v1",
            mini_batch_id="test-batch",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
        )
        job = FGCorrBuilderJob(runtime_config=runtime_cfg)
        # Provide minimal job_spec so eps/z_max are available
        job.job_spec = {
            "eps": 1e-6,
            "z_max": 6.0,
            "transforms": [
                "diff",
                "ratio",
                "z_mad",
                "abs_dev_over_mad",
                "z_mad_clipped",
                "z_mad_signed_pow3",
                "log_ratio",
            ],
        }

        out = job._compute_correlation_features(df, metrics=["m"])
        result = out.collect()[0]

        # Expectations:
        # diff = 20 - 10 = 10
        # ratio ≈ 20 / 10 = 2.0
        # z_mad ≈ 10 / 5 = 2.0
        self.assertAlmostEqual(result.m_diff, 10.0, places=6)
        self.assertAlmostEqual(result.m_ratio, 2.0, places=6)
        self.assertAlmostEqual(result.m_z_mad, 2.0, places=6)
        self.assertAlmostEqual(result.m_z_mad_clipped, 2.0, places=6)

        # Check signed power magnifier: sign(2.0) * |2.0|^3 = 8.0
        self.assertAlmostEqual(result.m_z_mad_signed_pow3, 8.0, places=6)

        # log_ratio ≈ log(2)
        self.assertAlmostEqual(result.m_log_ratio, 0.6931, places=3)

    def test_transform_subset_limits_outputs(self) -> None:
        schema = T.StructType(
            [
                T.StructField("m", T.DoubleType(), True),
                T.StructField("m_median", T.DoubleType(), True),
                T.StructField("m_mad", T.DoubleType(), True),
                T.StructField("m_iqr", T.DoubleType(), True),
            ]
        )
        df = self.spark.createDataFrame([(5.0, 10.0, 2.0, 4.0)], schema=schema)

        runtime_cfg = FGCorrJobRuntimeConfig(
            project_name="test-project",
            feature_spec_version="v1",
            mini_batch_id="test-batch",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
        )
        job = FGCorrBuilderJob(runtime_config=runtime_cfg)
        job.job_spec = {"transforms": ["z_mad"]}

        out = job._compute_correlation_features(df, metrics=["m"])
        self.assertIn("m_z_mad", out.columns)
        self.assertNotIn("m_ratio", out.columns)
        self.assertNotIn("m_log_ratio", out.columns)

    def test_metrics_default_merge_with_job_spec(self) -> None:
        schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("sessions_cnt_w_15m", T.DoubleType(), True),
                T.StructField("custom_metric", T.DoubleType(), True),
            ]
        )
        df = self.spark.createDataFrame(
            [("10.0.0.1", "w_15m", 10.0, 2.0)],
            schema=schema,
        )

        runtime_cfg = FGCorrJobRuntimeConfig(
            project_name="test-project",
            feature_spec_version="v1",
            mini_batch_id="test-batch",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
        )
        job = FGCorrBuilderJob(runtime_config=runtime_cfg)
        job.job_spec = {"metrics": ["custom_metric"]}

        metrics = job._get_metrics_to_compare(df)
        self.assertIn("sessions_cnt_w_15m", metrics)
        self.assertIn("custom_metric", metrics)

    def test_suspicion_features_compute(self) -> None:
        schema = T.StructType(
            [
                T.StructField("metric", T.DoubleType(), True),
                T.StructField("metric_median", T.DoubleType(), True),
                T.StructField("metric_p95", T.DoubleType(), True),
                T.StructField("metric_iqr", T.DoubleType(), True),
                T.StructField("metric_z_mad", T.DoubleType(), True),
            ]
        )
        df = self.spark.createDataFrame([(10.0, 5.0, 8.0, 2.0, 2.5)], schema=schema)

        runtime_cfg = FGCorrJobRuntimeConfig(
            project_name="test-project",
            feature_spec_version="v1",
            mini_batch_id="test-batch",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
        )
        job = FGCorrBuilderJob(runtime_config=runtime_cfg)
        job.job_spec = {
            "eps": 1e-6,
            "suspicion_metrics": ["metric"],
            "anomaly_strength_metrics": ["metric"],
        }

        out = job._compute_suspicion_features(df)
        row = out.collect()[0]
        self.assertAlmostEqual(row.metric_excess_over_p95, 2.0, places=6)
        self.assertAlmostEqual(row.metric_excess_ratio_p95, 1.25, places=2)
        self.assertAlmostEqual(row.metric_iqr_dev, 2.5, places=6)
        self.assertAlmostEqual(row.anomaly_strength_core, 2.5, places=6)

    def test_cold_start_baseline_selection(self) -> None:
        runtime_cfg = FGCorrJobRuntimeConfig(
            project_name="test-project",
            feature_spec_version="v1",
            mini_batch_id="test-batch",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
        )
        job = FGCorrBuilderJob(runtime_config=runtime_cfg)
        job.job_spec = {
            "join_keys": ["host_ip", "window_label"],
            "segment_join_keys": ["segment_id", "window_label"],
            "pair_counts": {"enabled": True},
        }

        fg_a_schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("segment_id", T.StringType(), True),
                T.StructField("dst_ip", T.StringType(), True),
                T.StructField("dst_port", T.IntegerType(), True),
                T.StructField("m", T.DoubleType(), True),
            ]
        )
        fg_a_rows = [
            ("10.0.0.1", "w_15m", "seg-a", "8.8.8.8", 53, 5.0),
            ("10.0.0.2", "w_15m", "seg-b", "1.1.1.1", 443, 5.0),
        ]
        fg_a_df = self.spark.createDataFrame(fg_a_rows, schema=fg_a_schema)

        metadata_schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("baseline_horizon", T.StringType(), True),
                T.StructField("is_cold_start", T.IntegerType(), True),
            ]
        )
        metadata_rows = [
            ("10.0.0.1", "w_15m", "7d", 0),
            ("10.0.0.2", "w_15m", "7d", 1),
        ]
        metadata_df = self.spark.createDataFrame(metadata_rows, schema=metadata_schema)

        baseline_schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("baseline_horizon", T.StringType(), True),
                T.StructField("m_median", T.DoubleType(), True),
                T.StructField("m_p25", T.DoubleType(), True),
                T.StructField("m_p75", T.DoubleType(), True),
                T.StructField("m_p95", T.DoubleType(), True),
                T.StructField("m_p99", T.DoubleType(), True),
                T.StructField("m_mad", T.DoubleType(), True),
                T.StructField("m_iqr", T.DoubleType(), True),
                T.StructField("m_support_count", T.IntegerType(), True),
            ]
        )
        host_baselines = self.spark.createDataFrame(
            [("10.0.0.1", "w_15m", "7d", 10.0, 1.0, 2.0, 3.0, 4.0, 1.0, 1.0, 100)],
            schema=baseline_schema,
        )

        segment_schema = T.StructType(
            [
                T.StructField("segment_id", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("baseline_horizon", T.StringType(), True),
                T.StructField("m_median", T.DoubleType(), True),
                T.StructField("m_p25", T.DoubleType(), True),
                T.StructField("m_p75", T.DoubleType(), True),
                T.StructField("m_p95", T.DoubleType(), True),
                T.StructField("m_p99", T.DoubleType(), True),
                T.StructField("m_mad", T.DoubleType(), True),
                T.StructField("m_iqr", T.DoubleType(), True),
                T.StructField("m_support_count", T.IntegerType(), True),
            ]
        )
        segment_baselines = self.spark.createDataFrame(
            [("seg-b", "w_15m", "7d", 100.0, 10.0, 20.0, 30.0, 40.0, 5.0, 5.0, 50)],
            schema=segment_schema,
        )

        pair_host_schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("dst_ip", T.StringType(), True),
                T.StructField("dst_port", T.IntegerType(), True),
                T.StructField("baseline_horizon", T.StringType(), True),
                T.StructField("pair_seen_count", T.IntegerType(), True),
                T.StructField("pair_last_seen_ts", T.TimestampType(), True),
                T.StructField("active_days", T.IntegerType(), True),
                T.StructField("pair_daily_avg", T.DoubleType(), True),
                T.StructField("pair_rarity_score", T.DoubleType(), True),
                T.StructField("is_new_pair_flag", T.IntegerType(), True),
                T.StructField("is_rare_pair_flag", T.IntegerType(), True),
            ]
        )
        pair_host_df = self.spark.createDataFrame(
            [("10.0.0.1", "8.8.8.8", 53, "7d", 7, None, 1, 7.0, 0.2, 0, 1)],
            schema=pair_host_schema,
        )

        pair_segment_schema = T.StructType(
            [
                T.StructField("segment_id", T.StringType(), True),
                T.StructField("dst_ip", T.StringType(), True),
                T.StructField("dst_port", T.IntegerType(), True),
                T.StructField("baseline_horizon", T.StringType(), True),
                T.StructField("pair_seen_count", T.IntegerType(), True),
                T.StructField("pair_last_seen_ts", T.TimestampType(), True),
                T.StructField("active_days", T.IntegerType(), True),
                T.StructField("pair_daily_avg", T.DoubleType(), True),
                T.StructField("pair_rarity_score", T.DoubleType(), True),
                T.StructField("is_new_pair_flag", T.IntegerType(), True),
                T.StructField("is_rare_pair_flag", T.IntegerType(), True),
            ]
        )
        pair_segment_df = self.spark.createDataFrame(
            [("seg-b", "1.1.1.1", 443, "7d", 3, None, 1, 3.0, 0.5, 0, 1)],
            schema=pair_segment_schema,
        )

        joined = job._apply_cold_start_baselines(
            fg_a_df=fg_a_df,
            metadata_df=metadata_df,
            host_baselines=host_baselines,
            segment_baselines=segment_baselines,
            pair_host_df=pair_host_df,
            pair_segment_df=pair_segment_df,
            horizon="7d",
        )

        results = {row.host_ip: row for row in joined.select("host_ip", "m_median", "pair_seen_count").collect()}
        self.assertEqual(results["10.0.0.1"].m_median, 10.0)
        self.assertEqual(results["10.0.0.1"].pair_seen_count, 7)
        self.assertEqual(results["10.0.0.2"].m_median, 100.0)
        self.assertEqual(results["10.0.0.2"].pair_seen_count, 3)


if __name__ == "__main__":
    unittest.main()
