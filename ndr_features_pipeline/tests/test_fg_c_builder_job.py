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
        job.job_spec = {"eps": 1e-6, "z_max": 6.0}

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


if __name__ == "__main__":
    unittest.main()
