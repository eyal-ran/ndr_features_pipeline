
"""Unit tests for FGBaselineBuilderJob.

These tests are intentionally lightweight and focus on:

- RuntimeConfig construction
- JobSpec loading integration point (mocked)
- Horizon bounds computation

Heavy Spark aggregations should be covered by integration tests in a
dedicated environment with real data.
"""

import unittest
from unittest.mock import patch, MagicMock

from ndr.processing.fg_b_builder_job import (
    FGBaselineJobRuntimeConfig,
    FGBaselineBuilderJob,
)


class TestFGBaselineBuilderJob(unittest.TestCase):
    def test_runtime_config_basic(self):
        cfg = FGBaselineJobRuntimeConfig(
            project_name="ndr_project",
            feature_spec_version="v1",
            reference_time_iso="2025-12-31T12:00:00Z",
            mode="REGULAR",
        )
        self.assertEqual(cfg.project_name, "ndr_project")
        self.assertEqual(cfg.feature_spec_version, "v1")
        self.assertEqual(cfg.reference_time_iso, "2025-12-31T12:00:00Z")
        self.assertEqual(cfg.mode, "REGULAR")

    @patch("ndr.processing.fg_b_builder_job.load_job_spec")
    def test_compute_horizon_bounds(self, mock_load_job_spec):
        mock_load_job_spec.return_value = {
            "horizons": ["7d"],
            "safety_gaps": {
                "7d": {"tail_days": 2, "head_days": 2}
            },
        }

        cfg = FGBaselineJobRuntimeConfig(
            project_name="ndr_project",
            feature_spec_version="v1",
            reference_time_iso="2025-12-31T00:00:00Z",
        )
        job = FGBaselineBuilderJob(runtime_config=cfg)
        job.job_spec = mock_load_job_spec.return_value

        bounds = job._compute_horizon_bounds("7d")
        self.assertIn("baseline_start_ts", bounds)
        self.assertIn("baseline_end_ts", bounds)
        self.assertEqual(bounds["baseline_horizon"], "7d")

    @patch("ndr.processing.fg_b_builder_job.S3Reader")
    @patch("ndr.processing.fg_b_builder_job.S3Writer")
    @patch("ndr.processing.fg_b_builder_job.load_job_spec")
    @patch("ndr.processing.fg_b_builder_job.SparkSession")
    def test_run_initialization(self, mock_spark, mock_load_job_spec, mock_writer_cls, mock_reader_cls):
        mock_load_job_spec.return_value = {
            "horizons": ["7d"],
            "safety_gaps": {"7d": {"tail_days": 2, "head_days": 2}},
            "fg_a_input": {"s3_prefix": "s3://dummy/fg_a/"},
            "fg_b_output": {"s3_prefix": "s3://dummy/fg_b/"},
            "anomaly_capping": {
                "key_metrics": ["sessions_cnt"],
                "z_max": 6.0,
                "w_anom": 0.1,
            },
            "support_min": {"sessions_cnt": 10},
            "segment_mapping": {"strategy": "ipv4_prefix", "prefix_length": 24},
            "baseline_metrics": ["sessions_cnt"],
        }

        spark_instance = MagicMock()
        mock_spark.builder.appName.return_value.config.return_value.getOrCreate.return_value = spark_instance

        # Mock FG-A DataFrame and all transformation calls to avoid heavy Spark logic
        df_mock = MagicMock()
        spark_instance.read.parquet.return_value = df_mock
        df_mock.filter.return_value = df_mock
        df_mock.withColumn.return_value = df_mock
        df_mock.groupBy.return_value.agg.return_value = df_mock
        df_mock.join.return_value = df_mock

        cfg = FGBaselineJobRuntimeConfig(
            project_name="ndr_project",
            feature_spec_version="v1",
            reference_time_iso="2025-12-31T00:00:00Z",
        )

        job = FGBaselineBuilderJob(runtime_config=cfg)
        job.run()

        mock_load_job_spec.assert_called_once()
        mock_spark.builder.appName.assert_called_with("fg_b_baseline_builder")


if __name__ == "__main__":
    unittest.main()
