
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

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from ndr.processing.fg_b_builder_job import (
    FGBaselineJobRuntimeConfig,
    FGBaselineBuilderJob,
)


class TestFGBaselineBuilderJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("fgb_test")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

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
    @patch("ndr.processing.fg_b_builder_job.load_project_parameters")
    @patch("ndr.processing.fg_b_builder_job.load_job_spec")
    @patch("ndr.processing.fg_b_builder_job.SparkSession")
    def test_run_initialization(
        self,
        mock_spark,
        mock_load_job_spec,
        mock_load_project_parameters,
        mock_writer_cls,
        mock_reader_cls,
    ):
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
        mock_load_project_parameters.return_value = {"ip_machine_mapping_s3_prefix": "s3://dummy/mapping/"}

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
        mock_load_project_parameters.assert_called_once()
        mock_spark.builder.appName.assert_called_with("fg_b_baseline_builder")

    def test_build_ip_metadata_flags(self):
        cfg = FGBaselineJobRuntimeConfig(
            project_name="ndr_project",
            feature_spec_version="v1",
            reference_time_iso="2025-12-31T00:00:00Z",
        )
        job = FGBaselineBuilderJob(runtime_config=cfg)
        job.spark = self.spark
        job.job_spec = {
            "join_keys": ["host_ip", "window_label"],
            "support_min": {"sessions_cnt": 50},
            "baseline_required_metrics": ["sessions_cnt"],
            "non_persistent_machine_prefixes": ["vdi-"],
        }
        job.project_parameters = {"ip_machine_mapping_s3_prefix": "s3://dummy/mapping/"}

        schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("window_label", T.StringType(), True),
                T.StructField("baseline_horizon", T.StringType(), True),
                T.StructField("sessions_cnt_support_count", T.IntegerType(), True),
            ]
        )
        rows = [
            ("10.0.0.1", "w_15m", "7d", 100),
            ("10.0.0.2", "w_15m", "7d", 10),
        ]
        host_df = self.spark.createDataFrame(rows, schema=schema)

        mapping_schema = T.StructType(
            [
                T.StructField("host_ip", T.StringType(), True),
                T.StructField("machine_name", T.StringType(), True),
            ]
        )
        mapping_rows = [
            ("10.0.0.1", "VDI-123"),
            ("10.0.0.2", "server-01"),
        ]
        mapping_df = self.spark.createDataFrame(mapping_rows, schema=mapping_schema)

        with patch.object(job, "_load_ip_machine_mapping", return_value=(mapping_df, "machine_name")):
            flags = job._build_ip_metadata_flags(host_df, "7d").collect()

        by_host = {row.host_ip: row for row in flags}
        self.assertEqual(by_host["10.0.0.1"].is_full_history, 1)
        self.assertEqual(by_host["10.0.0.1"].is_non_persistent_machine, 1)
        self.assertEqual(by_host["10.0.0.1"].is_cold_start, 1)

        self.assertEqual(by_host["10.0.0.2"].is_full_history, 0)
        self.assertEqual(by_host["10.0.0.2"].is_non_persistent_machine, 0)
        self.assertEqual(by_host["10.0.0.2"].is_cold_start, 1)


if __name__ == "__main__":
    unittest.main()
