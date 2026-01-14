
"""Unit tests for PairCountsBuilderJob.

These tests are intentionally light and mock out Spark and JobSpec loading,
to validate wiring and basic flow (not Spark aggregation correctness).
"""

import unittest
from unittest.mock import patch, MagicMock

from ndr.processing.pair_counts_builder_job import (
    PairCountsJobRuntimeConfig,
    PairCountsBuilderJob,
)


class TestPairCountsBuilderJob(unittest.TestCase):
    def test_runtime_config_basic(self):
        cfg = PairCountsJobRuntimeConfig(
            project_name="ndr_project",
            feature_spec_version="v1",
            mini_batch_id="batch_20251231T0000Z",
            batch_start_ts_iso="2025-12-31T00:00:00Z",
            batch_end_ts_iso="2025-12-31T00:15:00Z",
        )
        self.assertEqual(cfg.project_name, "ndr_project")
        self.assertEqual(cfg.feature_spec_version, "v1")
        self.assertEqual(cfg.mini_batch_id, "batch_20251231T0000Z")

    @patch("ndr.processing.pair_counts_builder_job.load_job_spec")
    def test_read_traffic_for_batch_path(self, mock_load_job_spec):
        mock_load_job_spec.return_value = {
            "traffic_input": {
                "s3_prefix": "s3://integration/parsed/",
                "layout": "batch_folder",
            },
            "pair_counts_output": {
                "s3_prefix": "s3://ndr-features/pair_counts/"
            },
        }

        # Minimal Spark mocking
        with patch("ndr.processing.pair_counts_builder_job.SparkSession") as mock_spark_cls:
            spark = MagicMock()
            mock_spark_cls.builder.appName.return_value.config.return_value.getOrCreate.return_value = spark

            df_mock = MagicMock()
            df_mock.columns = [
                "source_ip",
                "destination_ip",
                "destination_port",
                "event_start",
                "event_end",
            ]
            spark.read.json.return_value = df_mock

            cfg = PairCountsJobRuntimeConfig(
                project_name="ndr_project",
                feature_spec_version="v1",
                mini_batch_id="batch_20251231T0000Z",
                batch_start_ts_iso="2025-12-31T00:00:00Z",
                batch_end_ts_iso="2025-12-31T00:15:00Z",
            )

            job = PairCountsBuilderJob(runtime_config=cfg)
            job.job_spec = mock_load_job_spec.return_value
            job.spark = spark

            df_out = job._read_traffic_for_batch()
            self.assertIs(df_out, df_mock)
            spark.read.json.assert_called_once()
            args, kwargs = spark.read.json.call_args
            self.assertIn("batch_20251231T0000Z", args[0])


if __name__ == "__main__":
    unittest.main()
