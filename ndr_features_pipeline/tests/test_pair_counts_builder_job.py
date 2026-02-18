import sys
import types

# Stub pyspark for import-time compatibility in unit tests.
pyspark_module = types.ModuleType("pyspark")
sql_module = types.ModuleType("pyspark.sql")
functions_module = types.ModuleType("pyspark.sql.functions")
types_module = types.ModuleType("pyspark.sql.types")


class _DummySparkType:
    def __call__(self, *args, **kwargs):
        return self


for _name in [
    "StringType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "TimestampType",
    "DateType",
    "BooleanType",
    "StructType",
    "StructField",
]:
    setattr(types_module, _name, _DummySparkType())


class _SparkSession:
    pass

class _DataFrame:
    pass

sql_module.SparkSession = _SparkSession
sql_module.DataFrame = _DataFrame
sql_module.functions = functions_module
sql_module.types = types_module
pyspark_module.sql = sql_module

sys.modules.setdefault("boto3", types.ModuleType("boto3"))

sys.modules.setdefault("pyspark", pyspark_module)
sys.modules.setdefault("pyspark.sql", sql_module)
sys.modules.setdefault("pyspark.sql.functions", functions_module)
sys.modules.setdefault("pyspark.sql.types", types_module)

"""Unit tests for PairCountsBuilderJob.

These tests mock Spark and JobSpec loading to validate wiring and mapping behavior.
"""

import unittest
from unittest.mock import patch, MagicMock

from ndr.processing.pair_counts_builder_job import (
    PairCountsJobRuntimeConfig,
    PairCountsBuilderJob,
)
from ndr.config.job_spec_models import (
    PairCountsJobSpec,
    PairCountsTrafficInputSpec,
    PairCountsOutputSpec,
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

    @patch("ndr.processing.pair_counts_builder_job.load_pair_counts_job_spec")
    def test_read_traffic_for_batch_path(self, mock_load_job_spec):
        mock_load_job_spec.return_value = PairCountsJobSpec(
            traffic_input=PairCountsTrafficInputSpec(
                s3_prefix="s3://integration/parsed/",
                layout="batch_folder",
                field_mapping={
                    "source_ip": "source_ip",
                    "destination_ip": "destination_ip",
                    "destination_port": "destination_port",
                    "event_start": "event_start",
                    "event_end": "event_end",
                },
            ),
            pair_counts_output=PairCountsOutputSpec(
                s3_prefix="s3://ndr-features/pair_counts/"
            ),
        )

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
            args, _ = spark.read.json.call_args
            self.assertIn("batch_20251231T0000Z", args[0])


if __name__ == "__main__":
    unittest.main()
