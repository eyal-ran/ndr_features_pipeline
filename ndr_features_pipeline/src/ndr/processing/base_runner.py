"""NDR base runner module."""
from dataclasses import dataclass

from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame

from ndr.config.job_spec_models import JobSpec
from ndr.io.s3_reader import S3Reader
from ndr.io.s3_writer import S3Writer
from ndr.logging.logger import get_logger


class BaseRunner:
    """Minimal base class for processing jobs that manage their own flow."""

    def __init__(self) -> None:
        """Initialize the instance with required clients and runtime configuration."""
        self.logger = get_logger(self.__class__.__name__)


class BaseProcessingJobRunner:
    """Base class for Spark-based jobs that manage orchestration internally."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the instance with required clients and runtime configuration."""
        self.spark = spark
        self.logger = get_logger(self.__class__.__name__)


@dataclass
class RuntimeParams:
    """Runtime parameters passed from Step Functions / Pipeline."""

    project_name: str
    job_name: str
    mini_batch_s3_prefix: str
    feature_spec_version: str
    run_id: str
    slice_start_ts: str | None = None
    slice_end_ts: str | None = None
    extra: Dict[str, Any] | None = None


class BaseProcessingRunner:
    """Shared orchestration skeleton for all NDR processing jobs."""

    def __init__(self, spark: SparkSession, job_spec: JobSpec, runtime: RuntimeParams):
        """Initialize the instance with required clients and runtime configuration."""
        self.spark = spark
        self.job_spec = job_spec
        self.runtime = runtime
        self.logger = get_logger(f"{job_spec.job_name}-runner")
        self.reader = S3Reader(spark)
        self.writer = S3Writer()

    def run(self) -> None:
        """Execute the full job flow: read, DQ, transform, write."""
        self.logger.info(
            "Starting job %s run_id=%s", self.job_spec.job_name, self.runtime.run_id
        )
        df_raw = self._read_input()
        df_clean = self._apply_data_quality(df_raw)
        df_result = self._build_dataframe(df_clean)
        self._write_output(df_result)
        self.logger.info(
            "Completed job %s run_id=%s", self.job_spec.job_name, self.runtime.run_id
        )

    def _read_input(self) -> DataFrame:
        """Read input from S3 using the JobSpec input configuration."""
        input_spec = self.job_spec.input
        return self.reader.read_jsonlines_gzip(
            prefix=self.runtime.mini_batch_s3_prefix,
            projection=input_spec.schema_projection,
        )

    def _apply_data_quality(self, df: DataFrame) -> DataFrame:
        """Apply generic DQ rules shared across jobs (no-op by default)."""
        return df

    def _build_dataframe(self, df: DataFrame) -> DataFrame:
        """Concrete subclasses must implement this transformation."""
        raise NotImplementedError

    def _write_output(self, df: DataFrame) -> None:
        """Write the final DataFrame according to the JobSpec output config."""
        output = self.job_spec.output
        self.writer.write_parquet_partitioned(
            df=df,
            base_prefix=output.s3_prefix,
            partition_cols=output.partition_keys,
            mode=output.write_mode,
        )
