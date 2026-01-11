import argparse

from pyspark.sql import SparkSession

from ndr.config.job_spec_loader import JobSpecLoader
from ndr.processing.base_runner import RuntimeParams
from ndr.processing.delta_builder_job import run_delta_builder
from ndr.logging.logger import get_logger


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the delta builder job."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--job-name", required=False, default="delta_builder")
    parser.add_argument("--mini-batch-s3-prefix", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--job-spec-ddb-table-name", required=False)
    parser.add_argument("--slice-start-ts", required=False)
    parser.add_argument("--slice-end-ts", required=False)
    return parser.parse_args()


def main() -> None:
    """Entry point executed inside the SageMaker Spark Processing container."""
    args = parse_args()
    logger = get_logger("run_delta_builder_main")
    logger.info("Parsed arguments: %s", vars(args))

    spark = SparkSession.builder.appName("ndr-delta-builder").getOrCreate()

    loader = JobSpecLoader(table_name=args.job_spec_ddb_table_name)
    job_spec = loader.load(project_name=args.project_name, job_name=args.job_name)

    runtime = RuntimeParams(
        project_name=args.project_name,
        job_name=args.job_name,
        mini_batch_s3_prefix=args.mini_batch_s3_prefix,
        feature_spec_version=args.feature_spec_version,
        run_id=args.run_id,
        slice_start_ts=args.slice_start_ts or None,
        slice_end_ts=args.slice_end_ts or None,
    )

    run_delta_builder(spark=spark, job_spec=job_spec, runtime=runtime)
    spark.stop()


if __name__ == "__main__":
    main()
