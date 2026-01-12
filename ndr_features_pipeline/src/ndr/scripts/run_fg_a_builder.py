
"""Script entrypoint for FG-A builder Processing job.

This script is intended to be used as the `code` entry point for a
SageMaker ProcessingStep. It parses CLI arguments (provided as
`job_arguments` in the pipeline definition), constructs the FGABuilderConfig,
and executes the FGABuilderJob.

Expected usage (within a SageMaker Processing container):

    python -m ndr.scripts.run_fg_a_builder \
        --project-name my-ndr-project \
        --region-name eu-west-1 \
        --delta-s3-prefix s3://my-bucket/deltas/ \
        --output-s3-prefix s3://my-bucket/fg_a/ \
        --mini-batch-id mb_20250101T1200Z \
        --feature-spec-version fga_v1 \
        --feature-group-offline ndr-fga-offline \
        --write-to-feature-store true
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession

from ndr.processing.fg_a_builder_job import FGABuilderConfig, FGABuilderJob
from ndr.logging.logger import configure_root_logger  # assuming existing helper


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments provided by the ProcessingStep job_arguments."""
    parser = argparse.ArgumentParser(description="Run FG-A builder job")

    parser.add_argument("--project-name", required=True, help="Logical project name")
    parser.add_argument("--region-name", required=True, help="AWS region name")

    parser.add_argument(
        "--delta-s3-prefix",
        required=True,
        help="S3 prefix where delta Parquet files are stored",
    )
    parser.add_argument(
        "--output-s3-prefix",
        required=True,
        help="S3 prefix where FG-A Parquet files will be written",
    )
    parser.add_argument(
        "--mini-batch-id",
        required=True,
        help="Identifier of the triggering ETL mini-batch",
    )
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="FG-A feature spec version (e.g., fga_v1)",
    )

    parser.add_argument(
        "--feature-group-offline",
        required=False,
        default=None,
        help="Offline Feature Store feature group name (optional)",
    )
    parser.add_argument(
        "--feature-group-online",
        required=False,
        default=None,
        help="Online Feature Store feature group name (optional)",
    )
    parser.add_argument(
        "--write-to-feature-store",
        required=False,
        default="false",
        help="Whether to ingest FG-A rows into Feature Store (true/false)",
    )

    return parser.parse_args()


def create_spark_session(app_name: str = "fg_a_builder") -> SparkSession:
    """Create a SparkSession suitable for a SageMaker Processing job."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()


def main() -> None:
    # Configure logging (integrated with CloudWatch via SageMaker).
    configure_root_logger()
    logger = logging.getLogger(__name__)

    args = parse_args()

    logger.info("Starting FG-A builder Processing job with args: %s", vars(args))

    spark = create_spark_session()

    write_to_fs = str(args.write_to_feature_store).lower() in {"1", "true", "yes"}

    config = FGABuilderConfig(
        project_name=args.project_name,
        region_name=args.region_name,
        delta_s3_prefix=args.delta_s3_prefix,
        output_s3_prefix=args.output_s3_prefix,
        mini_batch_id=args.mini_batch_id,
        feature_spec_version=args.feature_spec_version,
        feature_group_name_offline=args.feature_group_offline,
        feature_group_name_online=args.feature_group_online,
        write_to_feature_store=write_to_fs,
    )

    job = FGABuilderJob(spark=spark, config=config)
    job.run()

    logger.info("FG-A builder Processing job completed successfully")


if __name__ == "__main__":
    main()
