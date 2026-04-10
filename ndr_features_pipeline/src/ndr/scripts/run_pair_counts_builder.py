"""Entry script for Pair-Counts builder.

Intended to be used as the command for a SageMaker ProcessingStep container.
In production, the SageMaker Pipeline will pass the same parameters via
`processing_step.arguments`. CLI parsing is mainly for local dev & debug.

Expected arguments (all required):

- --project-name          : Logical NDR project name (for JobSpec lookup).
- --feature-spec-version  : Feature-spec version (FG schema id).
- --mini-batch-id         : Identifier of the 15m ETL mini-batch.
- --raw-parsed-logs-s3-prefix : Canonical authoritative S3 pointer for this mini-batch.
- --batch-start-ts-iso    : ISO8601 start timestamp of the batch window.
- --batch-end-ts-iso      : ISO8601 end timestamp of the batch window.
"""

import argparse
import sys

from ndr.processing.pair_counts_builder_job import (
    PairCountsJobRuntimeConfig,
    run_pair_counts_builder_from_runtime_config,
)
from ndr.logging.logger import get_logger


LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse CLI arguments for Pair-Counts builder."""
    parser = argparse.ArgumentParser(description="Run Pair-Counts builder.")

    parser.add_argument(
        "--project-name",
        required=True,
        help="NDR project name used for JobSpec lookup.",
    )
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="Feature specification version (FG-A/B/C schema version).",        )
    parser.add_argument(
        "--mini-batch-id",
        required=True,
        help="Identifier of the 15m ETL mini-batch (used to locate S3 input prefix).",        )
    parser.add_argument(
        "--raw-parsed-logs-s3-prefix",
        default="",
        help="Canonical S3 prefix for this mini-batch (must end with /<mini_batch_id>/).",
    )
    parser.add_argument(
        "--batch-start-ts-iso",
        required=True,
        help="Batch start time (ISO8601, e.g. 2025-12-31T00:00:00Z).",        )
    parser.add_argument(
        "--batch-end-ts-iso",
        required=True,
        help="Batch end time (ISO8601, e.g. 2025-12-31T00:15:00Z).",        )
    parser.add_argument(
        "--batch-index-table-name",
        default="",
        help="Optional Batch Index table override used to resolve canonical raw/pair-counts prefixes.",
    )
    parser.add_argument(
        "--dpp-config-table-name",
        default="",
        help="Optional DPP config table override used for fallback policy/query resolution.",
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Command-line entry point."""
    args = parse_args(argv)

    LOGGER.info(
        "Starting Pair-Counts builder via CLI/runtime entrypoint.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "mini_batch_id": args.mini_batch_id,
            "raw_parsed_logs_s3_prefix": args.raw_parsed_logs_s3_prefix,
            "batch_start_ts_iso": args.batch_start_ts_iso,
            "batch_end_ts_iso": args.batch_end_ts_iso,
            "batch_index_table_name": args.batch_index_table_name,
            "dpp_config_table_name": args.dpp_config_table_name,
        },
    )

    runtime_config = PairCountsJobRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        mini_batch_id=args.mini_batch_id,
        raw_parsed_logs_s3_prefix=args.raw_parsed_logs_s3_prefix,
        batch_start_ts_iso=args.batch_start_ts_iso,
        batch_end_ts_iso=args.batch_end_ts_iso,
        batch_index_table_name=args.batch_index_table_name or None,
        dpp_config_table_name=args.dpp_config_table_name or None,
    )

    run_pair_counts_builder_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
