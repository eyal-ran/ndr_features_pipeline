"""Entry script for FG-A (current behaviour) feature builder.

This script is intended to be used as the command for a SageMaker
ProcessingStep container. It parses runtime parameters (also convenient
for local CLI runs) and delegates to the FG-A builder implementation.

Expected arguments (all required):

- --project-name          : Logical NDR project name (for JobSpec lookup).
- --feature-spec-version  : FG-A feature-spec version (schema id).
- --mini-batch-id         : Identifier of the 15m ETL mini-batch.
- --batch-start-ts-iso    : ISO8601 start timestamp of the batch window.
- --batch-end-ts-iso      : ISO8601 end timestamp of the batch window.
"""

import argparse
import sys

from ndr.processing.fg_a_builder_job import (
    FGABuilderJobRuntimeConfig,
    run_fg_a_builder_from_runtime_config,
)
from ndr.logging.logger import get_logger


LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse CLI arguments for FG-A builder."""
    parser = argparse.ArgumentParser(description="Run FG-A builder job.")

    parser.add_argument(
        "--project-name",
        required=True,
        help="Logical NDR project name used for JobSpec lookup.",
    )
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="FG-A feature specification version (schema id).",        )
    parser.add_argument(
        "--mini-batch-id",
        required=True,
        help="Identifier of the 15m ETL mini-batch triggering FG-A.",        )
    parser.add_argument(
        "--batch-start-ts-iso",
        required=True,
        help="Batch start time (ISO8601, e.g. 2025-12-31T00:00:00Z).",        )
    parser.add_argument(
        "--batch-end-ts-iso",
        required=True,
        help="Batch end time (ISO8601, e.g. 2025-12-31T00:15:00Z).",        )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Main entrypoint for FG-A builder."""
    args = parse_args(argv)

    LOGGER.info(
        "Starting FG-A builder via CLI/runtime entrypoint.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "mini_batch_id": args.mini_batch_id,
            "batch_start_ts_iso": args.batch_start_ts_iso,
            "batch_end_ts_iso": args.batch_end_ts_iso,
        },
    )

    runtime_config = FGABuilderJobRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        mini_batch_id=args.mini_batch_id,
        batch_start_ts_iso=args.batch_start_ts_iso,
        batch_end_ts_iso=args.batch_end_ts_iso,
    )

    run_fg_a_builder_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
