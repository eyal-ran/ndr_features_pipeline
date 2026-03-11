"""Entry script for Delta Builder Processing job.

This script is intended to be used as the command for a SageMaker
ProcessingStep container. It parses runtime parameters (for local CLI
use as well) and delegates to the delta-builder job implementation.

In production, the SageMaker Pipeline definition is expected to supply
the same parameters via `processing_step.arguments`, so the CLI parsing
here is mostly for local development and ad‑hoc debugging.

Expected arguments (all required):

- --project-name          : Logical NDR project name (for JobSpec lookup).
- --feature-spec-version  : Feature-spec version (schema id for delta/FGs).
- --mini-batch-id         : Identifier of the 15m ETL mini-batch.
- --raw-parsed-logs-s3-prefix : Canonical authoritative S3 pointer for this mini-batch.
- --mini-batch-s3-prefix       : Legacy alias for compatibility.
- --batch-start-ts-iso    : ISO8601 start timestamp of the batch window.
- --batch-end-ts-iso      : ISO8601 end timestamp of the batch window.
"""

import argparse
import sys

from ndr.processing.delta_builder_job import (
    DeltaBuilderJobRuntimeConfig,
    run_delta_builder_from_runtime_config,
)
from ndr.logging.logger import get_logger
from ndr.runtime_field_aliases import resolve_with_legacy_alias


LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse CLI arguments for Delta Builder.

    Parameters
    ----------
    argv : list[str] | None
        Optional override for ``sys.argv``; mainly for unit tests.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Run Delta Builder job.")

    parser.add_argument(
        "--project-name",
        required=True,
        help="NDR project name used for JobSpec lookup.",
    )
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="Feature specification version (delta/FG schema id).",        )
    parser.add_argument(
        "--mini-batch-id",
        required=True,
        help="Identifier of the 15m ETL mini-batch (used to locate inputs).",        )
    parser.add_argument(
        "--raw-parsed-logs-s3-prefix",
        default="",
        help="Canonical S3 prefix for this mini-batch (must end with /<mini_batch_id>/).",
    )
    parser.add_argument(
        "--mini-batch-s3-prefix",
        default="",
        help="Legacy alias for --raw-parsed-logs-s3-prefix.",
    )
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
    """Main entrypoint for Delta Builder.

    This function is executed when the module is run as a script inside the
    SageMaker Processing container.
    """
    args = parse_args(argv)

    raw_parsed_logs_s3_prefix = resolve_with_legacy_alias(
        canonical_value=args.raw_parsed_logs_s3_prefix,
        legacy_value=args.mini_batch_s3_prefix,
        canonical_name="raw_parsed_logs_s3_prefix",
        legacy_name="mini_batch_s3_prefix",
        context="run_delta_builder.parse_args",
    )
    if not raw_parsed_logs_s3_prefix:
        raise ValueError("raw_parsed_logs_s3_prefix is required")

    LOGGER.info(
        "Starting Delta Builder via CLI/runtime entrypoint.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "mini_batch_id": args.mini_batch_id,
            "raw_parsed_logs_s3_prefix": raw_parsed_logs_s3_prefix,
            "batch_start_ts_iso": args.batch_start_ts_iso,
            "batch_end_ts_iso": args.batch_end_ts_iso,
        },
    )

    runtime_config = DeltaBuilderJobRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        mini_batch_id=args.mini_batch_id,
        raw_parsed_logs_s3_prefix=raw_parsed_logs_s3_prefix,
        batch_start_ts_iso=args.batch_start_ts_iso,
        batch_end_ts_iso=args.batch_end_ts_iso,
    )

    run_delta_builder_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
