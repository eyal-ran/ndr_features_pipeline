"""
CLI entrypoint for FG-C correlation feature builder.

This script is invoked as a SageMaker Processing entrypoint, usually via
a ProcessingStep in a SageMaker Pipeline that is itself triggered from
AWS Step Functions.

Contract (agreed):
------------------
Required CLI arguments:
  --project-name
  --feature-spec-version
  --mini-batch-id
  --batch-start-ts-iso
  --batch-end-ts-iso

All structural configuration (S3 prefixes, baseline horizons, metric
lists, thresholds, etc.) is loaded from the JobSpec table inside the
FG-C builder job itself (fg_c_builder_job.FGCorrBuilderJob).

Example:
  python -m ndr.scripts.run_fg_c_builder \
    --project-name ndr-project \
    --feature-spec-version v1 \
    --mini-batch-id 2025-12-31T12:00:00Z \
    --batch-start-ts-iso 2025-12-31T12:00:00Z \
    --batch-end-ts-iso 2025-12-31T12:15:00Z
"""

from __future__ import annotations

import argparse

from ndr.logging.logger import get_logger
from ndr.processing.fg_c_builder_job import (
    FGCorrJobRuntimeConfig,
    run_fg_c_builder_from_runtime_config,
)

LOGGER = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for FG-C builder."""
    parser = argparse.ArgumentParser(description="Run FG-C correlation feature builder.")

    parser.add_argument(
        "--project-name",
        required=True,
        help="Logical project identifier used for JobSpec lookup (e.g. 'ndr-project').",
    )
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="Feature specification version (e.g. 'v1').",
    )
    parser.add_argument(
        "--mini-batch-id",
        required=True,
        help="Mini-batch identifier aligning with the 15m ETL cadence.",
    )
    parser.add_argument(
        "--batch-start-ts-iso",
        required=True,
        help="Batch window start timestamp (ISO8601, e.g. 2025-12-31T12:00:00Z).",
    )
    parser.add_argument(
        "--batch-end-ts-iso",
        required=True,
        help="Batch window end timestamp (exclusive, ISO8601).",
    )

    return parser.parse_args()


def main() -> None:
    """Main entrypoint used by SageMaker Processing."""
    args = _parse_args()

    LOGGER.info(
        "Starting FG-C builder from CLI.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "mini_batch_id": args.mini_batch_id,
            "batch_start_ts_iso": args.batch_start_ts_iso,
            "batch_end_ts_iso": args.batch_end_ts_iso,
        },
    )

    runtime_cfg = FGCorrJobRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        mini_batch_id=args.mini_batch_id,
        batch_start_ts_iso=args.batch_start_ts_iso,
        batch_end_ts_iso=args.batch_end_ts_iso,
    )

    run_fg_c_builder_from_runtime_config(runtime_cfg)
    LOGGER.info("FG-C builder finished successfully.")


if __name__ == "__main__":
    main()
