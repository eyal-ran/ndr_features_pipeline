"""Entry script for FG-B baseline builder.

This script is intended to be used as the command for a SageMaker ProcessingStep
container. It parses runtime parameters (for local CLI use as well) and then
delegates to FGBaselineBuilderJob.

In production, the SageMaker Pipeline definition is expected to supply the same
parameters via `processing_step.arguments`, so the CLI parsing here is mostly
for local development and ad-hoc debug.
"""

import argparse
import sys

from ndr.processing.fg_b_builder_job import (
    FGBaselineJobRuntimeConfig,
    run_fg_b_builder_from_runtime_config,
)
from ndr.logging.logger import get_logger


LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse CLI arguments for FG-B builder.

    Parameters
    ----------
    argv : list[str], optional
        Override for sys.argv, primarily for unit tests.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Run FG-B baseline builder.")

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
        "--reference-time-iso",
        required=True,
        help=(
            "Reference time (ISO8601, e.g. 2025-12-31T00:00:00Z) "
            "for baseline horizons (7d/30d)."
        ),
    )
    parser.add_argument(
        "--mode",
        default="REGULAR",
        choices=["REGULAR", "BACKFILL"],
        help="Execution mode: REGULAR (default) or BACKFILL.",
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    LOGGER.info(
        "Starting FG-B baseline builder via CLI/runtime entrypoint.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "reference_time_iso": args.reference_time_iso,
            "mode": args.mode,
        },
    )

    runtime_config = FGBaselineJobRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        reference_time_iso=args.reference_time_iso,
        mode=args.mode,
    )

    run_fg_b_builder_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
