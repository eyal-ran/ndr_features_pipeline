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
from datetime import datetime, timezone

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
        help="Feature specification version (FG-A/B/C schema version).",
    )
    parser.add_argument(
        "--reference-month",
        required=True,
        help="Reference month in YYYY/MM format (e.g. 2025/12).",
    )
    parser.add_argument(
        "--fg-a-layout",
        default="auto",
        choices=["auto", "wide", "long"],
        help="FG-A input layout mode for FG-B ingestion: auto, wide, or long.",
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Command-line entry point."""
    args = parse_args(argv)

    reference_time_iso = _reference_time_from_month(args.reference_month)
    LOGGER.info(
        "Starting FG-B baseline builder via CLI/runtime entrypoint.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "reference_month": args.reference_month,
            "reference_time_iso": reference_time_iso,
            "fg_a_layout": args.fg_a_layout,
        },
    )

    runtime_config = FGBaselineJobRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        reference_time_iso=reference_time_iso,
        mode="MONTHLY",
        fg_a_layout=args.fg_a_layout,
    )

    run_fg_b_builder_from_runtime_config(runtime_config)
    return 0


def _reference_time_from_month(reference_month: str) -> str:
    """Derive deterministic reference timestamp from month token."""
    try:
        parsed = datetime.strptime(reference_month, "%Y/%m")
    except ValueError as exc:
        raise ValueError("reference_month must use YYYY/MM format.") from exc
    return parsed.replace(
        tzinfo=timezone.utc,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
