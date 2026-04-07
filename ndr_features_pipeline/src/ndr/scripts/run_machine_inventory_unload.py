"""Entry script for Machine Inventory Unload job."""

import argparse
import sys

from ndr.logging.logger import get_logger
from ndr.processing.machine_inventory_unload_job import (
    MachineInventoryUnloadRuntimeConfig,
    run_machine_inventory_unload_from_runtime_config,
)


LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse command-line arguments for this script."""
    parser = argparse.ArgumentParser(
        description="Unload active machine inventory from Redshift to S3."
    )
    parser.add_argument(
        "--project-name",
        required=True,
        help="NDR project name used for JobSpec lookup.",
    )
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="Feature specification version (schema id).",
    )
    parser.add_argument(
        "--reference-month",
        required=True,
        help="Reference month in YYYY/MM format (e.g. 2025/12).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Command-line entry point."""
    args = parse_args(argv)
    LOGGER.info(
        "Starting machine inventory unload.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "reference_month": args.reference_month,
        },
    )

    runtime_config = MachineInventoryUnloadRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        reference_month=args.reference_month,
    )
    run_machine_inventory_unload_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
