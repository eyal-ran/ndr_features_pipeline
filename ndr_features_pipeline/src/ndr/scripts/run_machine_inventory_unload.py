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
        "--reference-month-iso",
        required=True,
        help="Reference month (ISO8601, e.g. 2025-12-01T00:00:00Z).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    LOGGER.info(
        "Starting machine inventory unload.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "reference_month_iso": args.reference_month_iso,
        },
    )

    runtime_config = MachineInventoryUnloadRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        reference_month_iso=args.reference_month_iso,
    )
    run_machine_inventory_unload_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
