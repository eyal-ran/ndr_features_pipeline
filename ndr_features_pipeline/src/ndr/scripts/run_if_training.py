"""Entry script for Isolation Forest training pipeline."""

import argparse
import sys

from ndr.logging.logger import get_logger
from ndr.processing.if_training_job import run_if_training_from_runtime_config
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig

LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse command-line arguments for this script."""
    parser = argparse.ArgumentParser(description="Run Isolation Forest training job.")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--execution-ts-iso", required=True)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Command-line entry point."""
    args = parse_args(argv)
    LOGGER.info(
        "Starting IF training job.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "run_id": args.run_id,
            "execution_ts_iso": args.execution_ts_iso,
        },
    )
    runtime_config = IFTrainingRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        run_id=args.run_id,
        execution_ts_iso=args.execution_ts_iso,
    )
    run_if_training_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
