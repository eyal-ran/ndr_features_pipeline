"""Entry script for the decoupled inference predictions job."""

import argparse
import sys

from ndr.logging.logger import get_logger
from ndr.processing.inference_predictions_job import run_inference_predictions_from_runtime_config
from ndr.processing.inference_predictions_spec import InferencePredictionsRuntimeConfig


LOGGER = get_logger(__name__)


def parse_args(argv=None):
    """Parse command-line arguments for this script."""
    parser = argparse.ArgumentParser(description="Run decoupled inference predictions.")
    parser.add_argument("--project-name", required=True, help="NDR project name.")
    parser.add_argument(
        "--feature-spec-version",
        required=True,
        help="Feature specification version (schema id).",
    )
    parser.add_argument("--mini-batch-id", required=True, help="Mini-batch identifier.")
    parser.add_argument(
        "--batch-start-ts-iso",
        required=True,
        help="Batch start timestamp (ISO8601).",
    )
    parser.add_argument(
        "--batch-end-ts-iso",
        required=True,
        help="Batch end timestamp (ISO8601).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Command-line entry point."""
    args = parse_args(argv)
    LOGGER.info(
        "Starting inference predictions.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "mini_batch_id": args.mini_batch_id,
            "batch_start_ts_iso": args.batch_start_ts_iso,
            "batch_end_ts_iso": args.batch_end_ts_iso,
        },
    )

    runtime_config = InferencePredictionsRuntimeConfig(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        mini_batch_id=args.mini_batch_id,
        batch_start_ts_iso=args.batch_start_ts_iso,
        batch_end_ts_iso=args.batch_end_ts_iso,
    )
    run_inference_predictions_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    sys.exit(main())
