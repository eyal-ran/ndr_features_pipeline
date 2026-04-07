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
    parser.add_argument("--ml-project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dpp-config-table-name", required=True)
    parser.add_argument("--mlp-config-table-name", required=True)
    parser.add_argument("--batch-index-table-name", required=True)
    parser.add_argument("--execution-ts-iso", required=True)
    parser.add_argument("--training-start-ts")
    parser.add_argument("--training-end-ts")
    parser.add_argument("--eval-start-ts")
    parser.add_argument("--eval-end-ts")
    parser.add_argument(
        "--stage",
        default="train",
        choices=["verify", "plan", "remediate", "reverify", "train", "publish", "attributes", "deploy"],
    )
    return parser.parse_args(argv)



def main(argv=None) -> int:
    """Command-line entry point."""
    args = parse_args(argv)
    LOGGER.info(
        "Starting IF training job.",
        extra={
            "project_name": args.project_name,
            "feature_spec_version": args.feature_spec_version,
            "ml_project_name": args.ml_project_name,
            "run_id": args.run_id,
            "dpp_config_table_name": args.dpp_config_table_name,
            "mlp_config_table_name": args.mlp_config_table_name,
            "batch_index_table_name": args.batch_index_table_name,
            "execution_ts_iso": args.execution_ts_iso,
            "training_start_ts": args.training_start_ts,
            "training_end_ts": args.training_end_ts,
            "eval_start_ts": args.eval_start_ts,
            "eval_end_ts": args.eval_end_ts,
            "stage": args.stage,
        },
    )
    runtime_config = IFTrainingRuntimeConfig(
        project_name=args.project_name,
        ml_project_name=args.ml_project_name,
        feature_spec_version=args.feature_spec_version,
        run_id=args.run_id,
        execution_ts_iso=args.execution_ts_iso,
        dpp_config_table_name=args.dpp_config_table_name,
        mlp_config_table_name=args.mlp_config_table_name,
        batch_index_table_name=args.batch_index_table_name,
        training_start_ts=args.training_start_ts,
        training_end_ts=args.training_end_ts,
        eval_start_ts=args.eval_start_ts,
        eval_end_ts=args.eval_end_ts,
        stage=args.stage,
    )
    run_if_training_from_runtime_config(runtime_config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
