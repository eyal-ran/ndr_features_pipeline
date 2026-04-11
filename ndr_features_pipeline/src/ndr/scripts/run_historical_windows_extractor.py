"""CLI entrypoint for historical mini-batch window extraction."""

from __future__ import annotations

import argparse
import sys

from ndr.logging.logger import get_logger
from ndr.processing.historical_windows_extractor_job import (
    HistoricalWindowsExtractorRuntimeConfig,
    HistoricalWindowsExtractorJob,
)

LOGGER = get_logger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Extract historical backfill windows from Palo Alto mini-batches.")
    parser.add_argument("--input-s3-prefix", required=True)
    parser.add_argument("--output-s3-prefix", required=True)
    parser.add_argument("--start-ts-iso", required=True)
    parser.add_argument("--end-ts-iso", required=True)
    parser.add_argument("--window-floor-minutes", default="8,23,38,53")
    parser.add_argument("--project-name", required=False)
    parser.add_argument("--feature-spec-version", required=False)
    parser.add_argument("--requested-families", required=False, default="")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    floor_minutes = [int(x.strip()) for x in args.window_floor_minutes.split(",") if x.strip()]
    requested_families = [item.strip() for item in args.requested_families.split(",") if item.strip()]
    runtime = HistoricalWindowsExtractorRuntimeConfig(
        input_s3_prefix=args.input_s3_prefix,
        output_s3_prefix=args.output_s3_prefix,
        start_ts_iso=args.start_ts_iso,
        end_ts_iso=args.end_ts_iso,
        window_floor_minutes=floor_minutes,
        project_name=args.project_name,
        preferred_feature_spec_version=args.feature_spec_version,
        requested_families=requested_families or None,
    )
    out_uri = HistoricalWindowsExtractorJob(runtime).run()
    LOGGER.info("Historical windows manifest written.", extra={"output_uri": out_uri})
    return 0


if __name__ == "__main__":
    sys.exit(main())
