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
    parser.add_argument("--feature-spec-version", required=False)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    floor_minutes = [int(x.strip()) for x in args.window_floor_minutes.split(",") if x.strip()]
    runtime = HistoricalWindowsExtractorRuntimeConfig(
        input_s3_prefix=args.input_s3_prefix,
        output_s3_prefix=args.output_s3_prefix,
        start_ts_iso=args.start_ts_iso,
        end_ts_iso=args.end_ts_iso,
        window_floor_minutes=floor_minutes,
        preferred_feature_spec_version=args.feature_spec_version,
    )
    out_uri = HistoricalWindowsExtractorJob(runtime).run()
    LOGGER.info("Historical windows manifest written.", extra={"output_uri": out_uri})
    return 0


if __name__ == "__main__":
    sys.exit(main())
