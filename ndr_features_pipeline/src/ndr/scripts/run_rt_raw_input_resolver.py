"""CLI entrypoint for RT pre-delta raw input resolver."""

from __future__ import annotations

import argparse
import json
import sys

from ndr.processing.rt_raw_input_resolver_job import (
    RtRawInputResolverRuntimeConfig,
    run_rt_raw_input_resolver,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run RT pre-delta raw input resolver")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--mini-batch-id", required=True)
    parser.add_argument("--batch-start-ts-iso", required=True)
    parser.add_argument("--batch-end-ts-iso", required=True)
    parser.add_argument("--raw-parsed-logs-s3-prefix", default="")
    parser.add_argument("--batch-index-table-name", default="")
    parser.add_argument("--dpp-config-table-name", default="")
    parser.add_argument("--request-id", default="")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    result = run_rt_raw_input_resolver(
        RtRawInputResolverRuntimeConfig(
            project_name=args.project_name,
            feature_spec_version=args.feature_spec_version,
            mini_batch_id=args.mini_batch_id,
            batch_start_ts_iso=args.batch_start_ts_iso,
            batch_end_ts_iso=args.batch_end_ts_iso,
            raw_parsed_logs_s3_prefix=args.raw_parsed_logs_s3_prefix,
            batch_index_table_name=args.batch_index_table_name or None,
            dpp_config_table_name=args.dpp_config_table_name or None,
            request_id=args.request_id or None,
        )
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
