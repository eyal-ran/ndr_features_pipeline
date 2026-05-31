"""CLI entrypoint for authoritative realtime artifact readiness evaluation."""

from __future__ import annotations
import argparse
import json
import sys
from ndr.processing.rt_readiness_checker_job import (
    RtReadinessCheckerRuntimeConfig,
    run_rt_readiness_checker,
)


def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--project-name", required=True)
    p.add_argument("--feature-spec-version", required=True)
    p.add_argument("--ml-project-name", required=True)
    p.add_argument("--mini-batch-id", required=True)
    p.add_argument("--batch-start-ts-iso", required=True)
    p.add_argument("--batch-end-ts-iso", required=True)
    p.add_argument("--readiness-cycle", required=True, type=int)
    p.add_argument("--artifacts-bucket-name", required=True)
    p.add_argument("--batch-index-table-name", default="")
    p.add_argument("--dpp-config-table-name", default="")
    return p.parse_args(argv)


def main(argv=None):
    a = parse_args(argv)
    result = run_rt_readiness_checker(
        RtReadinessCheckerRuntimeConfig(
            a.project_name,
            a.feature_spec_version,
            a.ml_project_name,
            a.mini_batch_id,
            a.batch_start_ts_iso,
            a.batch_end_ts_iso,
            a.readiness_cycle,
            a.artifacts_bucket_name,
            a.batch_index_table_name or None,
            a.dpp_config_table_name or None,
        )
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
