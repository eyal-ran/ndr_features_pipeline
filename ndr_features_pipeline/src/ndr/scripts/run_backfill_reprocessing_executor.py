"""Entrypoint for backfill range/family execution contract."""

from __future__ import annotations

import argparse
import json
import sys

from ndr.logging.logger import get_logger
from ndr.orchestration.backfill_execution_contract import build_execution_request

LOGGER = get_logger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Execute canonical backfill range contract.")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--artifact-family", required=True)
    parser.add_argument("--range-start-ts-iso", required=True)
    parser.add_argument("--range-end-ts-iso", required=True)
    parser.add_argument("--idempotency-key", required=False, default="")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    request = build_execution_request(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        artifact_family=args.artifact_family,
        range_start_ts_iso=args.range_start_ts_iso,
        range_end_ts_iso=args.range_end_ts_iso,
        idempotency_key=args.idempotency_key or None,
    )

    LOGGER.info(
        "Resolved backfill execution request.",
        extra={
            "contract_version": request.contract_version,
            "project_name": request.project_name,
            "feature_spec_version": request.feature_spec_version,
            "artifact_families": list(request.artifact_families),
            "range_start_ts_iso": request.range_start_ts_iso,
            "range_end_ts_iso": request.range_end_ts_iso,
            "idempotency_key": request.idempotency_key,
            "status": "Succeeded",
        },
    )

    print(
        json.dumps(
            {
                "contract_version": request.contract_version,
                "project_name": request.project_name,
                "feature_spec_version": request.feature_spec_version,
                "artifact_families": list(request.artifact_families),
                "range_start_ts_iso": request.range_start_ts_iso,
                "range_end_ts_iso": request.range_end_ts_iso,
                "idempotency_key": request.idempotency_key,
                "status": "Succeeded",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
