"""Entrypoint for backfill range/family execution contract."""

from __future__ import annotations

import argparse
import json
import sys

from ndr.config.project_parameters_loader import load_project_parameters
from ndr.logging.logger import get_logger
from ndr.orchestration.backfill_execution_contract import build_execution_request
from ndr.orchestration.backfill_family_dispatcher import (
    COMPLETION_ERROR_CODE,
    DISPATCHER_ERROR_CODE,
    BackfillFamilyDispatcher,
)

LOGGER = get_logger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Execute canonical backfill range contract.")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--artifact-family", required=True)
    parser.add_argument("--range-start-ts-iso", required=True)
    parser.add_argument("--range-end-ts-iso", required=True)
    parser.add_argument("--idempotency-key", required=False, default="")
    parser.add_argument("--dpp-config-table-name", required=False, default="")
    parser.add_argument("--retry-attempt", required=False, default="0")
    parser.add_argument("--missing-entries-json", required=False, default="")
    return parser.parse_args(argv)


def _build_missing_entries_payload(*, request, raw_json: str) -> list[dict]:
    if raw_json:
        parsed = json.loads(raw_json)
        if not isinstance(parsed, list):
            raise ValueError(f"{DISPATCHER_ERROR_CODE}: --missing-entries-json must decode to a list")
        return parsed

    entries: list[dict] = []
    for family in request.artifact_families:
        entries.append(
            {
                "family": family,
                "window_start_ts": request.range_start_ts_iso,
                "window_end_ts": request.range_end_ts_iso,
                "batch_ids": [],
                "required_inputs": [],
                "reason_code": "RANGE_REQUEST",
            }
        )
    return entries


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
    retry_attempt = int(args.retry_attempt)
    dpp_spec = load_project_parameters(
        project_name=request.project_name,
        feature_spec_version=request.feature_spec_version,
        dpp_table_name=args.dpp_config_table_name or None,
    )
    missing_entries = _build_missing_entries_payload(request=request, raw_json=args.missing_entries_json or "")

    dispatcher = BackfillFamilyDispatcher(
        project_name=request.project_name,
        feature_spec_version=request.feature_spec_version,
        requested_families=request.artifact_families,
        correlation_id=request.idempotency_key,
        retry_attempt=retry_attempt,
        dpp_spec=dpp_spec,
    )
    dispatch_response = dispatcher.dispatch(missing_entries=missing_entries)

    completion = dispatch_response["completion"]
    overall_status = "Succeeded" if completion["all_succeeded"] else "Failed"
    if not completion["all_succeeded"]:
        raise ValueError(
            f"{COMPLETION_ERROR_CODE}: failed_families={completion['failed_families']} "
            f"unresolved_families={completion['unresolved_families']}"
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
            "retry_attempt": retry_attempt,
            "status": overall_status,
            "family_results": dispatch_response["family_results"],
            "completion": completion,
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
                "retry_attempt": retry_attempt,
                "status": overall_status,
                "family_results": dispatch_response["family_results"],
                "completion": completion,
                "fg_b_baseline_results": [
                    item
                    for item in dispatch_response["family_results"]
                    if item.get("family") == "fg_b_baseline"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
