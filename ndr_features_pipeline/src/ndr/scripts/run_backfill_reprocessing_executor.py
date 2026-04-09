"""Entrypoint for backfill range/family execution contract."""

from __future__ import annotations

import argparse
import json
import sys

import boto3

from ndr.config.project_parameters_loader import load_project_parameters
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
    parser.add_argument("--dpp-config-table-name", required=False, default="")
    return parser.parse_args(argv)


def _resolve_fgb_pipeline_name(
    *,
    project_name: str,
    feature_spec_version: str,
    dpp_config_table_name: str | None,
) -> str:
    spec = load_project_parameters(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        dpp_table_name=dpp_config_table_name,
    )
    orchestration_targets = spec.get("orchestration_targets") or {}
    pipeline_name = str(orchestration_targets.get("fg_b_baseline") or "").strip()
    if not pipeline_name:
        raise ValueError(
            "BACKFILL_FGB_TARGET_MISSING: missing required DDB target "
            "spec.orchestration_targets.fg_b_baseline"
        )
    return pipeline_name


def _dispatch_fgb_baseline(
    *,
    project_name: str,
    feature_spec_version: str,
    range_start_ts_iso: str,
    range_end_ts_iso: str,
    idempotency_key: str,
    dpp_config_table_name: str | None,
) -> dict:
    pipeline_name = _resolve_fgb_pipeline_name(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        dpp_config_table_name=dpp_config_table_name,
    )
    sagemaker = boto3.client("sagemaker")
    try:
        response = sagemaker.start_pipeline_execution(
            PipelineName=pipeline_name,
            PipelineExecutionDescription=(
                f"backfill_reprocessing family=fg_b_baseline "
                f"idempotency_key={idempotency_key} "
                f"range={range_start_ts_iso}..{range_end_ts_iso}"
            ),
            PipelineParameters=[
                {"Name": "ProjectName", "Value": project_name},
                {"Name": "FeatureSpecVersion", "Value": feature_spec_version},
                {"Name": "ReferenceTimeIso", "Value": range_start_ts_iso},
                {"Name": "Mode", "Value": "baseline"},
            ],
            ClientRequestToken=idempotency_key,
        )
        return {
            "family": "fg_b_baseline",
            "status": "Started",
            "pipeline_name": pipeline_name,
            "pipeline_execution_arn": response["PipelineExecutionArn"],
            "reference_time_iso": range_start_ts_iso,
            "range_end_ts_iso": range_end_ts_iso,
        }
    except Exception as exc:  # pragma: no cover - exercised via unit tests with mocked client errors
        if "ConflictException" in str(exc) or "already exists" in str(exc).lower():
            return {
                "family": "fg_b_baseline",
                "status": "Skipped",
                "pipeline_name": pipeline_name,
                "failure_reason": "ExecutionAlreadyExists",
                "reference_time_iso": range_start_ts_iso,
                "range_end_ts_iso": range_end_ts_iso,
            }
        raise


def _dispatch_family(
    *,
    family: str,
    request,
    dpp_config_table_name: str | None,
) -> dict:
    if family == "fg_b_baseline":
        return _dispatch_fgb_baseline(
            project_name=request.project_name,
            feature_spec_version=request.feature_spec_version,
            range_start_ts_iso=request.range_start_ts_iso,
            range_end_ts_iso=request.range_end_ts_iso,
            idempotency_key=request.idempotency_key,
            dpp_config_table_name=dpp_config_table_name,
        )
    return {
        "family": family,
        "status": "HandledByBackfill15mPipeline",
    }


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
    family_results = [
        _dispatch_family(
            family=family,
            request=request,
            dpp_config_table_name=args.dpp_config_table_name or None,
        )
        for family in request.artifact_families
    ]
    overall_status = "Succeeded"
    if any(result.get("status") == "Failed" for result in family_results):
        overall_status = "Failed"

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
            "status": overall_status,
            "family_results": family_results,
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
                "status": overall_status,
                "family_results": family_results,
                "fg_b_baseline_results": [item for item in family_results if item.get("family") == "fg_b_baseline"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
