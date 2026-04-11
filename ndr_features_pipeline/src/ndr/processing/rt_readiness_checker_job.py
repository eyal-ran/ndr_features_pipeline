"""Realtime artifact readiness checker artifact producer (Task 3)."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from ndr.orchestration.contract_schemas_v3 import validate_payload
from ndr.orchestration.readiness_contracts import compute_rt_artifact_readiness_v3


def build_rt_readiness_idempotency_key(
    *,
    project_name: str,
    feature_spec_version: str,
    ml_project_name: str,
    mini_batch_id: str,
    readiness_cycle: int,
) -> str:
    raw = "|".join((project_name, feature_spec_version, ml_project_name, mini_batch_id, str(readiness_cycle)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_rt_readiness_artifact_key(
    *,
    project_name: str,
    feature_spec_version: str,
    ml_project_name: str,
    mini_batch_id: str,
    readiness_cycle: int,
) -> str:
    return (
        "orchestration/readiness/rt_artifact_readiness/v3/"
        f"{project_name}/{feature_spec_version}/{ml_project_name}/{mini_batch_id}/"
        f"cycle={readiness_cycle}/manifest.json"
    )


def compute_and_validate_rt_readiness_artifact(
    *,
    project_name: str,
    feature_spec_version: str,
    ml_project_name: str,
    mini_batch_id: str,
    readiness_cycle: int,
    required_families: list[str],
    missing_ranges: list[dict[str, str]],
) -> dict[str, Any]:
    payload = compute_rt_artifact_readiness_v3(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        ml_project_name=ml_project_name,
        mini_batch_id=mini_batch_id,
        required_families=required_families,
        missing_ranges=missing_ranges,
        idempotency_key=build_rt_readiness_idempotency_key(
            project_name=project_name,
            feature_spec_version=feature_spec_version,
            ml_project_name=ml_project_name,
            mini_batch_id=mini_batch_id,
            readiness_cycle=readiness_cycle,
        ),
    )
    validate_payload(payload, contract_version="rt_artifact_readiness.v3")
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce rt_artifact_readiness.v3 artifact")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--ml-project-name", required=True)
    parser.add_argument("--mini-batch-id", required=True)
    parser.add_argument("--readiness-cycle", type=int, required=True)
    parser.add_argument("--required-families-json", required=True)
    parser.add_argument("--missing-ranges-json", required=True)
    parser.add_argument("--output-manifest", required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    required_families = json.loads(args.required_families_json)
    missing_ranges = json.loads(args.missing_ranges_json)
    payload = compute_and_validate_rt_readiness_artifact(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        ml_project_name=args.ml_project_name,
        mini_batch_id=args.mini_batch_id,
        readiness_cycle=args.readiness_cycle,
        required_families=required_families,
        missing_ranges=missing_ranges,
    )
    output_path = Path(args.output_manifest)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
