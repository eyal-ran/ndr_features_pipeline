"""Monthly FG-B readiness checker artifact producer (Task 2, Option 1)."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from ndr.orchestration.contract_schemas_v3 import validate_payload
from ndr.orchestration.readiness_contracts import compute_monthly_fg_b_readiness_v3


def build_monthly_readiness_idempotency_key(
    *, project_name: str, feature_spec_version: str, reference_month: str, readiness_cycle: int
) -> str:
    raw = "|".join((project_name, feature_spec_version, reference_month, str(readiness_cycle)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_monthly_readiness_artifact_key(
    *, project_name: str, feature_spec_version: str, reference_month: str, readiness_cycle: int
) -> str:
    return (
        "orchestration/readiness/monthly_fg_b_readiness/v3/"
        f"{project_name}/{feature_spec_version}/{reference_month}/cycle={readiness_cycle}/manifest.json"
    )


def compute_and_validate_monthly_readiness_artifact(
    *,
    project_name: str,
    feature_spec_version: str,
    reference_month: str,
    readiness_cycle: int,
    required_families: list[str],
    missing_ranges: list[dict[str, str]],
) -> dict[str, Any]:
    payload = compute_monthly_fg_b_readiness_v3(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        reference_month=reference_month,
        required_families=required_families,
        missing_ranges=missing_ranges,
        idempotency_key=build_monthly_readiness_idempotency_key(
            project_name=project_name,
            feature_spec_version=feature_spec_version,
            reference_month=reference_month,
            readiness_cycle=readiness_cycle,
        ),
    )
    validate_payload(payload, contract_version="monthly_fg_b_readiness.v3")
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce monthly_fg_b_readiness.v3 artifact")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--reference-month", required=True)
    parser.add_argument("--readiness-cycle", type=int, required=True)
    parser.add_argument("--required-families-json", required=True)
    parser.add_argument("--missing-ranges-json", required=True)
    parser.add_argument("--output-manifest", required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    required_families = json.loads(args.required_families_json)
    missing_ranges = json.loads(args.missing_ranges_json)
    payload = compute_and_validate_monthly_readiness_artifact(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        reference_month=args.reference_month,
        readiness_cycle=args.readiness_cycle,
        required_families=required_families,
        missing_ranges=missing_ranges,
    )
    output_path = Path(args.output_manifest)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
