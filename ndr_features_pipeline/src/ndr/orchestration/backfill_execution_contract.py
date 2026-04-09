"""Canonical backfill execution contract helpers for SFN->pipeline handoff."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

from ndr.orchestration.backfill_contracts import ARTIFACT_FAMILY_ORDER, FAMILY_DEPENDENCIES

CONTRACT_VERSION = "backfill_execution_request.v2"
CONTRACT_ERROR_CODE = "BACKFILL_CONTRACT_VALIDATION_ERROR"

_FAMILY_ALIASES = {
    "fg_a_15m": "fg_a",
    "fgb": "fg_b_baseline",
}


@dataclass(frozen=True)
class BackfillExecutionRequest:
    project_name: str
    feature_spec_version: str
    artifact_families: tuple[str, ...]
    range_start_ts_iso: str
    range_end_ts_iso: str
    idempotency_key: str
    contract_version: str = CONTRACT_VERSION


def parse_artifact_families(artifact_family: str) -> tuple[str, ...]:
    """Parse canonical family list from runtime parameter.

    Accepts single family (`fg_c`) or comma-separated mixed families (`fg_c,fg_a`).
    Returned tuple is deterministic and dependency-safe.
    """
    raw = [item.strip() for item in artifact_family.split(",") if item.strip()]
    if not raw:
        raise ValueError(f"{CONTRACT_ERROR_CODE}: ArtifactFamily is required")

    requested = {_FAMILY_ALIASES.get(item, item) for item in raw}
    unknown = requested - set(ARTIFACT_FAMILY_ORDER)
    if unknown:
        raise ValueError(
            f"{CONTRACT_ERROR_CODE}: unknown ArtifactFamily values: {sorted(unknown)}"
        )

    expanded: set[str] = set()

    def _include_with_dependencies(family: str) -> None:
        for dependency in FAMILY_DEPENDENCIES.get(family, ()):
            _include_with_dependencies(dependency)
        expanded.add(family)

    for family in requested:
        _include_with_dependencies(family)

    ordered = tuple(f for f in ARTIFACT_FAMILY_ORDER if f in expanded)
    return ordered


def _parse_iso_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def validate_time_range(range_start_ts_iso: str, range_end_ts_iso: str) -> None:
    if not range_start_ts_iso or not range_end_ts_iso:
        raise ValueError(
            f"{CONTRACT_ERROR_CODE}: RangeStartTsIso and RangeEndTsIso are required"
        )
    start = _parse_iso_utc(range_start_ts_iso)
    end = _parse_iso_utc(range_end_ts_iso)
    if start >= end:
        raise ValueError(
            f"{CONTRACT_ERROR_CODE}: invalid time range: "
            f"RangeStartTsIso must be earlier than RangeEndTsIso ({range_start_ts_iso} >= {range_end_ts_iso})"
        )


def build_execution_request(
    *,
    project_name: str,
    feature_spec_version: str,
    artifact_family: str,
    range_start_ts_iso: str,
    range_end_ts_iso: str,
    idempotency_key: str | None = None,
) -> BackfillExecutionRequest:
    if not project_name:
        raise ValueError(f"{CONTRACT_ERROR_CODE}: ProjectName is required")
    if not feature_spec_version:
        raise ValueError(f"{CONTRACT_ERROR_CODE}: FeatureSpecVersion is required")

    validate_time_range(range_start_ts_iso=range_start_ts_iso, range_end_ts_iso=range_end_ts_iso)
    families = parse_artifact_families(artifact_family)

    if idempotency_key:
        resolved_idempotency_key = idempotency_key
    else:
        digest = hashlib.sha256(
            "|".join(
                [
                    project_name,
                    feature_spec_version,
                    ",".join(families),
                    range_start_ts_iso,
                    range_end_ts_iso,
                ]
            ).encode("utf-8")
        ).hexdigest()[:24]
        resolved_idempotency_key = f"bkf-{digest}"

    return BackfillExecutionRequest(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        artifact_families=families,
        range_start_ts_iso=range_start_ts_iso,
        range_end_ts_iso=range_end_ts_iso,
        idempotency_key=resolved_idempotency_key,
    )
