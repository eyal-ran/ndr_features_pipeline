"""Canonical IF-training missing-window manifest contract (Task 8.3)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

CONTRACT_VERSION = "if_training_missing_windows.v1"


def canonical_manifest(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a canonical manifest payload with schema-version guard."""
    return {
        "contract_version": CONTRACT_VERSION,
        "entries": validate_manifest_entries(entries),
    }


def validate_manifest_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate Task 8.3 canonical entry schema and normalize range ordering."""
    normalized: List[Dict[str, Any]] = []
    required = {"artifact_family", "ranges", "source", "ml_project_name", "project_name", "feature_spec_version", "run_id"}
    for entry in entries:
        missing = required - set(entry)
        if missing:
            raise ValueError(f"Missing fields in canonical missing-window manifest entry: {sorted(missing)}")
        ranges = _normalize_ranges(entry["ranges"])
        normalized.append(
            {
                "artifact_family": str(entry["artifact_family"]),
                "ranges": ranges,
                "source": str(entry["source"]),
                "ml_project_name": str(entry["ml_project_name"]),
                "project_name": str(entry["project_name"]),
                "feature_spec_version": str(entry["feature_spec_version"]),
                "run_id": str(entry["run_id"]),
            }
        )
    return normalized


def ensure_manifest(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    """Validate manifest wrapper and enforce backward-incompatible version guard."""
    payload = payload or {}
    contract_version = payload.get("contract_version")
    if contract_version != CONTRACT_VERSION:
        raise ValueError(
            f"Unsupported missing-window manifest version '{contract_version}'. Expected '{CONTRACT_VERSION}'."
        )
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("Missing-window manifest must include list field 'entries'")
    return {"contract_version": contract_version, "entries": validate_manifest_entries(entries)}


def from_missing_sources(
    *,
    missing_15m_windows: List[Dict[str, str]],
    missing_fgb_windows: List[Dict[str, Any]],
    project_name: str,
    feature_spec_version: str,
    ml_project_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """Build canonical entries from legacy missing-window detector outputs."""
    entries: List[Dict[str, Any]] = []
    if missing_15m_windows:
        entries.append(
            {
                "artifact_family": "fg_a_15m",
                "ranges": [
                    {"start_ts_iso": window["window_start_ts"], "end_ts_iso": window["window_end_ts"]}
                    for window in missing_15m_windows
                ],
                "source": "feature_partition_gap",
                "project_name": project_name,
                "feature_spec_version": feature_spec_version,
                "ml_project_name": ml_project_name,
                "run_id": run_id,
            }
        )
    if missing_fgb_windows:
        entries.append(
            {
                "artifact_family": "fg_b_daily",
                "ranges": [
                    {
                        "start_ts_iso": item["reference_time_iso"],
                        "end_ts_iso": (
                            datetime.fromisoformat(item["reference_time_iso"].replace("Z", "+00:00"))
                            .astimezone(timezone.utc)
                            + timedelta(days=1)
                        ).isoformat().replace("+00:00", "Z"),
                    }
                    for item in missing_fgb_windows
                ],
                "source": "feature_partition_gap",
                "project_name": project_name,
                "feature_spec_version": feature_spec_version,
                "ml_project_name": ml_project_name,
                "run_id": run_id,
            }
        )
    return canonical_manifest(entries)


def _normalize_ranges(ranges: List[Dict[str, str]]) -> List[Dict[str, str]]:
    parsed = sorted(
        (
            (
                datetime.fromisoformat(r["start_ts_iso"].replace("Z", "+00:00")).astimezone(timezone.utc),
                datetime.fromisoformat(r["end_ts_iso"].replace("Z", "+00:00")).astimezone(timezone.utc),
            )
            for r in ranges
        ),
        key=lambda item: item[0],
    )
    merged: List[tuple[datetime, datetime]] = []
    for start_ts, end_ts in parsed:
        if end_ts <= start_ts:
            raise ValueError("Each range must satisfy end_ts_iso > start_ts_iso")
        if not merged or start_ts > merged[-1][1]:
            merged.append((start_ts, end_ts))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end_ts))
    return [
        {
            "start_ts_iso": start_ts.isoformat().replace("+00:00", "Z"),
            "end_ts_iso": end_ts.isoformat().replace("+00:00", "Z"),
        }
        for start_ts, end_ts in merged
    ]
