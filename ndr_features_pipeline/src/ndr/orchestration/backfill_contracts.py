"""Canonical backfill planner and manifest contracts (Task 7.1)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"

ARTIFACT_FAMILY_ORDER: tuple[str, ...] = (
    "delta",
    "fg_a",
    "pair_counts",
    "fg_b_baseline",
    "fg_c",
)

FAMILY_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "delta": (),
    "fg_a": ("delta",),
    "pair_counts": ("delta",),
    "fg_b_baseline": (),
    "fg_c": ("fg_a", "pair_counts", "fg_b_baseline"),
}


@dataclass(frozen=True)
class TimeRange:
    start_ts: str
    end_ts: str

    def __post_init__(self) -> None:
        start = _parse_ts(self.start_ts)
        end = _parse_ts(self.end_ts)
        if start >= end:
            raise ValueError(f"Invalid range: start_ts must be < end_ts ({self.start_ts} >= {self.end_ts})")


@dataclass(frozen=True)
class FamilyPlan:
    family: str
    ranges: tuple[TimeRange, ...]
    execute: bool
    reason: str



def build_family_range_plan(
    *,
    family_ranges: dict[str, list[dict[str, str]]],
    requested_families: list[str] | None = None,
) -> list[FamilyPlan]:
    """Build canonical family/range selective plan with dependency-safe execution decisions."""
    _validate_families(family_ranges)
    requested = set(requested_families or [])
    if requested:
        unknown = requested - set(ARTIFACT_FAMILY_ORDER)
        if unknown:
            raise ValueError(f"Unknown requested families: {sorted(unknown)}")

    normalized: dict[str, tuple[TimeRange, ...]] = {
        family: _normalize_ranges(family_ranges.get(family, []))
        for family in ARTIFACT_FAMILY_ORDER
    }

    plans: list[FamilyPlan] = []
    for family in ARTIFACT_FAMILY_ORDER:
        ranges = normalized[family]
        has_work = bool(ranges)
        if requested:
            required = family in requested
            execute = required and has_work
            reason = "requested_and_missing" if execute else ("requested_but_complete" if required else "not_requested")
        else:
            execute = has_work
            reason = "missing_ranges_detected" if execute else "no_missing_ranges"

        if execute:
            for dep in FAMILY_DEPENDENCIES[family]:
                dep_ranges = normalized[dep]
                if not dep_ranges and dep not in requested:
                    execute = False
                    reason = f"dependency_missing:{dep}"
                    break

        plans.append(FamilyPlan(family=family, ranges=ranges, execute=execute, reason=reason))

    return plans



def build_execution_manifest(
    *,
    project_name: str,
    feature_spec_version: str,
    planner_mode: str,
    source: str,
    family_plan: list[FamilyPlan],
    run_id: str | None = None,
) -> dict[str, Any]:
    """Serialize canonical manifest consumed by backfill map and training remediation caller."""
    map_items = []
    family_entries = []
    for plan in family_plan:
        ranges = [{"start_ts": r.start_ts, "end_ts": r.end_ts} for r in plan.ranges]
        family_entries.append(
            {
                "family": plan.family,
                "execute": plan.execute,
                "reason": plan.reason,
                "ranges": ranges,
            }
        )
        if plan.execute:
            for r in plan.ranges:
                map_items.append(
                    {
                        "project_name": project_name,
                        "feature_spec_version": feature_spec_version,
                        "family": plan.family,
                        "range_start_ts": r.start_ts,
                        "range_end_ts": r.end_ts,
                    }
                )

    return {
        "contract_version": "backfill_manifest.v1",
        "generated_at": datetime.now(timezone.utc).strftime(ISO_Z),
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "planner_mode": planner_mode,
        "source": source,
        "run_id": run_id,
        "family_plan": family_entries,
        "map_items": map_items,
    }



def build_training_trigger_family_ranges(
    *,
    missing_15m_windows: list[dict[str, str]],
    missing_fgb_windows: list[dict[str, Any]],
) -> dict[str, list[dict[str, str]]]:
    """Translate training remediation manifests into canonical family/range planner inputs."""
    fg_15m_ranges = [{"start_ts": item["window_start_ts"], "end_ts": item["window_end_ts"]} for item in missing_15m_windows]
    return {
        "delta": fg_15m_ranges,
        "fg_a": fg_15m_ranges,
        "pair_counts": fg_15m_ranges,
        "fg_b_baseline": [
            {
                "start_ts": item["reference_time_iso"],
                "end_ts": (
                    _parse_ts(item["reference_time_iso"]).replace(hour=23, minute=59, second=59, microsecond=0)
                ).strftime(ISO_Z),
            }
            for item in missing_fgb_windows
        ],
        "fg_c": fg_15m_ranges,
    }



def _normalize_ranges(ranges: list[dict[str, str]]) -> tuple[TimeRange, ...]:
    parsed = sorted((TimeRange(start_ts=r["start_ts"], end_ts=r["end_ts"]) for r in ranges), key=lambda r: r.start_ts)
    if not parsed:
        return ()

    merged: list[TimeRange] = [parsed[0]]
    for current in parsed[1:]:
        previous = merged[-1]
        if _parse_ts(current.start_ts) <= _parse_ts(previous.end_ts):
            merged[-1] = TimeRange(start_ts=previous.start_ts, end_ts=max(previous.end_ts, current.end_ts))
        else:
            merged.append(current)
    return tuple(merged)



def _validate_families(family_ranges: dict[str, list[dict[str, str]]]) -> None:
    unknown = set(family_ranges) - set(ARTIFACT_FAMILY_ORDER)
    if unknown:
        raise ValueError(f"Unknown artifact families: {sorted(unknown)}")



def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
