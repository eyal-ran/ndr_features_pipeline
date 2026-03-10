"""Utilities for Palo Alto mini-batch path parsing and cron-window alignment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta


@dataclass(frozen=True)
class ParsedBatchPath:
    org1: str
    org2: str
    project_name: str
    year: str
    month: str
    day: str
    mini_batch_id: str


_TOGGLE_DEFAULTS_BY_ENV: dict[str, dict[str, bool]] = {
    "dev": {
        "enable_legacy_input_prefix_fallback": True,
        "enable_legacy_path_parser": True,
        "enable_s3_listing_fallback_for_backfill": True,
    },
    "stage": {
        "enable_legacy_input_prefix_fallback": False,
        "enable_legacy_path_parser": False,
        "enable_s3_listing_fallback_for_backfill": True,
    },
    "prod": {
        "enable_legacy_input_prefix_fallback": False,
        "enable_legacy_path_parser": False,
        "enable_s3_listing_fallback_for_backfill": False,
    },
}


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _resolve_runtime_environment() -> str:
    env = (
        os.getenv("NDR_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("APP_ENV")
        or "dev"
    ).strip().lower()
    return env if env in _TOGGLE_DEFAULTS_BY_ENV else "dev"


def is_migration_toggle_enabled(toggle_name: str) -> bool:
    """Resolve migration toggle from explicit env override or environment default."""
    explicit_value = os.getenv(toggle_name)
    if explicit_value is None:
        explicit_value = os.getenv(toggle_name.upper())
    if explicit_value is not None:
        return _parse_bool(explicit_value)

    env_name = _resolve_runtime_environment()
    return _TOGGLE_DEFAULTS_BY_ENV[env_name].get(toggle_name, False)


def _parse_canonical_parts(parts: list[str]) -> ParsedBatchPath:
    if len(parts) < 7:
        raise ValueError("canonical path requires at least 7 segments")
    if parts[0] != "fw_paloalto":
        raise ValueError("canonical project segment must be fw_paloalto")
    if not (parts[3].isdigit() and len(parts[3]) == 4):
        raise ValueError("canonical year must be YYYY")
    if not (parts[4].isdigit() and len(parts[4]) == 2):
        raise ValueError("canonical month must be MM")
    if not (parts[5].isdigit() and len(parts[5]) == 2):
        raise ValueError("canonical day must be dd")
    return ParsedBatchPath(
        project_name=parts[0],
        org1=parts[1],
        org2=parts[2],
        year=parts[3],
        month=parts[4],
        day=parts[5],
        mini_batch_id=parts[6],
    )


def _parse_legacy_parts(parts: list[str]) -> ParsedBatchPath:
    if len(parts) < 7:
        raise ValueError("legacy path requires at least 7 segments")
    if not (parts[3].isdigit() and len(parts[3]) == 4):
        raise ValueError("legacy year must be YYYY")
    if not (parts[4].isdigit() and len(parts[4]) == 2):
        raise ValueError("legacy month must be MM")
    if not (parts[5].isdigit() and len(parts[5]) == 2):
        raise ValueError("legacy day must be dd")
    return ParsedBatchPath(
        org1=parts[0],
        org2=parts[1],
        project_name=parts[2],
        year=parts[3],
        month=parts[4],
        day=parts[5],
        mini_batch_id=parts[6],
    )


def parse_batch_path_from_s3_key(s3_key: str) -> ParsedBatchPath:
    """Parse canonical Palo Alto batch path/key into batch identity fields.

    Accepted input forms include:
    - canonical object paths: ``project/org1/org2/2025/01/31/<batch-id>/<file>``
    - canonical batch-folder paths: ``project/org1/org2/2025/01/31/<batch-id>/``
    - legacy path order is accepted only when ``enable_legacy_path_parser`` is enabled.
    - S3 URIs for either form.
    """
    normalized = s3_key.strip()
    if normalized.startswith("s3://"):
        uri_without_scheme = normalized[len("s3://") :]
        bucket_sep = uri_without_scheme.find("/")
        normalized = "" if bucket_sep < 0 else uri_without_scheme[bucket_sep + 1 :]

    parts = [p for p in normalized.strip('/').split('/') if p]
    try:
        return _parse_canonical_parts(parts)
    except ValueError as exc:
        if not is_migration_toggle_enabled("enable_legacy_path_parser"):
            raise ValueError(
                f"Invalid canonical Palo Alto batch key (expected project/org1/org2/YYYY/MM/dd/batch): {s3_key}"
            ) from exc

    try:
        return _parse_legacy_parts(parts)
    except ValueError as exc:
        raise ValueError(
            f"Invalid Palo Alto batch key (expected canonical project/org1/org2/YYYY/MM/dd/batch): {s3_key}"
        ) from exc


def floor_to_window_minute(ts: datetime, floor_minutes: list[int]) -> datetime:
    """Floor timestamp to nearest prior minute in configured floor-minute set."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)

    mins = sorted(set(int(m) for m in floor_minutes))
    if not mins:
        raise ValueError("floor_minutes must not be empty")

    hour = ts.replace(second=0, microsecond=0)
    candidates = [m for m in mins if m <= ts.minute]
    if candidates:
        return hour.replace(minute=max(candidates))

    prev_hour = hour.replace(minute=0) - timedelta(hours=1)
    return prev_hour.replace(minute=max(mins))


def to_iso_z(ts: datetime) -> str:
    """Format datetime to ISO8601 Z."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def derive_window_bounds(source_ts: datetime, floor_minutes: list[int]) -> tuple[str, str]:
    """Derive (`batch_start_ts_iso`, `batch_end_ts_iso`) from source timestamp."""
    floored = floor_to_window_minute(source_ts, floor_minutes)
    return to_iso_z(floored), to_iso_z(source_ts)
