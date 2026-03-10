"""Utilities for Palo Alto mini-batch path parsing and cron-window alignment."""

from __future__ import annotations

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

def is_migration_toggle_enabled(toggle_name: str) -> bool:
    """Compatibility toggles are removed in Task 7 and always evaluate to disabled."""
    return False


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


def parse_batch_path_from_s3_key(s3_key: str) -> ParsedBatchPath:
    """Parse canonical Palo Alto batch path/key into batch identity fields.

    Accepted input forms include:
    - canonical object paths: ``project/org1/org2/2025/01/31/<batch-id>/<file>``
    - canonical batch-folder paths: ``project/org1/org2/2025/01/31/<batch-id>/``
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
        raise ValueError(
            f"Invalid canonical Palo Alto batch key (expected project/org1/org2/YYYY/MM/dd/batch): {s3_key}"
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
