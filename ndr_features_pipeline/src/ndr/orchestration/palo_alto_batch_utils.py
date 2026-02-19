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


def parse_batch_path_from_s3_key(s3_key: str) -> ParsedBatchPath:
    """Parse canonical Palo Alto batch path/key into batch identity fields.

    Accepted input forms include:
    - object paths: ``org1/org2/project/2025/01/31/<batch-id>/<file>``
    - batch-folder paths: ``org1/org2/project/2025/01/31/<batch-id>/``
    - S3 URIs for either form.
    """
    normalized = s3_key.strip()
    if normalized.startswith("s3://"):
        uri_without_scheme = normalized[len("s3://") :]
        bucket_sep = uri_without_scheme.find("/")
        normalized = "" if bucket_sep < 0 else uri_without_scheme[bucket_sep + 1 :]

    parts = [p for p in normalized.strip('/').split('/') if p]
    if len(parts) < 7:
        raise ValueError(f"Invalid Palo Alto batch key (expected >=7 segments): {s3_key}")

    return ParsedBatchPath(
        org1=parts[0],
        org2=parts[1],
        project_name=parts[2],
        year=parts[3],
        month=parts[4],
        day=parts[5],
        mini_batch_id=parts[6],
    )


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
