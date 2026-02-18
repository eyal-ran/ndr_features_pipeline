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
    """Parse canonical Palo Alto S3 key into batch identity fields."""
    parts = [p for p in s3_key.strip('/').split('/') if p]
    if len(parts) < 8:
        raise ValueError(f"Invalid Palo Alto batch key (expected >=8 segments): {s3_key}")

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
