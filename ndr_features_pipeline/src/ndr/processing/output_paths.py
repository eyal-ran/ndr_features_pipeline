"""NDR output paths module."""

from __future__ import annotations


from datetime import datetime, timezone


def _parse_iso8601(value: str) -> datetime:
    """Parse an ISO8601 timestamp, accepting a trailing Z."""
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_batch_output_prefix(
    base_prefix: str,
    dataset: str,
    batch_start_ts_iso: str,
    batch_id: str,
) -> str:
    """Build an S3 prefix including dataset name and batch timestamp/id.

    Example:
        s3://bucket/fg_a/ts=YYYY/MM/DD/HH/MM-batch_id=<id>
    """
    cleaned = base_prefix.rstrip("/")
    dataset_suffix = f"/{dataset}"
    if not cleaned.endswith(dataset_suffix):
        cleaned = f"{cleaned}{dataset_suffix}"

    dt = _parse_iso8601(batch_start_ts_iso)
    ts_path = dt.strftime("ts=%Y/%m/%d/%H/%M")
    return f"{cleaned}/{ts_path}-batch_id={batch_id}"
