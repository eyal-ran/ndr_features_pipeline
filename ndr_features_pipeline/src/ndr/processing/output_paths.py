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


def build_fg_b_publication_metadata(
    *,
    feature_spec_version: str,
    baseline_horizon: str,
    reference_time_iso: str,
    run_mode: str,
    run_batch_id: str,
    baseline_start_ts: str,
    baseline_end_ts: str,
) -> dict[str, str]:
    """Build deterministic FG-B publication metadata for canonical monthly snapshots.

    The created fields are derived from ``reference_time_iso`` to keep replay behavior
    deterministic under retries/re-executions.
    """
    reference_dt = _parse_iso8601(reference_time_iso).replace(microsecond=0)
    created_at = reference_dt.isoformat().replace('+00:00', 'Z')
    created_date = reference_dt.strftime('%Y-%m-%d')
    return {
        'feature_spec_version': feature_spec_version,
        'baseline_horizon': baseline_horizon,
        'reference_time_iso': created_at,
        'run_mode': run_mode,
        'run_batch_id': run_batch_id,
        'baseline_start_ts': baseline_start_ts,
        'baseline_end_ts': baseline_end_ts,
        'created_at': created_at,
        'created_date': created_date,
    }
