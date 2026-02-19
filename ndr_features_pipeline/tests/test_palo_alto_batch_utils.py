from datetime import datetime, timezone

from ndr.orchestration.palo_alto_batch_utils import (
    derive_window_bounds,
    floor_to_window_minute,
    parse_batch_path_from_s3_key,
)


def test_parse_batch_path_from_s3_object_key():
    parsed = parse_batch_path_from_s3_key("org1/org2/projectx/2025/01/31/mb-1/file.json.gz")
    assert parsed.project_name == "projectx"
    assert parsed.mini_batch_id == "mb-1"


def test_parse_batch_path_from_batch_folder_path_and_s3_uri():
    folder = parse_batch_path_from_s3_key("org1/org2/projectx/2025/01/31/mb-2/")
    uri = parse_batch_path_from_s3_key("s3://raw-bucket/org1/org2/projectx/2025/01/31/mb-3/")
    assert folder.mini_batch_id == "mb-2"
    assert uri.mini_batch_id == "mb-3"


def test_floor_to_window_minute_uses_08_23_38_53():
    ts = datetime(2025, 1, 1, 10, 40, 5, tzinfo=timezone.utc)
    floored = floor_to_window_minute(ts, [8, 23, 38, 53])
    assert floored.minute == 38


def test_derive_window_bounds_returns_iso_start_and_end():
    ts = datetime(2025, 1, 1, 10, 40, 5, tzinfo=timezone.utc)
    start_iso, end_iso = derive_window_bounds(ts, [8, 23, 38, 53])
    assert start_iso == "2025-01-01T10:38:00Z"
    assert end_iso == "2025-01-01T10:40:05Z"
