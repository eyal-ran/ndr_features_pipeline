from datetime import datetime, timezone

from ndr.orchestration.palo_alto_batch_utils import parse_batch_path_from_s3_key, floor_to_window_minute


def test_parse_batch_path_from_s3_key():
    parsed = parse_batch_path_from_s3_key("org1/org2/projectx/2025/01/31/mb-1/file.json.gz")
    assert parsed.project_name == "projectx"
    assert parsed.mini_batch_id == "mb-1"


def test_floor_to_window_minute_uses_08_23_38_53():
    ts = datetime(2025, 1, 1, 10, 40, 5, tzinfo=timezone.utc)
    floored = floor_to_window_minute(ts, [8, 23, 38, 53])
    assert floored.minute == 38
