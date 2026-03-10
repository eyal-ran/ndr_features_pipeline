import sys
import types
from datetime import datetime, timezone

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *_args, **_kwargs: None
dynamodb_module = types.ModuleType("boto3.dynamodb")
conditions_module = types.ModuleType("boto3.dynamodb.conditions")
conditions_module.Key = object
sys.modules.setdefault("boto3", boto3_stub)
sys.modules.setdefault("boto3.dynamodb", dynamodb_module)
sys.modules.setdefault("boto3.dynamodb.conditions", conditions_module)

from ndr.processing.historical_windows_extractor_job import (
    HistoricalWindowsExtractorRuntimeConfig,
    HistoricalWindowsExtractorJob,
)


class _Paginator:
    def paginate(self, Bucket, Prefix):
        assert Bucket == "bucket"
        assert Prefix == "raw"
        return [
            {
                "Contents": [
                    {
                        "Key": "raw/org1/org2/projA/2025/01/01/mb-1/file1.json.gz",
                        "LastModified": datetime(2025, 1, 1, 10, 40, tzinfo=timezone.utc),
                    }
                ]
            }
        ]


class _S3Client:
    def __init__(self):
        self.put_calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator()

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


class _BatchIndexLoader:
    def __init__(self, rows):
        self.rows = rows

    def lookup_forward(self, **_kwargs):
        return self.rows


def test_extractor_emits_expected_window_rows(monkeypatch):
    import ndr.processing.historical_windows_extractor_job as module

    fake_s3 = _S3Client()
    monkeypatch.setattr(module.boto3, "client", lambda *_a, **_k: fake_s3, raising=False)
    monkeypatch.setattr(module, "resolve_feature_spec_version", lambda **_k: "v9")

    runtime = HistoricalWindowsExtractorRuntimeConfig(
        input_s3_prefix="s3://bucket/raw",
        output_s3_prefix="s3://bucket/out",
        start_ts_iso="2025-01-01T00:00:00Z",
        end_ts_iso="2025-01-02T00:00:00Z",
        window_floor_minutes=[8, 23, 38, 53],
    )
    uri = HistoricalWindowsExtractorJob(runtime).run()
    assert uri.startswith("s3://bucket/out/historical_windows/")
    assert len(fake_s3.put_calls) == 1
    body = fake_s3.put_calls[0]["Body"].decode("utf-8")
    assert '"project_name": "projA"' in body
    assert '"feature_spec_version": "v9"' in body
    assert '"batch_start_ts_iso": "2025-01-01T10:38:00Z"' in body
    assert '"batch_end_ts_iso": "2025-01-01T10:40:00Z"' in body


def test_extractor_prefers_batch_index_rows(monkeypatch):
    import ndr.processing.historical_windows_extractor_job as module

    fake_s3 = _S3Client()
    monkeypatch.setattr(module.boto3, "client", lambda *_a, **_k: fake_s3, raising=False)
    monkeypatch.setattr(module, "resolve_feature_spec_version", lambda **_k: "v9")
    monkeypatch.setattr(
        module,
        "BatchIndexLoader",
        lambda: _BatchIndexLoader(
            [
                types.SimpleNamespace(
                    batch_id="mb-9",
                    batch_s3_prefix="s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-9/",
                    event_ts_utc="2025-01-01T10:40:00Z",
                )
            ]
        ),
    )

    runtime = HistoricalWindowsExtractorRuntimeConfig(
        input_s3_prefix="s3://bucket/raw/fw_paloalto",
        output_s3_prefix="s3://bucket/out",
        start_ts_iso="2025-01-01T00:00:00Z",
        end_ts_iso="2025-01-02T00:00:00Z",
        window_floor_minutes=[8, 23, 38, 53],
    )
    uri = HistoricalWindowsExtractorJob(runtime).run()
    assert uri.startswith("s3://bucket/out/historical_windows/")
    body = fake_s3.put_calls[0]["Body"].decode("utf-8")
    assert '"mini_batch_id": "mb-9"' in body


def test_extractor_errors_when_index_empty_and_fallback_disabled(monkeypatch):
    import ndr.processing.historical_windows_extractor_job as module

    fake_s3 = _S3Client()
    monkeypatch.setattr(module.boto3, "client", lambda *_a, **_k: fake_s3, raising=False)
    monkeypatch.setattr(module, "resolve_feature_spec_version", lambda **_k: "v9")
    monkeypatch.setattr(module, "BatchIndexLoader", lambda: _BatchIndexLoader([]))
    monkeypatch.setattr(module, "is_migration_toggle_enabled", lambda name: False)

    runtime = HistoricalWindowsExtractorRuntimeConfig(
        input_s3_prefix="s3://bucket/raw/fw_paloalto",
        output_s3_prefix="s3://bucket/out",
        start_ts_iso="2025-01-01T00:00:00Z",
        end_ts_iso="2025-01-02T00:00:00Z",
        window_floor_minutes=[8, 23, 38, 53],
    )

    try:
        HistoricalWindowsExtractorJob(runtime).run()
    except RuntimeError as exc:
        assert "enable_s3_listing_fallback_for_backfill is disabled" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")
