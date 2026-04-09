import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *_args, **_kwargs: None
sys.modules.setdefault("boto3", boto3_stub)
sys.modules.setdefault("boto3.dynamodb", types.ModuleType("boto3.dynamodb"))
conditions_module = types.ModuleType("boto3.dynamodb.conditions")
conditions_module.Key = object
sys.modules.setdefault("boto3.dynamodb.conditions", conditions_module)

from ndr.processing.raw_input_resolver import ERROR_CODE_FALLBACK_DISABLED, RawInputResolver


def test_complete_ingestion_resolves_to_ingestion_mode():
    resolution = RawInputResolver().resolve(
        ingestion_rows=[{"raw_parsed_logs_s3_prefix": "s3://bucket/raw/mb-1/"}],
        allow_redshift_fallback=False,
        dpp_spec={},
        artifact_family="delta",
        range_start_ts="2025-01-01T00:00:00Z",
        range_end_ts="2025-01-01T00:15:00Z",
        producer_flow="delta_builder",
    )

    assert resolution.source_mode == "ingestion"
    assert resolution.raw_input_s3_prefix == "s3://bucket/raw/mb-1/"


def test_missing_ingestion_with_fallback_enabled_resolves_to_redshift_mode(monkeypatch):
    import ndr.processing.raw_input_resolver as module

    monkeypatch.setattr(
        module,
        "load_backfill_fallback_contract",
        lambda **_k: (types.SimpleNamespace(), types.SimpleNamespace()),
    )
    monkeypatch.setattr(
        module,
        "execute_backfill_redshift_fallback",
        lambda **_k: [types.SimpleNamespace(unload_s3_prefix="s3://bucket/tmp/fallback/range_0000/attempt_01/")],
    )

    resolution = RawInputResolver().resolve(
        ingestion_rows=[],
        allow_redshift_fallback=True,
        dpp_spec={"backfill_redshift_fallback": {"enabled": True}},
        artifact_family="delta",
        range_start_ts="2025-01-01T00:00:00Z",
        range_end_ts="2025-01-01T00:15:00Z",
        producer_flow="historical_windows_extractor",
    )

    assert resolution.source_mode == "redshift_unload_fallback"
    assert resolution.raw_input_s3_prefix.startswith("s3://bucket/tmp/fallback/")


def test_missing_ingestion_with_fallback_disabled_fails_explicitly():
    try:
        RawInputResolver().resolve(
            ingestion_rows=[],
            allow_redshift_fallback=False,
            dpp_spec={},
            artifact_family="delta",
            range_start_ts="2025-01-01T00:00:00Z",
            range_end_ts="2025-01-01T00:15:00Z",
            producer_flow="delta_builder",
        )
    except RuntimeError as exc:
        assert ERROR_CODE_FALLBACK_DISABLED in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")
