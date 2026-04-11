from __future__ import annotations

import sys
import types

if "boto3" not in sys.modules:
    boto3_stub = types.ModuleType("boto3")
    boto3_stub.resource = lambda *_args, **_kwargs: object()
    sys.modules["boto3"] = boto3_stub
    sys.modules["boto3.dynamodb"] = types.ModuleType("boto3.dynamodb")
    conditions_module = types.ModuleType("boto3.dynamodb.conditions")
    conditions_module.Key = object
    sys.modules["boto3.dynamodb.conditions"] = conditions_module

from ndr.processing.raw_input_resolver import (
    ERROR_CODE_FALLBACK_DISABLED,
    ERROR_CODE_FALLBACK_EMPTY_RESULT,
    ERROR_CODE_FALLBACK_QUERY_CONTRACT_MISSING,
)
from ndr.processing.rt_raw_input_resolver_job import (
    RtRawInputResolverRuntimeConfig,
    run_rt_raw_input_resolver,
)


def test_rt_resolver_ingestion_available_path(monkeypatch):
    import ndr.processing.rt_raw_input_resolver_job as module

    updates: dict[str, object] = {}

    class _Table:
        def update_item(self, **kwargs):
            updates.update(kwargs)

    class _DDB:
        def Table(self, _name):
            return _Table()

    monkeypatch.setattr(module, "load_project_parameters", lambda *_a, **_k: {})
    monkeypatch.setattr(module, "resolve_batch_index_table_name", lambda _t: "idx")
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _DDB())

    result = run_rt_raw_input_resolver(
        RtRawInputResolverRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
            raw_parsed_logs_s3_prefix="s3://bucket/raw/mb-1/",
        )
    )

    assert result["source_mode"] == "ingestion"
    assert result["raw_input_s3_prefix"] == "s3://bucket/raw/mb-1/"
    assert updates["ExpressionAttributeValues"][":rt_flow_status"] == "RAW_INPUT_RESOLVED"


def test_rt_resolver_fallback_enabled_path(monkeypatch):
    import ndr.processing.rt_raw_input_resolver_job as module

    class _Table:
        def update_item(self, **_kwargs):
            return None

    class _DDB:
        def Table(self, _name):
            return _Table()

    monkeypatch.setattr(module, "load_project_parameters", lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": True}})
    monkeypatch.setattr(module, "resolve_batch_index_table_name", lambda _t: "idx")
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _DDB())
    monkeypatch.setattr(
        module.RawInputResolver,
        "resolve",
            lambda *_a, **_k: types.SimpleNamespace(
                source_mode="redshift_unload_fallback",
                raw_input_s3_prefix="s3://bucket/fallback/descriptor=delta/range_0000/attempt_01/",
                resolution_reason="ingestion_rows_missing",
                provenance={
                    "source_mode": "redshift_unload_fallback",
                    "resolution_reason": "ingestion_rows_missing",
                    "producer_flow": "rt_pre_delta_raw_input_resolver",
                    "request_id": "mb-1",
                    "resolved_at": "2025-01-01T00:00:00Z",
                },
            ),
        )

    result = run_rt_raw_input_resolver(
        RtRawInputResolverRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T00:00:00Z",
            batch_end_ts_iso="2025-01-01T00:15:00Z",
        )
    )

    assert result["source_mode"] == "redshift_unload_fallback"


def test_rt_resolver_fallback_disabled_failure(monkeypatch):
    import ndr.processing.rt_raw_input_resolver_job as module

    monkeypatch.setattr(module, "load_project_parameters", lambda *_a, **_k: {})
    try:
        run_rt_raw_input_resolver(
            RtRawInputResolverRuntimeConfig(
                project_name="proj",
                feature_spec_version="v1",
                mini_batch_id="mb-1",
                batch_start_ts_iso="2025-01-01T00:00:00Z",
                batch_end_ts_iso="2025-01-01T00:15:00Z",
            )
        )
    except RuntimeError as exc:
        assert ERROR_CODE_FALLBACK_DISABLED in str(exc)
    else:
        raise AssertionError("expected fallback disabled failure")


def test_rt_resolver_missing_query_contract_failure(monkeypatch):
    import ndr.processing.rt_raw_input_resolver_job as module

    monkeypatch.setattr(module, "load_project_parameters", lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": True}})

    try:
        run_rt_raw_input_resolver(
            RtRawInputResolverRuntimeConfig(
                project_name="proj",
                feature_spec_version="v1",
                mini_batch_id="mb-1",
                batch_start_ts_iso="2025-01-01T00:00:00Z",
                batch_end_ts_iso="2025-01-01T00:15:00Z",
            )
        )
    except RuntimeError as exc:
        assert ERROR_CODE_FALLBACK_QUERY_CONTRACT_MISSING in str(exc)
    else:
        raise AssertionError("expected query contract missing failure")


def test_rt_resolver_empty_fallback_result_failure(monkeypatch):
    import ndr.processing.rt_raw_input_resolver_job as module

    monkeypatch.setattr(module, "load_project_parameters", lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": True}})
    monkeypatch.setattr(
        module.RawInputResolver,
        "resolve",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError(f"{ERROR_CODE_FALLBACK_EMPTY_RESULT}: empty")),
    )

    try:
        run_rt_raw_input_resolver(
            RtRawInputResolverRuntimeConfig(
                project_name="proj",
                feature_spec_version="v1",
                mini_batch_id="mb-1",
                batch_start_ts_iso="2025-01-01T00:00:00Z",
                batch_end_ts_iso="2025-01-01T00:15:00Z",
            )
        )
    except RuntimeError as exc:
        assert ERROR_CODE_FALLBACK_EMPTY_RESULT in str(exc)
    else:
        raise AssertionError("expected empty fallback result failure")
