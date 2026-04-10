from __future__ import annotations

import sys
import types
from types import SimpleNamespace

if "pyspark" not in sys.modules:
    pyspark_module = types.ModuleType("pyspark")
    sql_module = types.ModuleType("pyspark.sql")
    sql_module.SparkSession = type("SparkSession", (), {"builder": None})
    sql_module.DataFrame = object
    sql_module.functions = types.SimpleNamespace()
    types_module = types.ModuleType("pyspark.sql.types")
    for _name in [
        "StringType",
        "IntegerType",
        "LongType",
        "DoubleType",
        "TimestampType",
        "DateType",
        "StructType",
        "StructField",
    ]:
        setattr(types_module, _name, type(_name, (), {}))
    sql_module.types = types_module
    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = sql_module
    sys.modules["pyspark.sql.types"] = types_module
    sys.modules["pyspark.sql.functions"] = sql_module.functions

if "boto3" not in sys.modules:
    boto3_stub = types.ModuleType("boto3")
    boto3_stub.client = lambda *_args, **_kwargs: object()
    boto3_stub.resource = lambda *_args, **_kwargs: object()
    sys.modules["boto3"] = boto3_stub
    conditions_module = types.ModuleType("boto3.dynamodb.conditions")
    conditions_module.Key = object
    sys.modules["boto3.dynamodb.conditions"] = conditions_module

from ndr.processing.pair_counts_builder_job import PairCountsJobRuntimeConfig, run_pair_counts_builder_from_runtime_config
from ndr.processing.raw_input_resolver import RawInputResolution


def _runtime() -> PairCountsJobRuntimeConfig:
    return PairCountsJobRuntimeConfig(
        project_name="proj",
        feature_spec_version="v1",
        mini_batch_id="mb-1",
        batch_start_ts_iso="2025-01-01T00:00:00Z",
        batch_end_ts_iso="2025-01-01T00:15:00Z",
        batch_index_table_name="idx",
        dpp_config_table_name="dpp",
    )


def test_pair_counts_ingestion_available_path(monkeypatch):
    import ndr.processing.pair_counts_builder_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            return SimpleNamespace(
                raw_parsed_logs_s3_prefix="s3://batch-index/raw/mb-1/",
                s3_prefixes={"dpp": {"pair_counts": "s3://batch-index/pair-counts/"}},
            )

    class _Job:
        def __init__(self, runtime_config):
            captured["runtime"] = runtime_config
            self.job_spec = None

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(
        module,
        "load_project_parameters",
        lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": True}},
    )
    monkeypatch.setattr(
        module,
        "RawInputResolver",
        lambda: SimpleNamespace(
            resolve=lambda **_kwargs: RawInputResolution(
                source_mode="ingestion",
                raw_input_s3_prefix="s3://batch-index/raw/mb-1/",
                resolution_reason="ingestion_rows_present",
                provenance={"source_mode": "ingestion", "resolution_reason": "ingestion_rows_present"},
            )
        ),
    )
    monkeypatch.setattr(module, "PairCountsBuilderJob", _Job)
    monkeypatch.setattr(
        module,
        "load_pair_counts_job_spec",
        lambda **_kwargs: SimpleNamespace(pair_counts_output=SimpleNamespace(s3_prefix="s3://job-spec/pair")),
    )
    monkeypatch.setattr(
        module,
        "_write_pair_counts_run_metadata",
        lambda **kwargs: captured.setdefault("metadata_runtime", kwargs["runtime_config"]),
    )

    run_pair_counts_builder_from_runtime_config(_runtime())

    assert captured["ran"] is True
    assert captured["runtime"].raw_parsed_logs_s3_prefix == "s3://batch-index/raw/mb-1/"
    assert captured["runtime"].raw_input_resolution["source_mode"] == "ingestion"


def test_pair_counts_ingestion_missing_fallback_enabled_path(monkeypatch):
    import ndr.processing.pair_counts_builder_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            return SimpleNamespace(
                raw_parsed_logs_s3_prefix="",
                s3_prefixes={"dpp": {"pair_counts": "s3://batch-index/pair-counts/"}},
            )

    class _Job:
        def __init__(self, runtime_config):
            captured["runtime"] = runtime_config
            self.job_spec = None

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(
        module,
        "load_project_parameters",
        lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": True}},
    )
    monkeypatch.setattr(
        module,
        "RawInputResolver",
        lambda: SimpleNamespace(
            resolve=lambda **_kwargs: RawInputResolution(
                source_mode="redshift_unload_fallback",
                raw_input_s3_prefix="s3://fallback/unload/range_0000/attempt_01/",
                resolution_reason="ingestion_rows_missing",
                provenance={
                    "source_mode": "redshift_unload_fallback",
                    "resolution_reason": "ingestion_rows_missing",
                },
            )
        ),
    )
    monkeypatch.setattr(module, "PairCountsBuilderJob", _Job)
    monkeypatch.setattr(
        module,
        "load_pair_counts_job_spec",
        lambda **_kwargs: SimpleNamespace(pair_counts_output=SimpleNamespace(s3_prefix="s3://job-spec/pair")),
    )
    monkeypatch.setattr(module, "_write_pair_counts_run_metadata", lambda **_kwargs: None)

    run_pair_counts_builder_from_runtime_config(_runtime())

    assert captured["ran"] is True
    assert captured["runtime"].raw_input_resolution["source_mode"] == "redshift_unload_fallback"


def test_pair_counts_fallback_disabled_failure_path(monkeypatch):
    import ndr.processing.pair_counts_builder_job as module

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            return SimpleNamespace(raw_parsed_logs_s3_prefix="", s3_prefixes={"dpp": {}})

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(
        module,
        "load_project_parameters",
        lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": False}},
    )

    def _fail(**_kwargs):
        raise RuntimeError("RAW_INPUT_FALLBACK_DISABLED: No ingestion rows resolved and Redshift fallback is disabled")

    monkeypatch.setattr(module, "RawInputResolver", lambda: SimpleNamespace(resolve=_fail))

    try:
        run_pair_counts_builder_from_runtime_config(_runtime())
    except RuntimeError as exc:
        assert "RAW_INPUT_FALLBACK_DISABLED" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")


def test_pair_counts_empty_fallback_result_failure_path(monkeypatch):
    import ndr.processing.pair_counts_builder_job as module

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            return SimpleNamespace(raw_parsed_logs_s3_prefix="", s3_prefixes={"dpp": {}})

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(
        module,
        "load_project_parameters",
        lambda *_a, **_k: {"backfill_redshift_fallback": {"enabled": True}},
    )

    def _fail(**_kwargs):
        raise RuntimeError("RAW_INPUT_FALLBACK_EMPTY_RESULT: fallback execution returned no ranges")

    monkeypatch.setattr(module, "RawInputResolver", lambda: SimpleNamespace(resolve=_fail))

    try:
        run_pair_counts_builder_from_runtime_config(_runtime())
    except RuntimeError as exc:
        assert "RAW_INPUT_FALLBACK_EMPTY_RESULT" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")
