from __future__ import annotations

import sys
import types
from types import SimpleNamespace

if "pyspark" not in sys.modules:
    pyspark_module = types.ModuleType("pyspark")
    sql_module = types.ModuleType("pyspark.sql")
    sql_module.SparkSession = type("SparkSession", (), {"builder": None})
    sql_module.DataFrame = object
    sql_module.Window = object
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

    class _Expr:
        def __and__(self, _other):
            return self

    class _Key:
        def __init__(self, name):
            self.name = name

        def eq(self, _value):
            return _Expr()

        def begins_with(self, _value):
            return _Expr()

    conditions_module.Key = _Key
    sys.modules["boto3.dynamodb.conditions"] = conditions_module

from ndr.processing.fg_c_builder_job import (
    FGCorrBuilderJob,
    FGCorrJobRuntimeConfig,
    run_fg_c_builder_from_runtime_config,
)
from ndr.processing.pair_counts_builder_job import (
    PairCountsJobRuntimeConfig,
    run_pair_counts_builder_from_runtime_config,
)


class _FakeRdd:
    def __init__(self, empty: bool):
        self._empty = empty

    def isEmpty(self):
        return self._empty


class _FakeDf:
    def __init__(self, columns: list[str], empty: bool = False):
        self.columns = columns
        self.rdd = _FakeRdd(empty)


def test_fgc_join_granularity_contract_rejects_drift():
    job = FGCorrBuilderJob(
        runtime_config=FGCorrJobRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
        )
    )
    job.job_spec = {"host_baseline_join_keys": ["host_ip", "role", "segment_id", "time_band", "window_label"]}
    try:
        job._validate_host_join_contract(["host_ip", "window_label"])
    except ValueError as exc:
        assert "must match FG-B host baseline granularity" in str(exc)
    else:
        raise AssertionError("Expected FG-C host join contract validation failure")


def test_fgc_fail_fast_when_required_baselines_missing():
    job = FGCorrBuilderJob(
        runtime_config=FGCorrJobRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
        )
    )
    try:
        job._require_non_empty_baselines(
            horizon="7d",
            host_baselines=_FakeDf(columns=[], empty=True),
            segment_baselines=_FakeDf(columns=[], empty=False),
            metadata_df=_FakeDf(columns=[], empty=False),
        )
    except ValueError as exc:
        assert "missing required FG-B baseline dependencies" in str(exc)
    else:
        raise AssertionError("Expected fail-fast baseline dependency validation")


def test_pair_counts_runtime_prefers_batch_index_prefixes(monkeypatch):
    import ndr.processing.pair_counts_builder_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            assert project_name == "proj"
            assert batch_id == "mb-1"
            return SimpleNamespace(
                raw_parsed_logs_s3_prefix="s3://batch-index/raw/mb-1/",
                s3_prefixes={"dpp": {"pair_counts": "s3://batch-index/pair-counts/2025/01/01/10/1"}},
            )

    class _Job:
        def __init__(self, runtime_config):
            captured["runtime_config"] = runtime_config
            captured["job_spec"] = None

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(
        module,
        "load_pair_counts_job_spec",
        lambda **_kwargs: SimpleNamespace(pair_counts_output=SimpleNamespace(s3_prefix="s3://job-spec/pair")),
    )
    monkeypatch.setattr(module, "PairCountsBuilderJob", _Job)

    run_pair_counts_builder_from_runtime_config(
        PairCountsJobRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
            batch_index_table_name="idx",
        )
    )

    assert captured["ran"] is True
    assert captured["runtime_config"].raw_parsed_logs_s3_prefix == "s3://batch-index/raw/mb-1/"


def test_fgc_runtime_prefers_batch_index_prefixes(monkeypatch):
    import ndr.processing.fg_c_builder_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            return SimpleNamespace(
                s3_prefixes={
                    "dpp": {
                        "fg_a": "s3://batch-index/fg_a/2025/01/01/10/1",
                        "fg_b": "s3://batch-index/fg_b/2025/01/01/10/1",
                        "fg_c": "s3://batch-index/fg_c/2025/01/01/10/1",
                        "pair_counts": "s3://batch-index/pair_counts/2025/01/01/10/1",
                    }
                }
            )

    class _Job:
        def __init__(self, runtime_config):
            self.runtime_config = runtime_config
            self.job_spec = {}

        def run(self):
            captured["job_spec"] = self.job_spec
            captured["ran"] = True

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(module, "FGCorrBuilderJob", _Job)
    monkeypatch.setattr(
        module,
        "load_job_spec",
        lambda **_kwargs: {"fg_a_input": {}, "fg_b_input": {}, "fg_c_output": {}, "pair_context_input": {}},
    )

    run_fg_c_builder_from_runtime_config(
        FGCorrJobRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
            batch_index_table_name="idx",
        )
    )

    assert captured["ran"] is True
    assert captured["job_spec"]["fg_a_input"]["s3_prefix"] == "s3://batch-index/fg_a/2025/01/01/10/1"
    assert captured["job_spec"]["fg_b_input"]["s3_prefix"] == "s3://batch-index/fg_b/2025/01/01/10/1"
    assert captured["job_spec"]["fg_c_output"]["s3_prefix"] == "s3://batch-index/fg_c/2025/01/01/10/1"
