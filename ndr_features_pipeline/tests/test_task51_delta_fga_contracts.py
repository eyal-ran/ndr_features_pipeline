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
    ]:
        setattr(types_module, _name, type(_name, (), {}))
    types_module.TimestampType = type("TimestampType", (), {})
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

from ndr.catalog.schema_manifest import build_delta_manifest
from ndr.config.job_spec_models import (
    DQSpec,
    EnrichmentSpec,
    InputSpec,
    JobSpec,
    OutputSpec,
    RoleMappingSpec,
)
from ndr.processing.base_runner import RuntimeParams
from ndr.processing.delta_builder_job import (
    DeltaBuilderRunner,
    DeltaBuilderJobRuntimeConfig,
    run_delta_builder_from_runtime_config,
)
from ndr.processing.fg_a_builder_job import (
    FGABuilderJobRuntimeConfig,
    run_fg_a_builder_from_runtime_config,
)


def _delta_job_spec(partition_keys: list[str]) -> JobSpec:
    return JobSpec(
        project_name="proj",
        job_name="delta_builder",
        feature_spec_version="v1",
        input=InputSpec(s3_prefix="s3://raw", format="json"),
        dq=DQSpec(),
        enrichment=EnrichmentSpec(),
        roles=[
            RoleMappingSpec(
                name="outbound",
                host_ip="source_ip",
                peer_ip="destination_ip",
                bytes_sent="source_bytes",
                bytes_recv="destination_bytes",
                peer_port="destination_port",
            )
        ],
        operators=[],
        output=OutputSpec(
            s3_prefix="s3://old/delta",
            format="parquet",
            partition_keys=partition_keys,
            write_mode="overwrite",
        ),
    )


def test_delta_manifest_includes_mini_batch_contract_fields():
    manifest_fields = set(build_delta_manifest().field_names)
    assert "mini_batch_id" in manifest_fields
    assert "feature_spec_version" in manifest_fields


def test_delta_partition_contract_rejects_non_canonical_partition_keys():
    runner = DeltaBuilderRunner(
        spark=None,  # type: ignore[arg-type]
        job_spec=_delta_job_spec(partition_keys=["dt", "hour"]),
        runtime=RuntimeParams(
            project_name="proj",
            job_name="delta_builder",
            raw_parsed_logs_s3_prefix="s3://raw",
            feature_spec_version="v1",
            run_id="mb-1",
        ),
    )

    try:
        runner._validate_partition_contract(df=SimpleNamespace(columns=["dt", "hh", "mm"]))  # type: ignore[arg-type]
    except ValueError as exc:
        assert "canonical partition contract" in str(exc)
    else:
        raise AssertionError("Expected partition contract validation to fail")


def test_run_delta_builder_runtime_uses_batch_index_paths(monkeypatch):
    import ndr.processing.delta_builder_job as module

    captured: dict[str, object] = {}

    class _Loader:
        def load(self, **_kwargs):
            return _delta_job_spec(partition_keys=["dt", "hh", "mm"])

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            assert project_name == "proj"
            assert batch_id == "mb-1"
            return SimpleNamespace(
                raw_parsed_logs_s3_prefix="s3://batch-index/raw/mb-1/",
                s3_prefixes={"dpp": {"delta": "s3://batch-index/delta/2025/01/01/10/1"}},
            )

    class _Spark:
        def stop(self):
            return None

    class _SparkBuilder:
        def appName(self, _name):
            return self

        def config(self, *_args, **_kwargs):
            return self

        def getOrCreate(self):
            return _Spark()

    monkeypatch.setattr(module, "JobSpecLoader", lambda: _Loader())
    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(module.SparkSession, "builder", _SparkBuilder())
    monkeypatch.setattr(
        module,
        "run_delta_builder",
        lambda spark, job_spec, runtime: captured.update(
            {"spark": spark, "job_spec": job_spec, "runtime": runtime}
        ),
    )

    run_delta_builder_from_runtime_config(
        DeltaBuilderJobRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
            batch_index_table_name="idx",
        )
    )

    runtime = captured["runtime"]
    assert runtime.raw_parsed_logs_s3_prefix == "s3://batch-index/raw/mb-1/"
    assert captured["job_spec"].output.s3_prefix == "s3://batch-index/delta/2025/01/01/10/1"


def test_run_fg_a_builder_runtime_prefers_batch_index_prefixes(monkeypatch):
    import ndr.processing.fg_a_builder_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            assert project_name == "proj"
            assert batch_id == "mb-1"
            return SimpleNamespace(
                s3_prefixes={
                    "dpp": {
                        "delta": "s3://batch-index/delta/2025/01/01/10/1",
                        "fg_a": "s3://batch-index/fg_a/2025/01/01/10/1",
                    }
                }
            )

    class _Spark:
        def stop(self):
            return None

    class _SparkBuilder:
        def appName(self, _name):
            return self

        def config(self, *_args, **_kwargs):
            return self

        def getOrCreate(self):
            return _Spark()

    class _FGAJob:
        def __init__(self, spark, config):
            captured["spark"] = spark
            captured["config"] = config

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(
        module,
        "load_job_spec",
        lambda **_kwargs: {
            "delta_input": {"s3_prefix": "s3://job-spec/delta"},
            "fg_a_output": {"s3_prefix": "s3://job-spec/fg_a"},
            "lookback30d": {},
        },
    )
    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(module.SparkSession, "builder", _SparkBuilder())
    monkeypatch.setattr(module, "FGABuilderJob", _FGAJob)

    run_fg_a_builder_from_runtime_config(
        FGABuilderJobRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
            batch_index_table_name="idx",
        )
    )

    config = captured["config"]
    assert config.delta_s3_prefix == "s3://batch-index/delta/2025/01/01/10/1"
    assert config.output_s3_prefix == "s3://batch-index/fg_a/2025/01/01/10/1"
    assert config.use_exact_batch_index_output_prefix is True
