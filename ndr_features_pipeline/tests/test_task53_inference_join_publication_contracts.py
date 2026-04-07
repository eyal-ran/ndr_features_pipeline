from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from datetime import datetime, timezone

if "pyspark" not in sys.modules:
    sys.modules["pyspark"] = types.ModuleType("pyspark")
if "pyspark.sql" not in sys.modules:
    sys.modules["pyspark.sql"] = types.ModuleType("pyspark.sql")
sql_module = sys.modules["pyspark.sql"]
sql_module.SparkSession = getattr(sql_module, "SparkSession", type("SparkSession", (), {"builder": None}))
sql_module.DataFrame = getattr(sql_module, "DataFrame", object)
sql_module.Row = getattr(sql_module, "Row", object)
sql_module.functions = getattr(sql_module, "functions", types.SimpleNamespace())
if "pyspark.sql.types" not in sys.modules:
    sys.modules["pyspark.sql.types"] = types.ModuleType("pyspark.sql.types")
types_module = sys.modules["pyspark.sql.types"]
for _name in [
    "StructType",
    "StructField",
    "StringType",
    "DoubleType",
    "TimestampType",
    "IntegerType",
    "LongType",
    "FloatType",
    "BooleanType",
    "DateType",
]:
    if not hasattr(types_module, _name):
        setattr(types_module, _name, type(_name, (), {}))
sql_module.types = types_module
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
        def __init__(self, _name):
            pass

        def eq(self, _value):
            return _Expr()

        def begins_with(self, _value):
            return _Expr()

    conditions_module.Key = _Key
    sys.modules["boto3.dynamodb.conditions"] = conditions_module
sys.modules.setdefault("boto3.dynamodb.conditions", types.ModuleType("boto3.dynamodb.conditions"))
if not hasattr(sys.modules["boto3.dynamodb.conditions"], "Key"):
    class _Expr:
        def __and__(self, _other):
            return self

    class _Key:
        def __init__(self, _name):
            pass

        def eq(self, _value):
            return _Expr()

        def begins_with(self, _value):
            return _Expr()

    sys.modules["boto3.dynamodb.conditions"].Key = _Key

from ndr.processing.inference_predictions_job import (
    SagemakerEndpointInvoker,
    run_inference_predictions_from_runtime_config,
)
from ndr.processing.inference_predictions_spec import (
    InferenceModelSpec,
    InferencePredictionsRuntimeConfig,
)
from ndr.processing.prediction_feature_join_job import run_prediction_feature_join_from_runtime_config


def test_inference_metadata_contract_is_deterministic():
    invoker = SagemakerEndpointInvoker.__new__(SagemakerEndpointInvoker)
    invoker.model_spec = InferenceModelSpec(endpoint_name="endpoint-x", model_version="mv-1")
    runtime = InferencePredictionsRuntimeConfig(
        project_name="proj",
        feature_spec_version="v1",
        mini_batch_id="mb-1",
        batch_start_ts_iso="2025-01-01T10:00:00Z",
        batch_end_ts_iso="2025-01-01T10:15:00Z",
        ml_project_name="ml-a",
    )
    inference_ts = datetime(2025, 1, 1, 10, 15, tzinfo=timezone.utc)
    meta = {"host_ip": "1.1.1.1", "window_label": "15m", "window_end_ts": "2025-01-01T10:15:00Z"}

    left = invoker._merge_predictions(
        [(meta, {"f1": 1.0})],
        [0.9],
        SimpleNamespace(score_column="prediction_score", label_column=None),
        runtime,
        inference_ts,
        {},
    )[0]
    right = invoker._merge_predictions(
        [(meta, {"f1": 1.0})],
        [0.9],
        SimpleNamespace(score_column="prediction_score", label_column=None),
        runtime,
        inference_ts,
        {},
    )[0]

    assert left["record_id"] == right["record_id"]
    assert left["model_version"] == "mv-1"
    assert left["model_name"] == "endpoint-x"
    assert left["inference_ts"] == inference_ts


def test_inference_runtime_uses_batch_index_branch_prefixes(monkeypatch):
    import ndr.processing.inference_predictions_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            assert project_name == "proj"
            assert batch_id == "mb-1"
            return SimpleNamespace(
                s3_prefixes={
                    "dpp": {"fg_a": "s3://idx/fg_a/2025/01/01/10/1/part-00000.parquet"},
                    "mlp": {"ml-a": {"predictions": "s3://idx/mlp/predictions/part-00000.parquet"}},
                }
            )

    class _Spark:
        def stop(self):
            return None

    class _SparkBuilder:
        def getOrCreate(self):
            return _Spark()

    class _Job:
        def __init__(self, spark, runtime_config, inference_spec):
            captured["runtime"] = runtime_config
            captured["spec"] = inference_spec

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr("ndr.config.job_spec_loader.load_job_spec", lambda **_kwargs: {
        "feature_inputs": {"fg_a": {"s3_prefix": "s3://job/fg_a"}},
        "join_keys": ["host_ip", "window_label", "window_end_ts"],
        "model": {"endpoint_name": "ep"},
        "output": {"s3_prefix": "s3://job/pred"},
    })
    monkeypatch.setattr(
        sys.modules["pyspark.sql"],
        "SparkSession",
        type("SparkSession", (), {"builder": _SparkBuilder()}),
        raising=False,
    )
    monkeypatch.setattr(module, "InferencePredictionsJob", _Job)

    run_inference_predictions_from_runtime_config(
        InferencePredictionsRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
            ml_project_name="ml-a",
            batch_index_table_name="idx",
        )
    )

    assert captured["ran"] is True
    assert captured["spec"].feature_inputs["fg_a"].exact_prefix is True
    assert captured["spec"].output.exact_prefix is True


def test_prediction_join_runtime_uses_same_branch_contract_snapshot(monkeypatch):
    import ndr.processing.prediction_feature_join_job as module

    captured: dict[str, object] = {}

    class _BatchIndex:
        def __init__(self, table_name=None):
            assert table_name == "idx"

        def get_batch(self, *, project_name: str, batch_id: str):
            return SimpleNamespace(
                s3_prefixes={
                    "dpp": {"fg_a": "s3://idx/fg_a/part-00000.parquet", "fg_c": "s3://idx/fg_c/part-00000.parquet"},
                    "mlp": {
                        "ml-a": {
                            "predictions": "s3://idx/pred/part-00000.parquet",
                            "prediction_join": "s3://idx/join/part-00000.parquet",
                        }
                    },
                }
            )

    class _Spark:
        def stop(self):
            return None

    class _SparkBuilder:
        def getOrCreate(self):
            return _Spark()

    class _Job:
        def __init__(self, spark, runtime_config, inference_spec, join_spec):
            captured["inference_spec"] = inference_spec
            captured["join_spec"] = join_spec

        def run(self):
            captured["ran"] = True

    monkeypatch.setattr(module, "BatchIndexLoader", _BatchIndex)
    monkeypatch.setattr(module, "load_job_spec", lambda **kwargs: {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://job/fg_a"},
            "fg_c": {"s3_prefix": "s3://job/fg_c"},
        },
        "join_keys": ["host_ip", "window_label", "window_end_ts"],
        "model": {"endpoint_name": "ep"},
        "output": {"s3_prefix": "s3://job/pred"},
    } if kwargs["job_name"] == "inference_predictions" else {
        "destination": {"type": "s3", "s3": {"s3_prefix": "s3://job/join", "write_mode": "overwrite"}},
    })
    monkeypatch.setattr(module, "SparkSession", type("SparkSession", (), {"builder": _SparkBuilder()}))
    monkeypatch.setattr(module, "PredictionFeatureJoinJob", _Job)

    run_prediction_feature_join_from_runtime_config(
        module.PredictionFeatureJoinRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            mini_batch_id="mb-1",
            batch_start_ts_iso="2025-01-01T10:00:00Z",
            batch_end_ts_iso="2025-01-01T10:15:00Z",
            ml_project_name="ml-a",
            batch_index_table_name="idx",
        )
    )

    assert captured["ran"] is True
    assert captured["inference_spec"].output.exact_prefix is True
    assert captured["join_spec"].s3_output.exact_prefix is True
