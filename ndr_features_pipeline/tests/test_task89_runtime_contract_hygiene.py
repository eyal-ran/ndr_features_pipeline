import sys
import types

import pytest

if "pyspark" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = object
    pyspark_sql.DataFrame = object
    pyspark_sql.functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql.types = types.ModuleType("pyspark.sql.types")
    for _name in ["StringType", "IntegerType", "LongType", "DoubleType", "TimestampType", "DateType"]:
        setattr(pyspark_sql.types, _name, type(_name, (), {}))
    pyspark.sql = pyspark_sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql.functions
    sys.modules["pyspark.sql.types"] = pyspark_sql.types

from ndr.processing.if_training_job import run_if_training_from_runtime_config
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig


def _runtime(ml_project_name: str = "ml-a") -> IFTrainingRuntimeConfig:
    return IFTrainingRuntimeConfig(
        project_name="proj-a",
        ml_project_name=ml_project_name,
        feature_spec_version="v1",
        run_id="run-89",
        execution_ts_iso="2024-04-01T00:00:00Z",
        dpp_config_table_name="dpp-contract-table",
        mlp_config_table_name="mlp-contract-table",
        batch_index_table_name="batch-index",
        training_start_ts="2024-03-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
    )


def test_runtime_tables_are_consumed_for_linkage_and_job_spec_loading(monkeypatch):
    observed = {}

    class _Loader:
        def __init__(self, dpp_table_name=None, mlp_table_name=None):
            observed["loader_dpp_table_name"] = dpp_table_name
            observed["loader_mlp_table_name"] = mlp_table_name

        def load_ml_project_names(self, *, project_name, feature_spec_version, job_name="project_parameters"):
            observed["linkage_lookup"] = {
                "project_name": project_name,
                "feature_spec_version": feature_spec_version,
                "job_name": job_name,
            }
            return ["ml-a", "ml-b"]

    class _Runner:
        def __init__(self, _spark, runtime_config, _training_spec, resolved_spec_payload=None):
            observed["runner_runtime_config"] = runtime_config
            observed["resolved_spec_payload"] = resolved_spec_payload

        def run(self):
            observed["runner_called"] = True

    monkeypatch.setattr("ndr.config.project_parameters_loader.ProjectParametersLoader", _Loader)
    monkeypatch.setattr(
        "ndr.config.job_spec_loader.load_job_spec",
        lambda **kwargs: observed.setdefault("load_job_spec_kwargs", kwargs) or {
            "feature_inputs": {"fg_a": {"s3_prefix": "s3://a"}, "fg_c": {"s3_prefix": "s3://c"}},
            "output": {"artifacts_s3_prefix": "s3://artifacts", "report_s3_prefix": "s3://reports"},
            "model": {"version": "v1"},
        },
    )
    monkeypatch.setattr("ndr.processing.if_training_job.parse_if_training_spec", lambda _payload: object())
    monkeypatch.setattr(sys.modules["pyspark.sql"], "SparkSession", types.SimpleNamespace(builder=types.SimpleNamespace(getOrCreate=lambda: object())), raising=False)
    monkeypatch.setattr("ndr.processing.if_training_job.IFTrainingJob", _Runner)

    run_if_training_from_runtime_config(_runtime())

    assert observed["loader_dpp_table_name"] == "dpp-contract-table"
    assert observed["loader_mlp_table_name"] == "mlp-contract-table"
    assert observed["linkage_lookup"]["project_name"] == "proj-a"
    assert observed["load_job_spec_kwargs"]["table_name"] == "dpp-contract-table"
    assert observed["runner_called"] is True


def test_runtime_rejects_unlinked_ml_project_name(monkeypatch):
    class _Loader:
        def __init__(self, dpp_table_name=None, mlp_table_name=None):
            assert dpp_table_name == "dpp-contract-table"
            assert mlp_table_name == "mlp-contract-table"

        def load_ml_project_names(self, **_kwargs):
            return ["ml-b"]

    monkeypatch.setattr("ndr.config.project_parameters_loader.ProjectParametersLoader", _Loader)

    with pytest.raises(ValueError, match="ml_project_name is not linked to project_name"):
        run_if_training_from_runtime_config(_runtime(ml_project_name="ml-a"))
