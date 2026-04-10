from datetime import datetime, timezone
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

from ndr.processing.if_training_job import IFTrainingJob
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig, parse_if_training_spec


class _DummySpark:
    pass


def _runtime(stage: str = "train") -> IFTrainingRuntimeConfig:
    return IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-a",
        feature_spec_version="v1",
        run_id="run-810",
        execution_ts_iso="2024-04-02T00:00:00Z",
        stage=stage,
        dpp_config_table_name="dpp",
        mlp_config_table_name="mlp",
        batch_index_table_name="batch-index",
        training_start_ts="2024-04-01T00:00:00Z",
        training_end_ts="2024-04-01T01:00:00Z",
    )


def _spec():
    return parse_if_training_spec(
        {
            "feature_inputs": {"fg_a": {"s3_prefix": "s3://a"}, "fg_c": {"s3_prefix": "s3://c"}},
            "output": {
                "artifacts_s3_prefix": "s3://artifacts",
                "report_s3_prefix": "s3://reports",
                "production_model_root": "s3://prod-models",
            },
            "model": {"version": "v1"},
        }
    )


def _history_plan():
    return {
        "batch_index_readiness": {
            "contract_version": "training_readiness_manifest.v1",
            "batch_index_evidence": {"selectors": {"pk": "proj"}},
        },
        "missing_windows_manifest": {
            "contract_version": "if_training_missing_windows.v1",
            "entries": [],
        },
    }


def test_task810_plan_gate_requires_verify_stage(monkeypatch):
    job = IFTrainingJob(_DummySpark(), _runtime(stage="plan"), _spec())
    written = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: written.setdefault(stage, payload))

    with pytest.raises(ValueError, match="Task 8 integration gate failed"):
        job._run_task8_integration_gate(stage="plan", verification={"stage": "reverify"})

    assert written["integration_gate"]["status"] == "failed"
    assert any(check["task_id"] == "8.2" and check["passed"] is False for check in written["integration_gate"]["checks"])


def test_task810_train_gate_validates_batch_index_readiness_when_present(monkeypatch):
    job = IFTrainingJob(_DummySpark(), _runtime(stage="train"), _spec())
    monkeypatch.setattr(job, "_write_stage_status", lambda *_args, **_kwargs: None)

    with pytest.raises(ValueError, match="Task 8 integration gate failed"):
        job._run_task8_integration_gate(
            stage="train",
            verification={"stage": "reverify"},
            history_plan={
                "missing_windows_manifest": {"contract_version": "if_training_missing_windows.v1", "entries": []},
                "batch_index_readiness": {"batch_index_evidence": {"selectors": {}}},
            },
        )


def test_task810_train_gate_passes_with_integrated_artifacts(monkeypatch):
    job = IFTrainingJob(_DummySpark(), _runtime(stage="train"), _spec())
    reports = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: reports.setdefault(stage, payload))

    result = job._run_task8_integration_gate(
        stage="train",
        verification={"stage": "reverify"},
        history_plan=_history_plan(),
    )

    assert result["status"] == "passed"
    assert [check["task_id"] for check in result["checks"]][:4] == ["8.1", "8.9", "8.6", "8.2"]
    assert reports["integration_gate"]["contract_version"] == "task8_integration_gate.v1"
