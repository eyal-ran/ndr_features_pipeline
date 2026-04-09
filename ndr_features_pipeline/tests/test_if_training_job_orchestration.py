import sys
import types
import json

import pytest

# Allow importing modules that require boto3/pyspark in this test environment.
if "boto3" not in sys.modules:
    sys.modules["boto3"] = types.SimpleNamespace()

if "pyspark" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = object
    pyspark_sql.DataFrame = object
    pyspark_sql.functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql.types = types.ModuleType("pyspark.sql.types")
    for _name in [
        "StringType", "IntegerType", "LongType", "DoubleType", "TimestampType", "DateType"
    ]:
        setattr(pyspark_sql.types, _name, type(_name, (), {}))
    pyspark.sql = pyspark_sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql.functions
    sys.modules["pyspark.sql.types"] = pyspark_sql.types

from ndr.processing.if_training_job import IFTrainingJob
from ndr.processing.if_training_spec import EvaluationWindowSpec, IFTrainingRuntimeConfig, parse_if_training_spec
from ndr.orchestration.training_missing_manifest import from_missing_sources




class _Comparable:
    def __lt__(self, _other):
        return True

    def __ge__(self, _other):
        return True

class _DummyRDD:
    @staticmethod
    def isEmpty():
        return False


class _DummyDF:
    def __init__(self):
        self.columns = ["host_ip", "window_label", "window_end_ts", "f1", "f2"]
        self.rdd = _DummyRDD()
        self.window_end_ts = _Comparable()

    def join(self, other, on=None, how=None):
        return self

    def filter(self, *_args, **_kwargs):
        return self


def _make_spec():
    return parse_if_training_spec(
        {
            "feature_inputs": {
                "fg_a": {"s3_prefix": "s3://fg-a"},
                "fg_c": {"s3_prefix": "s3://fg-c"},
            },
            "output": {
                "artifacts_s3_prefix": "s3://models/if_training",
                "report_s3_prefix": "s3://models/reports",
            },
            "model": {"version": "v2"},
        }
    )


def _missing_manifest(runtime, include_15m=False, include_fgb=False):
    missing_15m = [{"window_start_ts": "2024-04-01T00:00:00Z", "window_end_ts": "2024-04-01T00:15:00Z"}] if include_15m else []
    missing_fgb = [{"reference_time_iso": "2024-04-01T00:00:00Z", "horizons": ["7d"]}] if include_fgb else []
    return from_missing_sources(
        missing_15m_windows=missing_15m,
        missing_fgb_windows=missing_fgb,
        project_name=runtime.project_name,
        feature_spec_version=runtime.feature_spec_version,
        ml_project_name=runtime.ml_project_name,
        run_id=runtime.run_id,
    )


def test_run_orchestrates_artifact_before_deploy(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-1",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
        eval_start_ts="2024-04-01T00:00:00Z",
        eval_end_ts="2024-05-01T00:00:00Z",
    )
    spec = _make_spec()
    spec.toggles.enable_post_training_evaluation = False
    job = IFTrainingJob(_DummyDF(), runtime, spec)

    dummy_df = _DummyDF()
    call_order = []

    monkeypatch.setattr("ndr.processing.if_training_job.enforce_schema", lambda df, *_args, **_kwargs: df)
    monkeypatch.setattr("ndr.processing.if_training_job.split_metadata_and_feature_columns", lambda *_args, **_kwargs: (["host_ip"], ["f1", "f2"]))

    from datetime import datetime, timezone
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 4, 1, tzinfo=timezone.utc)))
    monkeypatch.setattr(job, "_read_windowed_input", lambda *_args, **_kwargs: dummy_df)
    monkeypatch.setattr(job, "_preflight_validation", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(job, "_fit_preprocessing_params", lambda *_args, **_kwargs: ({"f1": {}}, {"f1": {}}))
    monkeypatch.setattr(job, "_apply_preprocessing_with_params", lambda *_args, **_kwargs: dummy_df)
    monkeypatch.setattr(job, "_select_features", lambda *_args, **_kwargs: (["f1"], {"steps": ["test"]}))
    monkeypatch.setattr(job, "_tune_and_validate", lambda *_args, **_kwargs: ({"best_params": {}}, {"n_estimators": 100, "max_samples": 1.0, "max_features": 1.0, "contamination": 0.01, "bootstrap": False}, {"min_relative_improvement": {"passed": True}, "max_alert_volume_delta": {"passed": True}, "max_score_drift": {"passed": True}}, {"val_alert_rate": 0.1, "score_drift": 0.1}))
    monkeypatch.setattr(job, "_fit_final_model", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(job, "_build_metrics", lambda *_args, **_kwargs: {"ok": 1.0})

    def _persist(*_args, **_kwargs):
        call_order.append("persist")
        return {
            "bucket": "b",
            "run_prefix": "r",
            "model_key": "m",
            "model_tar_key": "mt",
            "model_hash": "h",
            "model_tar_s3_uri": "s3://b/r/model/model.tar.gz",
        }

    monkeypatch.setattr(job, "_persist_artifacts", _persist)
    monkeypatch.setattr(job, "_promote_latest_model_pointer", lambda *_args, **_kwargs: "s3://b/latest")
    monkeypatch.setattr(job, "_enforce_pre_train_reverify_gate", lambda: {"stage": "reverify", "needs_remediation": False, "missing_windows_manifest": _missing_manifest(runtime)})
    monkeypatch.setattr(job, "_load_required_history_plan", lambda: {"missing_windows_manifest": _missing_manifest(runtime)})
    monkeypatch.setattr(job, "_run_dependency_readiness_gate", lambda **_kwargs: {"status": "passed", "checks": []})
    monkeypatch.setattr(job, "_log_sagemaker_experiments", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(job, "_write_final_report_and_success", lambda *_args, **_kwargs: call_order.append("report"))

    job.run()
    assert call_order == ["persist", "report"]


def test_run_failure_writes_failure_artifacts(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-2",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
        eval_start_ts="2024-04-01T00:00:00Z",
        eval_end_ts="2024-05-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    from datetime import datetime, timezone
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 4, 1, tzinfo=timezone.utc)))

    calls = []
    monkeypatch.setattr(job, "_enforce_pre_train_reverify_gate", lambda: {"stage": "reverify", "needs_remediation": False, "missing_windows_manifest": _missing_manifest(runtime)})
    monkeypatch.setattr(job, "_load_required_history_plan", lambda: {"missing_windows_manifest": _missing_manifest(runtime)})
    monkeypatch.setattr(job, "_run_dependency_readiness_gate", lambda **_kwargs: {"status": "passed", "checks": []})

    def _boom(*_args, **_kwargs):
        raise ValueError("forced preflight failure")

    monkeypatch.setattr(job, "_read_windowed_input", _boom)
    monkeypatch.setattr(job, "_write_failure_report", lambda **_kwargs: calls.append("failure_report"))
    monkeypatch.setattr(job, "_write_failure_experiment_artifact", lambda **_kwargs: calls.append("failure_experiment"))

    try:
        job.run()
    except ValueError as exc:
        assert "forced preflight failure" in str(exc)
    else:
        raise AssertionError("Expected run() to raise")

    assert calls == ["failure_report", "failure_experiment"]


def test_write_preflight_failure_artifact_includes_resolved_payload(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-3",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
        eval_start_ts="2024-04-01T00:00:00Z",
        eval_end_ts="2024-05-01T00:00:00Z",
    )
    resolved_payload = {"feature_inputs": {"fg_a": "a", "fg_c": "c"}, "param": 1}
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec(), resolved_spec_payload=resolved_payload)

    writes = {}

    class _DummyS3Client:
        pass

    monkeypatch.setattr(sys.modules["boto3"], "client", lambda _name: _DummyS3Client(), raising=False)
    monkeypatch.setattr(
        "ndr.processing.if_training_job._put_json",
        lambda _client, _bucket, key, payload: writes.update({"key": key, "payload": payload}),
    )

    from datetime import datetime, timezone
    job._write_preflight_failure_artifact(
        train_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        train_end=datetime(2024, 4, 1, tzinfo=timezone.utc),
        reason="coverage below threshold",
        context={"dataset": "fg_a", "coverage_ratio": 0.2},
    )

    assert writes["key"].endswith("preflight_failure_context.json")
    assert "ml_project_name=ml-proj" in writes["key"]
    assert writes["payload"]["resolved_job_spec_payload"] == resolved_payload
    assert writes["payload"]["run_metadata"]["ml_project_name"] == "ml-proj"
    assert writes["payload"]["failure"]["stage"] == "preflight"


def test_invoke_evaluation_pipeline_propagates_ml_project_name(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-eval",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    class _SM:
        def __init__(self):
            self.params = None

        def start_pipeline_execution(self, **kwargs):
            self.params = kwargs["PipelineParameters"]
            return {"PipelineExecutionArn": "arn:exec:1"}

        def describe_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionStatus": "Succeeded"}

    sm = _SM()
    monkeypatch.setattr("ndr.processing.if_training_job.time.sleep", lambda *_args, **_kwargs: None)
    result = job._invoke_evaluation_pipeline(
        sagemaker_client=sm,
        pipeline_name="pipeline_inference_predictions",
        window_id="window-1",
        start_ts="2024-04-01T00:00:00Z",
        end_ts="2024-04-01T01:00:00Z",
        mode="inference",
    )
    param_names = {item["Name"] for item in sm.params}
    mlp_value = next(item["Value"] for item in sm.params if item["Name"] == "MlProjectName")
    assert result["status"] == "Succeeded"
    assert "MlProjectName" in param_names
    assert mlp_value == "ml-proj"


def test_branch_scoped_output_prefixes_include_ml_project_name():
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-branch",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())
    from datetime import datetime, timezone
    report_prefix = job._report_run_prefix("reports")
    artifact_prefix = job._artifact_run_prefix(
        "artifacts",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 4, 1, tzinfo=timezone.utc),
    )
    assert report_prefix.startswith("reports/ml_project_name=ml-proj/")
    assert "/run_id=run-branch" in report_prefix
    assert artifact_prefix.startswith("artifacts/ml_project_name=ml-proj/")


class _DummySageMakerClient:
    def __init__(self):
        self.associations = []
        self.batch_metrics = []
        self.updated = []

    def create_experiment(self, **_kwargs):
        return None

    def create_trial(self, **_kwargs):
        return None

    def create_trial_component(self, **_kwargs):
        return None

    def associate_trial_component(self, **kwargs):
        self.associations.append(kwargs)

    def batch_put_metrics(self, **kwargs):
        self.batch_metrics.append(kwargs)

    def update_trial_component(self, **kwargs):
        self.updated.append(kwargs)


def test_log_sagemaker_experiments_writes_rich_components(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-4",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
        eval_start_ts="2024-04-01T00:00:00Z",
        eval_end_ts="2024-05-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    sm_client = _DummySageMakerClient()
    monkeypatch.setattr(sys.modules["boto3"], "client", lambda _name: sm_client, raising=False)

    job._log_sagemaker_experiments(
        metrics={"val_alert_rate": 0.1},
        tuning_summary={"best_objective": 0.3, "baseline_objective": 0.4, "objective_improvement": 0.1},
        deployment_status={"attempted": True, "status": "updated"},
        preflight={"join_coverage_ratio": 0.9, "partition_coverage": {"fg_a": {"coverage_ratio": 0.9}}},
        report_s3_uri="s3://reports/run/final_training_report.json",
        selected_feature_count=10,
        all_feature_count=20,
        history_planner={"computed": {"w_required": {"start": "2024-01-01T00:00:00Z"}}},
        evaluation_windows=[{"window_id": "e1", "start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-02T00:00:00Z"}],
    )

    assert len(sm_client.associations) >= 4
    assert {item["TrialComponentName"] for item in sm_client.batch_metrics} >= {
        "if-training-v1-run-4-tuning",
        "if-training-v1-run-4-final",
    }
    final_update = [item for item in sm_client.updated if item["TrialComponentName"].endswith("-final")][0]
    assert final_update["Parameters"]["final_report_s3_uri"]["StringValue"].endswith("final_training_report.json")


def test_preflight_fails_on_underfilled_window(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-5",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
        eval_start_ts="2024-04-01T00:00:00Z",
        eval_end_ts="2024-05-01T00:00:00Z",
    )
    spec = _make_spec()
    spec.reliability.min_rows_per_window = 50
    job = IFTrainingJob(_DummyDF(), runtime, spec)

    class _Collectable:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

        def distinct(self):
            return self

        def count(self):
            return 100

        def where(self, *_args, **_kwargs):
            return self

    class _GroupBy:
        def count(self):
            return _Collectable([{"window_label": "15m", "count": 10}])

    class _PreflightDF:
        columns = ["host_ip", "window_label", "window_end_ts", "dt"]

        def count(self):
            return 100

        def select(self, *_args):
            return _Collectable([])

        def groupBy(self, *_args):
            return _GroupBy()

    captured = {}
    monkeypatch.setattr(job, "_write_preflight_failure_artifact", lambda **kwargs: captured.update(kwargs))

    from datetime import datetime, timezone
    try:
        job._preflight_validation(
            _PreflightDF(),
            _PreflightDF(),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 4, 1, tzinfo=timezone.utc),
        )
    except ValueError as exc:
        assert "window_label coverage" in str(exc)
    else:
        raise AssertionError("Expected underfilled window failure")

    assert captured["context"]["minimum_rows_per_window"] == 50

def _runtime_with_stage(stage: str, mode: str = "training") -> IFTrainingRuntimeConfig:
    return IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id=f"run-{stage}",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
        eval_start_ts="2024-04-01T00:00:00Z",
        eval_end_ts="2024-05-01T00:00:00Z",
        mode=mode,
        stage=stage,
    )


def test_publish_stage_writes_publication_metadata(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("publish", mode="production"), _make_spec())

    monkeypatch.setattr(
        job,
        "_load_final_training_report",
        lambda: {
            "final_model": {
                "model_image_copy_path": "s3://bucket/model.tar.gz",
                "artifact_hash": "abc123",
            },
            "validation_gates": {"min_relative_improvement": {"passed": True}},
        },
    )
    writes = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: writes.update({"stage": stage, "payload": payload}))

    job.run()

    assert writes["stage"] == "publish"
    assert writes["payload"]["status"] == "published"
    assert writes["payload"]["model_tar_s3_uri"] == "s3://bucket/model.tar.gz"


def test_deploy_stage_invokes_maybe_deploy_with_published_model(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("deploy", mode="production"), _make_spec())

    monkeypatch.setattr(
        job,
        "_load_stage_status",
        lambda *_args, **_kwargs: {
            "model_tar_s3_uri": "s3://bucket/model.tar.gz",
            "validation_gates": {"max_score_drift": {"passed": True}},
        },
    )
    observed = {}
    def _deploy_call(**kwargs):
        observed["deploy_call"] = kwargs
        return {"attempted": True, "status": "updated"}

    monkeypatch.setattr(job, "_maybe_deploy", _deploy_call)
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    job.run()

    assert observed["deploy_call"]["model_data_url"] == "s3://bucket/model.tar.gz"
    assert observed["stage"] == "deploy"


def test_non_production_publish_attributes_deploy_stages_skip(monkeypatch):
    observed = {}
    for stage in ("publish", "attributes", "deploy"):
        job = IFTrainingJob(_DummyDF(), _runtime_with_stage(stage, mode="evaluation"), _make_spec())
        monkeypatch.setattr(job, "_write_stage_status", lambda stage_name, payload: observed.update({stage_name: payload}))
        job.run()
    assert observed["publish"]["status"] == "skipped"
    assert observed["attributes"]["status"] == "skipped"
    assert observed["deploy"]["status"] == "skipped"


def test_remediate_stage_skips_when_no_missing_windows(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("remediate"), _make_spec())

    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"needs_remediation": False, "missing_windows_manifest": _missing_manifest(job.runtime_config)})
    monkeypatch.setattr(job, "_load_required_history_plan", lambda: {})
    observed = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    job.run()

    assert observed["stage"] == "remediation"
    assert observed["payload"]["status"] == "skipped"


def test_invalid_mode_fails_fast_with_contract_error():
    runtime = _runtime_with_stage("train", mode="invalid-mode")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())
    with pytest.raises(ValueError, match="IFTrainingModeContractError"):
        job.run()


def test_evaluation_mode_requires_eval_bounds_or_spec_windows():
    runtime = _runtime_with_stage("train", mode="evaluation")
    runtime.eval_start_ts = None
    runtime.eval_end_ts = None
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())
    with pytest.raises(ValueError, match="evaluation mode requires explicit evaluation bounds"):
        job.run()


def test_training_mode_skips_post_training_evaluation_pipeline_invocation(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("train", mode="training"), _make_spec())
    monkeypatch.setattr(job, "_invoke_evaluation_pipeline", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not invoke")))
    assert job._run_post_training_evaluation(
        [{"window_id": "w1", "start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T01:00:00Z"}]
    ) == []


def test_evaluation_mode_runs_post_training_evaluation_pipeline_invocation(monkeypatch):
    runtime = _runtime_with_stage("train", mode="evaluation")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())
    observed = {"calls": 0}

    class _S3:
        def put_object(self, **_kwargs):
            return None

    monkeypatch.setattr(sys.modules["boto3"], "client", lambda name: _S3() if name == "s3" else object(), raising=False)
    monkeypatch.setattr(job, "_run_dependency_readiness_gate", lambda **_kwargs: {"checks": [{"family": "inference", "resolved_target": "p-inf"}, {"family": "prediction_feature_join", "resolved_target": "p-join"}]})
    monkeypatch.setattr(job, "_invoke_evaluation_pipeline", lambda **_kwargs: observed.__setitem__("calls", observed["calls"] + 1) or {"status": "Succeeded", "pipeline_execution_arn": "arn", "failure_reason": ""})
    monkeypatch.setattr(job, "_count_inference_records_for_window", lambda **_kwargs: 1)

    manifests = job._run_post_training_evaluation(
        [{"window_id": "w1", "start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T01:00:00Z"}]
    )
    assert observed["calls"] == 2
    assert manifests[0]["metrics"]["status"] == "Succeeded"



def test_tune_and_validate_emits_fallback_telemetry_when_optuna_missing(monkeypatch):
    import builtins
    pytest.importorskip("numpy")
    pytest.importorskip("sklearn")
    runtime = _runtime_with_stage("train")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    class _SimplePandasLike:
        def __init__(self, rows):
            self._rows = rows

        def fillna(self, _value):
            return self

        @property
        def empty(self):
            return len(self._rows) == 0

        def to_numpy(self, dtype=float):
            return self._rows

    class _PandasCarrier:
        def __init__(self, rows):
            self._rows = rows

        def toPandas(self):
            return _SimplePandasLike(self._rows)

    class _Processed:
        def __init__(self, rows):
            self._rows = rows

        def select(self, *_args, **_kwargs):
            return _PandasCarrier(self._rows)

    train_processed = _Processed([[0.1, 1.0], [0.2, 1.1], [0.3, 1.2]])
    val_processed = _Processed([[0.15, 1.05], [0.25, 1.15], [0.35, 1.25]])

    observed = {}

    def _fallback(**kwargs):
        observed["fallback_called"] = True
        params = {
            "n_estimators": 200,
            "max_samples": 1.0,
            "max_features": 1.0,
            "contamination": 0.01,
            "bootstrap": False,
        }
        metrics = kwargs["evaluate_fn"](params)
        return ([{"trial": 0, "params": params, **metrics}], {})

    monkeypatch.setattr("ndr.processing.bayesian_search_fallback.run_bayesian_search", _fallback)

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "optuna":
            raise ImportError("optuna unavailable in test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)

    tuning_summary, _best_params, _gates, _val_metrics = job._tune_and_validate(train_processed, val_processed, ["f1", "f2"])

    assert observed["fallback_called"] is True
    assert tuning_summary["hpo_method"] == "local_bayesian_fallback"
    assert tuning_summary["hpo_fallback_used"] is True
    assert tuning_summary["hpo_fallback_activation_count"] == 1


def test_history_planner_computes_required_44_day_envelope():
    runtime = _runtime_with_stage("train")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())
    monkeypatch_manifest = from_missing_sources(
        missing_15m_windows=[],
        missing_fgb_windows=[],
        project_name=runtime.project_name,
        feature_spec_version=runtime.feature_spec_version,
        ml_project_name=runtime.ml_project_name,
        run_id=runtime.run_id,
    )
    job._derive_batch_index_readiness = lambda **_kwargs: {  # type: ignore[attr-defined]
        "training_readiness_manifest": {"batch_index_evidence": {}},
        "missing_windows_manifest": monkeypatch_manifest,
    }

    from datetime import datetime, timezone
    training_start = datetime(2024, 4, 1, tzinfo=timezone.utc)
    training_end = datetime(2024, 4, 2, tzinfo=timezone.utc)
    eval_windows = [{"window_id": "w1", "start_ts": "2024-04-10T00:00:00Z", "end_ts": "2024-04-11T00:00:00Z"}]

    plan = job._compute_history_plan(training_start, training_end, eval_windows)
    assert plan["computed"]["b_start"] == "2024-02-17T00:00:00Z"
    assert plan["computed"]["b_end"] == "2024-04-09T00:00:00Z"
    assert plan["computed"]["w_required"]["start"] == "2024-02-17T00:00:00Z"


def test_spec_eval_windows_override_legacy_fields():
    runtime = _runtime_with_stage("train")
    runtime.eval_start_ts = "2024-05-01T00:00:00Z"
    runtime.eval_end_ts = "2024-05-02T00:00:00Z"
    spec = _make_spec()
    spec.evaluation_windows = [
        EvaluationWindowSpec(window_id="w_spec", start_ts="2024-04-03T00:00:00Z", end_ts="2024-04-04T00:00:00Z")
    ]
    job = IFTrainingJob(_DummyDF(), runtime, spec)

    windows = job._resolve_evaluation_windows()
    assert windows[0]["window_id"] == "w_spec"
    assert windows[0]["start_ts"] == "2024-04-03T00:00:00Z"


def test_post_training_evaluation_writes_manifests(monkeypatch):
    runtime = _runtime_with_stage("train", mode="evaluation")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    class _S3:
        def __init__(self):
            self.keys = []

        def put_object(self, **kwargs):
            self.keys.append(kwargs["Key"])

    class _SM:
        def start_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionArn": "arn:aws:sagemaker:region:acct:pipeline/execution/1"}

        def describe_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionStatus": "Succeeded"}

    s3 = _S3()
    sm = _SM()

    def _client(name):
        if name == "s3":
            return s3
        if name == "sagemaker":
            return sm
        raise AssertionError(name)

    monkeypatch.setattr(sys.modules["boto3"], "client", _client, raising=False)
    monkeypatch.setattr(
        job,
        "_run_dependency_readiness_gate",
        lambda **_kwargs: {
            "checks": [
                {"family": "inference", "resolved_target": "pipeline_inference_predictions"},
                {"family": "prediction_feature_join", "resolved_target": "pipeline_prediction_feature_join"},
            ]
        },
    )
    windows = [{"window_id": "eval_1", "start_ts": "2024-04-03T00:00:00Z", "end_ts": "2024-04-04T00:00:00Z"}]
    manifests = job._run_post_training_evaluation(windows)
    assert manifests[0]["window_id"] == "eval_1"
    assert any(key.endswith("predictions_manifest.json") for key in s3.keys)
    assert manifests[0]["predictions_manifest"]["status"] == "Succeeded"


def test_post_training_evaluation_skips_join_when_publication_disabled(monkeypatch):
    runtime = _runtime_with_stage("train", mode="evaluation")
    spec = _make_spec()
    spec.toggles.enable_eval_join_publication = False
    job = IFTrainingJob(_DummyDF(), runtime, spec)

    class _S3:
        def put_object(self, **_kwargs):
            return None

    class _SM:
        def start_pipeline_execution(self, **kwargs):
            if kwargs["PipelineName"] == "pipeline_prediction_feature_join":
                raise AssertionError("join pipeline must not be called")
            return {"PipelineExecutionArn": "arn:aws:sagemaker:region:acct:pipeline/execution/1"}

        def describe_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionStatus": "Succeeded"}

    def _client(name):
        if name == "s3":
            return _S3()
        if name == "sagemaker":
            return _SM()
        raise AssertionError(name)

    monkeypatch.setattr(sys.modules["boto3"], "client", _client, raising=False)
    monkeypatch.setattr(
        job,
        "_run_dependency_readiness_gate",
        lambda **_kwargs: {
            "checks": [
                {"family": "inference", "resolved_target": "pipeline_inference_predictions"},
                {"family": "prediction_feature_join", "resolved_target": "pipeline_prediction_feature_join"},
            ]
        },
    )
    manifests = job._run_post_training_evaluation([
        {"window_id": "eval_1", "start_ts": "2024-04-03T00:00:00Z", "end_ts": "2024-04-04T00:00:00Z"}
    ])
    assert manifests[0]["join_manifest"]["status"] == "Skipped"


def test_remediation_stage_invokes_orchestrators(monkeypatch):
    job = IFTrainingJob(
        _DummyDF(),
        _runtime_with_stage("remediate"),
        _make_spec(),
        resolved_spec_payload={
            "orchestration_targets": {
                "backfill_15m": "sfn_ndr_backfill_reprocessing",
                "fg_b_baseline": "pipeline_fg_b_baseline",
                "inference": "pipeline_inference_predictions",
                "prediction_feature_join": "pipeline_prediction_feature_join",
            }
        },
    )

    monkeypatch.setattr(
        job,
        "_load_required_latest_verification_status",
        lambda: {"needs_remediation": True, "missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True, include_fgb=True)},
    )
    monkeypatch.setattr(
        job,
        "_load_required_history_plan",
        lambda *_args, **_kwargs: {"missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True, include_fgb=True)},
    )

    class _S3:
        def put_object(self, **_kwargs):
            return None

    class _SFN:
        def __init__(self):
            self.last_input = None

        def list_state_machines(self, **_kwargs):
            return {
                "stateMachines": [
                    {
                        "name": "sfn_ndr_backfill_reprocessing",
                        "stateMachineArn": "arn:aws:states:region:acct:stateMachine:ndr-backfill",
                    }
                ]
            }

        def describe_state_machine(self, **_kwargs):
            return {"name": "sfn_ndr_backfill_reprocessing"}

        def start_execution(self, **_kwargs):
            self.last_input = json.loads(_kwargs["input"])
            return {"executionArn": "arn:aws:states:region:acct:execution:sm:1"}

        def describe_execution(self, **_kwargs):
            return {"status": "SUCCEEDED"}

    class _SM:
        def describe_pipeline(self, **_kwargs):
            return {"PipelineArn": "arn:aws:sagemaker:region:acct:pipeline/x"}

        def start_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionArn": "arn:aws:sagemaker:region:acct:pipeline/execution/2"}

        def describe_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionStatus": "Succeeded"}

    sfn_client = _SFN()

    def _client(name):
        if name == "s3":
            return _S3()
        if name == "stepfunctions":
            return sfn_client
        if name == "sagemaker":
            return _SM()
        raise AssertionError(name)

    monkeypatch.setattr(sys.modules["boto3"], "client", _client, raising=False)
    monkeypatch.setattr("ndr.processing.if_training_job.time.sleep", lambda *_args, **_kwargs: None)

    observed = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    job.run()
    assert observed["stage"] == "remediation"
    assert observed["payload"]["status"] == "completed"
    assert observed["payload"]["records"][0]["backfill_execution"]["status"] == "Succeeded"
    assert observed["payload"]["records"][0]["fgb_execution"]["status"] == "Succeeded"
    assert sfn_client.last_input["contract_version"] == "NdrTrainingRemediationRequest.v1"
    assert sfn_client.last_input["manifest"]["contract_version"] == "backfill_manifest.v1"
    assert sfn_client.last_input["manifest"]["planner_mode"] == "caller_guided"


def test_backfill_execution_already_exists_is_handled_as_idempotent_skip(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("remediate"), _make_spec())

    class _SFN:
        def start_execution(self, **_kwargs):
            raise RuntimeError("ExecutionAlreadyExists")

    result = job._invoke_backfill_reprocessing(
        sfn_client=_SFN(),
        missing_15m=[{"start_ts_iso": "2024-04-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:15:00Z"}],
        chunk_index=1,
        chunk_hash="abc123",
        target={"resolved_target": "arn:aws:states:region:acct:stateMachine:ndr-backfill", "family": "backfill_15m"},
    )
    assert result["status"] == "Skipped"
    assert result["failure_reason"] == "ExecutionAlreadyExists"


def test_fgb_rebuild_not_capped_to_ten_references(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("remediate"), _make_spec())

    class _SM:
        def __init__(self):
            self.started = 0

        def start_pipeline_execution(self, **_kwargs):
            self.started += 1
            return {"PipelineExecutionArn": f"arn:exec:{self.started}"}

        def describe_pipeline_execution(self, **_kwargs):
            return {"PipelineExecutionStatus": "Succeeded"}

    sm = _SM()
    monkeypatch.setattr("ndr.processing.if_training_job.time.sleep", lambda *_args, **_kwargs: None)
    payload = [{"start_ts_iso": f"2024-04-{i:02d}T00:00:00Z", "end_ts_iso": f"2024-04-{i+1:02d}T00:00:00Z"} for i in range(1, 13)]
    result = job._invoke_fgb_baseline_rebuild(
        sm,
        payload,
        chunk_index=1,
        chunk_hash="abc123",
        target={"resolved_target": "pipeline_fg_b_baseline", "family": "fg_b_baseline"},
    )
    assert sm.started == 12
    assert result["status"] == "Succeeded"


def test_remediation_chunking_does_not_truncate_sparse_ranges(monkeypatch):
    job = IFTrainingJob(
        _DummyDF(),
        _runtime_with_stage("remediate"),
        _make_spec(),
        resolved_spec_payload={"remediation": {"chunk_size": 1}},
    )

    manifest = {
        "contract_version": "if_training_missing_windows.v1",
        "entries": [
            {
                "artifact_family": "fg_a_15m",
                "ranges": [
                    {"start_ts_iso": "2024-04-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:15:00Z"},
                    {"start_ts_iso": "2024-04-01T03:00:00Z", "end_ts_iso": "2024-04-01T03:15:00Z"},
                    {"start_ts_iso": "2024-04-01T08:00:00Z", "end_ts_iso": "2024-04-01T08:15:00Z"},
                ],
                "source": "feature_partition_gap",
                "project_name": job.runtime_config.project_name,
                "feature_spec_version": job.runtime_config.feature_spec_version,
                "ml_project_name": job.runtime_config.ml_project_name,
                "run_id": job.runtime_config.run_id,
            }
        ],
    }

    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"needs_remediation": True, "missing_windows_manifest": manifest})
    monkeypatch.setattr(job, "_load_required_history_plan", lambda *_args, **_kwargs: {"missing_windows_manifest": manifest})
    monkeypatch.setattr(
        job,
        "_run_dependency_readiness_gate",
        lambda **_kwargs: {"checks": [{"family": "backfill_15m", "resolved_target": "arn:sm"}, {"family": "fg_b_baseline", "resolved_target": "pipeline"}]},
    )
    monkeypatch.setattr("ndr.processing.if_training_job._put_json", lambda *_args, **_kwargs: None)

    backfill_calls = []
    monkeypatch.setattr(
        job,
        "_invoke_backfill_reprocessing",
        lambda **kwargs: backfill_calls.append(kwargs) or {"status": "Succeeded"},
    )
    monkeypatch.setattr(job, "_invoke_fgb_baseline_rebuild", lambda **_kwargs: {"status": "Skipped"})

    observed = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    class _DummyClient:
        def put_object(self, **_kwargs):
            return None

    monkeypatch.setattr(sys.modules["boto3"], "client", lambda _name: _DummyClient(), raising=False)
    job.run()

    assert observed["payload"]["chunk_count"] == 3
    assert len(backfill_calls) == 3
    assert sorted(call["chunk_index"] for call in backfill_calls) == [1, 2, 3]


def test_build_remediation_chunks_is_deterministic():
    job = IFTrainingJob(
        _DummyDF(),
        _runtime_with_stage("remediate"),
        _make_spec(),
        resolved_spec_payload={"remediation": {"chunk_size": 2}},
    )
    entries = [
        {
            "artifact_family": "fg_a_15m",
            "ranges": [
                {"start_ts_iso": "2024-04-01T02:00:00Z", "end_ts_iso": "2024-04-01T02:15:00Z"},
                {"start_ts_iso": "2024-04-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:15:00Z"},
            ],
            "source": "feature_partition_gap",
            "project_name": "proj",
            "feature_spec_version": "v1",
            "ml_project_name": "mlp",
            "run_id": "run-1",
        }
    ]
    first = job._build_remediation_chunks(entries)
    second = job._build_remediation_chunks(entries)
    assert first == second
    assert first[0]["chunk_size"] == 2
    assert "chunk_hash" in first[0]


def test_resolve_orchestration_target_uses_ddb_contract():
    runtime = _runtime_with_stage("train")
    spec = _make_spec()
    job = IFTrainingJob(
        _DummyDF(),
        runtime,
        spec,
        resolved_spec_payload={"orchestration_targets": {"fg_b_baseline": "pipeline_fg_b_override"}},
    )

    resolved = job._resolve_orchestration_target(
        family="fg_b_baseline",
        sfn_client=object(),
        sagemaker_client=object(),
        required=True,
    )
    assert resolved["resolution_source"] == "ddb_contract"
    assert resolved["resolved_target"] == "pipeline_fg_b_override"


def test_dependency_readiness_fails_fast_for_required_branch(monkeypatch):
    runtime = _runtime_with_stage("train")
    spec = _make_spec()
    spec.toggles.enable_auto_remediate_15m = True
    spec.remediation.enable_backfill_15m = True
    job = IFTrainingJob(
        _DummyDF(),
        runtime,
        spec,
        resolved_spec_payload={"orchestration_targets": {"backfill_15m": "sfn_ndr_backfill_reprocessing"}},
    )

    class _SFN:
        def describe_state_machine(self, **_kwargs):
            raise RuntimeError("AccessDenied")

        def list_state_machines(self, **_kwargs):
            return {"stateMachines": [{"name": "sfn_ndr_backfill_reprocessing", "stateMachineArn": "arn:sm"}]}

    class _SM:
        def describe_pipeline(self, **_kwargs):
            return {"PipelineArn": "arn:pipeline"}

    def _client(name):
        if name == "stepfunctions":
            return _SFN()
        if name == "sagemaker":
            return _SM()
        raise AssertionError(name)

    monkeypatch.setattr(sys.modules["boto3"], "client", _client, raising=False)
    monkeypatch.setattr(job, "_write_stage_status", lambda *_args, **_kwargs: None)
    with pytest.raises(ValueError, match="IFTrainingOrchestrationTargetContractError"):
        job._run_dependency_readiness_gate(
            stage="train",
            missing_15m_manifest=[{"start_ts_iso": "2024-01-01T00:00:00Z", "end_ts_iso": "2024-01-01T00:15:00Z"}],
            missing_fgb_manifest=[],
        )


def test_required_branch_target_missing_in_ddb_fails_with_contract_error():
    runtime = _runtime_with_stage("train")
    spec = _make_spec()
    spec.toggles.enable_auto_remediate_15m = True
    spec.remediation.enable_backfill_15m = True
    job = IFTrainingJob(_DummyDF(), runtime, spec, resolved_spec_payload={})

    with pytest.raises(ValueError, match="IFTrainingOrchestrationTargetContractError"):
        job._resolve_orchestration_target(
            family="backfill_15m",
            sfn_client=object(),
            sagemaker_client=object(),
            required=True,
        )


def test_evaluation_windows_spec_must_be_sorted_non_overlapping():
    runtime = _runtime_with_stage("train")
    spec = _make_spec()
    spec.evaluation_windows = [
        EvaluationWindowSpec(window_id="w1", start_ts="2024-04-03T00:00:00Z", end_ts="2024-04-04T00:00:00Z"),
        EvaluationWindowSpec(window_id="w2", start_ts="2024-04-03T12:00:00Z", end_ts="2024-04-05T00:00:00Z"),
    ]
    job = IFTrainingJob(_DummyDF(), runtime, spec)
    with pytest.raises(ValueError, match="non-overlapping and sorted"):
        job._resolve_evaluation_windows()


def test_remediation_routes_backfill_only_when_only_15m_missing(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("remediate"), _make_spec())

    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"needs_remediation": True, "missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True)})
    monkeypatch.setattr(
        job,
        "_load_required_history_plan",
        lambda *_args, **_kwargs: {"missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True)},
    )

    monkeypatch.setattr(
        job,
        "_run_dependency_readiness_gate",
        lambda **_kwargs: {
            "checks": [
                {"family": "backfill_15m", "resolved_target": "arn:sm"},
                {"family": "fg_b_baseline", "resolved_target": "pipeline_fg_b_baseline"},
            ]
        },
    )
    monkeypatch.setattr(job, "_invoke_backfill_reprocessing", lambda **_kwargs: {"status": "Succeeded"})
    monkeypatch.setattr(job, "_invoke_fgb_baseline_rebuild", lambda **_kwargs: {"status": "Succeeded"})
    monkeypatch.setattr("ndr.processing.if_training_job._put_json", lambda *_args, **_kwargs: None)

    observed = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    class _DummyClient:
        def put_object(self, **_kwargs):
            return None

    monkeypatch.setattr(sys.modules["boto3"], "client", lambda _name: _DummyClient(), raising=False)
    job.run()

    record = observed["payload"]["records"][0]
    assert record["actions"]["backfill_15m_invoked"] is True
    assert record["actions"]["fgb_rebuild_invoked"] is False


def test_remediation_routes_fgb_only_when_only_fgb_missing(monkeypatch):
    job = IFTrainingJob(_DummyDF(), _runtime_with_stage("remediate"), _make_spec())

    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"needs_remediation": True, "missing_windows_manifest": _missing_manifest(job.runtime_config, include_fgb=True)})
    monkeypatch.setattr(
        job,
        "_load_required_history_plan",
        lambda *_args, **_kwargs: {"missing_windows_manifest": _missing_manifest(job.runtime_config, include_fgb=True)},
    )

    monkeypatch.setattr(
        job,
        "_run_dependency_readiness_gate",
        lambda **_kwargs: {
            "checks": [
                {"family": "backfill_15m", "resolved_target": "arn:sm"},
                {"family": "fg_b_baseline", "resolved_target": "pipeline_fg_b_baseline"},
            ]
        },
    )
    monkeypatch.setattr(job, "_invoke_backfill_reprocessing", lambda **_kwargs: {"status": "Succeeded"})
    monkeypatch.setattr(job, "_invoke_fgb_baseline_rebuild", lambda **_kwargs: {"status": "Succeeded"})
    monkeypatch.setattr("ndr.processing.if_training_job._put_json", lambda *_args, **_kwargs: None)

    observed = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    class _DummyClient:
        def put_object(self, **_kwargs):
            return None

    monkeypatch.setattr(sys.modules["boto3"], "client", lambda _name: _DummyClient(), raising=False)
    job.run()

    record = observed["payload"]["records"][0]
    assert record["actions"]["backfill_15m_invoked"] is False
    assert record["actions"]["fgb_rebuild_invoked"] is True


def test_plan_stage_writes_history_plan_before_remediation(monkeypatch):
    runtime = _runtime_with_stage("plan")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    from datetime import datetime, timezone

    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"stage": "verify", "needs_remediation": True, "missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True)})
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 4, 1, tzinfo=timezone.utc)))
    monkeypatch.setattr(job, "_resolve_evaluation_windows", lambda: [])
    monkeypatch.setattr(
        job,
        "_compute_history_plan",
        lambda *_args, **_kwargs: {"missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True)},
    )
    observed = {}
    monkeypatch.setattr(job, "_write_history_plan", lambda payload: observed.update({"history_plan": payload}))
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    job.run()

    assert observed["history_plan"]["missing_windows_manifest"]["entries"][0]["artifact_family"] == "fg_a_15m"
    assert observed["stage"] == "planning"
    assert observed["payload"]["status"] == "completed"


def test_train_stage_hard_fails_when_reverify_has_unresolved_windows(monkeypatch):
    runtime = _runtime_with_stage("train")
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    monkeypatch.setattr(
        job,
        "_load_required_latest_verification_status",
        lambda: {"stage": "reverify", "needs_remediation": True, "missing_windows_manifest": _missing_manifest(job.runtime_config, include_15m=True)},
    )
    observed = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: observed.update({"stage": stage, "payload": payload}))

    with pytest.raises(ValueError, match="unresolved required windows"):
        job._enforce_pre_train_reverify_gate()

    assert observed["stage"] == "train_gate"
    assert observed["payload"]["status"] == "failed"


def test_write_inference_preprocessing_back_uses_dpp_table_and_job_name_version(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="ml-proj",
        feature_spec_version="v1",
        run_id="run-3",
        execution_ts_iso="2025-01-01T00:00:00Z",
        dpp_config_table_name="dpp_config",
        mlp_config_table_name="mlp_config",
        batch_index_table_name="batch_index",
        training_start_ts="2024-01-01T00:00:00Z",
        training_end_ts="2024-04-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    calls = {}

    class _Table:
        def get_item(self, Key):
            calls["get_item"] = Key
            return {"Item": {"spec": {"payload": {}}}}

        def update_item(self, **kwargs):
            calls["update_item"] = kwargs
            return {}

    class _DDB:
        def Table(self, name):
            calls["table_name"] = name
            return _Table()

    monkeypatch.setattr(sys.modules["boto3"], "resource", lambda *_a, **_k: _DDB(), raising=False)

    job._write_inference_preprocessing_back(["f1"], {"f1": {}}, {"f1": {}})
    assert calls["table_name"] == "dpp_config"
    assert calls["get_item"] == {"project_name": "proj", "job_name_version": "inference_predictions#v1"}
    assert calls["update_item"]["Key"] == {"project_name": "proj", "job_name_version": "inference_predictions#v1"}
