import sys
import types

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
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig, parse_if_training_spec




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


def test_run_orchestrates_artifact_before_deploy(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        feature_spec_version="v1",
        run_id="run-1",
        execution_ts_iso="2025-01-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

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

    def _deploy(*_args, **kwargs):
        call_order.append("deploy")
        assert kwargs["model_data_url"] == "s3://b/r/model/model.tar.gz"
        return {"attempted": False, "status": "skipped"}

    monkeypatch.setattr(job, "_persist_artifacts", _persist)
    monkeypatch.setattr(job, "_maybe_deploy", _deploy)
    monkeypatch.setattr(job, "_promote_latest_model_pointer", lambda *_args, **_kwargs: "s3://b/latest")
    monkeypatch.setattr(job, "_log_sagemaker_experiments", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(job, "_write_final_report_and_success", lambda *_args, **_kwargs: call_order.append("report"))

    job.run()
    assert call_order == ["persist", "deploy", "report"]


def test_run_failure_writes_failure_artifacts(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        feature_spec_version="v1",
        run_id="run-2",
        execution_ts_iso="2025-01-01T00:00:00Z",
    )
    job = IFTrainingJob(_DummyDF(), runtime, _make_spec())

    from datetime import datetime, timezone
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 4, 1, tzinfo=timezone.utc)))

    calls = []

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
        feature_spec_version="v1",
        run_id="run-3",
        execution_ts_iso="2025-01-01T00:00:00Z",
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
    assert writes["payload"]["resolved_job_spec_payload"] == resolved_payload
    assert writes["payload"]["failure"]["stage"] == "preflight"


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
        feature_spec_version="v1",
        run_id="run-4",
        execution_ts_iso="2025-01-01T00:00:00Z",
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
    )

    assert len(sm_client.associations) == 3
    assert {item["TrialComponentName"] for item in sm_client.batch_metrics} == {
        "if-training-v1-run-4-tuning",
        "if-training-v1-run-4-final",
    }
    final_update = [item for item in sm_client.updated if item["TrialComponentName"].endswith("-final")][0]
    assert final_update["Parameters"]["final_report_s3_uri"]["StringValue"].endswith("final_training_report.json")


def test_preflight_fails_on_underfilled_window(monkeypatch):
    runtime = IFTrainingRuntimeConfig(
        project_name="proj",
        feature_spec_version="v1",
        run_id="run-5",
        execution_ts_iso="2025-01-01T00:00:00Z",
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
