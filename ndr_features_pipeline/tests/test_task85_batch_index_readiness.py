from datetime import datetime, timezone
import sys
import types

if "boto3" not in sys.modules:
    boto3 = types.ModuleType("boto3")
    boto3.resource = lambda *_args, **_kwargs: None
    sys.modules["boto3"] = boto3
if "boto3.dynamodb" not in sys.modules:
    dynamodb = types.ModuleType("boto3.dynamodb")
    sys.modules["boto3.dynamodb"] = dynamodb
if "boto3.dynamodb.conditions" not in sys.modules:
    conditions = types.ModuleType("boto3.dynamodb.conditions")
    conditions.Key = object
    sys.modules["boto3.dynamodb.conditions"] = conditions

from ndr.orchestration.training_missing_manifest import ensure_manifest
from ndr.processing.if_training_job import IFTrainingJob
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig, parse_if_training_spec


class _DummySpark:
    pass


def _runtime(stage: str = "plan") -> IFTrainingRuntimeConfig:
    return IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="mlp_a",
        feature_spec_version="v1",
        run_id="run-85",
        execution_ts_iso="2024-04-02T00:00:00Z",
        stage=stage,
        dpp_config_table_name="dpp",
        mlp_config_table_name="mlp",
        batch_index_table_name="batch_index_authoritative",
        training_start_ts="2024-04-01T00:00:00Z",
        training_end_ts="2024-04-01T01:00:00Z",
    )


def _spec():
    return parse_if_training_spec(
        {
            "feature_inputs": {"fg_a": {"s3_prefix": "s3://a"}, "fg_c": {"s3_prefix": "s3://c"}},
            "output": {"artifacts_s3_prefix": "s3://artifacts", "report_s3_prefix": "s3://reports"},
            "model": {"version": "v1"},
        }
    )


def test_batch_index_readiness_derives_expected_observed_and_unresolved(monkeypatch):
    runtime = _runtime()
    job = IFTrainingJob(_DummySpark(), runtime, _spec())

    observed = {}

    class _Loader:
        def __init__(self, table_name=None):
            observed["table_name"] = table_name

        def lookup_forward(self, **_kwargs):
            return [
                type(
                    "Row",
                    (),
                    {
                        "project_name": "proj",
                        "batch_id": "b1",
                        "date_partition": "2024/04/01",
                        "etl_ts": "2024-04-01T00:00:00Z",
                        "ml_project_names": ["mlp_a"],
                    },
                )(),
            ]

    monkeypatch.setattr("ndr.config.batch_index_loader.BatchIndexLoader", _Loader)

    readiness = job._derive_batch_index_readiness(
        required_start=datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc),
        required_end=datetime(2024, 4, 1, 1, 0, tzinfo=timezone.utc),
        baseline_start=datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc),
        baseline_end=datetime(2024, 4, 2, 0, 0, tzinfo=timezone.utc),
        preflight=None,
    )

    assert observed["table_name"] == "batch_index_authoritative"
    manifest = readiness["training_readiness_manifest"]
    assert manifest["batch_index_evidence"]["selectors"]["pk"] == "proj"
    assert manifest["families"]["fg_a_15m"]["expected_ranges"][0]["start_ts_iso"] == "2024-04-01T00:00:00Z"
    assert manifest["families"]["fg_a_15m"]["observed_window_starts"] == ["2024-04-01T00:00:00Z"]
    assert manifest["families"]["fg_a_15m"]["unresolved_ranges"][0]["start_ts_iso"] == "2024-04-01T00:15:00Z"
    ensure_manifest(readiness["missing_windows_manifest"])


def test_planning_stage_emits_required_readiness_artifacts(monkeypatch):
    runtime = _runtime(stage="plan")
    job = IFTrainingJob(_DummySpark(), runtime, _spec())

    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"stage": "verify", "needs_remediation": True, "missing_windows_manifest": {"contract_version": "if_training_missing_windows.v1", "entries": []}})
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 4, 1, tzinfo=timezone.utc), datetime(2024, 4, 1, 1, tzinfo=timezone.utc)))
    monkeypatch.setattr(job, "_resolve_evaluation_windows", lambda: [])
    monkeypatch.setattr(
        job,
        "_compute_history_plan",
        lambda *_args, **_kwargs: {
            "batch_index_readiness": {"contract_version": "training_readiness_manifest.v1", "batch_index_evidence": {"selectors": {"pk": "proj"}}},
            "missing_windows_manifest": {"contract_version": "if_training_missing_windows.v1", "entries": []},
        },
    )
    monkeypatch.setattr(job, "_write_history_plan", lambda *_args, **_kwargs: None)

    written = {}
    monkeypatch.setattr(job, "_write_stage_status", lambda stage, payload: written.setdefault(stage, payload))

    job._run_planning_stage()

    assert "training_readiness_manifest" in written
    assert "missing_windows_manifest" in written
    assert "remediation_plan" in written
    assert written["remediation_plan"]["contract_version"] == "if_training_remediation_plan.v1"
