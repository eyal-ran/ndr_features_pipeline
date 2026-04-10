from datetime import datetime, timezone

import pytest

from ndr.orchestration.training_missing_manifest import from_missing_sources
from ndr.processing.if_training_job import IFTrainingJob
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig, parse_if_training_spec


class _DummySpark:
    pass


def _runtime(stage: str = "plan") -> IFTrainingRuntimeConfig:
    return IFTrainingRuntimeConfig(
        project_name="proj",
        ml_project_name="mlp_a",
        feature_spec_version="v1",
        run_id="run-task7",
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
            "output": {
                "artifacts_s3_prefix": "s3://artifacts",
                "report_s3_prefix": "s3://reports",
                "production_model_root": "s3://prod-models",
            },
            "model": {"version": "v1"},
        }
    )


def _manifest(runtime: IFTrainingRuntimeConfig, *, include_missing: bool) -> dict:
    return from_missing_sources(
        missing_15m_windows=[{"window_start_ts": "2024-04-01T00:15:00Z", "window_end_ts": "2024-04-01T00:30:00Z"}] if include_missing else [],
        missing_fgb_windows=[],
        project_name=runtime.project_name,
        feature_spec_version=runtime.feature_spec_version,
        ml_project_name=runtime.ml_project_name,
        run_id=runtime.run_id,
        as_of_ts="2024-04-01T01:00:00Z",
    )


def test_task7_recompute_overrides_stale_manifest_input(monkeypatch):
    job = IFTrainingJob(_DummySpark(), _runtime(), _spec())
    stale = _manifest(job.runtime_config, include_missing=False)

    monkeypatch.setattr(
        job,
        "_derive_batch_index_readiness",
        lambda **_kwargs: {
            "training_readiness_manifest": {"contract_version": "training_readiness_manifest.v2"},
            "missing_windows_manifest": _manifest(job.runtime_config, include_missing=True),
        },
    )
    recomputed = job._recompute_training_readiness_snapshot(
        gate_stage="plan",
        required_start=datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc),
        required_end=datetime(2024, 4, 1, 1, 0, tzinfo=timezone.utc),
        baseline_start=datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc),
        baseline_end=datetime(2024, 4, 1, 1, 0, tzinfo=timezone.utc),
        preflight=None,
        prior_manifest=stale,
    )

    assert len(recomputed["missing_windows_manifest"]["entries"]) == 1
    assert recomputed["drift"]["newly_missing_count"] == 1
    assert recomputed["drift"]["changed"] is True


def test_task7_pretrain_gate_hard_fails_on_unresolved_recomputed_windows(monkeypatch):
    job = IFTrainingJob(_DummySpark(), _runtime(stage="train"), _spec())
    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"stage": "reverify", "needs_remediation": False, "missing_windows_manifest": _manifest(job.runtime_config, include_missing=False)})
    monkeypatch.setattr(job, "_load_required_history_plan", lambda: {"missing_windows_manifest": _manifest(job.runtime_config, include_missing=False)})
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc), datetime(2024, 4, 1, 1, 0, tzinfo=timezone.utc)))
    monkeypatch.setattr(job, "_resolve_evaluation_windows", lambda: [])
    monkeypatch.setattr(
        job,
        "_recompute_training_readiness_snapshot",
        lambda **_kwargs: {
            "training_readiness_manifest": {"contract_version": "training_readiness_manifest.v2"},
            "missing_windows_manifest": _manifest(job.runtime_config, include_missing=True),
            "drift": {"changed": True},
        },
    )
    monkeypatch.setattr(job, "_write_stage_status", lambda *_args, **_kwargs: None)

    with pytest.raises(ValueError, match="IFTrainingPreTrainGateBlocked"):
        job._enforce_pre_train_reverify_gate()


def test_task7_pretrain_gate_allows_resolved_retry(monkeypatch):
    job = IFTrainingJob(_DummySpark(), _runtime(stage="train"), _spec())
    monkeypatch.setattr(job, "_load_required_latest_verification_status", lambda: {"stage": "reverify", "needs_remediation": False, "missing_windows_manifest": _manifest(job.runtime_config, include_missing=True)})
    monkeypatch.setattr(job, "_load_required_history_plan", lambda: {"missing_windows_manifest": _manifest(job.runtime_config, include_missing=True)})
    monkeypatch.setattr(job, "_resolve_training_window", lambda: (datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc), datetime(2024, 4, 1, 1, 0, tzinfo=timezone.utc)))
    monkeypatch.setattr(job, "_resolve_evaluation_windows", lambda: [])
    monkeypatch.setattr(
        job,
        "_recompute_training_readiness_snapshot",
        lambda **_kwargs: {
            "training_readiness_manifest": {"contract_version": "training_readiness_manifest.v2"},
            "missing_windows_manifest": _manifest(job.runtime_config, include_missing=False),
            "drift": {"changed": True, "resolved_since_prior_count": 1},
        },
    )
    monkeypatch.setattr(job, "_write_stage_status", lambda *_args, **_kwargs: None)

    verification = job._enforce_pre_train_reverify_gate()
    assert verification["stage"] == "reverify"


def test_task7_drift_metrics_calculation_is_deterministic():
    prior_entries = [
        {"artifact_family": "fg_a_15m", "ranges": [{"start_ts_iso": "2024-04-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:15:00Z"}]},
    ]
    current_entries = [
        {"artifact_family": "fg_a_15m", "ranges": [{"start_ts_iso": "2024-04-01T00:15:00Z", "end_ts_iso": "2024-04-01T00:30:00Z"}]},
    ]
    drift = IFTrainingJob._compute_missing_manifest_drift(prior_entries=prior_entries, current_entries=current_entries)
    assert drift == {
        "prior_missing_count": 1,
        "current_missing_count": 1,
        "newly_missing_count": 1,
        "resolved_since_prior_count": 1,
        "changed": True,
    }
