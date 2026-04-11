import sys
import types

sys.modules.setdefault("boto3", types.ModuleType("boto3"))

from ndr.pipeline.io_contract import (
    build_processing_step_launch_args,
    resolve_step_execution_contract,
    validate_artifact_hash,
    validate_deployment_readiness,
    resolve_step_code_uri,
    resolve_step_script_contract,
    validate_step_code_metadata,
)
from pathlib import Path


def test_resolve_step_script_contract_success():
    contract = resolve_step_script_contract(
        {
            "deployment_status": "READY",
            "scripts": {
                "steps": {
                    "DeltaBuilderStep": {
                        "code_prefix_s3": "s3://bucket/prefix/",
                        "entry_script": "run_delta_builder.py",
                    }
                }
            }
        },
        step_name="DeltaBuilderStep",
    )

    assert contract.script_s3_uri == "s3://bucket/prefix/run_delta_builder.py"


def test_resolve_step_script_contract_missing_step_raises():
    try:
        resolve_step_script_contract({"scripts": {"steps": {}}}, step_name="Missing")
    except ValueError as exc:
        assert "scripts.steps.Missing" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_resolve_step_code_uri_loads_pipeline_spec(monkeypatch):
    def _fake_load_job_spec(project_name, job_name, feature_spec_version, table_name=None):
        assert project_name == "ndr-project"
        assert job_name == "pipeline_15m_streaming"
        assert feature_spec_version == "v1"
        assert table_name is None
        return {
            "deployment_status": "READY",
            "scripts": {
                "steps": {
                    "DeltaBuilderStep": {
                        "code_prefix_s3": "s3://bucket/path",
                        "entry_script": "run_delta_builder.py",
                    }
                }
            }
        }

    monkeypatch.setattr("ndr.pipeline.io_contract.load_job_spec", _fake_load_job_spec)

    uri = resolve_step_code_uri(
        project_name="ndr-project",
        feature_spec_version="v1",
        pipeline_job_name="pipeline_15m_streaming",
        step_name="DeltaBuilderStep",
    )

    assert uri == "s3://bucket/path/run_delta_builder.py"


def test_resolve_step_code_uri_rejects_placeholder_inputs():
    try:
        resolve_step_code_uri(
            project_name="<required:ProjectName>",
            feature_spec_version="v1",
            pipeline_job_name="pipeline_15m_streaming",
            step_name="DeltaBuilderStep",
        )
    except ValueError as exc:
        assert "concrete value" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_resolve_step_code_uri_rejects_placeholder_markers():
    try:
        resolve_step_code_uri(
            project_name="ndr-project",
            feature_spec_version="<placeholder:FeatureSpecVersion>",
            pipeline_job_name="pipeline_15m_streaming",
            step_name="DeltaBuilderStep",
        )
    except ValueError as exc:
        assert "concrete value" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_validate_step_code_metadata_requires_packaging_fields():
    try:
        validate_step_code_metadata(
            {
                "scripts": {
                    "steps": {
                        "IFTrainingStep": {
                            "code_prefix_s3": "s3://bucket/path",
                            "entry_script": "run_if_training.py",
                            "code_metadata": {"code_artifact_s3_uri": "s3://bucket/artifacts/build/source.tar.gz"},
                        }
                    }
                }
            },
            step_name="IFTrainingStep",
        )
    except ValueError as exc:
        assert "Packaging metadata decision required" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_resolve_step_code_uri_training_requires_step_code_metadata(monkeypatch):
    monkeypatch.setenv("NDR_STEP_CODE_DUAL_READ_MODE", "0")
    monkeypatch.setattr(
        "ndr.pipeline.io_contract.load_job_spec",
        lambda **_kwargs: {
            "deployment_status": "READY",
            "scripts": {
                "steps": {
                    "IFTrainingStep": {
                        "code_prefix_s3": "s3://bucket/path",
                        "entry_script": "run_if_training.py",
                    }
                }
            }
        },
    )
    try:
        resolve_step_code_uri(
            project_name="ndr-project",
            feature_spec_version="v1",
            pipeline_job_name="pipeline_if_training",
            step_name="IFTrainingStep",
        )
    except ValueError as exc:
        assert "Packaging metadata decision required" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_resolve_step_code_uri_training_accepts_step_code_metadata(monkeypatch):
    monkeypatch.setattr(
        "ndr.pipeline.io_contract.load_job_spec",
        lambda **_kwargs: {
            "deployment_status": "READY",
            "scripts": {
                "steps": {
                    "IFTrainingStep": {
                        "code_prefix_s3": "s3://bucket/path",
                        "entry_script": "run_if_training.py",
                        "code_metadata": {
                            "code_artifact_s3_uri": "s3://bucket/artifacts/build/source.tar.gz",
                            "artifact_build_id": "build-123",
                            "artifact_sha256": "deadbeef",
                            "artifact_format": "tar.gz",
                        },
                    }
                }
            }
        },
    )
    uri = resolve_step_code_uri(
        project_name="ndr-project",
        feature_spec_version="v1",
        pipeline_job_name="pipeline_if_training",
        step_name="IFTrainingStep",
    )
    assert uri == "s3://bucket/path/run_if_training.py"


def test_resolve_step_execution_contract_prefers_artifact_uri(monkeypatch):
    monkeypatch.setattr(
        "ndr.pipeline.io_contract.load_job_spec",
        lambda **_kwargs: {
            "deployment_status": "READY",
            "scripts": {
                "steps": {
                    "DeltaBuilderStep": {
                        "code_prefix_s3": "s3://bucket/path",
                        "entry_script": "run_delta_builder.py",
                        "code_artifact_s3_uri": "s3://bucket/path/artifacts/build-1/source.tar.gz",
                        "artifact_build_id": "build-1",
                        "artifact_sha256": "abc123",
                        "artifact_format": "tar.gz",
                    }
                }
            },
        },
    )
    contract = resolve_step_execution_contract(
        project_name="ndr-project",
        feature_spec_version="v1",
        pipeline_job_name="pipeline_15m_streaming",
        step_name="DeltaBuilderStep",
    )
    assert contract.script_s3_uri.endswith("source.tar.gz")
    assert contract.uses_immutable_artifact is True


def test_validate_deployment_readiness_blocks_non_ready():
    try:
        validate_deployment_readiness({"deployment_status": "BOOTSTRAP_REQUIRED"}, pipeline_job_name="pipeline_15m_streaming")
    except ValueError as exc:
        assert "TASK8_DEPLOYMENT_NOT_READY" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_validate_artifact_hash_detects_mismatch():
    try:
        validate_artifact_hash(expected_sha256="abc", observed_sha256="def", step_name="DeltaBuilderStep")
    except ValueError as exc:
        assert "TASK8_ARTIFACT_HASH_MISMATCH" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_build_processing_step_launch_args_artifact_vs_legacy():
    artifact_args = build_processing_step_launch_args(
        entry_script="run_delta_builder.py",
        module_name="ndr.scripts.run_delta_builder",
        passthrough_args=["--project-name", "x"],
        artifact_uri="s3://bucket/source.tar.gz",
    )
    legacy_args = build_processing_step_launch_args(
        entry_script="run_delta_builder.py",
        module_name="ndr.scripts.run_delta_builder",
        passthrough_args=["--project-name", "x"],
        artifact_uri=None,
    )
    assert artifact_args[:2] == ["python", "run_delta_builder.py"]
    assert legacy_args[:3] == ["python", "-m", "ndr.scripts.run_delta_builder"]


def test_run_delta_builder_script_contract_includes_canonical_raw_parsed_logs_arg():
    script = Path("src/ndr/scripts/run_delta_builder.py").read_text()
    assert "--raw-parsed-logs-s3-prefix" in script


def test_run_pair_counts_script_contract_includes_canonical_raw_parsed_logs_arg():
    script = Path("src/ndr/scripts/run_pair_counts_builder.py").read_text()
    assert "--raw-parsed-logs-s3-prefix" in script


def test_inference_pipeline_defines_ml_project_name_parameter():
    source = Path("src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py").read_text()
    assert 'name="MlProjectName"' in source
    assert '"--ml-project-name"' in source


def test_prediction_join_pipeline_defines_ml_project_name_parameter():
    source = Path("src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py").read_text()
    assert 'name="MlProjectName"' in source
    assert '"--ml-project-name"' in source


def test_if_training_pipeline_defines_ml_project_name_parameter():
    source = Path("src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py").read_text()
    assert 'name="MlProjectName"' in source
    assert '"--ml-project-name"' in source


def test_historical_extractor_script_accepts_project_name():
    script = Path("src/ndr/scripts/run_historical_windows_extractor.py").read_text()
    assert "--project-name" in script


def test_historical_extractor_pipeline_passes_project_name():
    source = Path("src/ndr/pipeline/sagemaker_pipeline_definitions_backfill_historical_extractor.py").read_text()
    assert '"--project-name"' in source
