import sys
import types

sys.modules.setdefault("boto3", types.ModuleType("boto3"))

from ndr.pipeline.io_contract import (
    resolve_step_code_uri,
    resolve_step_script_contract,
    validate_step_code_metadata,
)
from pathlib import Path


def test_resolve_step_script_contract_success():
    contract = resolve_step_script_contract(
        {
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


def test_validate_step_code_metadata_requires_packaging_fields():
    try:
        validate_step_code_metadata(
            {
                "scripts": {
                    "steps": {
                        "IFTrainingStep": {
                            "code_prefix_s3": "s3://bucket/path",
                            "entry_script": "run_if_training.py",
                            "code_metadata": {"artifact_mode": "single_file"},
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
    monkeypatch.setattr(
        "ndr.pipeline.io_contract.load_job_spec",
        lambda **_kwargs: {
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
            "scripts": {
                "steps": {
                    "IFTrainingStep": {
                        "code_prefix_s3": "s3://bucket/path",
                        "entry_script": "run_if_training.py",
                        "code_metadata": {
                            "artifact_mode": "single_file",
                            "artifact_build_id": "build-123",
                            "artifact_sha256": "deadbeef",
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
