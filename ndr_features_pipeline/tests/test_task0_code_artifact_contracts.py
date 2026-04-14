import pytest

from ndr.orchestration.code_artifact_contracts import (
    CODE_ARTIFACT_VALIDATE_REPORT_VERSION,
    CODE_BUNDLE_BUILD_OUTPUT_VERSION,
    CODE_SMOKE_VALIDATE_REPORT_VERSION,
    load_schema,
    validate_code_artifact_validate_report,
    validate_code_bundle_build_output,
    validate_code_smoke_validate_report,
    validate_payload,
)
from ndr.orchestration.remediation_contracts import ContractValidationError


def _sha() -> str:
    return "a" * 64


def _build_output() -> dict:
    return {
        "contract_version": CODE_BUNDLE_BUILD_OUTPUT_VERSION,
        "project_name": "proj",
        "feature_spec_version": "v1",
        "artifact_build_id": "build-1",
        "artifact_sha256": _sha(),
        "artifact_format": "tar.gz",
        "step_artifacts": [
            {
                "artifact_family": "streaming",
                "pipeline_job_name": "pipeline_15m_streaming",
                "step_name": "DeltaBuilderStep",
                "entry_script": "src/ndr/scripts/run_delta_builder.py",
                "code_artifact_s3_uri": "s3://bucket/code/artifacts/build-1/source.tar.gz",
                "artifact_build_id": "build-1",
                "artifact_sha256": _sha(),
                "artifact_format": "tar.gz",
            }
        ],
    }


def _validate_report() -> dict:
    return {
        "contract_version": CODE_ARTIFACT_VALIDATE_REPORT_VERSION,
        "artifact_build_id": "build-1",
        "status": "PASS",
        "validated_steps": 1,
        "step_results": [
            {
                "artifact_family": "streaming",
                "pipeline_job_name": "pipeline_15m_streaming",
                "step_name": "DeltaBuilderStep",
                "status": "PASS",
                "error_code": "OK",
                "error_message": "",
                "retriable": False,
            }
        ],
    }


def _smoke_report() -> dict:
    return {
        "contract_version": CODE_SMOKE_VALIDATE_REPORT_VERSION,
        "artifact_build_id": "build-1",
        "status": "PASS",
        "step_results": [
            {
                "artifact_family": "streaming",
                "pipeline_job_name": "pipeline_15m_streaming",
                "step_name": "DeltaBuilderStep",
                "status": "PASS",
                "error_code": "OK",
                "error_message": "",
                "retriable": False,
            }
        ],
    }


def test_task0_code_artifact_contracts_positive_paths():
    validate_code_bundle_build_output(_build_output())
    validate_code_artifact_validate_report(_validate_report())
    validate_code_smoke_validate_report(_smoke_report())


def test_task0_code_artifact_contracts_missing_unknown_invalid_fail_fast():
    missing = _build_output()
    missing.pop("step_artifacts")
    with pytest.raises(ContractValidationError, match="CONTRACT_MISSING_REQUIRED_FIELD"):
        validate_code_bundle_build_output(missing)

    unknown = _validate_report()
    unknown["unexpected"] = "x"
    with pytest.raises(ContractValidationError, match="CONTRACT_UNKNOWN_FIELD"):
        validate_code_artifact_validate_report(unknown)

    invalid = _smoke_report()
    invalid["step_results"][0]["pipeline_job_name"] = "pipeline_unknown"
    with pytest.raises(ContractValidationError, match="CONTRACT_INVALID_FIELD"):
        validate_code_smoke_validate_report(invalid)


def test_task0_code_artifact_contract_version_compatibility_guardrails():
    with pytest.raises(ContractValidationError, match="CONTRACT_VERSION_UNSUPPORTED"):
        load_schema("code_bundle_build_output.v999")

    mismatch_payload = _build_output()
    mismatch_payload["contract_version"] = CODE_SMOKE_VALIDATE_REPORT_VERSION
    with pytest.raises(ContractValidationError, match="CONTRACT_VERSION_MISMATCH"):
        validate_payload(mismatch_payload, expected_contract_version=CODE_BUNDLE_BUILD_OUTPUT_VERSION)
