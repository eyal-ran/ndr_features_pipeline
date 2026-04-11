import json
from pathlib import Path

import pytest

from ndr.orchestration.deployment_contracts import (
    RECONCILIATION_REQUIRED_CHECKPOINT,
    assert_atomic_promotion_revision,
    build_promotion_spec,
    build_ready_commit_spec,
    build_rollback_spec,
    enforce_break_glass_reconciliation,
    normalize_deployment_request,
    should_mutate_pointers,
)

DEPLOYMENT_SFN_PATH = Path("docs/step_functions_jsonata/sfn_ndr_code_deployment_orchestrator.json")


def _sample_spec() -> dict:
    return {
        "deployment_status": "READY",
        "deployment_checkpoint": "steady_state_ready",
        "deployment_revision": 4,
        "scripts": {
            "steps": {
                "DeltaBuilderStep": {
                    "code_prefix_s3": "s3://bucket/code/DeltaBuilderStep/",
                    "entry_script": "run_delta_builder.py",
                    "code_artifact_s3_uri": "s3://bucket/code/DeltaBuilderStep/artifacts/build-4/source.tar.gz",
                    "artifact_build_id": "build-4",
                    "artifact_sha256": "sha-old",
                    "artifact_format": "tar.gz",
                }
            }
        },
    }


def test_deployment_sfn_contains_required_task9_states_and_rollback_path():
    states = json.loads(DEPLOYMENT_SFN_PATH.read_text(encoding="utf-8"))["States"]
    required = {
        "NormalizeDeploymentRequest",
        "LoadDeploymentConfigFromDdb",
        "StartCodeBundleBuildPipeline",
        "DescribeCodeBundleBuildPipeline",
        "StartArtifactValidationPipeline",
        "DescribeArtifactValidationPipeline",
        "PromoteArtifactPointers",
        "StartDeploymentSmokeValidationPipeline",
        "DescribeDeploymentSmokeValidationPipeline",
        "CommitDeploymentReady",
        "RollbackArtifactPointers",
    }
    assert required.issubset(states.keys())
    assert states["PostPromotionFailureChoice"]["Choices"][0]["Next"] == "RollbackArtifactPointers"


def test_deployment_sfn_has_shadow_and_authoritative_transitions():
    states = json.loads(DEPLOYMENT_SFN_PATH.read_text(encoding="utf-8"))["States"]
    mode_choice = states["DeploymentModeChoice"]["Choices"]
    shadow_branch = next(c for c in mode_choice if "shadow" in c["Condition"])
    authoritative_branch = next(c for c in mode_choice if "authoritative" in c["Condition"])
    assert shadow_branch["Next"] == "StartDeploymentSmokeValidationPipeline"
    assert authoritative_branch["Next"] == "PromoteArtifactPointers"


def test_normalize_request_builds_deterministic_build_id():
    req = normalize_deployment_request(
        {
            "project_name": "ndr",
            "feature_spec_version": "v1",
            "deployment_mode": "shadow",
            "request_id": "exec-123",
        }
    )
    assert req.artifact_build_id.startswith("auto-")


def test_shadow_mode_does_not_mutate_pointers():
    req = normalize_deployment_request(
        {
            "project_name": "ndr",
            "feature_spec_version": "v1",
            "deployment_mode": "shadow",
            "request_id": "exec-1",
            "artifact_build_id": "build-9",
        }
    )
    assert should_mutate_pointers(req) is False


def test_authoritative_promotion_captures_previous_pointers_and_marks_in_progress():
    spec = _sample_spec()
    req = normalize_deployment_request(
        {
            "project_name": "ndr",
            "feature_spec_version": "v1",
            "deployment_mode": "authoritative",
            "request_id": "exec-2",
            "artifact_build_id": "build-5",
        }
    )
    promoted = build_promotion_spec(
        pipeline_spec=spec,
        request=req,
        promoted_steps={
            "DeltaBuilderStep": {
                "code_artifact_s3_uri": "s3://bucket/code/DeltaBuilderStep/artifacts/build-5/source.tar.gz",
                "artifact_build_id": "build-5",
                "artifact_sha256": "sha-new",
                "artifact_format": "tar.gz",
            }
        },
    )
    step = promoted["scripts"]["steps"]["DeltaBuilderStep"]
    assert step["previous_artifact_build_id"] == "build-4"
    assert step["artifact_build_id"] == "build-5"
    assert promoted["deployment_status"] == "IN_PROGRESS"


def test_smoke_failure_rolls_back_previous_pointers():
    req = normalize_deployment_request(
        {
            "project_name": "ndr",
            "feature_spec_version": "v1",
            "deployment_mode": "authoritative",
            "request_id": "exec-3",
            "artifact_build_id": "build-5",
        }
    )
    promoted = build_promotion_spec(
        pipeline_spec=_sample_spec(),
        request=req,
        promoted_steps={
            "DeltaBuilderStep": {
                "code_artifact_s3_uri": "s3://bucket/code/DeltaBuilderStep/artifacts/build-5/source.tar.gz",
                "artifact_build_id": "build-5",
                "artifact_sha256": "sha-new",
                "artifact_format": "tar.gz",
            }
        },
    )
    rolled_back = build_rollback_spec(pipeline_spec=promoted, request=req, rollback_reason="smoke-failed")
    step = rolled_back["scripts"]["steps"]["DeltaBuilderStep"]
    assert step["artifact_build_id"] == "build-4"
    assert rolled_back["deployment_checkpoint"] == "rollback_completed"


def test_break_glass_marker_requires_reconciliation_before_promotion():
    req = normalize_deployment_request(
        {
            "project_name": "ndr",
            "feature_spec_version": "v1",
            "deployment_mode": "authoritative",
            "request_id": "exec-4",
            "artifact_build_id": "build-5",
        }
    )
    with pytest.raises(ValueError, match="TASK9_BREAK_GLASS_RECONCILIATION_REQUIRED"):
        enforce_break_glass_reconciliation(
            pipeline_spec={
                "deployment_manual_marker": "INC-123",
                "deployment_checkpoint": RECONCILIATION_REQUIRED_CHECKPOINT,
            },
            request=req,
        )


def test_atomic_promotion_contention_detected_by_revision_check():
    with pytest.raises(ValueError, match="TASK9_PROMOTION_CONTENTION"):
        assert_atomic_promotion_revision(expected_revision=5, current_spec={"deployment_revision": 6})


def test_ready_commit_marks_deployment_ready():
    req = normalize_deployment_request(
        {
            "project_name": "ndr",
            "feature_spec_version": "v1",
            "deployment_mode": "shadow",
            "request_id": "exec-5",
            "artifact_build_id": "build-6",
        }
    )
    committed = build_ready_commit_spec(pipeline_spec=_sample_spec(), request=req)
    assert committed["deployment_status"] == "READY"
    assert committed["deployment_checkpoint"] == "steady_state_ready"
