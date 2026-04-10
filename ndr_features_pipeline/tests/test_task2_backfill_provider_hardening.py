import pytest
import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *_args, **_kwargs: None
sys.modules.setdefault("boto3", boto3_stub)

from ndr.orchestration.backfill_family_dispatcher import (
    COMPLETION_ERROR_CODE,
    DISPATCHER_ERROR_CODE,
    BackfillCompletionVerifier,
    BackfillFamilyDispatcher,
)


class _FakeSageMaker:
    def __init__(self, *, failure_by_family=None):
        self.calls = []
        self.failure_by_family = failure_by_family or {}

    def start_pipeline_execution(self, **kwargs):
        family = next(item["Value"] for item in kwargs["PipelineParameters"] if item["Name"] == "ArtifactFamily")
        self.calls.append(kwargs)
        failure = self.failure_by_family.get(family)
        if failure:
            raise RuntimeError(failure)
        return {"PipelineExecutionArn": f"arn:aws:sagemaker:us-east-1:111:pipeline-execution/{family}"}


def _targets():
    return {
        "orchestration_targets": {
            "delta": "pipeline-delta",
            "fg_a": "pipeline-fga",
            "pair_counts": "pipeline-pair",
            "fg_b_baseline": "pipeline-fgb",
            "fg_c": "pipeline-fgc",
        }
    }


def test_unsupported_family_rejected_with_explicit_error_code():
    dispatcher = BackfillFamilyDispatcher(
        project_name="ndr-prod",
        feature_spec_version="v1",
        requested_families=("delta",),
        correlation_id="bkf-1",
        retry_attempt=0,
        dpp_spec=_targets(),
        sagemaker_client=_FakeSageMaker(),
    )

    with pytest.raises(ValueError, match=DISPATCHER_ERROR_CODE):
        dispatcher.dispatch(
            missing_entries=[
                {
                    "family": "unknown_family",
                    "window_start_ts": "2025-01-01T00:00:00Z",
                    "window_end_ts": "2025-01-01T00:15:00Z",
                    "batch_ids": ["b-1"],
                    "required_inputs": ["raw_logs"],
                    "reason_code": "MISSING_ARTIFACT",
                }
            ]
        )


def test_mixed_family_partial_missing_is_dependency_ordered_and_payload_normalized():
    fake_sm = _FakeSageMaker()
    dispatcher = BackfillFamilyDispatcher(
        project_name="ndr-prod",
        feature_spec_version="v1",
        requested_families=("delta", "fg_a", "pair_counts", "fg_c"),
        correlation_id="bkf-2",
        retry_attempt=0,
        dpp_spec=_targets(),
        sagemaker_client=fake_sm,
    )

    response = dispatcher.dispatch(
        missing_entries=[
            {
                "family": "fg_a",
                "window_start_ts": "2025-01-01T00:00:00Z",
                "window_end_ts": "2025-01-01T00:15:00Z",
                "batch_ids": ["b-2", "b-1"],
                "required_inputs": ["delta"],
                "reason_code": "MISSING_ARTIFACT",
            },
            {
                "family": "pair_counts",
                "window_start_ts": "2025-01-01T00:00:00Z",
                "window_end_ts": "2025-01-01T00:15:00Z",
                "batch_ids": ["b-3"],
                "required_inputs": ["delta"],
                "reason_code": "MISSING_ARTIFACT",
            },
            {
                "family": "fg_c",
                "window_start_ts": "2025-01-01T00:00:00Z",
                "window_end_ts": "2025-01-01T00:15:00Z",
                "batch_ids": ["b-3"],
                "required_inputs": ["fg_a", "pair_counts", "fg_b_baseline"],
                "reason_code": "MISSING_ARTIFACT",
            },
            {
                "family": "delta",
                "window_start_ts": "2025-01-01T00:00:00Z",
                "window_end_ts": "2025-01-01T00:15:00Z",
                "batch_ids": ["b-1"],
                "required_inputs": [],
                "reason_code": "MISSING_SOURCE",
            },
        ]
    )

    called_families = [
        next(item["Value"] for item in call["PipelineParameters"] if item["Name"] == "ArtifactFamily")
        for call in fake_sm.calls
    ]
    assert called_families == ["delta", "fg_a", "pair_counts", "fg_c"]
    assert response["completion"]["all_succeeded"] is True
    assert response["family_results"][1]["handler_payload"]["batch_ids"] == ["b-1", "b-2"]


def test_retry_success_and_duplicate_replay_idempotency():
    fake_sm = _FakeSageMaker(failure_by_family={"delta": "ConflictException: already exists"})
    dispatcher = BackfillFamilyDispatcher(
        project_name="ndr-prod",
        feature_spec_version="v1",
        requested_families=("delta",),
        correlation_id="bkf-3",
        retry_attempt=1,
        dpp_spec=_targets(),
        sagemaker_client=fake_sm,
    )

    response = dispatcher.dispatch(
        missing_entries=[
            {
                "family": "delta",
                "window_start_ts": "2025-01-01T00:00:00Z",
                "window_end_ts": "2025-01-01T00:15:00Z",
                "batch_ids": ["b-1"],
                "required_inputs": [],
                "reason_code": "MISSING_SOURCE",
            }
        ]
    )

    assert response["family_results"][0]["status"] == "DuplicateRequest"
    assert response["completion"]["all_succeeded"] is True


def test_completion_event_blocked_when_any_family_fails():
    verifier = BackfillCompletionVerifier(
        requested_families=("delta", "fg_a"),
        family_results=[
            {"family": "delta", "status": "Started"},
            {"family": "fg_a", "status": "Failed"},
        ],
    )
    verdict = verifier.verify()

    assert verdict["all_succeeded"] is False
    assert verdict["failed_families"] == ["fg_a"]
    assert verdict["verifier_code"] == COMPLETION_ERROR_CODE
