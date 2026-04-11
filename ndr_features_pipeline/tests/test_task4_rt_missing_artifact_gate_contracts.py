import json
from pathlib import Path


RT_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json")
BACKFILL_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json")


def _load_rt_states() -> dict:
    doc = json.loads(RT_STEP_FUNCTION_PATH.read_text(encoding="utf-8"))
    return doc["States"]["RunPerMlProjectBranch"]["ItemProcessor"]["States"]


def _load_backfill_states() -> dict:
    return json.loads(BACKFILL_STEP_FUNCTION_PATH.read_text(encoding="utf-8"))["States"]


def test_rt_missing_and_no_missing_branches_route_deterministically():
    states = _load_rt_states()

    success = next(
        choice
        for choice in states["FeaturesPipelineStatusChoice"]["Choices"]
        if choice["Condition"] == "{% $features_pipeline_status = 'Succeeded' %}"
    )
    assert success["Next"] == "ComputeRtArtifactReadiness"

    gate = states["CheckRtArtifactReadiness"]
    no_missing = next(choice for choice in gate["Choices"] if choice["Condition"] == "{% $rt_artifact_readiness_manifest.ready = true %}")
    assert no_missing["Next"] == "Start15mDependentFeaturesPipeline"

    missing_first_cycle = next(
        choice
        for choice in gate["Choices"]
        if choice["Condition"] == "{% $rt_artifact_readiness_manifest.ready = false and $rt_readiness_cycle = 0 %}"
    )
    assert missing_first_cycle["Next"] == "BuildRtBackfillRemediationRequest"
    assert gate["Default"] == "FailRtArtifactsUnresolvedAfterRemediation"


def test_rt_remediation_invocation_uses_backfill_contract_payload_and_revalidates_before_inference():
    states = _load_rt_states()

    request_expr = states["BuildRtBackfillRemediationRequest"]["Assign"]["rt_backfill_remediation_request"]
    assert "NdrBackfillRequest.v1" in request_expr
    assert "'consumer':'realtime'" in request_expr
    assert "'requested_families':$rt_artifact_readiness_manifest.required_families" in request_expr
    assert "'idempotency_key':$rt_artifact_readiness_manifest.idempotency_key" in request_expr

    invoke = states["InvokeRtBackfillRemediation"]
    assert invoke["Resource"] == "arn:aws:states:::states:startExecution.sync:2"
    assert invoke["Arguments"]["StateMachineArn"] == "${BackfillStateMachineArn}"
    assert invoke["Arguments"]["Input"] == "{% $rt_backfill_remediation_request %}"
    assert invoke["Next"] == "UpdateBatchIndexRemediationSucceeded"

    assert states["UpdateBatchIndexRemediationSucceeded"]["Next"] == "IncrementRtReadinessCycle"
    assert states["IncrementRtReadinessCycle"]["Next"] == "RecheckRtArtifactReadiness"
    assert states["RecheckRtArtifactReadiness"]["Next"] == "DescribeRtReadinessPipeline"


def test_rt_dependent_phase_pipeline_runs_after_readiness_gate():
    states = _load_rt_states()

    dependent_start = states["Start15mDependentFeaturesPipeline"]
    assert dependent_start["Arguments"]["PipelineName"] == "${PipelineName15mDependent}"
    dependent_params = {entry["Name"] for entry in dependent_start["Arguments"]["PipelineParameters"]}
    assert dependent_params == {"ProjectName", "FeatureSpecVersion", "MiniBatchId", "BatchStartTsIso", "BatchEndTsIso"}
    assert "RawParsedLogsS3Prefix" not in dependent_params

    choice = states["DependentFeaturesPipelineStatusChoice"]
    success = next(c for c in choice["Choices"] if c["Condition"] == "{% $dependent_pipeline_status = 'Succeeded' %}")
    assert success["Next"] == "StartInferencePipeline"


def test_rt_batch_index_status_updates_cover_requested_succeeded_and_failed_outcomes():
    states = _load_rt_states()

    requested = states["UpdateBatchIndexRemediationRequested"]
    assert requested["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:updateItem"
    assert "rt_remediation_status" in requested["Arguments"]["UpdateExpression"]
    assert requested["Arguments"]["ExpressionAttributeValues"][":rt_flow_status"]["S"] == "REMEDIATION_REQUESTED"

    succeeded = states["UpdateBatchIndexRemediationSucceeded"]
    assert succeeded["Arguments"]["ExpressionAttributeValues"][":rt_flow_status"]["S"] == "REMEDIATION_SUCCEEDED"
    assert succeeded["Arguments"]["ExpressionAttributeValues"][":rt_remediation_status"]["S"] == "SUCCEEDED"

    failed = states["UpdateBatchIndexRemediationFailed"]
    assert failed["Arguments"]["ExpressionAttributeValues"][":rt_flow_status"]["S"] == "REMEDIATION_FAILED"
    assert failed["Arguments"]["ExpressionAttributeValues"][":rt_remediation_status"]["S"] == "FAILED"
    assert failed["Next"] == "BranchFailed"


def test_rt_retry_and_idempotency_key_reuse_are_deterministic():
    states = _load_rt_states()

    invoke_retry = states["InvokeRtBackfillRemediation"]["Retry"][0]
    assert invoke_retry["ErrorEquals"] == ["StepFunctions.ExecutionLimitExceeded", "ThrottlingException", "States.TaskFailed"]
    assert invoke_retry["MaxAttempts"] == 3

    compute_state = states["ComputeRtArtifactReadiness"]
    assert compute_state["Arguments"]["PipelineName"] == "${PipelineNameRtReadiness}"
    assert compute_state["Next"] == "DescribeRtReadinessPipeline"

    initial_manifest_expr = states["BuildRtArtifactReadinessManifest"]["Assign"]["rt_artifact_readiness_manifest"]
    assert "RT_READINESS_ARTIFACT_MISSING" in initial_manifest_expr
    assert "$states.input.rt_artifact_readiness" not in initial_manifest_expr


def test_rt_gate_reads_and_validates_computed_v3_artifact() -> None:
    raw = RT_STEP_FUNCTION_PATH.read_text(encoding="utf-8")
    states = json.loads(raw)["States"]["RunPerMlProjectBranch"]["ItemProcessor"]["States"]

    read_state = states["ReadRtReadinessArtifact"]
    assert read_state["Resource"] == "arn:aws:states:::aws-sdk:s3:getObject"
    assert read_state["Arguments"]["Bucket"] == "${ArtifactsBucketName}"
    assert "orchestration/readiness/rt_artifact_readiness/v3/" in read_state["Arguments"]["Key"]
    assert "cycle=" in read_state["Arguments"]["Key"]
    assert read_state["Next"] == "BuildRtArtifactReadinessManifest"

    validate = states["ValidateRtArtifactReadinessManifest"]
    assert validate["Default"] == "CheckRtArtifactReadiness"
    assert "rt_artifact_readiness.v3" in json.dumps(validate)

    assert "$states.input.rt_artifact_readiness" not in raw


def test_rt_gate_fails_fast_for_missing_or_invalid_artifact() -> None:
    states = _load_rt_states()

    read_state = states["ReadRtReadinessArtifact"]
    assert read_state["Catch"][0]["Next"] == "FailRtReadinessArtifactMissing"

    assert states["FailRtReadinessArtifactMissing"]["Error"] == "RealtimeArtifactReadinessGateFailed"
    assert "RT_READINESS_ARTIFACT_MISSING" in states["FailRtReadinessArtifactMissing"]["Cause"]

    assert states["FailRtReadinessContractInvalid"]["Error"] == "RealtimeArtifactReadinessGateFailed"
    assert "RT_READINESS_CONTRACT_INVALID" in states["FailRtReadinessContractInvalid"]["Cause"]


def test_backfill_consumer_derives_window_from_contract_missing_ranges_when_start_end_not_provided():
    states = _load_backfill_states()
    resolve = states["ResolvePipelineRuntimeParams"]["Assign"]

    assert "$exists($states.input.missing_ranges[0].start_ts_iso) ? $min($states.input.missing_ranges.start_ts_iso)" in resolve["start_ts"]
    assert "$exists($states.input.missing_ranges[0].end_ts_iso) ? $max($states.input.missing_ranges.end_ts_iso)" in resolve["end_ts"]
