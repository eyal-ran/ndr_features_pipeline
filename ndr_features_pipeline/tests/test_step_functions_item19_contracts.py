import json
from pathlib import Path


STEP_FUNCTIONS_DIR = Path("docs/step_functions_jsonata")


def _load(name: str):
    return json.loads((STEP_FUNCTIONS_DIR / name).read_text())


def test_state_machines_remove_callback_and_approval_lambda_dependencies():
    for path in STEP_FUNCTIONS_DIR.glob("sfn_ndr_*.json"):
        text = path.read_text()
        assert "waitForTaskToken" not in text
        assert "PipelineCompletionCallbackLambdaArn" not in text
        assert "SupplementalBaselineLambdaArn" not in text
        assert "PublishJoinedPredictionsLambdaArn" not in text
        assert "ModelPublishLambdaArn" not in text
        assert "ModelAttributesRegistryLambdaArn" not in text
        assert "ModelDeployLambdaArn" not in text
        assert "HumanApprovalLambdaArn" not in text


def test_sagemaker_starts_have_describe_polling_pattern():
    for name in [
        "sfn_ndr_15m_features_inference.json",
        "sfn_ndr_monthly_fg_b_baselines.json",
        "sfn_ndr_backfill_reprocessing.json",
        "sfn_ndr_prediction_publication.json",
        "sfn_ndr_training_orchestrator.json",
    ]:
        doc = _load(name)
        states = doc["States"]
        start_tasks = [s for s in states.values() if isinstance(s, dict) and s.get("Resource") == "arn:aws:states:::aws-sdk:sagemaker:startPipelineExecution"]
        assert start_tasks, f"{name} should start at least one SageMaker pipeline"
        assert any(
            isinstance(s, dict) and s.get("Resource") == "arn:aws:states:::aws-sdk:sagemaker:describePipelineExecution"
            for s in states.values()
        ), f"{name} missing describePipelineExecution polling task"


def test_15m_flow_uses_batch_completion_payload_and_direct_publication_handoff():
    doc = _load("sfn_ndr_15m_features_inference.json")
    text = json.dumps(doc)
    assert "_SUCCESS" not in text
    parse_assign = doc["States"]["ParseIncomingProjectContext"]["Assign"]
    assert "batch_s3_path" in parse_assign["parsed_s3_key"]
    assert doc["States"]["StartPredictionPublicationWorkflow"]["Resource"] == "arn:aws:states:::states:startExecution.sync:2"


def test_prediction_publication_has_identity_lock_and_duplicate_suppression():
    doc = _load("sfn_ndr_prediction_publication.json")
    states = doc["States"]

    assert "AcquirePublicationLock" in states
    assert "DuplicatePublicationSuppressed" in states
    lock = states["AcquirePublicationLock"]
    assert lock["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem"
    assert "attribute_not_exists(pk)" in lock["Arguments"]["ConditionExpression"]
    assert states["DuplicatePublicationSuppressed"]["Type"] == "Succeed"

    assert "StartPublicationPipeline" not in states
    assert "DescribePublicationPipeline" not in states
    assert "PublishPipelineStatusChoice" not in states
    assert "WaitBeforePublishDescribe" not in states
    assert "IncrementPublishPollAttempt" not in states
    join_success = next(
        choice
        for choice in states["JoinPipelineStatusChoice"]["Choices"]
        if choice["Condition"] == "{% $prediction_join_pipeline_status = 'Succeeded' %}"
    )
    assert join_success["Next"] == "MarkPublicationSucceeded"


def test_training_orchestrator_has_verifier_remediation_with_retry_budget_two():
    doc = _load("sfn_ndr_training_orchestrator.json")
    states = doc["States"]
    assert "StartTrainingDataVerifier" in states
    assert "VerifierFailedRetryGate" in states
    choice = states["VerifierFailedRetryGate"]["Choices"][0]["Condition"]
    assert "$remediation_attempt < 2" in choice
    assert "TrainingVerifierRetryExhausted" in states


def test_monthly_fg_b_baseline_has_no_supplemental_pipeline_states():
    doc = _load("sfn_ndr_monthly_fg_b_baselines.json")
    states = doc["States"]

    assert "StartSupplementalBaselinePipeline" not in states
    assert "DescribeSupplementalPipeline" not in states
    assert "SupplementalPipelineStatusChoice" not in states
    assert "WaitBeforeSupplementalDescribe" not in states
    assert "IncrementSupplementalPollAttempt" not in states

    fgb_success = next(
        choice
        for choice in states["FGBPipelineStatusChoice"]["Choices"]
        if choice["Condition"] == "{% $fgb_pipeline_status = 'Succeeded' %}"
    )
    assert fgb_success["Next"] == "EmitBaselineReadyEvent"
