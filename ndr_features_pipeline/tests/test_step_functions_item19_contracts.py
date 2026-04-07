import json
from pathlib import Path


STEP_FUNCTIONS_DIR = Path("docs/step_functions_jsonata")


def _load(name: str):
    return json.loads((STEP_FUNCTIONS_DIR / name).read_text())


def _collect_states(state_machine: dict) -> dict:
    collected = {}

    def _visit(states: dict):
        for name, state in states.items():
            collected[name] = state
            if isinstance(state, dict) and state.get("Type") == "Map" and "ItemProcessor" in state:
                _visit(state["ItemProcessor"]["States"])

    _visit(state_machine["States"])
    return collected


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
        states = _collect_states(doc)
        start_tasks = [
            s for s in states.values() if isinstance(s, dict) and s.get("Resource") == "arn:aws:states:::aws-sdk:sagemaker:startPipelineExecution"
        ]
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
    branch_states = doc["States"]["RunPerMlProjectBranch"]["ItemProcessor"]["States"]
    assert branch_states["StartPredictionPublicationWorkflow"]["Resource"] == "arn:aws:states:::states:startExecution.sync:2"


def test_prediction_publication_has_identity_lock_and_duplicate_suppression():
    doc = _load("sfn_ndr_prediction_publication.json")
    states = _collect_states(doc)

    assert "RunPublicationPerMlProject" in doc["States"]
    assert doc["States"]["RunPublicationPerMlProject"]["Type"] == "Map"

    assert "AcquirePublicationLock" in states
    assert "DuplicatePublicationSuppressed" in states
    lock = states["AcquirePublicationLock"]
    assert lock["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem"
    assert "attribute_not_exists(pk)" in lock["Arguments"]["ConditionExpression"]
    assert states["DuplicatePublicationSuppressed"]["Type"] == "Succeed"

    pipeline_params = {p["Name"] for p in states["StartPredictionJoinPipeline"]["Arguments"]["PipelineParameters"]}
    assert "MlProjectName" in pipeline_params

    identity_expr = states["ResolvePublicationIdentity"]["Assign"]["publication_identity"]
    assert "ml_project_name" in identity_expr


def test_training_orchestrator_is_coarse_grained_single_training_pipeline():
    doc = _load("sfn_ndr_training_orchestrator.json")
    states = _collect_states(doc)

    assert "RunTrainingPerMlProject" in doc["States"]
    assert doc["States"]["RunTrainingPerMlProject"]["Type"] == "Map"

    assert "StartTrainingPipeline" in states
    assert "DescribeTrainingPipeline" in states
    assert "TrainingPipelineStatusChoice" in states

    assert "StartTrainingDataVerifier" not in states
    assert "StartMissingFeatureCreation" not in states
    assert "StartModelPublishPipeline" not in states
    assert "StartModelAttributesPipeline" not in states
    assert "StartModelDeployPipeline" not in states

    args = states["StartTrainingPipeline"]["Arguments"]
    assert args["PipelineName"] == "${PipelineNameIFTraining}"
    names = {p["Name"] for p in args["PipelineParameters"]}
    assert {"TrainingStartTs", "TrainingEndTs", "EvalStartTs", "EvalEndTs", "MissingWindowsOverride", "MlProjectName"}.issubset(names)


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



def test_item23_validation_failures_use_deterministic_error_code():
    for name in [
        "sfn_ndr_15m_features_inference.json",
        "sfn_ndr_monthly_fg_b_baselines.json",
        "sfn_ndr_backfill_reprocessing.json",
        "sfn_ndr_training_orchestrator.json",
    ]:
        doc = _load(name)
        states = doc["States"]
        fail_states = [v for v in states.values() if isinstance(v, dict) and v.get("Type") == "Fail" and v.get("Error") == "RuntimeParameterValidationError"]
        assert fail_states, f"{name} expected RuntimeParameterValidationError fail states"
        for state in fail_states:
            assert isinstance(state.get("Cause"), str)
            assert state["Cause"]


def test_15m_flow_writes_dual_batch_index_items_and_runs_per_ml_project_map():
    doc = _load("sfn_ndr_15m_features_inference.json")
    states = _collect_states(doc)

    assert states["AcquireMiniBatchLock"]["Next"] == "WriteBatchIndexBatchIdItem"

    put_batch_item_state = states["WriteBatchIndexBatchIdItem"]
    assert put_batch_item_state["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem"
    assert put_batch_item_state["Arguments"]["ConditionExpression"] == "attribute_not_exists(PK) AND attribute_not_exists(SK)"

    update_state = states["UpdateBatchIndexBatchIdItem"]
    assert update_state["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:updateItem"
    assert update_state["Arguments"]["ConditionExpression"] == "attribute_exists(PK) AND attribute_exists(SK)"
    assert "ml_project_names = :ml_project_names" in update_state["Arguments"]["UpdateExpression"]

    put_date_lookup_state = states["WriteBatchIndexDateLookupItem"]
    assert put_date_lookup_state["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem"
    assert put_date_lookup_state["Arguments"]["ConditionExpression"] == "attribute_not_exists(PK) AND attribute_not_exists(SK)"

    map_state = doc["States"]["RunPerMlProjectBranch"]
    assert map_state["Type"] == "Map"
    assert map_state["Items"] == "{% $ml_project_names %}"
    assert map_state["ItemSelector"]["ml_project_name"] == "{% $states.context.Map.Item.Value %}"

    features_params = {entry["Name"] for entry in states["Start15mFeaturesPipeline"]["Arguments"]["PipelineParameters"]}
    assert {"ProjectName", "FeatureSpecVersion", "MiniBatchId", "BatchStartTsIso", "BatchEndTsIso", "RawParsedLogsS3Prefix"}.issubset(features_params)
    assert "MlProjectName" not in features_params
    assert "MlProjectNamesJson" not in features_params

    inference_params = {entry["Name"] for entry in states["StartInferencePipeline"]["Arguments"]["PipelineParameters"]}
    assert "MlProjectName" in inference_params
    assert "RawParsedLogsS3Prefix" not in inference_params
    assert "MlProjectNamesJson" not in inference_params


def test_15m_flow_derives_slot15_from_timestamp_minute_buckets():
    doc = _load("sfn_ndr_15m_features_inference.json")
    slot_expr = doc["States"]["ResolvePipelineRuntimeParams"]["Assign"]["slot15"]

    assert "minute_utc <= 14 ? 1" in slot_expr
    assert "minute_utc <= 29 ? 2" in slot_expr
    assert "minute_utc <= 44 ? 3" in slot_expr
    assert ": 4" in slot_expr
