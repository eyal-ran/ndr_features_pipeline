import sys
import types


class _Param:
    def __init__(self, name, default_value=None):
        self.name = name
        self.default_value = default_value


class _Step:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def add_depends_on(self, _deps):
        return None


class _Pipeline:
    def __init__(self, name, parameters, steps, sagemaker_session):
        self.name = name
        self.parameters = parameters
        self.steps = steps
        self.sagemaker_session = sagemaker_session


class _Processor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _install_sagemaker_stubs():
    if "sagemaker" in sys.modules:
        return
    sagemaker = types.ModuleType("sagemaker")
    sagemaker_session = types.ModuleType("sagemaker.session")
    sagemaker_session.Session = lambda *args, **kwargs: {"args": args, "kwargs": kwargs}
    sagemaker.session = sagemaker_session

    spark_processing = types.ModuleType("sagemaker.spark.processing")
    spark_processing.PySparkProcessor = _Processor

    wf_params = types.ModuleType("sagemaker.workflow.parameters")
    wf_params.ParameterString = _Param
    wf_params.ParameterInteger = _Param

    wf_pipeline = types.ModuleType("sagemaker.workflow.pipeline")
    wf_pipeline.Pipeline = _Pipeline

    wf_steps = types.ModuleType("sagemaker.workflow.steps")
    wf_steps.ProcessingStep = _Step

    sys.modules["sagemaker"] = sagemaker
    sys.modules["sagemaker.session"] = sagemaker_session
    sys.modules["sagemaker.spark.processing"] = spark_processing
    sys.modules["sagemaker.workflow.parameters"] = wf_params
    sys.modules["sagemaker.workflow.pipeline"] = wf_pipeline
    sys.modules["sagemaker.workflow.steps"] = wf_steps


def test_pipeline_definitions_smoke_build_with_concrete_contracts(monkeypatch):
    _install_sagemaker_stubs()

    from ndr.pipeline import sagemaker_pipeline_definitions_backfill_historical_extractor as p_backfill_hist
    from ndr.pipeline import sagemaker_pipeline_definitions_if_training as p_train
    from ndr.pipeline import sagemaker_pipeline_definitions_inference as p_inf
    from ndr.pipeline import sagemaker_pipeline_definitions_prediction_feature_join as p_join
    from ndr.pipeline import sagemaker_pipeline_definitions_unified_with_fgc as p_unified

    monkeypatch.setattr(
        p_unified,
        "load_job_spec",
        lambda **_kwargs: {"processing_image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr:latest"},
    )
    for module in (p_unified, p_inf, p_join, p_train, p_backfill_hist):
        monkeypatch.setattr(
            module,
            "resolve_step_code_uri",
            lambda **kwargs: f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
        )

    p_unified.build_15m_streaming_pipeline(
        pipeline_name="p15m",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_unified.build_15m_dependent_pipeline(
        pipeline_name="p15m-dependent",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_unified.build_fg_b_baseline_pipeline(
        pipeline_name="pfgb",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_unified.build_machine_inventory_unload_pipeline(
        pipeline_name="pmachine",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_inf.build_inference_predictions_pipeline(
        pipeline_name="pinf",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_join.build_prediction_feature_join_pipeline(
        pipeline_name="pjoin",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_train.build_if_training_pipeline(
        pipeline_name="ptrain",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_backfill_hist.build_backfill_historical_extractor_pipeline(
        pipeline_name="pbackfill-historical",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )


def test_if_training_pipeline_enforces_verify_plan_remediate_reverify_train_sequence(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_if_training as p_train

    monkeypatch.setattr(
        p_train,
        "resolve_step_code_uri",
        lambda **kwargs: f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
    )
    pipeline = p_train.build_if_training_pipeline(
        pipeline_name="ptrain",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    steps = {step.name: step for step in pipeline.steps}
    assert "RemediationPlanningStep" in steps
    assert steps["RemediationPlanningStep"].kwargs["job_arguments"][-1] == "plan"
    assert steps["MissingFeatureCreationStep"].kwargs["depends_on"] == [steps["RemediationPlanningStep"]]
    assert steps["PostRemediationVerificationStep"].kwargs["depends_on"] == [steps["MissingFeatureCreationStep"]]
    assert steps["IFTrainingStep"].kwargs["depends_on"] == [steps["PostRemediationVerificationStep"]]


def test_if_training_pipeline_rejects_placeholder_contract_identity(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_if_training as p_train

    try:
        p_train.build_if_training_pipeline(
            pipeline_name="ptrain",
            role_arn="arn:aws:iam::123:role/x",
            default_bucket="bucket",
            region_name="us-east-1",
            project_name_for_contracts="<required:ProjectName>",
            feature_spec_version_for_contracts="v1",
        )
    except ValueError as exc:
        assert "concrete value" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_15m_pipelines_are_split_into_core_and_dependent_phases(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_unified_with_fgc as p_unified

    monkeypatch.setattr(
        p_unified,
        "resolve_step_code_uri",
        lambda **kwargs: f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
    )

    core = p_unified.build_15m_streaming_pipeline(
        pipeline_name="p15m-core",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    dependent = p_unified.build_15m_dependent_pipeline(
        pipeline_name="p15m-dependent",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )

    assert [step.name for step in core.steps] == ["DeltaBuilderStep", "FGABuilderStep", "PairCountsBuilderStep"]
    assert [step.name for step in dependent.steps] == ["FGCCorrBuilderStep"]


def test_backfill_historical_extractor_pipeline_rejects_placeholder_contract_identity(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_backfill_historical_extractor as p_backfill_hist

    try:
        p_backfill_hist.build_backfill_historical_extractor_pipeline(
            pipeline_name="pbackfill-historical",
            role_arn="arn:aws:iam::123:role/x",
            default_bucket="bucket",
            region_name="us-east-1",
            project_name_for_contracts="<required:ProjectName>",
            feature_spec_version_for_contracts="v1",
        )
    except ValueError as exc:
        assert "concrete value" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
