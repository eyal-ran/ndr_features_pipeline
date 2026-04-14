import sys
import types
from types import SimpleNamespace


class _Param:
    def __init__(self, name, default_value=None):
        self.name = name
        self.default_value = default_value


class _Step:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs
        outputs = kwargs.get("outputs", [])
        output_map = {
            output.output_name: SimpleNamespace(S3Output=SimpleNamespace(S3Uri=output.destination))
            for output in outputs
        }
        self.properties = SimpleNamespace(
            ProcessingOutputConfig=SimpleNamespace(
                Outputs=output_map,
            )
        )

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


class _ProcessingOutput:
    def __init__(self, output_name, source, destination):
        self.output_name = output_name
        self.source = source
        self.destination = destination


class _ProcessingInput:
    def __init__(self, source, destination, input_name=None):
        self.source = source
        self.destination = destination
        self.input_name = input_name


class _Join:
    def __init__(self, on, values):
        self.on = on
        self.values = values

    def __str__(self):
        return self.on.join(str(value) for value in self.values)


class _ExecutionVariables:
    PIPELINE_EXECUTION_ID = "execution-id"


def _install_sagemaker_stubs():
    if "sagemaker" in sys.modules:
        return
    sagemaker = types.ModuleType("sagemaker")
    sagemaker_session = types.ModuleType("sagemaker.session")
    sagemaker_session.Session = lambda *args, **kwargs: {"args": args, "kwargs": kwargs}
    sagemaker.session = sagemaker_session

    spark_processing = types.ModuleType("sagemaker.spark.processing")
    spark_processing.PySparkProcessor = _Processor
    processing = types.ModuleType("sagemaker.processing")
    processing.ProcessingInput = _ProcessingInput
    processing.ProcessingOutput = _ProcessingOutput

    wf_params = types.ModuleType("sagemaker.workflow.parameters")
    wf_params.ParameterString = _Param
    wf_params.ParameterInteger = _Param

    wf_functions = types.ModuleType("sagemaker.workflow.functions")
    wf_functions.Join = _Join

    wf_execution_variables = types.ModuleType("sagemaker.workflow.execution_variables")
    wf_execution_variables.ExecutionVariables = _ExecutionVariables

    wf_pipeline = types.ModuleType("sagemaker.workflow.pipeline")
    wf_pipeline.Pipeline = _Pipeline

    wf_steps = types.ModuleType("sagemaker.workflow.steps")
    wf_steps.ProcessingStep = _Step

    sys.modules["sagemaker"] = sagemaker
    sys.modules["sagemaker.session"] = sagemaker_session
    sys.modules["sagemaker.spark.processing"] = spark_processing
    sys.modules["sagemaker.processing"] = processing
    sys.modules["sagemaker.workflow.parameters"] = wf_params
    sys.modules["sagemaker.workflow.functions"] = wf_functions
    sys.modules["sagemaker.workflow.execution_variables"] = wf_execution_variables
    sys.modules["sagemaker.workflow.pipeline"] = wf_pipeline
    sys.modules["sagemaker.workflow.steps"] = wf_steps
    if "boto3" not in sys.modules:
        boto3 = types.ModuleType("boto3")
        boto3.client = lambda *args, **kwargs: SimpleNamespace()
        boto3.resource = lambda *args, **kwargs: SimpleNamespace()
        sys.modules["boto3"] = boto3


def test_pipeline_definitions_smoke_build_with_concrete_contracts(monkeypatch):
    _install_sagemaker_stubs()

    from ndr.pipeline import sagemaker_pipeline_definitions_backfill_historical_extractor as p_backfill_hist
    from ndr.pipeline import sagemaker_pipeline_definitions_code_artifact_validate as p_code_validate
    from ndr.pipeline import sagemaker_pipeline_definitions_code_bundle_build as p_code_build
    from ndr.pipeline import sagemaker_pipeline_definitions_code_smoke_validate as p_code_smoke
    from ndr.pipeline import sagemaker_pipeline_definitions_if_training as p_train
    from ndr.pipeline import sagemaker_pipeline_definitions_inference as p_inf
    from ndr.pipeline import sagemaker_pipeline_definitions_prediction_feature_join as p_join
    from ndr.pipeline import sagemaker_pipeline_definitions_unified_with_fgc as p_unified

    monkeypatch.setattr(
        p_unified,
        "load_job_spec",
        lambda **_kwargs: {"processing_image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr:latest"},
    )
    monkeypatch.setattr(
        p_unified,
        "resolve_step_execution_contract",
        lambda **kwargs: SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
            entry_script=f"{kwargs['step_name']}.py",
            code_artifact_s3_uri=None,
        ),
    )
    for module in (p_inf, p_join, p_train, p_backfill_hist):
        monkeypatch.setattr(
            module,
            "resolve_step_execution_contract",
            lambda **kwargs: SimpleNamespace(
                script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
                entry_script="run_stub.py",
                code_artifact_s3_uri=None,
            ),
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
    for module in (p_code_build, p_code_validate, p_code_smoke):
        monkeypatch.setattr(
            module,
            "resolve_step_execution_contract",
            lambda **kwargs: SimpleNamespace(
                script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
                entry_script=f"{kwargs['step_name']}.py",
                code_artifact_s3_uri=None,
            ),
        )

    p_code_build.build_code_bundle_build_pipeline(
        pipeline_name="pcode-build",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_code_validate.build_code_artifact_validate_pipeline(
        pipeline_name="pcode-validate",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    p_code_smoke.build_code_smoke_validate_pipeline(
        pipeline_name="pcode-smoke",
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
        "resolve_step_execution_contract",
        lambda **kwargs: SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
            entry_script="run_if_training.py",
            code_artifact_s3_uri=None,
        ),
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
        "resolve_step_execution_contract",
        lambda **kwargs: SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
            entry_script=f"{kwargs['step_name']}.py",
            code_artifact_s3_uri=None,
        ),
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

    assert [step.name for step in core.steps] == [
        "RTRawInputResolverStep",
        "DeltaBuilderStep",
        "FGABuilderStep",
        "PairCountsBuilderStep",
    ]
    assert [step.name for step in dependent.steps] == ["FGCCorrBuilderStep"]


def test_unified_pipelines_launch_args_dual_read_fallback_when_artifact_missing(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_unified_with_fgc as p_unified

    monkeypatch.setattr(
        p_unified,
        "load_job_spec",
        lambda **_kwargs: {"processing_image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr:latest"},
    )

    resolved_steps = []

    def _resolve_contract(**kwargs):
        step_name = kwargs["step_name"]
        resolved_steps.append(step_name)
        return SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{step_name}.py",
            entry_script=f"{step_name}.py",
            code_artifact_s3_uri=None,
        )

    monkeypatch.setattr(p_unified, "resolve_step_execution_contract", _resolve_contract)

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
    baseline = p_unified.build_fg_b_baseline_pipeline(
        pipeline_name="pfgb",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )
    unload = p_unified.build_machine_inventory_unload_pipeline(
        pipeline_name="pmachine",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )

    expected_modules = {
        "RTRawInputResolverStep": "ndr.scripts.run_rt_raw_input_resolver",
        "DeltaBuilderStep": "ndr.scripts.run_delta_builder",
        "FGABuilderStep": "ndr.scripts.run_fg_a_builder",
        "PairCountsBuilderStep": "ndr.scripts.run_pair_counts_builder",
        "FGCCorrBuilderStep": "ndr.scripts.run_fg_c_builder",
        "FGBaselineBuilderStep": "ndr.scripts.run_fg_b_builder",
        "MachineInventoryUnloadStep": "ndr.scripts.run_machine_inventory_unload",
    }
    all_steps = core.steps + dependent.steps + baseline.steps + unload.steps
    for step in all_steps:
        module_name = expected_modules[step.name]
        assert step.kwargs["job_arguments"][:3] == ["python", "-m", module_name]
        assert step.kwargs["code"].endswith(f"{step.name}.py")

    assert resolved_steps == list(expected_modules.keys())


def test_unified_pipelines_launch_args_use_entry_script_when_artifact_present(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_unified_with_fgc as p_unified

    monkeypatch.setattr(
        p_unified,
        "load_job_spec",
        lambda **_kwargs: {"processing_image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr:latest"},
    )

    def _resolve_contract(**kwargs):
        step_name = kwargs["step_name"]
        return SimpleNamespace(
            script_s3_uri=f"s3://artifacts/{step_name}/source.tar.gz",
            entry_script=f"src/ndr/scripts/{step_name}.py",
            code_artifact_s3_uri=f"s3://artifacts/{step_name}/source.tar.gz",
        )

    monkeypatch.setattr(p_unified, "resolve_step_execution_contract", _resolve_contract)

    pipelines = [
        p_unified.build_15m_streaming_pipeline(
            pipeline_name="p15m-core",
            role_arn="arn:aws:iam::123:role/x",
            default_bucket="bucket",
            region_name="us-east-1",
            project_name_for_contracts="proj",
            feature_spec_version_for_contracts="v1",
        ),
        p_unified.build_15m_dependent_pipeline(
            pipeline_name="p15m-dependent",
            role_arn="arn:aws:iam::123:role/x",
            default_bucket="bucket",
            region_name="us-east-1",
            project_name_for_contracts="proj",
            feature_spec_version_for_contracts="v1",
        ),
        p_unified.build_fg_b_baseline_pipeline(
            pipeline_name="pfgb",
            role_arn="arn:aws:iam::123:role/x",
            default_bucket="bucket",
            region_name="us-east-1",
            project_name_for_contracts="proj",
            feature_spec_version_for_contracts="v1",
        ),
        p_unified.build_machine_inventory_unload_pipeline(
            pipeline_name="pmachine",
            role_arn="arn:aws:iam::123:role/x",
            default_bucket="bucket",
            region_name="us-east-1",
            project_name_for_contracts="proj",
            feature_spec_version_for_contracts="v1",
        ),
    ]

    for pipeline in pipelines:
        for step in pipeline.steps:
            assert step.kwargs["job_arguments"][:2] == ["python", f"src/ndr/scripts/{step.name}.py"]
            assert step.kwargs["code"] == f"s3://artifacts/{step.name}/source.tar.gz"


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


def test_code_artifact_validate_pipeline_wires_build_manifest_handoff(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_code_artifact_validate as p_code_validate

    monkeypatch.setattr(
        p_code_validate,
        "resolve_step_execution_contract",
        lambda **kwargs: SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
            entry_script=f"{kwargs['step_name']}.py",
            code_artifact_s3_uri=None,
        ),
    )

    pipeline = p_code_validate.build_code_artifact_validate_pipeline(
        pipeline_name="pcode-validate",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )

    build_step, validate_step = pipeline.steps
    assert build_step.name == "CodeBundleBuildStep"
    assert validate_step.name == "CodeArtifactValidateStep"
    assert validate_step.kwargs["depends_on"] == [build_step]
    assert validate_step.kwargs["inputs"][0].destination.endswith("/build_manifest")
    assert "--build-manifest-in" in validate_step.kwargs["job_arguments"]
    assert "--validation-report-out" in validate_step.kwargs["job_arguments"]


def test_code_smoke_validate_pipeline_wires_full_dependency_chain(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_code_smoke_validate as p_code_smoke

    monkeypatch.setattr(
        p_code_smoke,
        "resolve_step_execution_contract",
        lambda **kwargs: SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
            entry_script=f"{kwargs['step_name']}.py",
            code_artifact_s3_uri=None,
        ),
    )

    pipeline = p_code_smoke.build_code_smoke_validate_pipeline(
        pipeline_name="pcode-smoke",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )

    build_step, validate_step, smoke_step = pipeline.steps
    assert validate_step.kwargs["depends_on"] == [build_step]
    assert smoke_step.kwargs["depends_on"] == [validate_step]
    assert len(smoke_step.kwargs["inputs"]) == 2
    assert smoke_step.kwargs["inputs"][0].destination.endswith("/build_manifest")
    assert smoke_step.kwargs["inputs"][1].destination.endswith("/validation_report")
    assert "--validation-report-in" in smoke_step.kwargs["job_arguments"]


def test_code_bundle_build_pipeline_emits_versioned_manifest_output(monkeypatch):
    _install_sagemaker_stubs()
    from ndr.pipeline import sagemaker_pipeline_definitions_code_bundle_build as p_code_build

    monkeypatch.setattr(
        p_code_build,
        "resolve_step_execution_contract",
        lambda **kwargs: SimpleNamespace(
            script_s3_uri=f"s3://code/{kwargs['pipeline_job_name']}/{kwargs['step_name']}.py",
            entry_script=f"{kwargs['step_name']}.py",
            code_artifact_s3_uri=None,
        ),
    )

    pipeline = p_code_build.build_code_bundle_build_pipeline(
        pipeline_name="pcode-build",
        role_arn="arn:aws:iam::123:role/x",
        default_bucket="bucket",
        region_name="us-east-1",
        project_name_for_contracts="proj",
        feature_spec_version_for_contracts="v1",
    )

    build_step = pipeline.steps[0]
    output = build_step.kwargs["outputs"][0]
    assert build_step.name == "CodeBundleBuildStep"
    assert output.output_name == "BuildManifestOutput"
    assert output.source.endswith("/build_manifest")
    assert "code-artifact-handoffs" in str(output.destination)
    assert "--manifest-out" in build_step.kwargs["job_arguments"]
