from __future__ import annotations


"""SageMaker pipeline definition for IF training (FG-A + FG-C)."""

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep

from ndr.pipeline.io_contract import resolve_step_code_uri

PIPELINE_JOB_NAME = "pipeline_if_training"


def _build_if_training_step(
    *,
    processor: PySparkProcessor,
    step_name: str,
    project_name: ParameterString,
    feature_spec_version: ParameterString,
    run_id: ParameterString,
    execution_ts_iso: ParameterString,
    training_start_ts: ParameterString,
    training_end_ts: ParameterString,
    eval_start_ts: ParameterString,
    eval_end_ts: ParameterString,
    missing_windows_override: ParameterString,
    stage: str,
    depends_on: list[ProcessingStep] | None = None,
) -> ProcessingStep:
    """Build one stage within the unified IF training pipeline."""
    resolved_code_uri = resolve_step_code_uri(
        project_name=project_name.default_value,
        feature_spec_version=feature_spec_version.default_value,
        pipeline_job_name=PIPELINE_JOB_NAME,
        step_name=step_name,
    )
    return ProcessingStep(
        name=step_name,
        processor=processor,
        code=resolved_code_uri,
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_if_training",
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--run-id",
            run_id,
            "--execution-ts-iso",
            execution_ts_iso,
            "--training-start-ts",
            training_start_ts,
            "--training-end-ts",
            training_end_ts,
            "--eval-start-ts",
            eval_start_ts,
            "--eval-end-ts",
            eval_end_ts,
            "--missing-windows-override",
            missing_windows_override,
            "--stage",
            stage,
        ],
        depends_on=depends_on,
        inputs=[],
        outputs=[],
    )


def build_if_training_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
) -> Pipeline:
    """Execute the unified IF training lifecycle in one pipeline."""
    session = sagemaker.session.Session(default_bucket=default_bucket)

    project_name = ParameterString(name="ProjectName", default_value="<required:ProjectName>")
    feature_spec_version = ParameterString(name="FeatureSpecVersion", default_value="<required:FeatureSpecVersion>")
    run_id = ParameterString(name="RunId", default_value="<required:RunId>")
    execution_ts_iso = ParameterString(name="ExecutionTsIso", default_value="<required:ExecutionTsIso>")
    training_start_ts = ParameterString(name="TrainingStartTs", default_value="<required:TrainingStartTs>")
    training_end_ts = ParameterString(name="TrainingEndTs", default_value="<required:TrainingEndTs>")
    eval_start_ts = ParameterString(name="EvalStartTs", default_value="<required:EvalStartTs>")
    eval_end_ts = ParameterString(name="EvalEndTs", default_value="<required:EvalEndTs>")
    missing_windows_override = ParameterString(name="MissingWindowsOverride", default_value="[]")

    processing_image_uri = ParameterString(
        name="ProcessingImageUri",
        default_value="123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest",
    )
    processing_instance_type = ParameterString(name="ProcessingInstanceType", default_value="ml.m5.4xlarge")
    processing_instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)

    processor = PySparkProcessor(
        base_job_name="ndr-if-training",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=processing_instance_count,
        instance_type=processing_instance_type,
        image_uri=processing_image_uri,
        sagemaker_session=session,
    )

    verifier_step = _build_if_training_step(
        processor=processor,
        step_name="TrainingDataVerifierStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="verify",
    )
    remediation_step = _build_if_training_step(
        processor=processor,
        step_name="MissingFeatureCreationStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="remediate",
        depends_on=[verifier_step],
    )
    reverification_step = _build_if_training_step(
        processor=processor,
        step_name="PostRemediationVerificationStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="reverify",
        depends_on=[remediation_step],
    )
    training_step = _build_if_training_step(
        processor=processor,
        step_name="IFTrainingStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="train",
        depends_on=[reverification_step],
    )
    publish_step = _build_if_training_step(
        processor=processor,
        step_name="ModelPublishStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="publish",
        depends_on=[training_step],
    )
    attributes_step = _build_if_training_step(
        processor=processor,
        step_name="ModelAttributesStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="attributes",
        depends_on=[publish_step],
    )
    deploy_step = _build_if_training_step(
        processor=processor,
        step_name="ModelDeployStep",
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        run_id=run_id,
        execution_ts_iso=execution_ts_iso,
        training_start_ts=training_start_ts,
        training_end_ts=training_end_ts,
        eval_start_ts=eval_start_ts,
        eval_end_ts=eval_end_ts,
        missing_windows_override=missing_windows_override,
        stage="deploy",
        depends_on=[attributes_step],
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            run_id,
            execution_ts_iso,
            training_start_ts,
            training_end_ts,
            eval_start_ts,
            eval_end_ts,
            missing_windows_override,
            processing_image_uri,
            processing_instance_type,
            processing_instance_count,
        ],
        steps=[
            verifier_step,
            remediation_step,
            reverification_step,
            training_step,
            publish_step,
            attributes_step,
            deploy_step,
        ],
        sagemaker_session=session,
    )
