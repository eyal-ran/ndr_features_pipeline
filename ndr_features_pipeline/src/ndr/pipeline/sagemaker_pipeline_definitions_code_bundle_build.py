from __future__ import annotations

"""SageMaker deployment pipeline definition for code bundle builds."""

import sagemaker
from sagemaker.processing import ProcessingOutput
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.execution_variables import ExecutionVariables
from sagemaker.workflow.functions import Join
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep

from ndr.pipeline.io_contract import resolve_step_execution_contract

PIPELINE_JOB_NAME = "pipeline_code_bundle_build"
_BUILD_STEP_NAME = "CodeBundleBuildStep"
_BUILD_OUTPUT_NAME = "BuildManifestOutput"
_BUILD_OUTPUT_SOURCE_DIR = "/opt/ml/processing/output/build_manifest"
_BUILD_OUTPUT_FILE = f"{_BUILD_OUTPUT_SOURCE_DIR}/code_bundle_build_output.json"


def _build_handoff_s3_uri(*, default_bucket: str, pipeline_name: str, artifact_name: str):
    return Join(
        on="",
        values=[
            "s3://",
            default_bucket,
            "/ndr/deployment/code-artifact-handoffs/",
            pipeline_name,
            "/",
            ExecutionVariables.PIPELINE_EXECUTION_ID,
            "/",
            artifact_name,
        ],
    )


def build_code_bundle_build_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
    project_name_for_contracts: str,
    feature_spec_version_for_contracts: str,
) -> Pipeline:
    session = sagemaker.session.Session(default_bucket=default_bucket)
    project_name = ParameterString(name="ProjectName", default_value="<required:ProjectName>")
    feature_spec_version = ParameterString(name="FeatureSpecVersion", default_value="<required:FeatureSpecVersion>")
    artifact_build_id = ParameterString(name="ArtifactBuildId", default_value="<required:ArtifactBuildId>")

    processing_image_uri = ParameterString(
        name="ProcessingImageUri",
        default_value="123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest",
    )
    processing_instance_type = ParameterString(name="ProcessingInstanceType", default_value="ml.m5.2xlarge")
    processing_instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)

    processor = PySparkProcessor(
        base_job_name="ndr-code-bundle-build",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=processing_instance_count,
        instance_type=processing_instance_type,
        image_uri=processing_image_uri,
        sagemaker_session=session,
    )

    build_contract = resolve_step_execution_contract(
        project_name=project_name_for_contracts,
        feature_spec_version=feature_spec_version_for_contracts,
        pipeline_job_name=PIPELINE_JOB_NAME,
        step_name=_BUILD_STEP_NAME,
    )
    build_step = ProcessingStep(
        name=_BUILD_STEP_NAME,
        processor=processor,
        code=build_contract.script_s3_uri,
        job_arguments=[
            "python",
            build_contract.entry_script,
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--artifact-build-id",
            artifact_build_id,
            "--region-name",
            region_name,
            "--manifest-out",
            _BUILD_OUTPUT_FILE,
        ],
        inputs=[],
        outputs=[
            ProcessingOutput(
                output_name=_BUILD_OUTPUT_NAME,
                source=_BUILD_OUTPUT_SOURCE_DIR,
                destination=_build_handoff_s3_uri(
                    default_bucket=default_bucket,
                    pipeline_name=pipeline_name,
                    artifact_name="build_manifest",
                ),
            )
        ],
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            artifact_build_id,
            processing_image_uri,
            processing_instance_type,
            processing_instance_count,
        ],
        steps=[build_step],
        sagemaker_session=session,
    )
