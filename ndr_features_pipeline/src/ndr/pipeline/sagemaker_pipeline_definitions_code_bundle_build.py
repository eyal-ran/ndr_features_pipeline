from __future__ import annotations

"""SageMaker deployment pipeline definition for code bundle builds."""

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep

PIPELINE_JOB_NAME = "pipeline_code_bundle_build"


def build_code_bundle_build_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
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

    step = ProcessingStep(
        name="CodeBundleBuildStep",
        processor=processor,
        code="s3://placeholder/ndr/deployment/run_code_bundle_build.py",
        job_arguments=[
            "python",
            "run_code_bundle_build.py",
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--artifact-build-id",
            artifact_build_id,
        ],
        inputs=[],
        outputs=[],
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
        steps=[step],
        sagemaker_session=session,
    )
