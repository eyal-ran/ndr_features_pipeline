from __future__ import annotations


"""SageMaker pipeline definition for IF training (FG-A + FG-C)."""

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep


def build_if_training_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
) -> Pipeline:
    """Execute the build if training pipeline stage of the workflow."""
    session = sagemaker.session.Session(default_bucket=default_bucket)

    project_name = ParameterString(name="ProjectName", default_value="ndr-project")
    feature_spec_version = ParameterString(name="FeatureSpecVersion", default_value="v1")
    run_id = ParameterString(name="RunId", default_value="manual-run")
    execution_ts_iso = ParameterString(name="ExecutionTsIso", default_value="2025-01-01T00:00:00Z")

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

    training_step = ProcessingStep(
        name="IFTrainingStep",
        processor=processor,
        code="src/ndr/scripts/run_if_training.py",
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
        ],
        inputs=[],
        outputs=[],
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            run_id,
            execution_ts_iso,
            processing_image_uri,
            processing_instance_type,
            processing_instance_count,
        ],
        steps=[training_step],
        sagemaker_session=session,
    )
