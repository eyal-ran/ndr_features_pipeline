from __future__ import annotations


"""SageMaker pipeline definition for decoupled inference predictions."""

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep

from ndr.pipeline.io_contract import resolve_step_code_uri

PIPELINE_JOB_NAME = "pipeline_inference_predictions"


def build_inference_predictions_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
) -> Pipeline:
    """Create the decoupled inference predictions pipeline.

    CLI contract for run_inference_predictions.py:
      --project-name
      --feature-spec-version
      --mini-batch-id
      --batch-start-ts-iso
      --batch-end-ts-iso
    """

    session = sagemaker.session.Session(default_bucket=default_bucket)

    project_name = ParameterString(
        name="ProjectName",
        default_value="<required:ProjectName>",
    )
    feature_spec_version = ParameterString(
        name="FeatureSpecVersion",
        default_value="<required:FeatureSpecVersion>",
    )
    mini_batch_id = ParameterString(
        name="MiniBatchId",
        default_value="<required:MiniBatchId>",
    )
    batch_start_ts_iso = ParameterString(
        name="BatchStartTsIso",
        default_value="<required:BatchStartTsIso>",
    )
    batch_end_ts_iso = ParameterString(
        name="BatchEndTsIso",
        default_value="<required:BatchEndTsIso>",
    )

    processing_image_uri = ParameterString(
        name="ProcessingImageUri",
        default_value="123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest",
    )
    processing_instance_type = ParameterString(
        name="ProcessingInstanceType",
        default_value="ml.m5.4xlarge",
    )
    processing_instance_count = ParameterInteger(
        name="ProcessingInstanceCount",
        default_value=1,
    )

    processor = PySparkProcessor(
        base_job_name="ndr-inference-predictions",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=processing_instance_count,
        instance_type=processing_instance_type,
        image_uri=processing_image_uri,
        sagemaker_session=session,
    )

    resolved_code_uri = resolve_step_code_uri(
        project_name=project_name.default_value,
        feature_spec_version=feature_spec_version.default_value,
        pipeline_job_name=PIPELINE_JOB_NAME,
        step_name="InferencePredictionsStep",
    )

    inference_step = ProcessingStep(
        name="InferencePredictionsStep",
        processor=processor,
        code=resolved_code_uri,
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_inference_predictions",
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--mini-batch-id",
            mini_batch_id,
            "--batch-start-ts-iso",
            batch_start_ts_iso,
            "--batch-end-ts-iso",
            batch_end_ts_iso,
        ],
        inputs=[],
        outputs=[],
    )

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            mini_batch_id,
            batch_start_ts_iso,
            batch_end_ts_iso,
            processing_image_uri,
            processing_instance_type,
            processing_instance_count,
        ],
        steps=[inference_step],
        sagemaker_session=session,
    )

    return pipeline
