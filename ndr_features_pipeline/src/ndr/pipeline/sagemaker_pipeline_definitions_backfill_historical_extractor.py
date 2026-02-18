from __future__ import annotations

"""SageMaker pipeline definition for historical windows extraction."""

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep

from ndr.pipeline.io_contract import resolve_step_code_uri

PIPELINE_JOB_NAME = "pipeline_backfill_historical_extractor"


def build_backfill_historical_extractor_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
) -> Pipeline:
    """Create pipeline that extracts historical mini-batch windows for backfill map runs."""
    session = sagemaker.session.Session(default_bucket=default_bucket)

    project_name = ParameterString(name="ProjectName", default_value="<required:ProjectName>")
    feature_spec_version = ParameterString(name="FeatureSpecVersion", default_value="<required:FeatureSpecVersion>")
    start_ts_iso = ParameterString(name="StartTsIso", default_value="<required:StartTsIso>")
    end_ts_iso = ParameterString(name="EndTsIso", default_value="<required:EndTsIso>")
    input_s3_prefix = ParameterString(name="InputS3Prefix", default_value="<required:InputS3Prefix>")
    output_s3_prefix = ParameterString(name="OutputS3Prefix", default_value="<required:OutputS3Prefix>")

    processing_image_uri = ParameterString(
        name="ProcessingImageUri",
        default_value="123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest",
    )
    processing_instance_type = ParameterString(name="ProcessingInstanceType", default_value="ml.m5.4xlarge")
    processing_instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)

    processor = PySparkProcessor(
        base_job_name="ndr-backfill-historical-extractor",
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
        step_name="HistoricalWindowsExtractorStep",
    )

    step = ProcessingStep(
        name="HistoricalWindowsExtractorStep",
        processor=processor,
        code=resolved_code_uri,
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_historical_windows_extractor",
            "--input-s3-prefix",
            input_s3_prefix,
            "--output-s3-prefix",
            output_s3_prefix,
            "--start-ts-iso",
            start_ts_iso,
            "--end-ts-iso",
            end_ts_iso,
            "--window-floor-minutes",
            "8,23,38,53",
            "--feature-spec-version",
            feature_spec_version,
        ],
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            start_ts_iso,
            end_ts_iso,
            input_s3_prefix,
            output_s3_prefix,
            processing_image_uri,
            processing_instance_type,
            processing_instance_count,
        ],
        steps=[step],
        sagemaker_session=session,
    )
