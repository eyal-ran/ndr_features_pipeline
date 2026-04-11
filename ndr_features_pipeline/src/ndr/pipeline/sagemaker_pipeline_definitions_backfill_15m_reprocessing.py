from __future__ import annotations

"""Dedicated backfill execution pipeline matching SFN family/range invocation contract."""

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep

from ndr.pipeline.io_contract import build_processing_step_launch_args, resolve_step_execution_contract

PIPELINE_JOB_NAME = "pipeline_backfill_15m_reprocessing"


def build_backfill_15m_reprocessing_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
    project_name_for_contracts: str,
    feature_spec_version_for_contracts: str,
) -> Pipeline:
    """Create backfill execution pipeline with canonical family/range runtime parameters."""
    session = sagemaker.session.Session(default_bucket=default_bucket)

    project_name = ParameterString(name="ProjectName", default_value="<required:ProjectName>")
    feature_spec_version = ParameterString(name="FeatureSpecVersion", default_value="<required:FeatureSpecVersion>")
    artifact_family = ParameterString(name="ArtifactFamily", default_value="<required:ArtifactFamily>")
    range_start_ts_iso = ParameterString(name="RangeStartTsIso", default_value="<required:RangeStartTsIso>")
    range_end_ts_iso = ParameterString(name="RangeEndTsIso", default_value="<required:RangeEndTsIso>")
    idempotency_key = ParameterString(name="IdempotencyKey", default_value="")

    processing_image_uri = ParameterString(
        name="ProcessingImageUri",
        default_value="123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest",
    )
    processing_instance_type = ParameterString(name="ProcessingInstanceType", default_value="ml.m5.4xlarge")
    processing_instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)

    processor = PySparkProcessor(
        base_job_name="ndr-backfill-15m-reprocessing",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=processing_instance_count,
        instance_type=processing_instance_type,
        image_uri=processing_image_uri,
        sagemaker_session=session,
    )

    contract = resolve_step_execution_contract(
        project_name=project_name_for_contracts,
        feature_spec_version=feature_spec_version_for_contracts,
        pipeline_job_name=PIPELINE_JOB_NAME,
        step_name="BackfillRangeExecutorStep",
    )

    step = ProcessingStep(
        name="BackfillRangeExecutorStep",
        processor=processor,
        code=contract.script_s3_uri,
        job_arguments=build_processing_step_launch_args(
            entry_script=contract.entry_script,
            module_name="ndr.scripts.run_backfill_reprocessing_executor",
            artifact_uri=contract.code_artifact_s3_uri,
            passthrough_args=[
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--artifact-family",
            artifact_family,
            "--range-start-ts-iso",
            range_start_ts_iso,
            "--range-end-ts-iso",
            range_end_ts_iso,
            "--idempotency-key",
            idempotency_key,
            ],
        ),
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            artifact_family,
            range_start_ts_iso,
            range_end_ts_iso,
            idempotency_key,
            processing_image_uri,
            processing_instance_type,
            processing_instance_count,
        ],
        steps=[step],
        sagemaker_session=session,
    )
