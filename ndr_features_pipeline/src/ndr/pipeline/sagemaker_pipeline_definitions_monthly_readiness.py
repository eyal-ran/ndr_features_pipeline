"""SageMaker pipeline definition for monthly FG-B dependency readiness artifacts."""

from __future__ import annotations
import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from ndr.pipeline.io_contract import (
    build_processing_step_launch_args,
    resolve_step_execution_contract,
)

PIPELINE_JOB_NAME = "pipeline_monthly_readiness"


def build_monthly_readiness_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
    project_name_for_contracts: str,
    feature_spec_version_for_contracts: str,
) -> Pipeline:
    session = sagemaker.session.Session(default_bucket=default_bucket)
    project = ParameterString(
        name="ProjectName", default_value="<required:ProjectName>"
    )
    version = ParameterString(
        name="FeatureSpecVersion", default_value="<required:FeatureSpecVersion>"
    )
    month = ParameterString(
        name="ReferenceMonth", default_value="<required:ReferenceMonth>"
    )
    cycle = ParameterString(name="ReadinessCycle", default_value="0")
    bucket = ParameterString(name="ArtifactsBucketName", default_value=default_bucket)
    index = ParameterString(name="BatchIndexTableName", default_value="batch_index")
    dpp = ParameterString(name="DppConfigTableName", default_value="dpp_config")
    instance = ParameterString(
        name="ProcessingInstanceType", default_value="ml.m5.xlarge"
    )
    count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)
    processor = PySparkProcessor(
        base_job_name="ndr-monthly-readiness",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=count,
        instance_type=instance,
        sagemaker_session=session,
    )
    contract = resolve_step_execution_contract(
        project_name=project_name_for_contracts,
        feature_spec_version=feature_spec_version_for_contracts,
        pipeline_job_name=PIPELINE_JOB_NAME,
        step_name="MonthlyFgBReadinessCheckerStep",
    )
    step = ProcessingStep(
        name="MonthlyFgBReadinessCheckerStep",
        processor=processor,
        code=contract.script_s3_uri,
        job_arguments=build_processing_step_launch_args(
            entry_script=contract.entry_script,
            module_name="ndr.scripts.run_monthly_readiness_checker",
            artifact_uri=contract.code_artifact_s3_uri,
            passthrough_args=[
                "--project-name",
                project,
                "--feature-spec-version",
                version,
                "--reference-month",
                month,
                "--readiness-cycle",
                cycle,
                "--artifacts-bucket-name",
                bucket,
                "--batch-index-table-name",
                index,
                "--dpp-config-table-name",
                dpp,
            ],
        ),
    )
    return Pipeline(
        name=pipeline_name,
        parameters=[
            project,
            version,
            month,
            cycle,
            bucket,
            index,
            dpp,
            instance,
            count,
        ],
        steps=[step],
        sagemaker_session=session,
    )
