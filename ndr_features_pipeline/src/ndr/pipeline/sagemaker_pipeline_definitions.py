from __future__ import annotations
from typing import List, Optional

from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import Processor, ProcessingInput, ProcessingOutput


def build_delta_builder_pipeline(
    region: str,
    role_arn: str,
    pipeline_name: str,
    base_job_name: str,
    code_s3_uri: str,
) -> Pipeline:
    """Create a SageMaker Pipeline with a single Spark ProcessingStep to run Delta Builder."""
    project_name = ParameterString("project_name")
    job_name = ParameterString("job_name", default_value="delta_builder")
    mini_batch_s3_prefix = ParameterString("mini_batch_s3_prefix")
    feature_spec_version = ParameterString("feature_spec_version")
    run_id = ParameterString("run_id")
    job_spec_ddb_table_name = ParameterString("job_spec_ddb_table_name")
    slice_start_ts = ParameterString("slice_start_ts", default_value="")
    slice_end_ts = ParameterString("slice_end_ts", default_value="")

    spark_processor = PySparkProcessor(
        base_job_name=base_job_name,
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_type="ml.m5.4xlarge",
        instance_count=1,
    )

    step_delta = ProcessingStep(
        name="DeltaBuilderStep",
        processor=spark_processor,
        inputs=[],
        outputs=[],
        code=code_s3_uri,
        job_arguments=[
            "--project-name", project_name,
            "--job-name", job_name,
            "--mini-batch-s3-prefix", mini_batch_s3_prefix,
            "--feature-spec-version", feature_spec_version,
            "--run-id", run_id,
            "--job-spec-ddb-table-name", job_spec_ddb_table_name,
            "--slice-start-ts", slice_start_ts,
            "--slice-end-ts", slice_end_ts,
        ],
    )

    return Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            job_name,
            mini_batch_s3_prefix,
            feature_spec_version,
            run_id,
            job_spec_ddb_table_name,
            slice_start_ts,
            slice_end_ts,
        ],
        steps=[step_delta],
    )


def build_delta_and_fg_a_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
    processing_image_uri: str,
) -> Pipeline:
    """Build a SageMaker Pipeline with Delta Builder + FG-A Builder.

    Parameters
    ----------
    pipeline_name:
        Name of the SageMaker Pipeline.
    role_arn:
        IAM role ARN used by SageMaker to run Processing jobs.
    default_bucket:
        Default S3 bucket used for intermediate artifacts.
    region_name:
        AWS region.
    processing_image_uri:
        URI of the SageMaker Processing container image (built-in or custom)
        that contains the `ndr` package and Spark runtime.

    Returns
    -------
    Pipeline
        A Pipeline object that can be created and executed via the SDK or
        invoked from Step Functions.

    Pipeline Parameters (exposed for Step Functions)
    -----------------------------------------------
    - project_name
    - mini_batch_id
    - feature_spec_version
    - delta_s3_prefix
    - fg_a_output_s3_prefix
    - fg_a_feature_group_offline
    - fg_a_feature_group_online
    - write_fg_a_to_feature_store ("true"/"false")
    """

    # ------------------------------------------------------------------
    # Pipeline parameters (to be set by Step Functions)
    # ------------------------------------------------------------------

    project_name = ParameterString(
        name="ProjectName",
        default_value="ndr-project",
    )

    mini_batch_id = ParameterString(
        name="MiniBatchId",
        default_value="dummy-mini-batch-id",
    )

    feature_spec_version = ParameterString(
        name="FeatureSpecVersion",
        default_value="fga_v1",
    )

    delta_s3_prefix = ParameterString(
        name="DeltaS3Prefix",
        default_value=f"s3://{default_bucket}/ndr/deltas/",
    )

    fg_a_output_s3_prefix = ParameterString(
        name="FgaOutputS3Prefix",
        default_value=f"s3://{default_bucket}/ndr/fg_a/",
    )

    fg_a_feature_group_offline = ParameterString(
        name="FgaFeatureGroupOffline",
        default_value="",
    )

    fg_a_feature_group_online = ParameterString(
        name="FgaFeatureGroupOnline",
        default_value="",
    )

    write_fg_a_to_feature_store = ParameterString(
        name="WriteFgaToFeatureStore",
        default_value="false",
    )

    # ------------------------------------------------------------------
    # Common Processor definition (Spark-capable Processing container)
    # ------------------------------------------------------------------

    processor = Processor(
        role=role_arn,
        image_uri=processing_image_uri,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        volume_size_in_gb=100,
        max_runtime_in_seconds=3600,
        sagemaker_session=sagemaker.session.Session(),
    )

    # ------------------------------------------------------------------
    # Delta Builder Processing Step
    # ------------------------------------------------------------------
    # Assumes you already have a script `ndr/scripts/run_delta_builder.py`
    # that accepts project_name, mini_batch_id, etc., similar to FG-A builder.

    delta_step = ProcessingStep(
        name="DeltaBuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_delta_builder.py",
        job_arguments=[
            "--project-name", project_name,
            "--region-name", region_name,
            "--mini-batch-id", mini_batch_id,
            # any other delta-specific arguments you require (input/output prefixes, etc.)
        ],
        inputs=[
            # If needed, mount any configuration or reference data here as ProcessingInput
        ],
        outputs=[
            # You may specify an output here if you want to capture logs or metadata.
            # The delta parquet dataset itself is typically written directly to S3 by the job.
        ],
    )

    # ------------------------------------------------------------------
    # FG-A Builder Processing Step
    # ------------------------------------------------------------------

    fg_a_step = ProcessingStep(
        name="FgaBuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_fg_a_builder.py",
        job_arguments=[
            "--project-name", project_name,
            "--region-name", region_name,
            "--delta-s3-prefix", delta_s3_prefix,
            "--output-s3-prefix", fg_a_output_s3_prefix,
            "--mini-batch-id", mini_batch_id,
            "--feature-spec-version", feature_spec_version,
            "--feature-group-offline", fg_a_feature_group_offline,
            "--feature-group-online", fg_a_feature_group_online,
            "--write-to-feature-store", write_fg_a_to_feature_store,
        ],
        inputs=[
            # If FG-A builder needs config files (port sets, etc.) as local files,
            # you can define ProcessingInput(s) here.
        ],
        outputs=[
            # FG-A parquet is written directly to S3 by the job. Optionally,
            # you can expose an output for inspection or downstream steps.
            ProcessingOutput(
                output_name="fg_a_parquet",
                source="/opt/ml/processing/output/fg_a_tmp",
                destination=fg_a_output_s3_prefix,
            )
        ],
    )

    # FG-A depends on Delta (i.e., run FG-A only after delta step completes).
    fg_a_step.add_depends_on([delta_step])

    # ------------------------------------------------------------------
    # Assemble pipeline
    # ------------------------------------------------------------------

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            mini_batch_id,
            feature_spec_version,
            delta_s3_prefix,
            fg_a_output_s3_prefix,
            fg_a_feature_group_offline,
            fg_a_feature_group_online,
            write_fg_a_to_feature_store,
        ],
        steps=[delta_step, fg_a_step],
        sagemaker_session=sagemaker.session.Session(),
    )

    return pipeline

