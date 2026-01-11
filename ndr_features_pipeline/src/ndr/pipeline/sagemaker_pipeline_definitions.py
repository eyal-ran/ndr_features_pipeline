from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.steps import ProcessingStep


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
