from __future__ import annotations


"""Unified SageMaker pipeline definitions for the NDR project (updated).

This module defines three pipelines wired to the *current* run_*.py CLI
contracts:

1. build_delta_builder_pipeline(...)
   - Simple one-step pipeline that runs the Delta Builder for a given 15m slice.
   - Intended mainly for backfill / debugging / ad-hoc runs.

   CLI for run_delta_builder.py:
     --project-name
     --feature-spec-version
     --mini-batch-id
     --batch-start-ts-iso
     --batch-end-ts-iso

2. build_15m_streaming_pipeline(...)
   - Main 15m streaming pipeline:
        Delta Builder  ->  FG-A Builder  ->  Pair-Counts Builder
   - All steps share the same PySparkProcessor and the unified CLI:

     --project-name
     --feature-spec-version
     --mini-batch-id
     --batch-start-ts-iso
     --batch-end-ts-iso

3. build_fg_b_baseline_pipeline(...)
4. build_machine_inventory_unload_pipeline(...)
   - Baseline pipeline for FG-B (7d / 30d horizons), using an abstracted
     contract pushed into JobSpec configuration rather than CLI flags.

   CLI for run_fg_b_builder.py:
     --project-name
     --feature-spec-version
     --reference-time-iso
     --mode  (REGULAR | BACKFILL)

All structural configuration (S3 prefixes, port sets, VDI patterns,
DQ thresholds, baseline windows, etc.) is read inside the jobs from the
SageMaker projects JobSpec table in DynamoDB and/or environment variables.
The pipelines themselves only pass *runtime* parameters (batch times, modes).
"""

import sagemaker
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.spark.processing import PySparkProcessor

from ndr.config.job_spec_loader import load_job_spec

# ---------------------------------------------------------------------------
# 1. Backfill / simple Delta-only pipeline
# ---------------------------------------------------------------------------


def build_delta_builder_pipeline(
    region: str,
    role_arn: str,
    pipeline_name: str,
    base_job_name: str,
    code_s3_uri: str,
) -> Pipeline:
    """Create a SageMaker Pipeline with a single Spark ProcessingStep (Delta Builder).

    This pipeline is intended for backfill or ad-hoc runs of the delta builder.

    CLI contract for run_delta_builder.py:

    Required:
    - --project-name          (str)
    - --feature-spec-version  (str)
    - --mini-batch-id         (str)
    - --batch-start-ts-iso    (str, ISO8601)
    - --batch-end-ts-iso      (str, ISO8601)
    """

    session = sagemaker.session.Session()

    processor = PySparkProcessor(
        base_job_name=base_job_name,
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        sagemaker_session=session,
    )

    # Pipeline parameters
    project_name = ParameterString(
        name="ProjectName",
        default_value="ndr-project",
    )
    feature_spec_version = ParameterString(
        name="FeatureSpecVersion",
        default_value="v1",
    )
    mini_batch_id = ParameterString(
        name="MiniBatchId",
        default_value="dummy-mini-batch-id",
    )
    batch_start_ts_iso = ParameterString(
        name="BatchStartTsIso",
        default_value="2025-01-01T00:00:00Z",
    )
    batch_end_ts_iso = ParameterString(
        name="BatchEndTsIso",
        default_value="2025-01-01T00:15:00Z",
    )

    delta_step = ProcessingStep(
        name="DeltaBuilderStep",
        processor=processor,
        # For backfill/simple use cases we allow passing the code S3 URI
        # directly; this is expected to point to the delta builder entrypoint
        # (e.g. a zipped project with src/ndr/scripts/run_delta_builder.py).
        code=code_s3_uri,
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_delta_builder",
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
        ],
        steps=[delta_step],
        sagemaker_session=session,
    )

    return pipeline


# ---------------------------------------------------------------------------
# 2. Main 15m streaming pipeline: Delta -> FG-A -> Pair-Counts
# ---------------------------------------------------------------------------


def build_15m_streaming_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
) -> Pipeline:
    """Create the main 15m streaming pipeline (Delta, FG-A, Pair-Counts).

    Steps (in order):

    1. DeltaBuilderStep
       - runs ndr.scripts.run_delta_builder
       - builds 15m delta tables from parsed Palo Alto logs

    2. FGABuilderStep
       - runs ndr.scripts.run_fg_a_builder
       - builds FG-A (current behaviour) features on top of deltas

    3. PairCountsBuilderStep
       - runs ndr.scripts.run_pair_counts_builder
       - builds pair-counts dataset for (src_ip, dst_ip, dst_port) for that 15m slice

    All steps share the same runtime parameters:

      --project-name
      --feature-spec-version
      --mini-batch-id
      --batch-start-ts-iso
      --batch-end-ts-iso

    Structural config (S3 prefixes, DQ rules, etc.) is loaded inside the jobs
    via JobSpec / environment.
    """

    session = sagemaker.session.Session(default_bucket=default_bucket)

    processor = PySparkProcessor(
        base_job_name="ndr-15m-streaming",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        sagemaker_session=session,
    )

    # Pipeline parameters (wired from Step Functions)
    project_name = ParameterString(
        name="ProjectName",
        default_value="ndr-project",
    )
    feature_spec_version = ParameterString(
        name="FeatureSpecVersion",
        default_value="v1",
    )
    mini_batch_id = ParameterString(
        name="MiniBatchId",
        default_value="dummy-mini-batch-id",
    )
    batch_start_ts_iso = ParameterString(
        name="BatchStartTsIso",
        default_value="2025-01-01T00:00:00Z",
    )
    batch_end_ts_iso = ParameterString(
        name="BatchEndTsIso",
        default_value="2025-01-01T00:15:00Z",
    )

    # ------------------------------------------------------------------ #
    # Step 1: Delta Builder                                             #
    # ------------------------------------------------------------------ #
    delta_step = ProcessingStep(
        name="DeltaBuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_delta_builder.py",
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_delta_builder",
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

    # ------------------------------------------------------------------ #
    # Step 2: FG-A Builder                                              #
    # ------------------------------------------------------------------ #
    fg_a_step = ProcessingStep(
        name="FGABuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_fg_a_builder.py",
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_fg_a_builder",
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
    fg_a_step.add_depends_on([delta_step])

    # ------------------------------------------------------------------ #
    # Step 3: Pair-Counts Builder                                       #
    # ------------------------------------------------------------------ #
    pair_counts_step = ProcessingStep(
        name="PairCountsBuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_pair_counts_builder.py",
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_pair_counts_builder",
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
    pair_counts_step.add_depends_on([fg_a_step])

    # ------------------------------------------------------------------ #
    # Step 4: FG-C Correlation Builder                                  #
    # ------------------------------------------------------------------ #
    fg_c_step = ProcessingStep(
        name="FGCCorrBuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_fg_c_builder.py",
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_fg_c_builder",
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
    fg_c_step.add_depends_on([pair_counts_step])

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            mini_batch_id,
            batch_start_ts_iso,
            batch_end_ts_iso,
        ],
        steps=[delta_step, fg_a_step, pair_counts_step, fg_c_step],
        sagemaker_session=session,
    )

    return pipeline


# ---------------------------------------------------------------------------
# 3. FG-B baseline pipeline (REGULAR/BACKFILL)
# ---------------------------------------------------------------------------


def build_fg_b_baseline_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
) -> Pipeline:
    """Create the FG-B baseline pipeline.

    This is a coarse-grained pipeline that runs the FG-B builder Spark job
    to compute host/segment/pair baselines over 7d / 30d horizons.

    CLI contract for run_fg_b_builder.py:

    Required:
    - --project-name          (str)
    - --feature-spec-version  (str)
    - --reference-time-iso    (str, ISO8601)
    - --mode                  (str)  # REGULAR | BACKFILL

    The actual horizon windows, capping logic, support thresholds, and
    backfill ranges are configured in JobSpec / environment.
    """

    session = sagemaker.session.Session(default_bucket=default_bucket)

    processor = PySparkProcessor(
        base_job_name="ndr-fg-b-baseline",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        sagemaker_session=session,
    )

    # Pipeline parameters
    project_name = ParameterString(
        name="ProjectName",
        default_value="ndr-project",
    )
    feature_spec_version = ParameterString(
        name="FeatureSpecVersion",
        default_value="v1",
    )
    reference_time_iso = ParameterString(
        name="ReferenceTimeIso",
        default_value="2025-12-31T00:00:00Z",
    )
    mode_param = ParameterString(
        name="Mode",
        default_value="REGULAR",  # or BACKFILL
    )

    fg_b_step = ProcessingStep(
        name="FGBaselineBuilderStep",
        processor=processor,
        code="src/ndr/scripts/run_fg_b_builder.py",
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_fg_b_builder",
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--reference-time-iso",
            reference_time_iso,
            "--mode",
            mode_param,
        ],
        inputs=[],
        outputs=[],
    )
    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            reference_time_iso,
            mode_param,
        ],
        steps=[fg_b_step],
        sagemaker_session=session,
    )

    return pipeline


# ---------------------------------------------------------------------------
# 4. Machine inventory unload pipeline (monthly)
# ---------------------------------------------------------------------------


def build_machine_inventory_unload_pipeline(
    pipeline_name: str,
    role_arn: str,
    default_bucket: str,
    region_name: str,
    project_name_value: str = "ndr-project",
    feature_spec_version_value: str = "v1",
) -> Pipeline:
    """Create the monthly machine inventory unload pipeline.

    CLI contract for run_machine_inventory_unload.py:

    Required:
    - --project-name          (str)
    - --feature-spec-version  (str)
    - --reference-month-iso   (str, ISO8601)
    """

    session = sagemaker.session.Session(default_bucket=default_bucket)

    project_name = ParameterString(
        name="ProjectName",
        default_value=project_name_value,
    )
    feature_spec_version = ParameterString(
        name="FeatureSpecVersion",
        default_value=feature_spec_version_value,
    )
    reference_month_iso = ParameterString(
        name="ReferenceMonthIso",
        default_value="2025-12-01T00:00:00Z",
    )

    instance_type = ParameterString(
        name="InstanceType",
        default_value="ml.m5.4xlarge",
    )
    instance_count = ParameterInteger(
        name="InstanceCount",
        default_value=1,
    )

    job_spec = load_job_spec(
        project_name=project_name_value,
        job_name="machine_inventory_unload",
        feature_spec_version=feature_spec_version_value,
    )
    image_uri = job_spec.get("processing_image_uri")
    if not image_uri:
        raise ValueError(
            "JobSpec for machine_inventory_unload must provide processing_image_uri."
        )

    processor = PySparkProcessor(
        base_job_name="ndr-machine-inventory-unload",
        framework_version="3.5",
        py_version="py312",
        role=role_arn,
        instance_count=instance_count,
        instance_type=instance_type,
        image_uri=image_uri,
        sagemaker_session=session,
    )

    unload_step = ProcessingStep(
        name="MachineInventoryUnloadStep",
        processor=processor,
        code="src/ndr/scripts/run_machine_inventory_unload.py",
        job_arguments=[
            "python",
            "-m",
            "ndr.scripts.run_machine_inventory_unload",
            "--project-name",
            project_name,
            "--feature-spec-version",
            feature_spec_version,
            "--reference-month-iso",
            reference_month_iso,
        ],
        inputs=[],
        outputs=[],
    )

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            project_name,
            feature_spec_version,
            reference_month_iso,
            instance_type,
            instance_count,
        ],
        steps=[unload_step],
        sagemaker_session=session,
    )

    return pipeline
