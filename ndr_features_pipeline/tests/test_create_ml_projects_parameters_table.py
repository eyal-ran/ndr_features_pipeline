import sys
import types

# The module under test imports boto3/botocore at import-time, but these tests only
# exercise pure payload builders. Stub the AWS SDK modules to keep tests hermetic.
sys.modules.setdefault("boto3", types.ModuleType("boto3"))

botocore_module = types.ModuleType("botocore")
exceptions_module = types.ModuleType("botocore.exceptions")


class _ClientError(Exception):
    pass


exceptions_module.ClientError = _ClientError
botocore_module.exceptions = exceptions_module
sys.modules.setdefault("botocore", botocore_module)
sys.modules.setdefault("botocore.exceptions", exceptions_module)

from ndr.scripts.create_ml_projects_parameters_table import (
    _build_bootstrap_items,
    build_split_seed_items,
    build_split_table_contracts,
    resolve_routing_table_name,
)


def _items_by_job(items):
    return {item["job_name_version"].split("#", 1)[0]: item for item in items}


def test_bootstrap_items_include_all_runtime_jobs():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)

    expected_jobs = {
        "delta_builder",
        "fg_a_builder",
        "pair_counts_builder",
        "fg_b_builder",
        "fg_c_builder",
        "machine_inventory_unload",
        "inference_predictions",
        "prediction_feature_join",
        "if_training",
        "project_parameters",
        "pipeline_15m_streaming",
        "pipeline_15m_dependent",
        "pipeline_fg_b_baseline",
        "pipeline_machine_inventory_unload",
        "pipeline_inference_predictions",
        "pipeline_prediction_feature_join",
        "pipeline_if_training",
        "pipeline_backfill_historical_extractor",
        "pipeline_backfill_15m_reprocessing",
    }

    assert set(by_job.keys()) == expected_jobs


def test_machine_inventory_unload_seed_has_pipeline_required_image_uri():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)

    machine_inventory_spec = by_job["machine_inventory_unload"]["spec"]
    assert machine_inventory_spec["processing_image_uri"]


def test_prediction_feature_join_seed_has_required_destination_payload():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)

    join_spec = by_job["prediction_feature_join"]["spec"]
    assert join_spec["destination"]["type"] == "s3"
    assert join_spec["destination"]["s3"]["s3_prefix"].startswith("s3://")


def test_pipeline_seed_items_have_runtime_and_script_contracts():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)

    pipeline_spec = by_job["pipeline_15m_streaming"]["spec"]
    assert pipeline_spec["required_runtime_params"] == [
        "ProjectName",
        "FeatureSpecVersion",
        "MiniBatchId",
        "BatchStartTsIso",
        "BatchEndTsIso",
        "RawParsedLogsS3Prefix",
    ]

    delta_step = pipeline_spec["scripts"]["steps"]["DeltaBuilderStep"]
    resolver_step = pipeline_spec["scripts"]["steps"]["RTRawInputResolverStep"]
    assert resolver_step["entry_script"] == "run_rt_raw_input_resolver.py"
    assert delta_step["code_prefix_s3"].startswith("s3://")
    assert delta_step["entry_script"] == "run_delta_builder.py"
    assert delta_step["data_prefixes"]["input_traffic"].startswith("s3://")
    assert "FGCCorrBuilderStep" not in pipeline_spec["scripts"]["steps"]

    dependent_spec = by_job["pipeline_15m_dependent"]["spec"]
    assert dependent_spec["required_runtime_params"] == [
        "ProjectName",
        "FeatureSpecVersion",
        "MiniBatchId",
        "BatchStartTsIso",
        "BatchEndTsIso",
    ]
    assert dependent_spec["scripts"]["steps"]["FGCCorrBuilderStep"]["entry_script"] == "run_fg_c_builder.py"

    extractor_spec = by_job["pipeline_backfill_historical_extractor"]["spec"]
    assert extractor_spec["required_runtime_params"] == [
        "ProjectName",
        "FeatureSpecVersion",
        "StartTsIso",
        "EndTsIso",
        "InputS3Prefix",
        "OutputS3Prefix",
        "RequestedFamilies",
    ]
    extractor_step = extractor_spec["scripts"]["steps"]["HistoricalWindowsExtractorStep"]
    assert extractor_step["entry_script"] == "run_historical_windows_extractor.py"

    backfill_spec = by_job["pipeline_backfill_15m_reprocessing"]["spec"]
    assert backfill_spec["required_runtime_params"] == [
        "ProjectName",
        "FeatureSpecVersion",
        "ArtifactFamily",
        "RangeStartTsIso",
        "RangeEndTsIso",
        "IdempotencyKey",
    ]
    backfill_step = backfill_spec["scripts"]["steps"]["BackfillRangeExecutorStep"]
    assert backfill_step["entry_script"] == "run_backfill_reprocessing_executor.py"

    project_defaults = by_job["project_parameters"]["spec"]["defaults"]
    assert project_defaults["MiniBatchId"] == "auto-mini-batch"
    assert project_defaults["WindowCronExpression"] == "8-59/15 * * * ? *"
    assert project_defaults["WindowFloorMinutes"] == [8, 23, 38, 53]
    assert project_defaults["HistoricalInputS3Prefix"].startswith("s3://")
    assert project_defaults["HistoricalWindowsOutputS3Prefix"].startswith("s3://")


def test_pipeline_seed_items_include_artifact_and_deployment_contracts():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)
    spec = by_job["pipeline_15m_streaming"]["spec"]
    assert spec["deployment_status"] == "BOOTSTRAP_REQUIRED"
    assert spec["deployment_checkpoint"] == "phase_a_seed_pending"
    step = spec["scripts"]["steps"]["DeltaBuilderStep"]
    assert step["code_artifact_s3_uri"].endswith("/artifacts/<required:ArtifactBuildId>/source.tar.gz")
    assert step["artifact_build_id"] == "<required:ArtifactBuildId>"
    assert step["artifact_sha256"] == "<required:ArtifactSha256>"
    assert step["artifact_format"] == "tar.gz"


def test_pair_counts_seed_has_required_field_mapping_contract():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)

    pair_counts_spec = by_job["pair_counts_builder"]["spec"]
    field_mapping = pair_counts_spec["traffic_input"]["field_mapping"]
    assert set(field_mapping.keys()) == {
        "source_ip",
        "destination_ip",
        "destination_port",
        "event_start",
        "event_end",
    }


def test_resolve_routing_table_name_defaults_to_expected_value():
    assert resolve_routing_table_name() == "ml_projects_routing"


def test_delta_builder_seed_has_field_mapping_contract():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)
    field_mapping = by_job["delta_builder"]["spec"]["input"]["field_mapping"]
    assert set(field_mapping.keys()) == {
        "source_ip", "destination_ip", "source_port", "destination_port",
        "source_bytes", "destination_bytes", "event_start", "event_end"
    }



def test_item19_pipeline_seed_items_present_with_expected_runtime_params():
    items = _build_bootstrap_items("ndr-prod", "v1", "ndr-team")
    by_job = _items_by_job(items)

    unified_spec = by_job["pipeline_if_training"]["spec"]
    assert unified_spec["required_runtime_params"] == [
        "ProjectName",
        "FeatureSpecVersion",
        "RunId",
        "ExecutionTsIso",
        "DppConfigTableName",
        "MlpConfigTableName",
        "BatchIndexTableName",
        "TrainingStartTs",
        "TrainingEndTs",
        "EvalStartTs",
        "EvalEndTs",
        "Mode",
        "MlProjectName",
    ]

    assert "pipeline_prediction_publish" not in by_job
    assert "pipeline_supplemental_baseline" not in by_job
    assert {"TrainingDataVerifierStep", "MissingFeatureCreationStep", "PostRemediationVerificationStep", "IFTrainingStep", "ModelPublishStep", "ModelAttributesStep", "ModelDeployStep"}.issubset(unified_spec["scripts"]["steps"])

    if_training_spec = by_job["if_training"]["spec"]
    assert if_training_spec["orchestration_targets"]["backfill_15m"] == "sfn_ndr_backfill_reprocessing"


def test_split_table_contracts_use_canonical_table_names():
    contracts = build_split_table_contracts()
    assert set(contracts.keys()) == {
        "dpp_config",
        "mlp_config",
        "batch_index",
        "ml_projects_routing",
        "processing_lock",
        "publication_lock",
    }
    assert contracts["dpp_config"]["KeySchema"][1]["AttributeName"] == "job_name_version"
    assert contracts["mlp_config"]["KeySchema"][0]["AttributeName"] == "ml_project_name"
    assert contracts["ml_projects_routing"]["KeySchema"][0]["AttributeName"] == "org_key"
    assert contracts["processing_lock"]["KeySchema"][0]["AttributeName"] == "pk"
    assert contracts["publication_lock"]["KeySchema"][1]["AttributeName"] == "sk"


def test_split_seed_items_enforce_reciprocal_linkage_fields():
    items = build_split_seed_items("fw_paloalto", "network_anomaly", "v1", owner="ndr")
    dpp_item = items["dpp_config"][0]
    mlp_item = items["mlp_config"][0]
    assert dpp_item["ml_project_name"] == "network_anomaly"
    assert mlp_item["project_name"] == "fw_paloalto"
