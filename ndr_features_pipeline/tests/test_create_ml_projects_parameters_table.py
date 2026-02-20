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
    resolve_routing_table_name,
)


def _items_by_job(items):
    return {item["job_name"].split("#", 1)[0]: item for item in items}


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
        "pipeline_fg_b_baseline",
        "pipeline_machine_inventory_unload",
        "pipeline_inference_predictions",
        "pipeline_prediction_feature_join",
        "pipeline_if_training",
        "pipeline_backfill_historical_extractor",
        "pipeline_supplemental_baseline",
        "pipeline_training_data_verifier",
        "pipeline_missing_feature_creation",
        "pipeline_model_publish",
        "pipeline_model_attributes",
        "pipeline_model_deploy",
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
    ]

    delta_step = pipeline_spec["scripts"]["steps"]["DeltaBuilderStep"]
    assert delta_step["code_prefix_s3"].startswith("s3://")
    assert delta_step["entry_script"] == "run_delta_builder.py"
    assert delta_step["data_prefixes"]["input_traffic"].startswith("s3://")

    extractor_step = by_job["pipeline_backfill_historical_extractor"]["spec"]["scripts"]["steps"]["HistoricalWindowsExtractorStep"]
    assert extractor_step["entry_script"] == "run_historical_windows_extractor.py"

    project_defaults = by_job["project_parameters"]["spec"]["defaults"]
    assert project_defaults["MiniBatchId"] == "auto-mini-batch"
    assert project_defaults["WindowCronExpression"] == "8-59/15 * * * ? *"
    assert project_defaults["WindowFloorMinutes"] == [8, 23, 38, 53]
    assert project_defaults["HistoricalInputS3Prefix"].startswith("s3://")
    assert project_defaults["HistoricalWindowsOutputS3Prefix"].startswith("s3://")


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

    verifier_spec = by_job["pipeline_training_data_verifier"]["spec"]
    assert verifier_spec["required_runtime_params"] == [
        "ProjectName",
        "FeatureSpecVersion",
        "TrainingStartTs",
        "TrainingEndTs",
        "EvalStartTs",
        "EvalEndTs",
    ]


    assert "pipeline_prediction_publish" not in by_job
    deploy_spec = by_job["pipeline_model_deploy"]["spec"]
    assert "ModelDeployStep" in deploy_spec["scripts"]["steps"]
