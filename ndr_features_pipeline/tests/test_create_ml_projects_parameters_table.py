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

from ndr.scripts.create_ml_projects_parameters_table import _build_bootstrap_items


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
