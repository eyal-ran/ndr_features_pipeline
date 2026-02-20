import sys
import types

pyspark_module = types.ModuleType("pyspark")
sql_module = types.ModuleType("pyspark.sql")
sql_module.DataFrame = object
sql_module.SparkSession = object
sql_module.functions = types.SimpleNamespace()
pyspark_module.sql = sql_module
sys.modules.setdefault("pyspark", pyspark_module)
sys.modules.setdefault("pyspark.sql", sql_module)

boto3_module = types.ModuleType("boto3")
boto3_module.client = lambda *args, **kwargs: object()
sys.modules.setdefault("boto3", boto3_module)

from ndr.processing.inference_predictions_spec import InferenceOutputSpec
from ndr.processing.prediction_feature_join_job import _validate_publication_output_contract


def test_validate_publication_output_contract_requires_prediction_keys():
    output_spec = InferenceOutputSpec(
        s3_prefix="s3://bucket/prefix",
        format="parquet",
        partition_keys=["feature_spec_version", "dt"],
        write_mode="overwrite",
        dataset="prediction_feature_join",
    )
    try:
        _validate_publication_output_contract(
            columns=["host_ip", "window_label", "window_end_ts", "feature_spec_version", "dt"],
            join_keys=["host_ip", "window_label", "window_end_ts"],
            output_spec=output_spec,
            destination_type="s3",
        )
    except ValueError as exc:
        assert "prediction_score" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing prediction_score")


def test_validate_publication_output_contract_requires_overwrite_for_idempotency():
    output_spec = InferenceOutputSpec(
        s3_prefix="s3://bucket/prefix",
        format="parquet",
        partition_keys=["feature_spec_version", "dt"],
        write_mode="append",
        dataset="prediction_feature_join",
    )

    try:
        _validate_publication_output_contract(
            columns=[
                "host_ip",
                "window_label",
                "window_end_ts",
                "feature_spec_version",
                "prediction_score",
                "dt",
            ],
            join_keys=["host_ip", "window_label", "window_end_ts"],
            output_spec=output_spec,
            destination_type="redshift",
        )
    except ValueError as exc:
        assert "write_mode='overwrite'" in str(exc)
    else:
        raise AssertionError("Expected ValueError for non-idempotent write_mode")
